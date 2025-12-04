import math
import os
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd

from integrations.amazon_spapi import (
    search_by_gtin,
    search_by_title,
    get_buybox_price,
    search_catalog_items,
    _extract_catalog_item,
    _load_config_from_env,
)
from lib.ebay_search import search_items

# -----------------------------------------------------------------------------#
# Caches em memória para evitar chamadas repetidas
# -----------------------------------------------------------------------------#
_gtin_cache: Dict[str, Optional[Dict[str, Any]]] = {}
_asin_price_cache: Dict[str, Optional[Dict[str, Any]]] = {}
_title_cache: Dict[str, Optional[Dict[str, Any]]] = {}

# Limites padrão (podem ser sobrescritos por .env / st.secrets)
DEFAULT_DISCOVERY_MAX_PAGES = int(os.getenv("AMAZON_DISCOVERY_MAX_PAGES", 60))
DEFAULT_DISCOVERY_PAGE_SIZE = int(os.getenv("AMAZON_DISCOVERY_PAGE_SIZE", 20))  # API aceita até ~20 por página
DEFAULT_DISCOVERY_MAX_ITEMS = int(os.getenv("AMAZON_DISCOVERY_MAX_ITEMS", 500))

# -----------------------------------------------------------------------------#
# Heurísticas de BSR (mantidas para uso futuro, mas NÃO usadas como filtro
# quando min_monthly_sales_est = 0).
# -----------------------------------------------------------------------------#
CATEGORY_BSR_ANCHORS: Dict[str, List[Tuple[int, int]]] = {
    "default": [
        (5_000, 1_750),
        (20_000, 630),
        (50_000, 280),
        (100_000, 126),
        (300_000, 49),
        (800_000, 14),
        (2_000_000, 4),
    ],
}


def _normalize_category_key(display_group: Optional[str]) -> str:
    if not display_group:
        return "default"
    return "default"


SALES_SCALE = 0.3  # fator conservador global (mantido, mas pouco relevante agora)


def _estimate_monthly_sales_from_bsr(rank: Optional[int], display_group: Optional[str]) -> Optional[int]:
    """
    Converte BSR em vendas/mês estimadas via interpolação log-log.
    Mantido para compatibilidade, mas só é aplicado se min_monthly_sales_est > 0.
    """
    if rank is None or rank <= 0:
        return None

    key = _normalize_category_key(display_group)
    anchors = CATEGORY_BSR_ANCHORS.get(key, CATEGORY_BSR_ANCHORS["default"])
    anchors = sorted(anchors, key=lambda x: x[0])

    if rank <= anchors[0][0]:
        return anchors[0][1]
    if rank >= anchors[-1][0]:
        return max(anchors[-1][1], 0)

    for i in range(len(anchors) - 1):
        r1, s1 = anchors[i]
        r2, s2 = anchors[i + 1]
        if r1 <= rank <= r2:
            lr1, lr2 = math.log10(r1), math.log10(r2)
            ls1, ls2 = math.log10(s1), math.log10(s2)
            lr = math.log10(rank)
            t = (lr - lr1) / (lr2 - lr1)
            ls = ls1 + t * (ls2 - ls1)
            est = int(max(10 ** ls, 0))
            est = int(est * SALES_SCALE)
            return max(est, 0)

    return None


def _demand_bucket_from_sales(est_monthly: Optional[int]) -> Optional[str]:
    if est_monthly is None or est_monthly <= 0:
        return None
    if est_monthly >= 300:
        return "🔥 Altíssima"
    if est_monthly >= 100:
        return "Alta"
    if est_monthly >= 30:
        return "Média"
    if est_monthly >= 10:
        return "Moderada"
    if est_monthly >= 3:
        return "Baixa"
    return "Muito baixa"


def _normalize_gtin_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    return s


def _find_gtin_column(df: pd.DataFrame) -> Optional[str]:
    candidate_cols = [
        "gtin",
        "GTIN",
        "upc_ean_isbn",
        "UPC_EAN_ISBN",
        "upc",
        "UPC",
        "ean",
        "EAN",
        "isbn",
        "ISBN",
    ]
    for col in candidate_cols:
        if col in df.columns:
            return col
    return None


# -----------------------------------------------------------------------------#
# Fluxo legado eBay-first (mantido para compatibilidade, não é o foco agora)
# -----------------------------------------------------------------------------#
def match_ebay_to_amazon(
    df_ebay: pd.DataFrame,
    amazon_price_min: Optional[float] = None,
    amazon_price_max: Optional[float] = None,
    amazon_offer_type: str = "any",
    max_gtin_lookups: Optional[int] = 500,
    max_title_lookups: Optional[int] = 200,
    max_price_lookups: Optional[int] = 500,
    min_monthly_sales_est: Optional[int] = None,
    progress_cb: Optional[callable] = None,
) -> pd.DataFrame:
    """
    Fluxo legado eBay-first. Mantido para compatibilidade com outras telas.
    NÃO é utilizado na tela Minerar atual.
    """
    if df_ebay.empty:
        return df_ebay.iloc[0:0].copy()

    gtin_col = _find_gtin_column(df_ebay)
    offer_type_norm = (amazon_offer_type or "").strip().lower()

    results: List[Dict[str, Any]] = []

    gtin_lookups = 0
    title_lookups = 0
    price_lookups = 0

    total = len(df_ebay)

    for idx, row in df_ebay.iterrows():
        gtin = _normalize_gtin_value(row.get(gtin_col)) if gtin_col else None
        title_val = (row.get("title") or "").strip()

        am_item = None
        asin = None
        match_basis = None

        # GTIN -> Amazon
        if gtin:
            if gtin in _gtin_cache:
                am_item = _gtin_cache[gtin]
            else:
                if max_gtin_lookups is not None and gtin_lookups >= max_gtin_lookups:
                    am_item = None
                else:
                    gtin_lookups += 1
                    try:
                        am_item = search_by_gtin(gtin)
                    except Exception:
                        am_item = None
                _gtin_cache[gtin] = am_item

            if am_item and am_item.get("asin"):
                asin = am_item["asin"]
                match_basis = "gtin"

        # Título -> Amazon
        if (not asin) and title_val:
            if title_val in _title_cache:
                am_item = _title_cache[title_val]
            else:
                if max_title_lookups is not None and title_lookups >= max_title_lookups:
                    am_item = None
                else:
                    title_lookups += 1
                    try:
                        am_item = search_by_title(title_val, original_title=title_val)
                    except Exception:
                        am_item = None
                _title_cache[title_val] = am_item

            if am_item and am_item.get("asin"):
                asin = am_item["asin"]
                match_basis = "title"

        if not asin or not am_item:
            continue

        # Preço (buybox)
        if asin in _asin_price_cache:
            price_info = _asin_price_cache[asin]
        else:
            if max_price_lookups is not None and price_lookups >= max_price_lookups:
                price_info = None
            else:
                price_lookups += 1
                try:
                    price_info = get_buybox_price(asin)
                except Exception:
                    price_info = None
            _asin_price_cache[asin] = price_info

        if not price_info or price_info.get("price") is None:
            continue

        price = float(price_info["price"])
        currency = price_info.get("currency") or ""
        is_prime = bool(price_info.get("is_prime") or False)
        fulfillment_channel = (price_info.get("fulfillment_channel") or "").upper()

        # Filtro de preço
        if amazon_price_min is not None and price < amazon_price_min:
            continue
        if amazon_price_max is not None and price > amazon_price_max:
            continue

        # Filtro de oferta (Prime/FBA/FBM)
        if offer_type_norm in ("prime", "fba"):
            if not (is_prime or fulfillment_channel == "AMAZON"):
                continue
        elif offer_type_norm in ("fbm", "merchant", "mf"):
            if fulfillment_channel == "AMAZON":
                continue

        # BSR -> vendas estimadas (usado só se min_monthly_sales_est > 0)
        rank = am_item.get("sales_rank")
        cat_display = am_item.get("sales_rank_category")
        est_monthly = _estimate_monthly_sales_from_bsr(rank, cat_display)
        if min_monthly_sales_est is not None and min_monthly_sales_est > 0:
            if est_monthly is None or est_monthly < min_monthly_sales_est:
                continue
        demand_bucket = _demand_bucket_from_sales(est_monthly)
        cat_key = _normalize_category_key(cat_display)

        combined = row.to_dict()
        combined.update(
            {
                "amazon_asin": asin,
                "amazon_title": am_item.get("title"),
                "amazon_brand": am_item.get("brand"),
                "amazon_browse_node_id": am_item.get("browse_node_id"),
                "amazon_browse_node_name": am_item.get("browse_node_name"),
                "amazon_sales_rank_raw": rank,
                "amazon_sales_rank": rank,
                "amazon_sales_rank_category": cat_display,
                "amazon_demand_category_key": cat_key,
                "amazon_est_monthly_sales": est_monthly,
                "amazon_demand_bucket": demand_bucket,
                "amazon_price": price,
                "amazon_currency": currency,
                "amazon_is_prime": is_prime,
                "amazon_fulfillment_channel": fulfillment_channel,
                "amazon_product_url": f"https://www.amazon.com/dp/{asin}",
                "amazon_match_basis": match_basis,
            }
        )

        results.append(combined)

        if progress_cb is not None:
            progress_cb(idx + 1, total)

    if not results:
        return df_ebay.iloc[0:0].copy()

    return pd.DataFrame(results)


# -----------------------------------------------------------------------------#
# Auxiliar: cache de preço para fluxo Amazon-first
# -----------------------------------------------------------------------------#
def _get_buybox_price_cached(asin: str) -> Optional[Dict[str, Any]]:
    if asin in _asin_price_cache:
        return _asin_price_cache[asin]
    try:
        price_info = get_buybox_price(asin)
    except Exception:
        price_info = None
    _asin_price_cache[asin] = price_info
    return price_info


# -----------------------------------------------------------------------------#
# NOVO fluxo Amazon-first: descobrir N produtos "brutos" na Amazon
# -----------------------------------------------------------------------------#
def _discover_amazon_products(
    kw: Optional[str],
    amazon_price_min: Optional[float],
    amazon_price_max: Optional[float],
    amazon_offer_type: str,
    min_monthly_sales_est: Optional[int],
    browse_node_id: Optional[int],
    max_pages: int,
    page_size: int,
    max_items: int,
    progress_cb: Optional[callable],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Descobre produtos na Amazon aplicando filtros de preço, oferta e (opcionalmente) vendas estimadas.

    🔹 Mantém apenas itens que têm preço/oferta conhecido na Amazon.
    🔹 Tenta chegar em até max_items (ex.: 500) ASINs distintos com preço.
    🔹 Se browse_node_id vier preenchido, filtra os resultados para manter apenas
       itens cuja browse_node_id (classificationId) bate com esse valor (quando disponível).
    """
    # fallback genérico se vier vazio
    if not kw:
        kw = "a"

    cfg = _load_config_from_env()
    marketplace_id = cfg.marketplace_id or "ATVPDKIKX0DER"

    offer_type_norm = (amazon_offer_type or "any").strip().lower()

    found: List[Dict[str, Any]] = []
    seen_asins: set[str] = set()

    # filtro por browse_node_id (classificationId)
    node_filter: Optional[str] = None
    if browse_node_id is not None:
        try:
            node_filter = str(int(browse_node_id))
        except (TypeError, ValueError):
            node_filter = str(browse_node_id)

    # para feedback visual
    estimated_total = max_items
    done = 0

    stats: Dict[str, Any] = {
        "catalog_seen": 0,            # quantos itens brutos vieram da SP-API
        "with_price": 0,              # quantos tinham preço
        "kept": 0,                    # quantos foram mantidos na lista final
        "skipped_price_filter": 0,    # descartados por faixa de preço min/max
        "skipped_offer": 0,           # descartados por tipo de oferta (Prime/FBA/FBM)
        "skipped_sales": 0,           # descartados por min_monthly_sales_est (se usar)
        "skipped_no_price": 0,        # sem preço conhecido (sem oferta / sem pricing)
        "skipped_other_node": 0,      # filtrados por não baterem o browse_node_id
        "dup_asins": 0,               # ASINs duplicados ignorados
        "errors_api": 0,              # erros ao chamar a SP-API
        "last_error": "",             # texto do último erro de API (se houver)
    }

    def _run_search(keyword: str):
        nonlocal done

        for page in range(1, max_pages + 1):
            # já atingimos o alvo de itens com preço? então para
            if len(found) >= max_items:
                break

            try:
                items = search_catalog_items(
                    keywords=keyword,
                    page_size=page_size,
                    page=page,
                    included_data="summaries,identifiers,salesRanks",
                )
            except Exception as e:
                stats["errors_api"] += 1
                stats["last_error"] = str(e)
                break

            if not items:
                # página vazia → provavelmente chegamos no fim
                break

            for raw_item in items:
                if len(found) >= max_items:
                    break

                stats["catalog_seen"] += 1

                extracted = _extract_catalog_item(raw_item, marketplace_id)
                asin = extracted.get("asin")
                if not asin:
                    continue

                # filtro por browse_node_id (classificationId) – se a Amazon informar
                if node_filter:
                    bn = extracted.get("browse_node_id")
                    if bn is not None and str(bn) != node_filter:
                        stats["skipped_other_node"] += 1
                        continue

                # evita repetir o mesmo produto
                if asin in seen_asins:
                    stats["dup_asins"] += 1
                    continue

                # tenta pegar preço (BuyBox ou Lowest)
                price_info = _get_buybox_price_cached(asin)
                if not price_info or price_info.get("price") is None:
                    stats["skipped_no_price"] += 1
                    continue

                stats["with_price"] += 1

                price = float(price_info["price"])
                currency = price_info.get("currency") or ""
                is_prime = bool(price_info.get("is_prime") or False)
                fulfillment_channel = (price_info.get("fulfillment_channel") or "").upper()

                # filtros de faixa de preço (caso você use no futuro)
                if amazon_price_min is not None and price < amazon_price_min:
                    stats["skipped_price_filter"] += 1
                    continue
                if amazon_price_max is not None and price > amazon_price_max:
                    stats["skipped_price_filter"] += 1
                    continue

                # filtro por tipo de oferta (Prime/FBA/FBM) – hoje estamos usando "any"
                if offer_type_norm in ("prime", "fba"):
                    if not (is_prime or fulfillment_channel == "AMAZON"):
                        stats["skipped_offer"] += 1
                        continue
                elif offer_type_norm in ("fbm", "merchant", "mf"):
                    if fulfillment_channel == "AMAZON":
                        stats["skipped_offer"] += 1
                        continue

                # BSR → estimativa de vendas (fica pronto pro futuro, mas hoje não filtra)
                rank = extracted.get("sales_rank")
                cat_display = extracted.get("sales_rank_category")
                est_monthly = _estimate_monthly_sales_from_bsr(rank, cat_display)
                if est_monthly is None:
                    est_monthly = 0

                if min_monthly_sales_est and min_monthly_sales_est > 0:
                    if est_monthly < min_monthly_sales_est:
                        stats["skipped_sales"] += 1
                        continue

                demand_bucket = _demand_bucket_from_sales(est_monthly)
                cat_key = _normalize_category_key(cat_display)

                found.append(
                    {
                        "amazon_asin": asin,
                        "amazon_title": extracted.get("title"),
                        "amazon_brand": extracted.get("brand"),
                        "amazon_browse_node_id": extracted.get("browse_node_id"),
                        "amazon_browse_node_name": extracted.get("browse_node_name"),
                        "amazon_sales_rank_raw": rank,
                        "amazon_sales_rank": rank,
                        "amazon_sales_rank_category": cat_display,
                        "amazon_demand_category_key": cat_key,
                        "amazon_est_monthly_sales": est_monthly,
                        "amazon_demand_bucket": demand_bucket,
                        "amazon_price": price,
                        "amazon_currency": currency,
                        "amazon_is_prime": is_prime,
                        "amazon_fulfillment_channel": fulfillment_channel,
                        "amazon_product_url": f"https://www.amazon.com/dp/{asin}",
                        "gtin": extracted.get("gtin"),
                        "gtin_type": extracted.get("gtin_type"),
                    }
                )
                seen_asins.add(asin)
                stats["kept"] = len(found)

                done += 1
                if progress_cb:
                    progress_cb(done, estimated_total, "amazon")

    # primeira tentativa com a keyword montada a partir da categoria/subcategoria
    _run_search(kw)

    # fallback: se nada for encontrado com a keyword da categoria, tenta algo bem amplo
    if not found and kw.strip().lower() != "a":
        _run_search("a")

    # não reordena pela demanda/BSR – você quer "bruto" mesmo.
    return found[:max_items], stats


def discover_amazon_products(
    kw: Optional[str],
    amazon_price_min: Optional[float],
    amazon_price_max: Optional[float],
    amazon_offer_type: str,
    min_monthly_sales_est: Optional[int],
    browse_node_id: Optional[int] = None,
    max_pages: int = DEFAULT_DISCOVERY_MAX_PAGES,
    page_size: int = DEFAULT_DISCOVERY_PAGE_SIZE,
    max_items: int = DEFAULT_DISCOVERY_MAX_ITEMS,
    progress_cb: Optional[callable] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Wrapper público para descoberta de produtos na Amazon.

    Parâmetros:
      - kw: keyword final (user_kw + amazon_kw da categoria/subcategoria).
      - browse_node_id: classificationId da categoria/subcategoria, se existir.
      - max_items: número máximo de ASINs distintos desejados (ex.: 500).
    """
    return _discover_amazon_products(
        kw=kw,
        amazon_price_min=amazon_price_min,
        amazon_price_max=amazon_price_max,
        amazon_offer_type=amazon_offer_type,
        min_monthly_sales_est=min_monthly_sales_est,
        browse_node_id=browse_node_id,
        max_pages=max_pages,
        page_size=page_size,
        max_items=max_items,
        progress_cb=progress_cb,
    )


# -----------------------------------------------------------------------------#
# Fluxo Amazon-first + eBay (mantido para uso futuro / outras telas)
# -----------------------------------------------------------------------------#
def discover_amazon_and_match_ebay(
    kw: Optional[str],
    amazon_price_min: Optional[float],
    amazon_price_max: Optional[float],
    amazon_offer_type: str,
    min_monthly_sales_est: Optional[int],
    ebay_price_min: Optional[float],
    ebay_price_max: Optional[float],
    ebay_condition: str,
    ebay_category_ids: List[int],
    progress_cb: Optional[callable] = None,
) -> pd.DataFrame:
    """
    Fluxo Amazon-first completo (Amazon -> eBay).
    Mantido para compatibilidade, NÃO usado na fase atual da tela Minerar.
    """
    max_pages = DEFAULT_DISCOVERY_MAX_PAGES
    page_size = DEFAULT_DISCOVERY_PAGE_SIZE
    max_items = DEFAULT_DISCOVERY_MAX_ITEMS

    amazon_items, _ = _discover_amazon_products(
        kw=kw,
        amazon_price_min=amazon_price_min,
        amazon_price_max=amazon_price_max,
        amazon_offer_type=amazon_offer_type,
        min_monthly_sales_est=min_monthly_sales_est,
        browse_node_id=None,  # fluxo Amazon->eBay ainda não filtra por categoria Amazon
        max_pages=max_pages,
        page_size=page_size,
        max_items=max_items,
        progress_cb=progress_cb,
    )

    if not amazon_items:
        return pd.DataFrame()

    # dedup Amazon por ASIN, mantendo as primeiras ocorrências
    seen_asin = set()
    uniq_amazon: List[Dict[str, Any]] = []
    for it in amazon_items:
        asin = it.get("amazon_asin")
        if asin and asin not in seen_asin:
            uniq_amazon.append(it)
            seen_asin.add(asin)

    matches: List[Dict[str, Any]] = []
    total_amz = len(uniq_amazon)

    for idx, am in enumerate(uniq_amazon, start=1):
        search_term = _normalize_gtin_value(am.get("gtin")) or (am.get("amazon_title") or "")
        if not search_term:
            if progress_cb:
                progress_cb(idx, total_amz, "ebay")
            continue

        ebay_found: List[Dict[str, Any]] = []
        cat_ids = ebay_category_ids or [None]

        for cat_id in cat_ids:
            try:
                items = search_items(
                    category_id=cat_id,
                    keyword=search_term,
                    price_min=ebay_price_min,
                    price_max=ebay_price_max,
                    condition=None if ebay_condition == "ANY" else ebay_condition,
                    limit_per_page=200,
                    max_pages=5,
                )
                ebay_found.extend(items)
            except Exception:
                continue

        if not ebay_found:
            if progress_cb:
                progress_cb(idx, total_amz, "ebay")
            continue

        ebay_df = pd.DataFrame(ebay_found)
        ebay_df["price"] = pd.to_numeric(ebay_df["price"], errors="coerce")
        if ebay_price_min is not None:
            ebay_df = ebay_df[ebay_df["price"] >= (ebay_price_min - 1e-9)]
        if ebay_price_max is not None:
            ebay_df = ebay_df[ebay_df["price"] <= (ebay_price_max + 1e-9)]

        if ebay_df.empty:
            if progress_cb:
                progress_cb(idx, total_amz, "ebay")
            continue

        ebay_df = ebay_df.sort_values(by=["price"], ascending=True).reset_index(drop=True)
        best = ebay_df.iloc[0].to_dict()

        combined = dict(best)
        combined.update(am)
        combined["amazon_match_basis"] = combined.get("amazon_match_basis") or "amazon_first"
        matches.append(combined)

        if progress_cb:
            progress_cb(idx, total_amz, "ebay")

    if not matches:
        return pd.DataFrame()

    # dedup final por ASIN mantendo o menor preço eBay
    df_matches = pd.DataFrame(matches)
    if "amazon_asin" in df_matches.columns and "price" in df_matches.columns:
        df_matches["price"] = pd.to_numeric(df_matches["price"], errors="coerce")
        df_matches = (
            df_matches.sort_values(by=["amazon_asin", "price"], ascending=[True, True])
            .drop_duplicates(subset=["amazon_asin"], keep="first")
        )

    return df_matches.reset_index(drop=True)


def match_amazon_list_to_ebay(
    amazon_items: List[Dict[str, Any]],
    ebay_price_min: Optional[float],
    ebay_price_max: Optional[float],
    ebay_condition: str,
    ebay_category_ids: List[int],
    progress_cb: Optional[callable] = None,
) -> pd.DataFrame:
    """
    Usa uma lista já descoberta na Amazon para procurar fornecedores no eBay.
    Mantido para compatibilidade, não usado no fluxo simplificado atual.
    """
    if not amazon_items:
        return pd.DataFrame()

    # dedup Amazon por ASIN
    uniq: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for it in amazon_items:
        asin = it.get("amazon_asin")
        if asin and asin not in seen:
            uniq.append(it)
            seen.add(asin)

    matches: List[Dict[str, Any]] = []
    total = len(uniq)

    for idx, am in enumerate(uniq, start=1):
        term = _normalize_gtin_value(am.get("gtin")) or (am.get("amazon_title") or "")
        if not term:
            if progress_cb:
                progress_cb(idx, total, "ebay")
            continue

        ebay_found: List[Dict[str, Any]] = []
        cat_ids = ebay_category_ids or [None]
        for cat_id in cat_ids:
            try:
                items = search_items(
                    category_id=cat_id,
                    keyword=term,
                    price_min=ebay_price_min,
                    price_max=ebay_price_max,
                    condition=None if ebay_condition == "ANY" else ebay_condition,
                    limit_per_page=200,
                    max_pages=5,
                )
                ebay_found.extend(items)
            except Exception:
                continue

        if not ebay_found:
            if progress_cb:
                progress_cb(idx, total, "ebay")
            continue

        ebay_df = pd.DataFrame(ebay_found)
        ebay_df["price"] = pd.to_numeric(ebay_df["price"], errors="coerce")
        if ebay_price_min is not None:
            ebay_df = ebay_df[ebay_df["price"] >= (ebay_price_min - 1e-9)]
        if ebay_price_max is not None:
            ebay_df = ebay_df[ebay_df["price"] <= (ebay_price_max + 1e-9)]
        if ebay_df.empty:
            if progress_cb:
                progress_cb(idx, total, "ebay")
            continue

        ebay_df = ebay_df.sort_values(by=["price"], ascending=True).reset_index(drop=True)
        best = ebay_df.iloc[0].to_dict()
        combined = dict(best)
        combined.update(am)
        combined["amazon_match_basis"] = combined.get("amazon_match_basis") or "amazon_first"
        matches.append(combined)

        if progress_cb:
            progress_cb(idx, total, "ebay")

    if not matches:
        return pd.DataFrame()

    df_matches = pd.DataFrame(matches)
    if "amazon_asin" in df_matches.columns and "price" in df_matches.columns:
        df_matches["price"] = pd.to_numeric(df_matches["price"], errors="coerce")
        df_matches = (
            df_matches.sort_values(by=["amazon_asin", "price"], ascending=[True, True])
            .drop_duplicates(subset=["amazon_asin"], keep="first")
        )
    return df_matches.reset_index(drop=True)

import math
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd

import os
from functools import lru_cache

from integrations.amazon_spapi import (
    search_by_gtin,
    search_by_title,
    get_buybox_price,
    search_catalog_items,
    _extract_catalog_item,
    _load_config_from_env,
)
from lib.ebay_search import search_items


# Caches em memória para evitar chamadas repetidas
_gtin_cache: Dict[str, Optional[Dict[str, Any]]] = {}
_asin_price_cache: Dict[str, Optional[Dict[str, Any]]] = {}
_title_cache: Dict[str, Optional[Dict[str, Any]]] = {}

# Limites padrão (podem ser sobrescritos por .env / st.secrets)
# Objetivo: explorar mais itens na Amazon antes de filtrar
# Para chegar perto de 1000 itens: 50 páginas x 20 itens/página = 1000
DEFAULT_DISCOVERY_MAX_PAGES = int(os.getenv("AMAZON_DISCOVERY_MAX_PAGES", 50))
DEFAULT_DISCOVERY_PAGE_SIZE = int(os.getenv("AMAZON_DISCOVERY_PAGE_SIZE", 20))  # API aceita até ~20 por página
DEFAULT_DISCOVERY_MAX_ITEMS = int(os.getenv("AMAZON_DISCOVERY_MAX_ITEMS", 1000))

# Cada tupla: (BSR, vendas_mensais_estimadas) - ancoras conservadoras por cluster
CATEGORY_BSR_ANCHORS: Dict[str, List[Tuple[int, int]]] = {
    # Home & Kitchen (principal)
    "home_kitchen": [
        (5_000,    2_500),
        (20_000,     900),
        (50_000,     400),
        (100_000,    180),
        (300_000,     70),
        (800_000,     20),
        (2_000_000,   5),
    ],
    # Categorias mais "quentes" (escala 1.2)
    "beauty_personal_care": [
        (5_000,    3_000),
        (20_000,   1_080),
        (50_000,     480),
        (100_000,    216),
        (300_000,     84),
        (800_000,     24),
        (2_000_000,    6),
    ],
    "grocery": [
        (5_000,    3_000),
        (20_000,   1_080),
        (50_000,     480),
        (100_000,    216),
        (300_000,     84),
        (800_000,     24),
        (2_000_000,    6),
    ],
    "clothing": [
        (5_000,    3_000),
        (20_000,   1_080),
        (50_000,     480),
        (100_000,    216),
        (300_000,     84),
        (800_000,     24),
        (2_000_000,    6),
    ],
    # Categorias "quentes" padrão (escala 1.0)
    "health_household": [
        (5_000,    2_500),
        (20_000,     900),
        (50_000,     400),
        (100_000,    180),
        (300_000,     70),
        (800_000,     20),
        (2_000_000,    5),
    ],
    "baby": [
        (5_000,    2_500),
        (20_000,     900),
        (50_000,     400),
        (100_000,    180),
        (300_000,     70),
        (800_000,     20),
        (2_000_000,    5),
    ],
    "shoes": [
        (5_000,    2_500),
        (20_000,     900),
        (50_000,     400),
        (100_000,    180),
        (300_000,     70),
        (800_000,     20),
        (2_000_000,    5),
    ],
    "video_games": [
        (5_000,    2_500),
        (20_000,     900),
        (50_000,     400),
        (100_000,    180),
        (300_000,     70),
        (800_000,     20),
        (2_000_000,    5),
    ],
    # Categorias "médias" (escala 0.7)
    "toys_games": [
        (5_000,    1_750),
        (20_000,     630),
        (50_000,     280),
        (100_000,    126),
        (300_000,     49),
        (800_000,     14),
        (2_000_000,    4),
    ],
    "sports_outdoors": [
        (5_000,    1_750),
        (20_000,     630),
        (50_000,     280),
        (100_000,    126),
        (300_000,     49),
        (800_000,     14),
        (2_000_000,    4),
    ],
    "pet_supplies": [
        (5_000,    1_750),
        (20_000,     630),
        (50_000,     280),
        (100_000,    126),
        (300_000,     49),
        (800_000,     14),
        (2_000_000,    4),
    ],
    "arts_crafts": [
        (5_000,    1_750),
        (20_000,     630),
        (50_000,     280),
        (100_000,    126),
        (300_000,     49),
        (800_000,     14),
        (2_000_000,    4),
    ],
    "garden_outdoors": [
        (5_000,    1_750),
        (20_000,     630),
        (50_000,     280),
        (100_000,    126),
        (300_000,     49),
        (800_000,     14),
        (2_000_000,    4),
    ],
    "office_products": [
        (5_000,    1_750),
        (20_000,     630),
        (50_000,     280),
        (100_000,    126),
        (300_000,     49),
        (800_000,     14),
        (2_000_000,    4),
    ],
    "tools_home_improvement": [
        (5_000,    1_750),
        (20_000,     630),
        (50_000,     280),
        (100_000,    126),
        (300_000,     49),
        (800_000,     14),
        (2_000_000,    4),
    ],
    "electronics": [
        (5_000,    1_750),
        (20_000,     630),
        (50_000,     280),
        (100_000,    126),
        (300_000,     49),
        (800_000,     14),
        (2_000_000,    4),
    ],
    # Categorias mais "frias" (escala 0.5)
    "automotive": [
        (5_000,    1_250),
        (20_000,     450),
        (50_000,     200),
        (100_000,     90),
        (300_000,     35),
        (800_000,     10),
        (2_000_000,    2),
    ],
    "industrial_scientific": [
        (5_000,    1_250),
        (20_000,     450),
        (50_000,     200),
        (100_000,     90),
        (300_000,     35),
        (800_000,     10),
        (2_000_000,    2),
    ],
    "musical_instruments": [
        (5_000,    1_250),
        (20_000,     450),
        (50_000,     200),
        (100_000,     90),
        (300_000,     35),
        (800_000,     10),
        (2_000_000,    2),
    ],
    "luggage_travel": [
        (5_000,    1_250),
        (20_000,     450),
        (50_000,     200),
        (100_000,     90),
        (300_000,     35),
        (800_000,     10),
        (2_000_000,    2),
    ],
    "jewelry": [
        (5_000,    1_250),
        (20_000,     450),
        (50_000,     200),
        (100_000,     90),
        (300_000,     35),
        (800_000,     10),
        (2_000_000,    2),
    ],
    # Books separado
    "books": [
        (1_000,    8_000),
        (5_000,    3_000),
        (10_000,   1_000),
        (50_000,     200),
        (100_000,     80),
        (300_000,     20),
        (1_000_000,   5),
        (2_000_000,   2),
    ],
    # Fallback genérico
    "default": [
        (5_000,    1_750),
        (20_000,     630),
        (50_000,     280),
        (100_000,    126),
        (300_000,     49),
        (800_000,     14),
        (2_000_000,    4),
    ],
}


def _normalize_category_key(display_group: Optional[str]) -> str:
    if not display_group:
        return "default"
    g = display_group.lower()
    if "kitchen" in g or "home" in g:
        return "home_kitchen"
    if "beauty" in g or "personal care" in g:
        return "beauty_personal_care"
    if "health" in g or "household" in g:
        return "health_household"
    if "baby" in g:
        return "baby"
    if "toy" in g or "game" in g:
        return "toys_games"
    if "sport" in g or "outdoor" in g:
        return "sports_outdoors"
    if "pet" in g:
        return "pet_supplies"
    if "grocery" in g or "gourmet" in g:
        return "grocery"
    if "electronic" in g:
        return "electronics"
    if "office" in g:
        return "office_products"
    if "tool" in g or "home improvement" in g:
        return "tools_home_improvement"
    if "automotive" in g:
        return "automotive"
    if "garden" in g or "outdoor" in g or "patio" in g or "lawn" in g:
        return "garden_outdoors"
    if "arts" in g or "craft" in g or "sewing" in g:
        return "arts_crafts"
    if "musical" in g or "instrument" in g:
        return "musical_instruments"
    if "industrial" in g or "scientific" in g:
        return "industrial_scientific"
    if "video game" in g:
        return "video_games"
    if "book" in g:
        return "books"
    if "clothing" in g or "apparel" in g:
        return "clothing"
    if "shoe" in g:
        return "shoes"
    if "jewel" in g:
        return "jewelry"
    if "luggage" in g or "travel" in g:
        return "luggage_travel"
    return "default"


SALES_SCALE = 0.3  # fator conservador global (mais rígido)


def _estimate_monthly_sales_from_bsr(rank: Optional[int], display_group: Optional[str]) -> Optional[int]:
    """
    Converte BSR em vendas/mês estimadas (conservador) via interpolação log-log em pontos de ancoragem.
    Sempre faz clamp no menor/maior anchor; não zera acima de 300k.
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

        if amazon_price_min is not None and price < amazon_price_min:
            continue
        if amazon_price_max is not None and price > amazon_price_max:
            continue

        if offer_type_norm in ("prime", "fba"):
            if not (is_prime or fulfillment_channel == "AMAZON"):
                continue
        elif offer_type_norm in ("fbm", "merchant", "mf"):
            if fulfillment_channel == "AMAZON":
                continue

        rank = am_item.get("sales_rank")
        cat_display = am_item.get("sales_rank_category")
        est_monthly = _estimate_monthly_sales_from_bsr(rank, cat_display)
        if min_monthly_sales_est is not None:
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


# ---------------------------------------------------------------------------
# Fluxo Amazon-first: descobre na Amazon e busca fornecedores no eBay
# ---------------------------------------------------------------------------

def _get_buybox_price_cached(asin: str) -> Optional[Dict[str, Any]]:
    if asin in _asin_price_cache:
        return _asin_price_cache[asin]
    try:
        price_info = get_buybox_price(asin)
    except Exception:
        price_info = None
    _asin_price_cache[asin] = price_info
    return price_info


def _discover_amazon_products(
    kw: Optional[str],
    amazon_price_min: Optional[float],
    amazon_price_max: Optional[float],
    amazon_offer_type: str,
    min_monthly_sales_est: Optional[int],
    max_pages: int,
    page_size: int,
    max_items: int,
    progress_cb: Optional[callable],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Descobre produtos na Amazon aplicando filtros de preço, oferta e vendas estimadas.
    Retorna uma lista de dicts com campos já enriquecidos (BSR, vendas estimadas, etc.).
    """
    if not kw:
        kw = "a"  # fallback genérico para permitir busca sem keyword explícita

    offer_type_norm = (amazon_offer_type or "any").strip().lower()
    found: List[Dict[str, Any]] = []

    # total estimado para feedback ao usuário (usamos max_items como teto desejado)
    estimated_total = max_items
    done = 0

    stats = {
        "catalog_seen": 0,
        "with_price": 0,
        "kept": 0,
        "skipped_price_filter": 0,
        "skipped_offer": 0,
        "skipped_sales": 0,
        "skipped_no_price": 0,
    }

    def _run_search(keyword: str):
        nonlocal done
        for page in range(1, max_pages + 1):
            if len(found) >= max_items:
                break
            items = search_catalog_items(
                keywords=keyword,
                page_size=page_size,
                page=page,
                included_data="summaries,identifiers,salesRanks",
            )
            if not items:
                break

            for raw_item in items:
                if len(found) >= max_items:
                    break

                stats["catalog_seen"] += 1

                extracted = _extract_catalog_item(raw_item, _load_config_from_env().marketplace_id)
                asin = extracted.get("asin")
                if not asin:
                    continue

                price_info = _get_buybox_price_cached(asin)
                if not price_info or price_info.get("price") is None:
                    stats["skipped_no_price"] += 1
                    # Se não há preço e não há filtro de preço mínimo, deixamos passar como None
                    if amazon_price_min:
                        continue
                    price = None
                    currency = None
                    is_prime = False
                    fulfillment_channel = ""
                else:
                    stats["with_price"] += 1
                    price = float(price_info["price"])
                    currency = price_info.get("currency") or ""
                    is_prime = bool(price_info.get("is_prime") or False)
                    fulfillment_channel = (price_info.get("fulfillment_channel") or "").upper()

                if price is not None:
                    if amazon_price_min is not None and price < amazon_price_min:
                        stats["skipped_price_filter"] += 1
                        continue
                    if amazon_price_max is not None and price > amazon_price_max:
                        stats["skipped_price_filter"] += 1
                        continue

                if offer_type_norm in ("prime", "fba"):
                    if not (is_prime or fulfillment_channel == "AMAZON"):
                        stats["skipped_offer"] += 1
                        continue
                elif offer_type_norm in ("fbm", "merchant", "mf"):
                    if fulfillment_channel == "AMAZON":
                        stats["skipped_offer"] += 1
                        continue

                rank = extracted.get("sales_rank")
                cat_display = extracted.get("sales_rank_category")
                est_monthly = _estimate_monthly_sales_from_bsr(rank, cat_display)
                if min_monthly_sales_est is not None:
                    if est_monthly is None or est_monthly < min_monthly_sales_est:
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
                stats["kept"] += 1
                done += 1
                if progress_cb:
                    progress_cb(done, estimated_total, "amazon")

    # primeira tentativa com a keyword informada (ou fallback "a")
    _run_search(kw)

    # se nada encontrado e havia keyword específica, tenta um fallback amplo "a"
    if not found and kw and kw.strip() and kw.strip().lower() != "a":
        kw_fallback = "a"
        _run_search(kw_fallback)

    # ordenar por demanda e rank
    found.sort(
        key=lambda x: (
            -(x.get("amazon_est_monthly_sales") or 0),
            x.get("amazon_sales_rank") or 10**9,
        )
    )
    return found[:max_items], stats


def discover_amazon_products(
    kw: Optional[str],
    amazon_price_min: Optional[float],
    amazon_price_max: Optional[float],
    amazon_offer_type: str,
    min_monthly_sales_est: Optional[int],
    max_pages: int = DEFAULT_DISCOVERY_MAX_PAGES,
    page_size: int = DEFAULT_DISCOVERY_PAGE_SIZE,
    max_items: int = DEFAULT_DISCOVERY_MAX_ITEMS,
    progress_cb: Optional[callable] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Wrapper público para descoberta de produtos na Amazon, retornando itens e stats de debug.
    """
    return _discover_amazon_products(
        kw=kw,
        amazon_price_min=amazon_price_min,
        amazon_price_max=amazon_price_max,
        amazon_offer_type=amazon_offer_type,
        min_monthly_sales_est=min_monthly_sales_est,
        max_pages=max_pages,
        page_size=page_size,
        max_items=max_items,
        progress_cb=progress_cb,
    )


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
    Fluxo Amazon-first: encontra produtos relevantes na Amazon, depois busca fornecedores no eBay.
    Retorna DataFrame pronto para exibição, no mesmo formato geral usado hoje.
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
        max_pages=max_pages,
        page_size=page_size,
        max_items=max_items,
        progress_cb=progress_cb,
    )

    if not amazon_items:
        return pd.DataFrame()

    # dedup Amazon por ASIN, mantendo os mais relevantes (já ordenados)
    seen_asin = set()
    uniq_amazon: List[Dict[str, Any]] = []
    for it in amazon_items:
        asin = it.get("amazon_asin")
        if asin and asin not in seen_asin:
            uniq_amazon.append(it)
            seen_asin.add(asin)

    matches: List[Dict[str, Any]] = []
    total_amz = len(uniq_amazon)
    offer_type_norm = (amazon_offer_type or "any").strip().lower()

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
    Mantém apenas o fornecedor mais barato por ASIN. Retorna DataFrame pronto.
    """
    if not amazon_items:
        return pd.DataFrame()

    # Se foi passado um DataFrame convertido para dicts, preserva metadados de debug se existirem
    debug_stats = None
    if isinstance(amazon_items, dict) and "__stats" in amazon_items:
        debug_stats = amazon_items.get("__stats")

    # dedup Amazon por ASIN
    uniq = []
    seen = set()
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

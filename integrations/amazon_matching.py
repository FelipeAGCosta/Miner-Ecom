from typing import Optional, Dict, Any, List

import pandas as pd

from integrations.amazon_spapi import search_by_gtin, search_by_title, get_buybox_price


# Caches em memoria para evitar chamadas repetidas
_gtin_cache: Dict[str, Optional[Dict[str, Any]]] = {}
_asin_price_cache: Dict[str, Optional[Dict[str, Any]]] = {}
_title_cache: Dict[str, Optional[Dict[str, Any]]] = {}


def _normalize_gtin_value(value: Any) -> Optional[str]:
    """Converte o valor de GTIN para string limpa ou None."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    return s


def _find_gtin_column(df: pd.DataFrame) -> Optional[str]:
    """Tenta descobrir qual coluna do DF e o GTIN/UPC/EAN."""
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
    progress_cb: Optional[callable] = None,
) -> pd.DataFrame:
    """
    Recebe o DataFrame de resultados do eBay (ja filtrado/enriquecido) e
    tenta fazer match com a Amazon. Prioriza GTIN e usa fallback por titulo.

    Regras:
    - GTIN: para cada GTIN valido, chama search_by_gtin(gtin) (com cache).
    - Fallback: se nao houver GTIN ou nao encontrar, tenta search_by_title(titulo) (com cache).
    - Para cada ASIN encontrado, chama get_buybox_price(asin) (com cache).
    - Aplica filtros de preco Amazon (min/max) e tipo de oferta (Prime/FBA/FBM).
    - Retorna apenas as linhas que passaram nos filtros do eBay e da Amazon.

    Colunas adicionadas (prefixo amazon_):
    - amazon_asin, amazon_title, amazon_brand, amazon_browse_node_id, amazon_browse_node_name,
      amazon_sales_rank, amazon_sales_rank_category, amazon_price, amazon_currency,
      amazon_is_prime, amazon_fulfillment_channel, amazon_product_url, amazon_match_basis
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

        # --- Amazon catalog via GTIN ---
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

        # --- Fallback por titulo se GTIN falhou ou nao existe ---
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

        # --- Amazon pricing: ASIN -> BuyBox price (com cache) ---
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

        combined = row.to_dict()
        combined.update(
            {
                "amazon_asin": asin,
                "amazon_title": am_item.get("title"),
                "amazon_brand": am_item.get("brand"),
                "amazon_browse_node_id": am_item.get("browse_node_id"),
                "amazon_browse_node_name": am_item.get("browse_node_name"),
                "amazon_sales_rank": am_item.get("sales_rank"),
                "amazon_sales_rank_category": am_item.get("sales_rank_category"),
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

from typing import Optional, Dict, Any, List

import pandas as pd

from integrations.amazon_spapi import search_by_gtin, get_buybox_price


# Caches em memória para evitar chamadas repetidas
_gtin_cache: Dict[str, Optional[Dict[str, Any]]] = {}
_asin_price_cache: Dict[str, Optional[Dict[str, Any]]] = {}


def _normalize_gtin_value(value: Any) -> Optional[str]:
    """Converte o valor de GTIN para string limpa ou None."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    return s


def _find_gtin_column(df: pd.DataFrame) -> Optional[str]:
    """Tenta descobrir qual coluna do DF é o GTIN/UPC/EAN."""
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
    max_price_lookups: Optional[int] = 500,
) -> pd.DataFrame:
    """
    Recebe o DataFrame de resultados do eBay (já filtrado/enriquecido) e
    tenta fazer match com a Amazon pelo GTIN.

    Regras:
    - Só considera linhas com GTIN válido.
    - Para cada GTIN, chama search_by_gtin(gtin) (com cache forte).
    - Para cada ASIN encontrado, chama get_buybox_price(asin) (com cache).
    - Aplica filtros de preço Amazon (min/max) e tipo de oferta (Prime/FBA/FBM).
    - Retorna apenas as linhas em que:
        - passou nos filtros do eBay (já embutidos nesse df_ebay)
        - E passou nos filtros Amazon (preço + tipo de oferta).

    Colunas adicionadas (prefixo amazon_):
    - amazon_asin
    - amazon_title
    - amazon_brand
    - amazon_browse_node_id
    - amazon_browse_node_name
    - amazon_sales_rank
    - amazon_sales_rank_category
    - amazon_price
    - amazon_currency
    - amazon_is_prime
    - amazon_fulfillment_channel
    - amazon_product_url
    """
    if df_ebay.empty:
        return df_ebay.iloc[0:0].copy()

    gtin_col = _find_gtin_column(df_ebay)
    if gtin_col is None:
        # Sem GTIN não dá pra fazer match nessa fase
        return df_ebay.iloc[0:0].copy()

    offer_type_norm = (amazon_offer_type or "").strip().lower()

    results: List[Dict[str, Any]] = []

    gtin_lookups = 0
    price_lookups = 0

    for _, row in df_ebay.iterrows():
        gtin = _normalize_gtin_value(row.get(gtin_col))
        if not gtin:
            continue

        # --- Amazon catalog: GTIN -> ASIN + BSR etc. (com cache) ---
        if gtin in _gtin_cache:
            am_item = _gtin_cache[gtin]
        else:
            if max_gtin_lookups is not None and gtin_lookups >= max_gtin_lookups:
                # já atingiu limite de chamadas de catálogo, pula próximos
                continue
            gtin_lookups += 1
            try:
                am_item = search_by_gtin(gtin)
            except Exception:
                am_item = None
            _gtin_cache[gtin] = am_item

        if not am_item or not am_item.get("asin"):
            continue

        asin = am_item["asin"]

        # --- Amazon pricing: ASIN -> BuyBox price (com cache) ---
        if asin in _asin_price_cache:
            price_info = _asin_price_cache[asin]
        else:
            if max_price_lookups is not None and price_lookups >= max_price_lookups:
                # limite de chamadas de pricing atingido
                continue
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

        # --- Filtro de preço Amazon ---
        if amazon_price_min is not None and price < amazon_price_min:
            continue
        if amazon_price_max is not None and price > amazon_price_max:
            continue

        # --- Filtro de tipo de oferta (Prime/FBA/FBM/any) ---
        if offer_type_norm in ("prime", "fba"):
            # Queremos somente ofertas elegíveis para Prime / FBA
            # Heurística: is_prime == True OU fulfillment_channel == "AMAZON"
            if not (is_prime or fulfillment_channel == "AMAZON"):
                continue
        elif offer_type_norm in ("fbm", "merchant", "mf"):
            # Quer só merchant fulfillment (não FBA)
            if fulfillment_channel == "AMAZON":
                continue
        # "any" não filtra nada

        # --- Linha combinada eBay + Amazon ---
        combined = row.to_dict()
        combined.update(
            {
                "amazon_asin": asin,
                "amazon_title": am_item.get("title"),
                "amazon_brand": am_item.get("brand"),
                "amazon_browse_node_id": am_item.get("browse_node_id"),
                "amazon_browse_node_name": am_item.get("browse_node_name"),
                "amazon_sales_rank": am_item.get("sales_rank"),
                "amazon_sales_rank_category": am_item.get(
                    "sales_rank_category"
                ),
                "amazon_price": price,
                "amazon_currency": currency,
                "amazon_is_prime": is_prime,
                "amazon_fulfillment_channel": fulfillment_channel,
                "amazon_product_url": f"https://www.amazon.com/dp/{asin}",
            }
        )
        results.append(combined)

    if not results:
        return df_ebay.iloc[0:0].copy()

    return pd.DataFrame(results)

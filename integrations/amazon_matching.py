import math
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd

from integrations.amazon_spapi import search_by_gtin, search_by_title, get_buybox_price


# Caches em memoria para evitar chamadas repetidas
_gtin_cache: Dict[str, Optional[Dict[str, Any]]] = {}
_asin_price_cache: Dict[str, Optional[Dict[str, Any]]] = {}
_title_cache: Dict[str, Optional[Dict[str, Any]]] = {}

# Pontos de ancoragem BSR -> vendas/mês (conservador) por cluster de categoria
CATEGORY_BSR_ANCHORS: Dict[str, List[Tuple[int, int]]] = {
    "home_kitchen": [
        (100, 1200),
        (500, 650),
        (1_000, 350),
        (5_000, 120),
        (20_000, 35),
        (100_000, 10),
        (300_000, 3),
    ],
    "beauty_personal_care": [
        (100, 1400),
        (500, 700),
        (1_000, 400),
        (5_000, 150),
        (20_000, 45),
        (100_000, 12),
        (300_000, 4),
    ],
    "health_household": [
        (100, 1200),
        (500, 600),
        (1_000, 320),
        (5_000, 110),
        (20_000, 32),
        (100_000, 9),
        (300_000, 3),
    ],
    "baby": [
        (100, 900),
        (500, 350),
        (1_000, 220),
        (5_000, 80),
        (20_000, 25),
        (100_000, 7),
        (300_000, 2),
    ],
    "toys_games": [
        (100, 500),
        (500, 120),
        (1_000, 70),
        (5_000, 25),
        (20_000, 8),
        (100_000, 2),
        (300_000, 1),
    ],
    "sports_outdoors": [
        (100, 700),
        (500, 250),
        (1_000, 150),
        (5_000, 50),
        (20_000, 16),
        (100_000, 4),
        (300_000, 1),
    ],
    "pet_supplies": [
        (100, 700),
        (500, 260),
        (1_000, 160),
        (5_000, 55),
        (20_000, 18),
        (100_000, 5),
        (300_000, 1),
    ],
    "grocery": [
        (100, 800),
        (500, 280),
        (1_000, 180),
        (5_000, 60),
        (20_000, 18),
        (100_000, 5),
        (300_000, 1),
    ],
    "electronics": [
        (100, 2000),
        (500, 800),
        (1_000, 320),
        (5_000, 90),
        (20_000, 25),
        (100_000, 6),
        (300_000, 1),
    ],
    "office_products": [
        (100, 600),
        (500, 220),
        (1_000, 140),
        (5_000, 45),
        (20_000, 14),
        (100_000, 3),
        (300_000, 1),
    ],
    "tools_home_improvement": [
        (100, 700),
        (500, 250),
        (1_000, 160),
        (5_000, 55),
        (20_000, 17),
        (100_000, 4),
        (300_000, 1),
    ],
    "automotive": [
        (100, 500),
        (500, 180),
        (1_000, 110),
        (5_000, 35),
        (20_000, 11),
        (100_000, 3),
        (300_000, 1),
    ],
    "garden_outdoors": [
        (100, 700),
        (500, 260),
        (1_000, 160),
        (5_000, 55),
        (20_000, 17),
        (100_000, 4),
        (300_000, 1),
    ],
    "arts_crafts": [
        (100, 900),
        (500, 320),
        (1_000, 190),
        (5_000, 65),
        (20_000, 20),
        (100_000, 5),
        (300_000, 1),
    ],
    "musical_instruments": [
        (100, 400),
        (500, 150),
        (1_000, 90),
        (5_000, 30),
        (20_000, 9),
        (100_000, 2),
        (300_000, 1),
    ],
    "industrial_scientific": [
        (100, 300),
        (500, 110),
        (1_000, 70),
        (5_000, 25),
        (20_000, 8),
        (100_000, 2),
        (300_000, 1),
    ],
    "video_games": [
        (100, 1500),
        (500, 500),
        (1_000, 250),
        (5_000, 70),
        (20_000, 18),
        (100_000, 4),
        (300_000, 1),
    ],
    "books": [
        (100, 7000),
        (500, 3000),
        (1_000, 1500),
        (5_000, 300),
        (10_000, 40),
        (100_000, 8),
        (300_000, 2),
    ],
    "clothing": [
        (100, 1500),
        (500, 600),
        (1_000, 350),
        (5_000, 100),
        (20_000, 30),
        (100_000, 7),
        (300_000, 2),
    ],
    "shoes": [
        (100, 800),
        (500, 300),
        (1_000, 180),
        (5_000, 60),
        (20_000, 18),
        (100_000, 5),
        (300_000, 1),
    ],
    "jewelry": [
        (100, 400),
        (500, 150),
        (1_000, 90),
        (5_000, 30),
        (20_000, 10),
        (100_000, 3),
        (300_000, 1),
    ],
    "luggage_travel": [
        (100, 600),
        (500, 220),
        (1_000, 140),
        (5_000, 45),
        (20_000, 14),
        (100_000, 3),
        (300_000, 1),
    ],
    "default": [
        (100, 800),
        (500, 280),
        (1_000, 170),
        (5_000, 55),
        (20_000, 17),
        (100_000, 4),
        (300_000, 1),
    ],
}


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


def _estimate_monthly_sales_from_bsr(rank: Optional[int], display_group: Optional[str]) -> Optional[int]:
    """
    Converte BSR em vendas/mês estimadas (conservador) via interpolação log-log em pontos de ancoragem.
    """
    if rank is None or rank <= 0:
        return None
    if rank > 300_000:
        return 0

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
            return max(est, 1)

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
    Recebe o DataFrame de resultados do eBay (ja filtrado/enriquecido) e
    tenta fazer match com a Amazon. Prioriza GTIN e usa fallback por titulo.
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

        est_monthly = _estimate_monthly_sales_from_bsr(
            am_item.get("sales_rank"),
            am_item.get("sales_rank_category"),
        )

        if min_monthly_sales_est is not None:
            if est_monthly is None or est_monthly < min_monthly_sales_est:
                continue

        demand_bucket = _demand_bucket_from_sales(est_monthly)

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

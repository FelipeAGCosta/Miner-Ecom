import os
import time
from typing import Dict, List, Optional, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lib.ebay_auth import get_app_token

# ────────────────────────────────────────────────────────────────────────────────
# Configurações e Constantes
# ────────────────────────────────────────────────────────────────────────────────

def _base_url() -> str:
    env = (os.getenv("EBAY_ENV") or "").lower().strip()
    if "sand" in env:
        return "https://api.sandbox.ebay.com/buy/browse/v1"
    return "https://api.ebay.com/buy/browse/v1"

BASE = _base_url()

SITE_ID = os.getenv("EBAY_BROWSE_SITE_ID", "0")  # 0 = US
MARKETPLACE_ID = os.getenv("EBAY_MARKETPLACE_ID", "EBAY_US")
CURRENCY = os.getenv("EBAY_CURRENCY", "USD")

CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", 5))
READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", 30))

_retry = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)

_session = requests.Session()
_session.mount("https://", HTTPAdapter(max_retries=_retry))
_session.mount("http://", HTTPAdapter(max_retries=_retry))

# ────────────────────────────────────────────────────────────────────────────────
# Exceções
# ────────────────────────────────────────────────────────────────────────────────

class EbayAuthError(Exception):
    pass

class EbayRequestError(Exception):
    pass

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _auth_headers() -> Dict[str, str]:
    try:
        token = get_app_token()
    except Exception as e:
        raise EbayAuthError(f"Falha ao obter token do eBay: {type(e).__name__}: {e}")

    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
        "X-EBAY-C-ENDUSERCTX": f"contextualLocation=country=US,zip=00000;siteid={SITE_ID}",
    }

def _money_val(m: Any) -> Optional[float]:
    try:
        if not isinstance(m, dict):
            return None
        v = m.get("value")
        return float(v) if v is not None else None
    except Exception:
        return None

def _extract_qty(obj: Dict[str, Any]) -> Optional[int]:
    # 1) estimatedAvailabilities
    est = obj.get("estimatedAvailabilities", [])
    if isinstance(est, list) and est:
        q = (est[0] or {}).get("estimatedAvailableQuantity")
        if isinstance(q, int):
            return q

    # 2) availability.shipToLocationAvailability.quantity
    avail = obj.get("availability")
    if isinstance(avail, dict):
        ship = avail.get("shipToLocationAvailability")
        if isinstance(ship, dict):
            q = ship.get("quantity")
            if isinstance(q, int):
                return q

    return None

def _condition_to_ids(condition: Optional[str]) -> Optional[List[int]]:
    """
    Converte strings simples em conditionIds do eBay.
    NEW -> 1000
    USED -> 3000
    REFURB -> conjunto aproximado
    """
    if not condition:
        return None
    c = condition.strip().upper()
    if c in ("NEW", "NOVO"):
        return [1000]
    if c in ("USED", "USADO"):
        return [3000]
    if c in ("REFURB", "REFURBISHED", "RECONDICIONADO"):
        return [2000, 2010, 2020, 2030]
    return None

def _build_filter(
    price_min: Optional[float],
    price_max: Optional[float],
    condition_ids: Optional[List[int]],
) -> str:
    parts = ["buyingOptions:{FIXED_PRICE}"]

    if condition_ids:
        joined = "|".join(str(x) for x in condition_ids)
        parts.append(f"conditionIds:{{{joined}}}")

    if price_min is not None or price_max is not None:
        if price_min is None:
            parts.append(f"price:[..{price_max}]")
        elif price_max is None:
            parts.append(f"price:[{price_min}..]")
        else:
            parts.append(f"price:[{price_min}..{price_max}]")
        parts.append(f"priceCurrency:{CURRENCY}")

    return ",".join(parts)

def _normalize_summary(s: Dict[str, Any]) -> Dict[str, Any]:
    price = s.get("price", {}) or {}
    seller = s.get("seller", {}) or {}

    price_val = _money_val(price)
    currency = price.get("currency", CURRENCY)

    ship_cost = None
    ship_opts = s.get("shippingOptions") or []
    if isinstance(ship_opts, list) and ship_opts:
        ship_cost = _money_val((ship_opts[0] or {}).get("shippingCost"))

    total = None
    if price_val is not None:
        total = price_val + (ship_cost or 0.0)

    item = {
        "item_id": s.get("itemId"),
        "title": s.get("title"),
        "price": price_val,
        "shipping": ship_cost,
        "total": total,
        "currency": currency,
        "condition": s.get("condition"),
        "condition_id": s.get("conditionId"),
        "seller": seller.get("username"),
        "category_id": int(s.get("categoryId")) if s.get("categoryId") else None,
        "item_url": s.get("itemWebUrl"),
        "available_qty": None,
        "qty_flag": "UNKNOWN",
        "brand": s.get("brand"),
        "mpn": s.get("mpn"),
        "gtin": s.get("gtin"),
    }

    q = _extract_qty(s)
    if isinstance(q, int):
        item["available_qty"] = q
        item["qty_flag"] = "EXACT"

    return item

# ────────────────────────────────────────────────────────────────────────────────
# Funções públicas
# ────────────────────────────────────────────────────────────────────────────────

def search_by_category(
    category_id: int,
    source_price_min: Optional[float] = 15.0,
    source_price_max: Optional[float] = None,
    condition: str = "NEW",
    limit_per_page: int = 50,
    max_pages: int = 2,
) -> List[dict]:
    """
    Consulta a Browse API do eBay por category_id, aplicando filtros.
    """
    headers = _auth_headers()
    cond_ids = _condition_to_ids(condition)

    params_base = {
        "category_ids": str(category_id),
        "limit": str(max(1, min(200, int(limit_per_page)))),
        "filter": _build_filter(source_price_min, source_price_max, cond_ids),
        "fieldgroups": "EXTENDED",
        "sort": "price",
    }

    items: List[dict] = []
    offset = 0

    for _ in range(max_pages):
        params = dict(params_base)
        params["offset"] = str(offset)

        try:
            r = _session.get(
                f"{BASE}/item_summary/search",
                headers=headers,
                params=params,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
        except Exception as e:
            raise EbayRequestError(f"Falha de rede ao consultar Browse: {type(e).__name__}: {e}")

        if r.status_code != 200:
            raise EbayRequestError(f"Erro Browse API: {r.status_code} {r.text}")

        data = r.json() or {}
        summaries = data.get("itemSummaries", []) or []
        if not summaries:
            break

        for s in summaries:
            items.append(_normalize_summary(s))

        total = int(data.get("total", 0))
        offset += int(params_base["limit"])
        if offset >= total:
            break

        time.sleep(0.08)

    return items

def get_item_detail(item_id: str) -> dict:
    """
    Busca detalhes de um item específico através da Browse API.
    Tenta fieldgroups e faz fallback sem fieldgroups se necessário.
    """
    headers = _auth_headers()
    url = f"{BASE}/item/{item_id}"

    def _do(fieldgroups: Optional[str]):
        params = {}
        if fieldgroups:
            params["fieldgroups"] = fieldgroups
        return _session.get(url, headers=headers, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

    r = _do("PRODUCT,ADDITIONAL_SELLER_DETAILS")
    if r.status_code == 400:
        r = _do(None)

    if r.status_code != 200:
        raise EbayRequestError(f"Erro item detail {item_id}: {r.status_code} {r.text}")

    d = r.json() or {}

    out = {
        "item_id": d.get("itemId"),
        "available_qty": None,
        "qty_flag": "UNKNOWN",
        "brand": d.get("brand"),
        "mpn": d.get("mpn"),
        "gtin": None,
        "category_id": int(d.get("categoryId")) if d.get("categoryId") else None,
    }

    q = _extract_qty(d)
    if isinstance(q, int):
        out["available_qty"] = q
        out["qty_flag"] = "EXACT"

    prod = d.get("product", {})
    if isinstance(prod, dict):
        gtins = prod.get("gtin")
        if isinstance(gtins, list) and gtins:
            out["gtin"] = gtins[0]

        aspects = prod.get("aspects", {})
        if isinstance(aspects, dict):
            if not out["brand"]:
                out["brand"] = (aspects.get("Brand") or [None])[0]
            if not out["mpn"]:
                out["mpn"] = (aspects.get("MPN") or aspects.get("Manufacturer Part Number") or [None])[0]

    return out

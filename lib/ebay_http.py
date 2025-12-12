# lib/ebay_http.py — FIX category_ids como query param (não no filter)
import os, time, requests
from typing import Dict, Any, List, Tuple
from lib.ebay_auth import get_app_token

BASE = "https://api.ebay.com/buy/browse/v1"
SITE_ID = os.getenv("EBAY_BROWSE_SITE_ID", "0")

def _auth_headers():
    token = get_app_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-EBAY-C-ENDUSERCTX": f"contextualLocation=country=US,zip=00000;siteid={SITE_ID}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
    }

def _price_filter(min_v: float | None, max_v: float | None) -> str | None:
    if min_v is None and max_v is None:
        return None
    if min_v is not None and max_v is not None:
        return f"price:[{min_v}..{max_v}]"
    if min_v is not None:
        return f"price:[{min_v}..]"
    return f"price:[..{max_v}]"

def _flatten_item(s: Dict[str, Any]) -> Dict[str, Any]:
    price = s.get("price") or {}
    out = {
        "item_id": s.get("itemId"),
        "title": s.get("title"),
        "price": float(price.get("value")) if price.get("value") is not None else None,
        "currency": price.get("currency"),
        "condition": s.get("condition"),
        "seller": (s.get("seller") or {}).get("username"),
        "category_id": int(s.get("categoryId")) if s.get("categoryId") else None,
        "item_url": s.get("itemWebUrl"),
        "available_qty": None, "qty_flag": "EXACT",
        "brand": s.get("brand"), "mpn": s.get("mpn"), "gtin": s.get("gtin"),
    }
    est = s.get("estimatedAvailabilities") or []
    if isinstance(est, list) and est:
        q = est[0].get("estimatedAvailableQuantity")
        if isinstance(q, int):
            out["available_qty"] = q
            out["qty_flag"] = "EXACT"
    return out

def search_with_refinements(
    category_id: int | None,
    q: str | None,
    price_min: float | None,
    price_max: float | None,
    condition: str | None,
    limit_per_page: int = 200,
    max_pages: int = 10,
    want_refinements: bool = True,
    extra_filters: List[str] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    refinements: Dict[str, Any] = {}
    seen = set()

    # monta FILTER apenas com filtros que pertencem ao filter=
    filters: List[str] = []
    pf = _price_filter(price_min, price_max)
    if pf: filters.append(pf)
    if condition: filters.append(f"conditions:{{{condition}}}")
    if extra_filters: filters.extend(extra_filters)

    params = {
        "limit": min(200, max(1, int(limit_per_page))),
        "offset": 0,
        "sort": "price",
        "fieldgroups": "EXTENDED" if want_refinements else None,
        "q": q if q else None,
        "filter": ",".join(filters) if filters else None,
    }
    # 👉 category_ids é QUERY PARAM separado (corrige erro 12001)
    if category_id:
        params["category_ids"] = str(int(category_id))

    headers = _auth_headers()
    offset = 0
    for _ in range(max_pages):
        p = {k: v for k, v in params.items() if v not in (None, "", [])}
        r = requests.get(f"{BASE}/item_summary/search", params=p, headers=headers, timeout=40)
        if r.status_code != 200:
            raise RuntimeError(f"eBay Browse error {r.status_code}: {r.text}")
        data = r.json() or {}

        if want_refinements and not refinements:
            refinements = data.get("refinement", {}) or {}

        arr = data.get("itemSummaries", []) or []
        if not arr: break
        for it in arr:
            iid = it.get("itemId")
            if iid and iid in seen: continue
            seen.add(iid)
            items.append(_flatten_item(it))

        total = data.get("total", 0)
        offset = data.get("offset", 0) + len(arr)
        if offset >= total: break
        params["offset"] = offset
        time.sleep(0.08)

    return items, refinements

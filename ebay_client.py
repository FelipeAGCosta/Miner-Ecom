import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lib.ebay_auth import get_app_token


# ────────────────────────────────────────────────────────────────────────────────
# Configurações e Constantes
# ────────────────────────────────────────────────────────────────────────────────

def _utcnow_naive() -> datetime:
    # evita DeprecationWarning do utcnow()
    return datetime.now(timezone.utc).replace(tzinfo=None)

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
    """Erro relacionado à autenticação com o eBay."""
    pass

class EbayRequestError(Exception):
    """Erro genérico nas requisições para a API do eBay."""
    pass


# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _auth_headers() -> Dict[str, str]:
    """
    Cabeçalhos necessários para chamadas à Browse API.
    """
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

def _extract_availability(obj: Dict[str, Any]) -> Tuple[Optional[int], str, Optional[str], Optional[int], Optional[str], int]:
    """
    Retorna:
      (available_qty_exact, qty_flag, threshold_type, threshold, availability_status, min_available_qty)

    Regras finais (como você pediu):
      - se estimatedAvailableQuantity existir -> EXACT + min = qty
      - se thresholdType=MORE_THAN e threshold=10 -> MORE_THAN + min = 10
      - se não vier nada -> UNKNOWN + min = 1
      - se status OUT_OF_STOCK/SOLD_OUT -> min = 0
    """
    qty_exact: Optional[int] = None
    qty_flag = "UNKNOWN"
    threshold_type: Optional[str] = None
    threshold: Optional[int] = None
    status: Optional[str] = None

    est = obj.get("estimatedAvailabilities", [])
    if isinstance(est, list) and est:
        e0 = est[0] or {}
        if isinstance(e0, dict):
            status = e0.get("estimatedAvailabilityStatus") or e0.get("availabilityStatus")

            q = e0.get("estimatedAvailableQuantity")
            if isinstance(q, int):
                qty_exact = q
                qty_flag = "EXACT"
            else:
                tt = e0.get("availabilityThresholdType")
                th = e0.get("availabilityThreshold")
                if tt is not None:
                    threshold_type = str(tt).strip().upper()
                if isinstance(th, int):
                    threshold = th
                if threshold_type and threshold is not None:
                    qty_flag = threshold_type  # ex: MORE_THAN

    # fallback extra (às vezes vem quantity aqui)
    if qty_exact is None:
        avail = obj.get("availability")
        if isinstance(avail, dict):
            ship = avail.get("shipToLocationAvailability")
            if isinstance(ship, dict):
                q2 = ship.get("quantity")
                if isinstance(q2, int):
                    qty_exact = q2
                    qty_flag = "EXACT"

    # min qty
    min_qty = 1
    if status is not None and str(status).strip().upper() in ("OUT_OF_STOCK", "SOLD_OUT"):
        min_qty = 0
    elif qty_exact is not None:
        min_qty = int(qty_exact)
    elif threshold_type == "MORE_THAN" and threshold is not None:
        # você pediu "10 redondo"
        min_qty = int(threshold)
    else:
        min_qty = 1

    return qty_exact, qty_flag, threshold_type, threshold, (str(status).strip().upper() if status else None), min_qty

def _condition_to_ids(condition: Optional[str]) -> Optional[List[int]]:
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

def _do_get(url: str, headers: Dict[str, str], params: Optional[Dict[str, str]] = None) -> requests.Response:
    try:
        r = _session.get(url, headers=headers, params=(params or {}), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    except Exception as e:
        raise EbayRequestError(f"Falha de rede: {type(e).__name__}: {e}")

    if r.status_code == 429:
        time.sleep(1.0)
        r = _session.get(url, headers=headers, params=(params or {}), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

    return r


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

    qty_exact, qty_flag, thr_type, thr, status, min_qty = _extract_availability(s)
    now = _utcnow_naive()

    out = {
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

        "available_qty": qty_exact,
        "qty_flag": qty_flag,
        "availability_threshold_type": thr_type,
        "availability_threshold": thr,
        "availability_status": status,
        "min_available_qty": min_qty,
        "availability_updated_at": now,

        "brand": s.get("brand"),
        "mpn": s.get("mpn"),
        "gtin": s.get("gtin"),
    }

    return out


# ────────────────────────────────────────────────────────────────────────────────
# Browse: search
# ────────────────────────────────────────────────────────────────────────────────

def search_item_summaries(
    q: Optional[str] = None,
    gtin: Optional[str] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    condition_ids: Optional[List[int]] = None,
    limit: int = 20,
) -> List[dict]:
    headers = _auth_headers()
    params: Dict[str, str] = {}

    if gtin:
        params["gtin"] = str(gtin).strip()
    else:
        params["q"] = (q or "a").strip()

    params["limit"] = str(max(1, min(50, int(limit))))
    params["offset"] = "0"
    params["filter"] = _build_filter(price_min, price_max, condition_ids)

    r = _do_get(f"{BASE}/item_summary/search", headers=headers, params=params)

    if r.status_code != 200:
        raise EbayRequestError(f"Erro Browse search: {r.status_code} {r.text[:500]}")

    data = r.json() or {}
    items = data.get("itemSummaries", []) or []
    return [_normalize_summary(x) for x in items]


def search_by_category(
    category_id: int,
    source_price_min: float = 15.0,
    condition: str = "NEW",
    limit_per_page: int = 50,
    max_pages: int = 2,
) -> List[dict]:
    headers = _auth_headers()
    cond_ids = _condition_to_ids(condition)

    params_base = {
        "category_ids": str(category_id),
        "limit": str(max(1, min(200, int(limit_per_page)))),
        "filter": _build_filter(source_price_min, None, cond_ids),
        "fieldgroups": "EXTENDED",
        "sort": "price",
    }

    items: List[dict] = []
    offset = 0

    for _ in range(max_pages):
        params = dict(params_base)
        params["offset"] = str(offset)

        r = _do_get(f"{BASE}/item_summary/search", headers=headers, params=params)

        if r.status_code != 200:
            raise EbayRequestError(f"Erro Browse API: {r.status_code} {r.text[:500]}")

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


# ────────────────────────────────────────────────────────────────────────────────
# Browse: getItem (leve) para disponibilidade
# ────────────────────────────────────────────────────────────────────────────────

def get_item_availability(item_id: str) -> Dict[str, Any]:
    """
    Pega APENAS dados de disponibilidade de um item (getItem), usando fieldgroups=COMPACT.
    Isso é o caminho estável (getItems bulk é Limited Release e pode dar 403).
    """
    headers = _auth_headers()
    url = f"{BASE}/item/{item_id}"

    r = _do_get(url, headers=headers, params={"fieldgroups": "COMPACT"})
    if r.status_code != 200:
        raise EbayRequestError(f"Erro getItem {item_id}: {r.status_code} {r.text[:500]}")

    d = r.json() or {}

    qty_exact, qty_flag, thr_type, thr, status, min_qty = _extract_availability(d)
    now = _utcnow_naive()

    return {
        "item_id": d.get("itemId") or item_id,
        "available_qty": qty_exact,
        "qty_flag": qty_flag,
        "availability_threshold_type": thr_type,
        "availability_threshold": thr,
        "availability_status": status,
        "min_available_qty": min_qty,
        "availability_updated_at": now,
    }


def get_items_availability(item_ids: List[str], batch_size: int = 20, per_item_sleep: float = 0.05) -> List[Dict[str, Any]]:
    """
    Tenta getItems (bulk). Se der 403 (Limited Release / sem permissão),
    faz fallback automático para getItem (COMPACT) item a item.
    """
    if not item_ids:
        return []

    # dedup/clean
    ids = [str(x).strip() for x in item_ids if x]
    ids = list(dict.fromkeys([x for x in ids if x]))
    if not ids:
        return []

    headers = _auth_headers()
    out: List[Dict[str, Any]] = []

    # 1) tenta bulk (pode falhar com 403)
    try:
        for i in range(0, len(ids), max(1, int(batch_size))):
            chunk = ids[i:i + max(1, int(batch_size))]
            r = _do_get(f"{BASE}/item", headers=headers, params={"item_ids": ",".join(chunk)})

            if r.status_code == 403:
                # fallback total
                raise EbayRequestError(f"Erro Browse getItems: 403 {r.text[:500]}")

            if r.status_code != 200:
                raise EbayRequestError(f"Erro Browse getItems: {r.status_code} {r.text[:500]}")

            data = r.json() or {}
            items = data.get("items", []) or []
            now = _utcnow_naive()

            for it in items:
                if not isinstance(it, dict):
                    continue
                item_id = it.get("itemId")
                if not item_id:
                    continue
                qty_exact, qty_flag, thr_type, thr, status, min_qty = _extract_availability(it)
                out.append({
                    "item_id": item_id,
                    "available_qty": qty_exact,
                    "qty_flag": qty_flag,
                    "availability_threshold_type": thr_type,
                    "availability_threshold": thr,
                    "availability_status": status,
                    "min_available_qty": min_qty,
                    "availability_updated_at": now,
                })

            time.sleep(0.05)

        return out

    except EbayRequestError:
        # 2) fallback estável: getItem (COMPACT)
        out2: List[Dict[str, Any]] = []
        for iid in ids:
            try:
                out2.append(get_item_availability(iid))
            except Exception as e:
                # não quebra o batch inteiro
                out2.append({
                    "item_id": iid,
                    "available_qty": None,
                    "qty_flag": f"ERROR:{type(e).__name__}",
                    "availability_threshold_type": None,
                    "availability_threshold": None,
                    "availability_status": None,
                    "min_available_qty": 1,
                    "availability_updated_at": _utcnow_naive(),
                })
            time.sleep(float(per_item_sleep))
        return out2
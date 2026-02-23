"""
Client simples para a eBay Browse API (site US).

Responsabilidades principais:
- Obter access token via Client Credentials (OAuth2).
- Fazer busca por categoria (item_summary/search).
- Buscar detalhe de um item específico (item/{item_id}).
- Buscar disponibilidade/estoque (COMPACT) para refresh rápido.

Este módulo é usado como integração de baixo nível; a normalização final e
persistência dos dados ficam em outros módulos (por exemplo, lib/db.py).
"""

from __future__ import annotations

import base64
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Any

import httpx

# ---------------------------------------------------------------------------
# Configuração básica (via .env)
# ---------------------------------------------------------------------------

EBAY_ENV = os.getenv("EBAY_ENV", "production").lower().strip()
EBAY_CLIENT_ID = os.getenv("EBAY_CLIENT_ID", "")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET", "")

def _api_base() -> str:
    if "sand" in EBAY_ENV:
        return "https://api.sandbox.ebay.com"
    return "https://api.ebay.com"

API_BASE = _api_base()

IDENTITY_URL = f"{API_BASE}/identity/v1/oauth2/token"
BROWSE_SEARCH_URL = f"{API_BASE}/buy/browse/v1/item_summary/search"
BROWSE_ITEM_URL = f"{API_BASE}/buy/browse/v1/item"  # + /{item_id}

MARKETPLACE_ID = (os.getenv("EBAY_MARKETPLACE_ID") or "EBAY_US").strip()
CURRENCY = (os.getenv("EBAY_CURRENCY") or "USD").strip()

# Cache simples de token em memória: {"app": (access_token, expires_at_epoch)}
_token_cache: Dict[str, Tuple[str, float]] = {}

# Retry simples
_RETRY_STATUSES = {429, 500, 502, 503, 504}

# ---------------------------------------------------------------------------
# Exceções
# ---------------------------------------------------------------------------

class EbayAuthError(Exception):
    """Erro relacionado à autenticação com a API do eBay."""
    pass

class EbayRequestError(Exception):
    """Erro em chamadas à Browse API (HTTP != 200, etc.)."""
    pass

# ---------------------------------------------------------------------------
# Autenticação (Client Credentials)
# ---------------------------------------------------------------------------

def _basic_auth_header() -> str:
    """
    Monta o header HTTP Basic Auth a partir de EBAY_CLIENT_ID/EBAY_CLIENT_SECRET.
    """
    if not EBAY_CLIENT_ID or not EBAY_CLIENT_SECRET:
        raise EbayAuthError("EBAY_CLIENT_ID/EBAY_CLIENT_SECRET ausentes no .env")

    raw = f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")

def get_app_token() -> str:
    """
    Obtém um access_token via Client Credentials (escopo api_scope).
    Usa cache em memória até a expiração reportada pela própria API.
    """
    now = time.time()
    cached = _token_cache.get("app")

    # Reaproveita token se ainda estiver com folga de 60s antes de expirar
    if cached and (cached[1] - 60) > now:
        return cached[0]

    headers = {
        "Authorization": _basic_auth_header(),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }

    with httpx.Client(timeout=30) as client:
        resp = client.post(IDENTITY_URL, headers=headers, data=data)

    if resp.status_code != 200:
        raise EbayAuthError(f"Falha ao obter token: {resp.status_code} {resp.text}")

    payload = resp.json()
    access_token = payload["access_token"]
    expires_in = int(payload.get("expires_in", 7200))

    _token_cache["app"] = (access_token, now + expires_in)
    return access_token

# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)

def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
    }

def _request_get(url: str, headers: Dict[str, str], params: Optional[Dict[str, str]] = None, timeout: float = 40.0) -> httpx.Response:
    params = params or {}
    last: Optional[httpx.Response] = None

    for attempt in range(1, 6):
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(url, headers=headers, params=params)

        last = resp
        if resp.status_code not in _RETRY_STATUSES:
            return resp

        # backoff simples
        if resp.status_code == 429:
            time.sleep(min(2.0, 0.35 * attempt))
        else:
            time.sleep(0.20 * attempt)

    return last  # type: ignore

def _extract_availability(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrai disponibilidade/estoque a partir de estimatedAvailabilities.

    Regras finais:
      - estimatedAvailableQuantity -> available_qty (EXACT) e min_available_qty = qty
      - availabilityThresholdType=MORE_THAN e threshold=10 -> min_available_qty = 10
      - se nada vier -> min_available_qty = 1
      - OUT_OF_STOCK/SOLD_OUT -> min_available_qty = 0
    """
    available_qty: Optional[int] = None
    qty_flag = "UNKNOWN"
    thr_type: Optional[str] = None
    thr: Optional[int] = None
    status: Optional[str] = None

    est = d.get("estimatedAvailabilities") or []
    if isinstance(est, list) and est:
        e0 = est[0] or {}
        if isinstance(e0, dict):
            status = e0.get("estimatedAvailabilityStatus") or e0.get("availabilityStatus")

            q = e0.get("estimatedAvailableQuantity")
            if isinstance(q, int):
                available_qty = q
                qty_flag = "EXACT"
            else:
                tt = e0.get("availabilityThresholdType")
                th = e0.get("availabilityThreshold")
                if tt is not None:
                    thr_type = str(tt).strip().upper()
                if isinstance(th, int):
                    thr = th
                if thr_type and thr is not None:
                    qty_flag = thr_type  # ex: MORE_THAN

    min_qty = 1
    if status and str(status).strip().upper() in ("OUT_OF_STOCK", "SOLD_OUT"):
        min_qty = 0
    elif available_qty is not None:
        min_qty = int(available_qty)
    elif thr_type == "MORE_THAN" and thr is not None:
        min_qty = int(thr)  # você pediu 10 redondo
    else:
        min_qty = 1

    return {
        "available_qty": available_qty,
        "qty_flag": qty_flag,
        "availability_threshold_type": thr_type,
        "availability_threshold": thr,
        "availability_status": (str(status).strip().upper() if status else None),
        "min_available_qty": min_qty,
        "availability_updated_at": _utcnow_naive(),
    }

def _build_filter(source_price_min: Optional[float], condition: str) -> str:
    """
    Monta a string do parâmetro 'filter' da Browse API.
    Exemplo: 'price:[15..],conditions:{NEW}'
    """
    parts: List[str] = []

    if source_price_min is not None:
        parts.append(f"price:[{source_price_min}..]")

    if condition:
        parts.append(f"conditions:{{{condition}}}")

    return ",".join(parts)

def _money_to_float(x: Any) -> Optional[float]:
    try:
        if not isinstance(x, dict):
            return None
        v = x.get("value")
        return float(v) if v is not None else None
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Browse API: search
# ---------------------------------------------------------------------------

def search_by_category(
    category_id: int,
    source_price_min: float = 15.0,
    condition: str = "NEW",
    limit_per_page: int = 50,
    max_pages: int = 2,
) -> List[dict]:
    """
    Consulta a Browse API por category_id, aplicando filtros de preço e condição.
    - Pagina até max_pages.
    - Retorna uma lista de itens "achatados" (dicts).
    """
    token = get_app_token()
    headers = _headers(token)

    params_base = {
        "category_ids": str(category_id),
        "limit": str(limit_per_page),
        "filter": _build_filter(source_price_min, condition),
        "fieldgroups": "EXTENDED",
    }

    items: List[dict] = []
    offset = 0

    for _ in range(max_pages):
        params = dict(params_base)
        params["offset"] = str(offset)

        resp = _request_get(BROWSE_SEARCH_URL, headers=headers, params=params, timeout=40.0)
        if resp.status_code != 200:
            raise EbayRequestError(f"Erro Browse API: {resp.status_code} {resp.text}")

        data = resp.json() or {}
        summaries = data.get("itemSummaries", []) or []

        for s in summaries:
            price_val = _money_to_float(s.get("price") or {})
            currency_val = (s.get("price") or {}).get("currency") or CURRENCY

            avail = _extract_availability(s)

            item: Dict[str, object] = {
                "item_id": s.get("itemId"),
                "title": s.get("title"),
                "price": price_val,
                "currency": currency_val,
                "condition": s.get("condition"),
                "seller": (s.get("seller") or {}).get("username"),
                "category_id": int(s.get("categoryId")) if s.get("categoryId") else None,
                "item_url": s.get("itemWebUrl"),

                # estoque
                "available_qty": avail["available_qty"],
                "qty_flag": avail["qty_flag"],
                "availability_threshold_type": avail["availability_threshold_type"],
                "availability_threshold": avail["availability_threshold"],
                "availability_status": avail["availability_status"],
                "min_available_qty": avail["min_available_qty"],
                "availability_updated_at": avail["availability_updated_at"],

                # extras
                "brand": s.get("brand"),
                "mpn": s.get("mpn"),
                "gtin": s.get("gtin"),
            }

            items.append(item)

        total = int(data.get("total", 0))
        offset += limit_per_page

        if offset >= total or not summaries:
            break

    return items

# ---------------------------------------------------------------------------
# Browse API: getItem (detalhe) + getItem(COMPACT)
# ---------------------------------------------------------------------------

def get_item_detail(item_id: str) -> dict:
    """
    Busca detalhe de um item específico na Browse API.
    Mantém compatibilidade com seu uso antigo, mas agora também retorna campos de disponibilidade.
    """
    token = get_app_token()
    headers = _headers(token)

    url = f"{BROWSE_ITEM_URL}/{item_id}"

    def _do_req(fieldgroups: Optional[str]):
        params: Dict[str, str] = {}
        if fieldgroups:
            params["fieldgroups"] = fieldgroups
        return _request_get(url, headers=headers, params=params, timeout=40.0)

    resp = _do_req("PRODUCT,ADDITIONAL_SELLER_DETAILS")
    if resp.status_code == 400:
        resp = _do_req(None)

    if resp.status_code == 404:
        return {
            "item_id": item_id,
            "available_qty": None,
            "qty_flag": "NOT_FOUND",
            "availability_threshold_type": None,
            "availability_threshold": None,
            "availability_status": None,
            "min_available_qty": 1,
            "availability_updated_at": _utcnow_naive(),
            "brand": None,
            "mpn": None,
            "gtin": None,
            "category_id": None,
        }

    if resp.status_code == 429:
        return {
            "item_id": item_id,
            "available_qty": None,
            "qty_flag": "RATE_LIMIT",
            "availability_threshold_type": None,
            "availability_threshold": None,
            "availability_status": None,
            "min_available_qty": 1,
            "availability_updated_at": _utcnow_naive(),
            "brand": None,
            "mpn": None,
            "gtin": None,
            "category_id": None,
        }

    if resp.status_code != 200:
        raise EbayRequestError(f"Erro item detail {item_id}: {resp.status_code} {resp.text}")

    d = resp.json() or {}
    avail = _extract_availability(d)

    out: Dict[str, object] = {
        "item_id": d.get("itemId") or item_id,
        "available_qty": avail["available_qty"],
        "qty_flag": avail["qty_flag"],
        "availability_threshold_type": avail["availability_threshold_type"],
        "availability_threshold": avail["availability_threshold"],
        "availability_status": avail["availability_status"],
        "min_available_qty": avail["min_available_qty"],
        "availability_updated_at": avail["availability_updated_at"],

        "brand": d.get("brand"),
        "mpn": d.get("mpn"),
        "gtin": None,
        "category_id": int(d.get("categoryId")) if d.get("categoryId") else None,
    }

    prod = d.get("product") or {}
    if isinstance(prod, dict):
        gtins = prod.get("gtin")
        if isinstance(gtins, list) and gtins:
            out["gtin"] = gtins[0]

        aspects = prod.get("aspects") or {}
        if not out.get("brand"):
            out["brand"] = (aspects.get("Brand") or [None])[0]
        if not out.get("mpn"):
            out["mpn"] = (aspects.get("MPN") or aspects.get("Manufacturer Part Number") or [None])[0]

    return out

def get_item_availability(item_id: str) -> dict:
    """
    GET /item/{item_id}?fieldgroups=COMPACT
    COMPACT deve ser usado sozinho (não combinar).
    Retorna dados suficientes para refresh rápido (estoque/status/threshold).
    """
    token = get_app_token()
    headers = _headers(token)

    url = f"{BROWSE_ITEM_URL}/{item_id}"
    resp = _request_get(url, headers=headers, params={"fieldgroups": "COMPACT"}, timeout=30.0)

    if resp.status_code == 404:
        return {
            "item_id": item_id,
            "available_qty": None,
            "qty_flag": "NOT_FOUND",
            "availability_threshold_type": None,
            "availability_threshold": None,
            "availability_status": None,
            "min_available_qty": 1,
            "availability_updated_at": _utcnow_naive(),
        }

    if resp.status_code == 429:
        return {
            "item_id": item_id,
            "available_qty": None,
            "qty_flag": "RATE_LIMIT",
            "availability_threshold_type": None,
            "availability_threshold": None,
            "availability_status": None,
            "min_available_qty": 1,
            "availability_updated_at": _utcnow_naive(),
        }

    if resp.status_code != 200:
        raise EbayRequestError(f"Erro getItem(COMPACT) {item_id}: {resp.status_code} {resp.text}")

    d = resp.json() or {}
    avail = _extract_availability(d)
    return {
        "item_id": d.get("itemId") or item_id,
        "available_qty": avail["available_qty"],
        "qty_flag": avail["qty_flag"],
        "availability_threshold_type": avail["availability_threshold_type"],
        "availability_threshold": avail["availability_threshold"],
        "availability_status": avail["availability_status"],
        "min_available_qty": avail["min_available_qty"],
        "availability_updated_at": avail["availability_updated_at"],
    }

def get_items_availability(item_ids: List[str], per_item_sleep: float = 0.05) -> List[dict]:
    """
    Refresh de disponibilidade em lote (estável): chama getItem(COMPACT) item a item.
    """
    if not item_ids:
        return []

    ids = [str(x).strip() for x in item_ids if x]
    ids = list(dict.fromkeys([x for x in ids if x]))
    out: List[dict] = []

    for iid in ids:
        try:
            out.append(get_item_availability(iid))
        except Exception as e:
            out.append({
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

    return out
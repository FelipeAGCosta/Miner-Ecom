# lib/ebay_search.py
"""
Busca de itens na eBay Browse API (item_summary/search), usada no fluxo de mineração.

- Suporta busca por category_ids e/ou palavra-chave (q).
- Aplica filtros de faixa de preço e condição via filter=.
- Usa sessão HTTP com retry/backoff.
- Retorna lista "achatada" de itens, compatível com o restante do app.
"""

import os
import time
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lib.ebay_auth import get_app_token

BASE = "https://api.ebay.com/buy/browse/v1"
SITE_ID = os.getenv("EBAY_BROWSE_SITE_ID", "0")
MARKETPLACE_ID = os.getenv("EBAY_MARKETPLACE_ID", "EBAY_US")

CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", 5))
READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", 30))

# Sessão HTTP compartilhada com retry/backoff para chamadas GET
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


def _auth_headers() -> Dict[str, str]:
    """
    Monta cabeçalhos de autenticação e contexto para Browse API.
    Token é obtido via get_app_token (com cache/Redis).
    """
    token = get_app_token()
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
        "X-EBAY-C-ENDUSERCTX": (
            f"contextualLocation=country=US,zip=00000;siteid={SITE_ID}"
        ),
    }


def _price_filter(min_v: Optional[float], max_v: Optional[float]) -> Optional[str]:
    """
    Constrói o trecho de filter= para faixa de preço:
    - price:[MIN..MAX]
    - price:[MIN..]
    - price:[..MAX]
    """
    if min_v is None and max_v is None:
        return None
    if min_v is not None and max_v is not None:
        return f"price:[{min_v}..{max_v}]"
    if min_v is not None:
        return f"price:[{min_v}..]"
    return f"price:[..{max_v}]"


def _flatten_item(s: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza um itemSummary da Browse API em um dict "achatado".
    """
    price = s.get("price") or {}
    seller = s.get("seller") or {}
    currency = price.get("currency") or "USD"

    try:
        price_val = float(price.get("value")) if price.get("value") is not None else None
    except Exception:
        price_val = None

    out: Dict[str, Any] = {
        "item_id": s.get("itemId"),
        "title": s.get("title"),
        "price": price_val,
        "currency": currency,
        "condition": s.get("condition"),
        "seller": seller.get("username"),
        "category_id": int(s.get("categoryId")) if s.get("categoryId") else None,
        "item_url": s.get("itemWebUrl"),
        "available_qty": None,
        "qty_flag": "EXACT",
        "brand": s.get("brand"),
        "mpn": s.get("mpn"),
        "gtin": s.get("gtin"),
    }

    est = s.get("estimatedAvailabilities") or []
    if isinstance(est, list) and est:
        q = est[0].get("estimatedAvailableQuantity")
        if isinstance(q, int):
            out["available_qty"] = q
            out["qty_flag"] = "EXACT"

    return out


def search_items(
    category_id: Optional[int],
    keyword: Optional[str],
    price_min: Optional[float],
    price_max: Optional[float],
    condition: Optional[str],
    limit_per_page: int = 200,
    max_pages: int = 10,
) -> List[Dict[str, Any]]:
    """
    Busca itens na Browse API com category_ids e/ou q SEM colocar isso dentro de filter.
    Retorna lista achatada de itens (dicts).
    """
    if not category_id and not (keyword and keyword.strip()):
        raise ValueError(
            "search_items requer pelo menos um category_id ou uma palavra-chave (q)."
        )

    headers = _auth_headers()

    filters: List[str] = []
    pf = _price_filter(price_min, price_max)
    if pf:
        filters.append(pf)
    if condition:
        filters.append(f"conditions:{{{condition}}}")

    # Sanitiza limit uma vez e usa sempre o mesmo valor
    limit = min(200, max(1, int(limit_per_page)))

    params: Dict[str, Any] = {
        "limit": limit,
        "offset": 0,
    }
    if filters:
        params["filter"] = ",".join(filters)

    if keyword and keyword.strip():
        params["q"] = keyword.strip()

    if category_id:
        params["category_ids"] = str(int(category_id))

    items: List[Dict[str, Any]] = []
    offset = 0

    for _ in range(max_pages):
        params["offset"] = offset

        resp = _session.get(
            f"{BASE}/item_summary/search",
            headers=headers,
            params=params,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )

        if resp.status_code == 400 and "12001" in resp.text:
            # chamada sem q/category_ids → não deveria acontecer, mas se acontecer
            # avisamos e interrompemos estas páginas
            raise RuntimeError(f"Erro Browse API: 400 {resp.text}")

        if resp.status_code != 200:
            raise RuntimeError(f"Erro Browse API: {resp.status_code} {resp.text}")

        data = resp.json() or {}
        arr = data.get("itemSummaries", []) or []
        if not arr:
            break

        for s in arr:
            items.append(_flatten_item(s))

        total = int(data.get("total", 0))

        # avanço de página: offset sempre múltiplo de limit
        offset += limit
        if offset >= total:
            break

        time.sleep(0.1)

    return items

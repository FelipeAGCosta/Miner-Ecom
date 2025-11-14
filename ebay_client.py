import os
import time
from typing import Dict, List, Tuple, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lib.ebay_auth import get_app_token

# ────────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────────
BASE = "https://api.ebay.com/buy/browse/v1"
# SiteID (0 = US). Mantém compatibilidade com seu .env
SITE_ID = os.getenv("EBAY_BROWSE_SITE_ID", "0")
# Marketplace para header
MARKETPLACE_ID = os.getenv("EBAY_MARKETPLACE_ID", "EBAY_US")

# Timeouts (ajustáveis via .env, com defaults seguros)
CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", 5))
READ_TIMEOUT    = float(os.getenv("HTTP_READ_TIMEOUT", 30))

# Sessão HTTP com retry/backoff para chamadas eBay (resiliente a 429/5xx)
_retry = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=0.5,  # 0.5, 1, 2, 4, 8
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
_session = requests.Session()
_session.mount("https://", HTTPAdapter(max_retries=_retry))
_session.mount("http://",  HTTPAdapter(max_retries=_retry))

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
    token = get_app_token()  # usa cache/redis + retry definidos em lib.ebay_auth
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
        "X-EBAY-C-ENDUSERCTX": f"contextualLocation=country=US,zip=00000;siteid={SITE_ID}",
    }

def _build_filter(source_price_min: Optional[float], condition: Optional[str]) -> str:
    """
    Monta a string do parâmetro 'filter' da Browse API.
    Ex.: 'price:[15..],conditions:{NEW}'
    """
    parts = []
    if source_price_min is not None:
        parts.append(f"price:[{source_price_min}..]")
    if condition:
        # Browse aceita 'conditions:{NEW|USED|REFURBISHED}'
        parts.append(f"conditions:{{{condition}}}")
    return ",".join(parts)

def _normalize_summary(s: Dict[str, any]) -> Dict[str, any]:
    price = s.get("price") or {}
    seller = s.get("seller") or {}
    currency = price.get("currency") or "USD"
    try:
        price_val = float(price.get("value")) if price.get("value") is not None else None
    except Exception:
        price_val = None

    item = {
        "item_id": s.get("itemId"),
        "title": s.get("title"),
        "price": price_val,
        "currency": currency,
        "condition": s.get("condition"),
        "seller": seller.get("username"),
        "category_id": int(s.get("categoryId")) if s.get("categoryId") else None,
        "item_url": s.get("itemWebUrl"),
        "available_qty": None,  # pode vir via estimatedAvailabilities, ou só no detalhe
        "qty_flag": "EXACT",
        "brand": s.get("brand"),
        "mpn": s.get("mpn"),
        "gtin": s.get("gtin"),
    }

    est = s.get("estimatedAvailabilities") or []
    if isinstance(est, list) and est:
        q = est[0].get("estimatedAvailableQuantity")
        if isinstance(q, int):
            item["available_qty"] = q
            item["qty_flag"] = "EXACT"

    return item

# ────────────────────────────────────────────────────────────────────────────────
# API pública usada pelo projeto (assinaturas compatíveis)
# ────────────────────────────────────────────────────────────────────────────────
def search_by_category(
    category_id: int,
    source_price_min: float = 15.0,
    condition: str = "NEW",
    limit_per_page: int = 50,
    max_pages: int = 2,
) -> List[dict]:
    """
    Consulta a Browse API por category_id, aplicando filtros de preço (mínimo) e condição.
    Paginado. Retorna lista de itens normalized (dicts) compatível com o app.
    """
    headers = _auth_headers()
    params_base = {
        "category_ids": str(category_id),
        "limit": str(max(1, min(200, int(limit_per_page)))),
        "filter": _build_filter(source_price_min, condition),
        # EXTENDED traz mais campos (quando disponíveis) sem “refinements”
        "fieldgroups": "EXTENDED",
        "sort": "price",  # crescente
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

        # micro pausa para respeitar rate limits
        time.sleep(0.08)

    return items


def get_item_detail(item_id: str) -> dict:
    """
    Busca detalhe de um item específico (Browse /item/{item_id}).
    Tenta 'PRODUCT,ADDITIONAL_SELLER_DETAILS' e faz fallback sem fieldgroups.
    """
    headers = _auth_headers()
    url = f"{BASE}/item/{item_id}"

    def _do(fieldgroups: Optional[str]):
        params = {}
        if fieldgroups:
            params["fieldgroups"] = fieldgroups
        return _session.get(url, headers=headers, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

    # 1ª tentativa: com fieldgroups úteis
    r = _do("PRODUCT,ADDITIONAL_SELLER_DETAILS")
    if r.status_code == 400:
        # fallback sem fieldgroups (alguns itens dão 400 com combos específicos)
        r = _do(None)

    if r.status_code != 200:
        raise EbayRequestError(f"Erro item detail {item_id}: {r.status_code} {r.text}")

    d = r.json() or {}
    out = {
        "item_id": d.get("itemId"),
        "available_qty": None,
        "qty_flag": "EXACT",
        "brand": d.get("brand"),
        "mpn": d.get("mpn"),
        "gtin": None,
        "category_id": int(d.get("categoryId")) if d.get("categoryId") else None,
    }

    # estimatedAvailabilities
    est = d.get("estimatedAvailabilities") or []
    if isinstance(est, list) and est:
        q = est[0].get("estimatedAvailableQuantity")
        if isinstance(q, int):
            out["available_qty"] = q
            out["qty_flag"] = "EXACT"

    # product.gtin (lista) + aspects (Brand/MPN)
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

import base64
import os
import time
from typing import Dict, List, Tuple

import httpx

EBAY_ENV = os.getenv("EBAY_ENV", "production").lower()
EBAY_CLIENT_ID = os.getenv("EBAY_CLIENT_ID", "")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET", "")

# Endpoints (production) São públicos.
IDENTITY_URL = "https://api.ebay.com/identity/v1/oauth2/token"
BROWSE_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
MARKETPLACE_ID = "EBAY_US"  # .com (Estados Unidos)

# Cache simples do token em memória
_token_cache: Dict[str, Tuple[str, float]] = {}  # {"app": (access_token, expires_at_epoch)}


class EbayAuthError(Exception):
    pass


class EbayRequestError(Exception):
    pass


def _basic_auth_header() -> str:
    if not EBAY_CLIENT_ID or not EBAY_CLIENT_SECRET:
        raise EbayAuthError("EBAY_CLIENT_ID/EBAY_CLIENT_SECRET ausentes no .env")
    raw = f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def get_app_token() -> str:
    """
    Obtém um access_token via Client Credentials.
    Escopo mínimo para Browse API pública: api_scope.
    Cacheia até expirar.
    """
    now = time.time()
    cached = _token_cache.get("app")
    if cached and cached[1] - 60 > now:  # margem de 60s
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


def _build_filter(source_price_min: float, condition: str) -> str:
    # price:[MIN..]  e  conditions:{NEW}
    parts = []
    if source_price_min is not None:
        parts.append(f"price:[{source_price_min}..]")
    if condition:
        parts.append(f"conditions:{{{condition}}}")
    return ",".join(parts)


def search_by_category(
    category_id: int,
    source_price_min: float = 15.0,
    condition: str = "NEW",
    limit_per_page: int = 50,
    max_pages: int = 2,
) -> List[dict]:
    """
    Consulta a Browse API por category_id, aplicando filtros de preço e condição.
    Pagina até max_pages. Retorna uma lista de itens "achatados" (dicts).
    Obs.: o campo de quantidade pode não estar presente em todos os anúncios.
    """
    token = get_app_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
    }

    params_base = {
        "category_ids": str(category_id),
        "limit": str(limit_per_page),
        "filter": _build_filter(source_price_min, condition),
        "fieldgroups": "EXTENDED",  # pede campos extras (estimatedAvailabilities, brand, gtin, etc.)
    }

    items: List[dict] = []
    offset = 0
    for _ in range(max_pages):
        params = dict(params_base)
        params["offset"] = str(offset)
        with httpx.Client(timeout=40) as client:
            resp = client.get(BROWSE_SEARCH_URL, headers=headers, params=params)
        if resp.status_code != 200:
            raise EbayRequestError(f"Erro Browse API: {resp.status_code} {resp.text}")

        data = resp.json()
        summaries = data.get("itemSummaries", []) or []

        for s in summaries:
            item = {
                "item_id": s.get("itemId"),
                "title": s.get("title"),
                "price": float(s.get("price", {}).get("value")) if s.get("price") else None,
                "currency": s.get("price", {}).get("currency") if s.get("price") else None,
                "condition": s.get("condition"),
                "seller": (s.get("seller", {}) or {}).get("username"),
                "category_id": int(s.get("categoryId")) if s.get("categoryId") else None,
                "item_url": s.get("itemWebUrl"),
                "available_qty": None,
                "qty_flag": "EXACT",
                "brand": None,
                "mpn": None,
                "gtin": None,
            }

            # disponibilidade estimada
            est = s.get("estimatedAvailabilities") or []
            if isinstance(est, list) and est:
                q = est[0].get("estimatedAvailableQuantity")
                if isinstance(q, int):
                    item["available_qty"] = q
                    item["qty_flag"] = "EXACT"

            if "brand" in s:
                item["brand"] = s.get("brand")
            if "mpn" in s:
                item["mpn"] = s.get("mpn")
            if "gtin" in s:
                item["gtin"] = s.get("gtin")

            items.append(item)

        total = int(data.get("total", 0))
        offset += limit_per_page
        if offset >= total or not summaries:
            break

    return items


def get_item_detail(item_id: str) -> dict:
    """
    Busca detalhe de um item específico.
    """
    token = get_app_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
    }
    url = BROWSE_SEARCH_URL.replace("item_summary/search", f"item/{item_id}")

    # No detalhe, os fieldgroups válidos são:
    # COMPACT, PRODUCT, ADDITIONAL_SELLER_DETAILS, CHARITY_DETAILS
    def _do_req(fieldgroups: str | None):
        params = {}
        if fieldgroups:
            params["fieldgroups"] = fieldgroups
        with httpx.Client(timeout=40) as client:
            return client.get(url, headers=headers, params=params)

    # 1ª tentativa: PRODUCT + ADDITIONAL_SELLER_DETAILS (tende a trazer estoque e atributos)
    resp = _do_req("PRODUCT,ADDITIONAL_SELLER_DETAILS")
    if resp.status_code == 400:
        # Fallback: tenta sem fieldgroups
        resp = _do_req(None)

    # 404: item removido / não encontrado
    if resp.status_code == 404:
        return {
            "item_id": item_id,
            "available_qty": None,
            "qty_flag": "NOT_FOUND",
            "brand": None,
            "mpn": None,
            "gtin": None,
            "category_id": None,
        }

    # 429: estourou limite de requisições para este recurso
    if resp.status_code == 429:
        # Podemos só marcar e seguir; não vale derrubar a mineração inteira.
        return {
            "item_id": item_id,
            "available_qty": None,
            "qty_flag": "RATE_LIMIT",
            "brand": None,
            "mpn": None,
            "gtin": None,
            "category_id": None,
        }

    if resp.status_code != 200:
        raise EbayRequestError(f"Erro item detail {item_id}: {resp.status_code} {resp.text}")

    d = resp.json() or {}
    out = {
        "item_id": d.get("itemId"),
        "available_qty": None,
        "qty_flag": "EXACT",
        "brand": d.get("brand"),
        "mpn": d.get("mpn"),
        "gtin": None,
        "category_id": int(d.get("categoryId")) if d.get("categoryId") else None,
    }

    # estimatedAvailabilities (se a API expuser)
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
        if not out["brand"]:
            out["brand"] = (aspects.get("Brand") or [None])[0]
        if not out["mpn"]:
            out["mpn"] = (aspects.get("MPN") or aspects.get("Manufacturer Part Number") or [None])[0]

    return out

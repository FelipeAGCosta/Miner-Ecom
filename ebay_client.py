import os
import time
from typing import Dict, List, Tuple, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lib.ebay_auth import get_app_token

# ────────────────────────────────────────────────────────────────────────────────
# Configurações e Constantes
# ────────────────────────────────────────────────────────────────────────────────
BASE = "https://api.ebay.com/buy/browse/v1"
SITE_ID = os.getenv("EBAY_BROWSE_SITE_ID", "0")  # Site ID (0 = US) compatível com .env
MARKETPLACE_ID = os.getenv("EBAY_MARKETPLACE_ID", "EBAY_US")

# Configuração de Timeouts (ajustáveis via .env)
CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", 5))
READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", 30))

# Configuração de Retry/Backoff para chamadas HTTP (para lidar com 429/5xx)
_retry = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=0.5,  # Atraso progressivo (0.5, 1, 2, 4, 8)
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)

_session = requests.Session()
_session.mount("https://", HTTPAdapter(max_retries=_retry))
_session.mount("http://", HTTPAdapter(max_retries=_retry))

# ────────────────────────────────────────────────────────────────────────────────
# Exceções Personalizadas
# ────────────────────────────────────────────────────────────────────────────────
class EbayAuthError(Exception):
    """Erro relacionado à autenticação com o eBay."""
    pass

class EbayRequestError(Exception):
    """Erro genérico nas requisições para a API do eBay."""
    pass

# ────────────────────────────────────────────────────────────────────────────────
# Helpers (Funções auxiliares)
# ────────────────────────────────────────────────────────────────────────────────

def _auth_headers() -> Dict[str, str]:
    """
    Retorna os cabeçalhos de autenticação necessários para as chamadas à API do eBay.
    """
    token = get_app_token()  # Obtém o token de acesso do eBay via cache/redis + retry da lib lib.ebay_auth
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
        "X-EBAY-C-ENDUSERCTX": f"contextualLocation=country=US,zip=00000;siteid={SITE_ID}",
    }

def _build_filter(source_price_min: Optional[float], condition: Optional[str]) -> str:
    """
    Monta a string para o filtro de preço e condição a ser passado à Browse API.
    Exemplo: 'price:[15..],conditions:{NEW}'
    """
    parts = []
    if source_price_min is not None:
        parts.append(f"price:[{source_price_min}..]")
    if condition:
        parts.append(f"conditions:{{{condition}}}")
    return ",".join(parts)

def _normalize_summary(s: Dict[str, any]) -> Dict[str, any]:
    """
    Normaliza o resumo do item retornado pela Browse API, extraindo os campos necessários.
    """
    price = s.get("price", {})
    seller = s.get("seller", {})
    currency = price.get("currency", "USD")
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
        "available_qty": None,  # Pode vir via estimatedAvailabilities ou apenas no detalhe
        "qty_flag": "EXACT",
        "brand": s.get("brand"),
        "mpn": s.get("mpn"),
        "gtin": s.get("gtin"),
    }

    # Verificando a disponibilidade estimada
    est = s.get("estimatedAvailabilities", [])
    if isinstance(est, list) and est:
        q = est[0].get("estimatedAvailableQuantity")
        if isinstance(q, int):
            item["available_qty"] = q
            item["qty_flag"] = "EXACT"

    return item

# ────────────────────────────────────────────────────────────────────────────────
# Funções Públicas: Consultas à Browse API do eBay
# ────────────────────────────────────────────────────────────────────────────────

def search_by_category(
    category_id: int,
    source_price_min: float = 15.0,
    condition: str = "NEW",
    limit_per_page: int = 50,
    max_pages: int = 2,
) -> List[dict]:
    """
    Consulta a Browse API do eBay por category_id, aplicando filtros de preço e condição.
    Retorna uma lista de itens com as informações necessárias para o aplicativo.
    """
    headers = _auth_headers()
    params_base = {
        "category_ids": str(category_id),
        "limit": str(max(1, min(200, int(limit_per_page)))),
        "filter": _build_filter(source_price_min, condition),
        "fieldgroups": "EXTENDED",  # Solicita campos extras (quando disponíveis)
        "sort": "price",  # Ordenação crescente por preço
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

        # Respeita limites de requisição com micro pausa
        time.sleep(0.08)

    return items

def get_item_detail(item_id: str) -> dict:
    """
    Busca detalhes de um item específico através da Browse API do eBay.
    Tenta 'PRODUCT,ADDITIONAL_SELLER_DETAILS' e faz fallback sem fieldgroups, se necessário.
    """
    headers = _auth_headers()
    url = f"{BASE}/item/{item_id}"

    def _do(fieldgroups: Optional[str]):
        params = {}
        if fieldgroups:
            params["fieldgroups"] = fieldgroups
        return _session.get(url, headers=headers, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

    # Tentativa com 'PRODUCT,ADDITIONAL_SELLER_DETAILS'
    r = _do("PRODUCT,ADDITIONAL_SELLER_DETAILS")
    if r.status_code == 400:
        # Fallback sem fieldgroups (alguns itens podem causar erro com combos específicos)
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

    # Verificando 'estimatedAvailabilities' (se exposto pela API)
    est = d.get("estimatedAvailabilities", [])
    if isinstance(est, list) and est:
        q = est[0].get("estimatedAvailableQuantity")
        if isinstance(q, int):
            out["available_qty"] = q
            out["qty_flag"] = "EXACT"

    # Produto GTIN (caso presente) e aspectos (Brand, MPN)
    prod = d.get("product", {})
    if isinstance(prod, dict):
        gtins = prod.get("gtin")
        if isinstance(gtins, list) and gtins:
            out["gtin"] = gtins[0]
        aspects = prod.get("aspects", {})
        if not out["brand"]:
            out["brand"] = (aspects.get("Brand") or [None])[0]
        if not out["mpn"]:
            out["mpn"] = (aspects.get("MPN") or aspects.get("Manufacturer Part Number") or [None])[0]

    return out

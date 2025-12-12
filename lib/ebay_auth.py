# lib/ebay_auth.py
import base64
import os
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lib.redis_cache import cache_get, cache_set

# Namespace fixo para o token de app
_NS = "ebay_app_token"
_SCOPE = "https://api.ebay.com/oauth/api_scope"
_TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"

EBAY_CLIENT_ID     = os.getenv("EBAY_CLIENT_ID", "")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET", "")

# Timeouts ajustáveis por .env (opcional)
CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", 5))
READ_TIMEOUT    = float(os.getenv("HTTP_READ_TIMEOUT", 30))

# Sessão HTTP com retry/backoff (robusta contra 429/5xx/transientes)
_retry = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=0.5,  # 0.5, 1, 2, 4, 8s
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["POST"],
    raise_on_status=False,
)
_session = requests.Session()
_session.mount("https://", HTTPAdapter(max_retries=_retry))
_session.mount("http://",  HTTPAdapter(max_retries=_retry))

def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")

def _request_new_token() -> str:
    if not EBAY_CLIENT_ID or not EBAY_CLIENT_SECRET:
        raise RuntimeError("EBAY_CLIENT_ID/EBAY_CLIENT_SECRET ausentes no .env.")

    headers = {
        "Authorization": _basic_auth_header(EBAY_CLIENT_ID, EBAY_CLIENT_SECRET),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "scope": _SCOPE,
    }
    resp = _session.post(
        _TOKEN_URL,
        headers=headers,
        data=data,
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Falha ao obter token do eBay: {resp.status_code} {resp.text}")

    js = resp.json() or {}
    access_token: Optional[str] = js.get("access_token")
    expires_in: int = int(js.get("expires_in", 7200))
    if not access_token:
        raise RuntimeError(f"Resposta de token sem access_token: {js}")

    # TTL com margem de 60s
    ttl = max(60, expires_in - 60)
    cache_set(_NS, {"scope": _SCOPE}, access_token, ttl_sec=ttl)
    return access_token

def get_app_token() -> str:
    """
    Obtém token de aplicação (client_credentials) com cache no Redis.
    Se o Redis estiver indisponível, faz fallback para obter diretamente o token.
    """
    # 1) Tenta cache
    try:
        tok = cache_get(_NS, {"scope": _SCOPE})
        if tok:
            return tok
    except Exception:
        # Falha no Redis não deve derrubar o app
        pass

    # 2) Solicita novo token e grava em cache (se possível)
    return _request_new_token()

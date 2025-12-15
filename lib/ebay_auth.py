"""
lib.ebay_auth

Responsável por obter e cachear (via Redis) o access token de aplicação
(Client Credentials) da eBay (Browse API / demais APIs públicas).

- Usa EBAY_CLIENT_ID / EBAY_CLIENT_SECRET do .env.
- Faz retry/backoff em falhas 429/5xx.
- Usa Redis como cache principal; em caso de falha no Redis, faz fallback
  para solicitar o token diretamente.
"""

from __future__ import annotations

import base64
import os
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lib.redis_cache import cache_get, cache_set

# ---------------------------------------------------------------------------
# Constantes / configuração
# ---------------------------------------------------------------------------

# Namespace fixo para o token de app no Redis
_NS = "ebay_app_token"
_SCOPE = "https://api.ebay.com/oauth/api_scope"
_TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"

EBAY_CLIENT_ID = os.getenv("EBAY_CLIENT_ID", "")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET", "")

# Timeouts ajustáveis via .env (valores padrão seguros)
CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", 5))
READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", 30))

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
_session.mount("http://", HTTPAdapter(max_retries=_retry))


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------


def _basic_auth_header(client_id: str, client_secret: str) -> str:
    """
    Monta o header HTTP Basic Auth a partir de client_id/client_secret.
    """
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _request_new_token() -> str:
    """
    Solicita um novo access_token de aplicação na eBay via Client Credentials.

    Lança RuntimeError em caso de:
      - credenciais ausentes no .env
      - resposta inválida ou sem access_token
    """
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
        raise RuntimeError(
            f"Falha ao obter token do eBay: {resp.status_code} {resp.text}"
        )

    js = resp.json() or {}
    access_token: Optional[str] = js.get("access_token")
    expires_in: int = int(js.get("expires_in", 7200))

    if not access_token:
        raise RuntimeError(f"Resposta de token sem access_token: {js}")

    # TTL com margem de 60s para evitar usar token na beira da expiração
    ttl = max(60, expires_in - 60)
    cache_set(_NS, {"scope": _SCOPE}, access_token, ttl_sec=ttl)

    return access_token


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------


def get_app_token() -> str:
    """
    Retorna um access token de aplicação (Client Credentials) para a eBay.

    Fluxo:
      1) Tenta obter do cache Redis (cache_get).
      2) Em caso de falha no Redis ou cache vazio, solicita um novo token
         (_request_new_token) e, se possível, grava de volta no Redis.

    Exceções:
      - RuntimeError em falhas ao solicitar um novo token à eBay.
    """
    # 1) Tenta cache (falhas de Redis não devem derrubar o app)
    try:
        tok = cache_get(_NS, {"scope": _SCOPE})
        if tok:
            return tok
    except Exception:
        # Falha no Redis → segue para obter novo token
        pass

    # 2) Solicita novo token e grava em cache (caso possível)
    return _request_new_token()

import os
import time
import hmac
import json
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import quote

import requests

from pathlib import Path
from dotenv import load_dotenv

# Carrega o .env da raiz do projeto
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)

class SellingPartnerAPIError(Exception):
    """Erro genérico da Selling Partner API."""


class SellingPartnerAuthError(SellingPartnerAPIError):
    """Erro ao obter token de acesso da LWA."""


@dataclass
class SPAPIConfig:
    lwa_client_id: str
    lwa_client_secret: str
    refresh_token: str

    aws_access_key: str
    aws_secret_key: str

    region: str          # "na", "eu", "fe"
    marketplace_id: str  # e.g. "ATVPDKIKX0DER"
    seller_id: str       # ex: "AQIH56OJ0KSYJ"

    @property
    def endpoint_host(self) -> str:
        return f"sellingpartnerapi-{self.region}.amazon.com"

    @property
    def aws_region(self) -> str:
        if self.region == "na":
            return "us-east-1"
        if self.region == "eu":
            return "eu-west-1"
        if self.region == "fe":
            return "us-west-2"
        return "us-east-1"


def _load_config_from_env() -> SPAPIConfig:
    missing = []

    def getenv(name: str, default: Optional[str] = None) -> Optional[str]:
        value = os.getenv(name, default)
        if value is None or value == "":
            missing.append(name)
        return value

    cfg = SPAPIConfig(
        lwa_client_id=getenv("SPAPI_CLIENT_ID") or "",
        lwa_client_secret=getenv("SPAPI_CLIENT_SECRET") or "",
        refresh_token=getenv("SPAPI_REFRESH_TOKEN") or "",
        aws_access_key=getenv("SPAPI_AWS_ACCESS_KEY_ID") or "",
        aws_secret_key=getenv("SPAPI_AWS_SECRET_ACCESS_KEY") or "",
        region=getenv("SPAPI_REGION", "na") or "na",
        marketplace_id=getenv("SPAPI_MARKETPLACE_ID", "ATVPDKIKX0DER") or "ATVPDKIKX0DER",
        seller_id=getenv("SPAPI_SELLER_ID") or "",
    )

    if missing:
        raise RuntimeError(f"Variáveis SP-API ausentes no .env: {', '.join(missing)}")

    return cfg


_access_token_cache: Dict[str, Any] = {
    "token": None,
    "expires_at": 0.0,
}


def _get_lwa_access_token(cfg: SPAPIConfig) -> str:
    now = time.time()
    if _access_token_cache["token"] and now < _access_token_cache["expires_at"]:
        return _access_token_cache["token"]

    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": cfg.refresh_token,
        "client_id": cfg.lwa_client_id,
        "client_secret": cfg.lwa_client_secret,
    }

    resp = requests.post(url, data=data, timeout=15)
    if resp.status_code != 200:
        raise SellingPartnerAuthError(
            f"Falha ao obter LWA access token ({resp.status_code}): {resp.text}"
        )

    payload = resp.json()
    access_token = payload.get("access_token")
    expires_in = payload.get("expires_in", 3600)

    if not access_token:
        raise SellingPartnerAuthError(f"Resposta LWA sem access_token: {payload}")

    _access_token_cache["token"] = access_token
    _access_token_cache["expires_at"] = now + int(expires_in) - 60

    return access_token


def _sign_sp_api_request(
    cfg: SPAPIConfig,
    method: str,
    path: str,
    query_params: Optional[Dict[str, Any]],
    body: Optional[str],
    access_token: str,
) -> Dict[str, str]:
    service = "execute-api"
    aws_region = cfg.aws_region
    host = cfg.endpoint_host

    if not path.startswith("/"):
        path = "/" + path

    if query_params:
        qp_items = []
        for key in sorted(query_params.keys()):
            value = query_params[key]
            if value is None:
                continue
            qp_items.append(
                f"{quote(str(key), safe='-_.~')}={quote(str(value), safe='-_.~')}"
            )
        canonical_querystring = "&".join(qp_items)
    else:
        canonical_querystring = ""

    if body is None:
        body = ""
    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()

    t = datetime.now(timezone.utc)
    amzdate = t.strftime("%Y%m%dT%H%M%SZ")
    datestamp = t.strftime("%Y%m%d")

    canonical_headers = (
        f"host:{host}\n"
        f"x-amz-date:{amzdate}\n"
        f"x-amz-access-token:{access_token}\n"
    )
    signed_headers = "host;x-amz-date;x-amz-access-token"

    canonical_request = "\n".join(
        [
            method,
            path,
            canonical_querystring,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    canonical_request_hash = hashlib.sha256(
        canonical_request.encode("utf-8")
    ).hexdigest()

    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = f"{datestamp}/{aws_region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [algorithm, amzdate, credential_scope, canonical_request_hash]
    )

    def _sign(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    k_date = _sign(("AWS4" + cfg.aws_secret_key).encode("utf-8"), datestamp)
    k_region = _sign(k_date, aws_region)
    k_service = _sign(k_region, service)
    k_signing = _sign(k_service, "aws4_request")
    signature = hmac.new(
        k_signing, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    authorization_header = (
        f"{algorithm} "
        f"Credential={cfg.aws_access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, "
        f"Signature={signature}"
    )

    headers = {
        "host": host,
        "x-amz-date": amzdate,
        "x-amz-access-token": access_token,
        "Authorization": authorization_header,
        "content-type": "application/json",
    }
    return headers


def _request_sp_api(
    cfg: SPAPIConfig,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    access_token = _get_lwa_access_token(cfg)

    body_str = json.dumps(json_body) if json_body is not None else ""
    headers = _sign_sp_api_request(
        cfg=cfg,
        method=method,
        path=path,
        query_params=params,
        body=body_str,
        access_token=access_token,
    )

    base_url = f"https://{cfg.endpoint_host}"
    url = base_url + path

    resp = requests.request(
        method=method,
        url=url,
        params=params,
        data=body_str if body_str else None,
        headers=headers,
        timeout=timeout,
    )

    if resp.status_code >= 400:
        raise SellingPartnerAPIError(
            f"Erro SP-API {resp.status_code} para {path}: {resp.text}"
        )

    if not resp.text:
        return {}

    try:
        return resp.json()
    except json.JSONDecodeError:
        raise SellingPartnerAPIError(
            f"Resposta SP-API não é JSON para {path}: {resp.text[:200]}"
        )


# ---------------- Funções de alto nível -----------------


def get_catalog_item(asin: str) -> Dict[str, Any]:
    cfg = _load_config_from_env()
    path = f"/catalog/2022-04-01/items/{asin}"
    params = {
        "marketplaceIds": cfg.marketplace_id,
    }

    return _request_sp_api(
        cfg=cfg,
        method="GET",
        path=path,
        params=params,
    )


def debug_ping() -> Dict[str, Any]:
    cfg = _load_config_from_env()
    path = "/sellers/v1/marketplaceParticipations"

    return _request_sp_api(
        cfg=cfg,
        method="GET",
        path=path,
        params=None,
    )


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "catalog" and len(sys.argv) == 3:
        asin_arg = sys.argv[2]
        print(f"Buscando catálogo para ASIN={asin_arg} ...")
        data = get_catalog_item(asin_arg)
        print(json.dumps(data, indent=2))
    else:
        print("Testando conexão com SP-API (sellers/v1/marketplaceParticipations)...")
        data = debug_ping()
        print(json.dumps(data, indent=2))

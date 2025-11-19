import os
import time
import hmac
import json
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List
from urllib.parse import quote
from pathlib import Path

import requests
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Carregar .env da raiz do projeto
# ---------------------------------------------------------------------------

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)


# ---------------------------------------------------------------------------
# Exceções e configuração
# ---------------------------------------------------------------------------


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
        # Endpoints oficiais por região
        # na -> North America, eu -> Europe, fe -> Far East
        return f"sellingpartnerapi-{self.region}.amazon.com"

    @property
    def aws_region(self) -> str:
        # Mapeia a região lógica da SP-API para a região AWS usada na assinatura
        if self.region == "na":
            return "us-east-1"
        if self.region == "eu":
            return "eu-west-1"
        if self.region == "fe":
            return "us-west-2"
        # fallback defensivo
        return "us-east-1"


def _load_config_from_env() -> SPAPIConfig:
    """Carrega configuração da SP-API a partir das variáveis de ambiente (.env)."""
    missing: List[str] = []

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


# ---------------------------------------------------------------------------
# LWA access token (Login With Amazon)
# ---------------------------------------------------------------------------

_access_token_cache: Dict[str, Any] = {
    "token": None,
    "expires_at": 0.0,
}


def _get_lwa_access_token(cfg: SPAPIConfig) -> str:
    """
    Obtém (ou reutiliza, se ainda válido) um access token da Login With Amazon (LWA),
    usando o refresh_token. O token costuma valer ~3600s.
    """
    now = time.time()
    if _access_token_cache["token"] and now < _access_token_cache["expires_at"]:
        return _access_token_cache["token"]  # reaproveita até perto do vencimento

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

    # Guarda com margem de segurança de 60s
    _access_token_cache["token"] = access_token
    _access_token_cache["expires_at"] = now + int(expires_in) - 60

    return access_token


# ---------------------------------------------------------------------------
# Assinatura AWS SigV4 e chamada genérica
# ---------------------------------------------------------------------------

def _sign_sp_api_request(
    cfg: SPAPIConfig,
    method: str,
    path: str,
    query_params: Optional[Dict[str, Any]],
    body: Optional[str],
    access_token: str,
) -> Dict[str, str]:
    """
    Monta cabeçalhos de assinatura AWS Signature V4 para a SP-API.
    Service: execute-api
    """
    service = "execute-api"
    aws_region = cfg.aws_region
    host = cfg.endpoint_host

    if not path.startswith("/"):
        path = "/" + path

    # Query string canonicalizada
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

    # Corpo
    if body is None:
        body = ""
    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()

    # Datas
    t = datetime.now(timezone.utc)
    amzdate = t.strftime("%Y%m%dT%H%M%SZ")
    datestamp = t.strftime("%Y%m%d")

    # Cabeçalhos canônicos
    canonical_headers = (
        f"host:{host}\n"
        f"x-amz-date:{amzdate}\n"
        f"x-amz-access-token:{access_token}\n"
    )
    signed_headers = "host;x-amz-date;x-amz-access-token"

    # Canonical request
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

    # Chave de assinatura
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
    """
    Faz uma chamada genérica à SP-API (já autenticando e assinando).
    Retorna o JSON da resposta ou lança SellingPartnerAPIError.
    """
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


# ---------------------------------------------------------------------------
# Funções de alto nível - Catalog
# ---------------------------------------------------------------------------

def get_catalog_item(asin: str) -> Dict[str, Any]:
    """
    Consulta de catálogo para um ASIN, usando a versão 2022-04-01
    do Catalog Items API, já trazendo identifiers e salesRanks.
    """
    cfg = _load_config_from_env()
    path = f"/catalog/2022-04-01/items/{asin}"
    params = {
        "marketplaceIds": cfg.marketplace_id,
        "includedData": "summaries,identifiers,salesRanks",
    }

    return _request_sp_api(
        cfg=cfg,
        method="GET",
        path=path,
        params=params,
    )


def debug_ping() -> Dict[str, Any]:
    """
    Chamada simples à SP-API para teste.
    Usa /sellers/v1/marketplaceParticipations para verificar se a conta
    está correta e se o seller participa do marketplace.
    """
    cfg = _load_config_from_env()
    path = "/sellers/v1/marketplaceParticipations"

    return _request_sp_api(
        cfg=cfg,
        method="GET",
        path=path,
        params=None,
    )


def search_by_gtin(gtin: str) -> Optional[Dict[str, Any]]:
    """
    Busca item de catálogo por GTIN (UPC/EAN/ISBN) usando
    /catalog/2022-04-01/items.

    Estratégia:
    - Normaliza o GTIN (strip).
    - Decide tipos mais prováveis com base no tamanho (UPC/EAN/ISBN).
    - Tenta em ordem: tipos específicos + GTIN.
    - Se nenhuma chamada retornar items, devolve None.
    """
    cfg = _load_config_from_env()
    gtin_clean = gtin.strip()

    # Decide candidatos pelo tamanho
    length = len(gtin_clean)
    candidates: List[str] = []

    if length == 12:
        candidates = ["UPC", "GTIN"]
    elif length == 13:
        candidates = ["EAN", "GTIN"]
    elif length in (10, 13):
        # ISBN-10 / ISBN-13
        candidates = ["ISBN", "GTIN"]
    else:
        # fallback bem genérico, tenta de tudo
        candidates = ["GTIN", "UPC", "EAN", "ISBN"]

    last_data: Optional[Dict[str, Any]] = None
    item: Optional[Dict[str, Any]] = None

    for ident_type in candidates:
        path = "/catalog/2022-04-01/items"
        params = {
            "marketplaceIds": cfg.marketplace_id,
            "identifiers": gtin_clean,
            "identifiersType": ident_type,
            "includedData": "summaries,identifiers,salesRanks",
        }

        try:
            data = _request_sp_api(
                cfg=cfg,
                method="GET",
                path=path,
                params=params,
            )
        except SellingPartnerAPIError as e:
            msg = str(e)
            if "404" in msg or "NOT_FOUND" in msg:
                continue
            raise

        last_data = data
        items = data.get("items") or []
        if items:
            item = items[0]
            break

    if not item:
        return None

    asin = item.get("asin")

    summaries = item.get("summaries") or []
    summary = None
    for s in summaries:
        if s.get("marketplaceId") == cfg.marketplace_id:
            summary = s
            break
    if summary is None and summaries:
        summary = summaries[0]

    title = summary.get("itemName") if summary else None
    brand = summary.get("brand") if summary else None

    browse_node_id = None
    browse_node_name = None
    if summary and summary.get("browseClassification"):
        bc = summary["browseClassification"]
        browse_node_id = bc.get("classificationId")
        browse_node_name = bc.get("displayName")

    gtin_value = None
    gtin_type = None
    identifiers = item.get("identifiers") or []
    chosen_identifier = None

    for ident in identifiers:
        if ident.get("marketplaceId") == cfg.marketplace_id:
            ids_list = ident.get("identifiers") or []
            if ids_list:
                chosen_identifier = ids_list[0]
                break

    if chosen_identifier is None and identifiers:
        ids_list = identifiers[0].get("identifiers") or []
        if ids_list:
            chosen_identifier = ids_list[0]

    if chosen_identifier:
        gtin_value = chosen_identifier.get("identifier")
        gtin_type = chosen_identifier.get("identifierType")

    sales_rank = None
    sales_rank_category = None
    sales = item.get("salesRanks") or []
    best_rank = None
    best_title = None

    for sr in sales:
        if sr.get("marketplaceId") != cfg.marketplace_id:
            continue
        classification_ranks = sr.get("classificationRanks") or []
        for cr in classification_ranks:
            rank_val = cr.get("rank")
            title_val = cr.get("title")
            if isinstance(rank_val, int):
                if best_rank is None or rank_val < best_rank:
                    best_rank = rank_val
                    best_title = title_val

    if best_rank is not None:
        sales_rank = best_rank
        sales_rank_category = best_title

    return {
        "asin": asin,
        "marketplace_id": cfg.marketplace_id,
        "title": title,
        "brand": brand,
        "browse_node_id": browse_node_id,
        "browse_node_name": browse_node_name,
        "gtin": gtin_value or gtin_clean,
        "gtin_type": gtin_type,
        "sales_rank": sales_rank,
        "sales_rank_category": sales_rank_category,
    }


# ---------------------------------------------------------------------------
# Product Pricing - BuyBox / Lowest price
# ---------------------------------------------------------------------------

def get_buybox_price(asin: str, item_condition: str = "New") -> Optional[Dict[str, Any]]:
    """
    Usa /products/pricing/v0/items/{asin}/offers para tentar obter:
    - preço da BuyBox (se existir)
    - fallback para LowestPrices se não houver BuyBox

    Retorna dict com:
    {
        "asin": ...,
        "price": float,
        "currency": str,
        "is_prime": bool | None,
        "fulfillment_channel": str | None,  # AMAZON / MERCHANT / etc.
    }
    ou None se não conseguir extrair um preço.
    """
    cfg = _load_config_from_env()
    path = f"/products/pricing/v0/items/{asin}/offers"
    params = {
        "MarketplaceId": cfg.marketplace_id,
        "ItemCondition": item_condition,
        "CustomerType": "Consumer",
    }

    data = _request_sp_api(
        cfg=cfg,
        method="GET",
        path=path,
        params=params,
    )

    # Muitos endpoints da SP-API usam "payload" na raiz
    payload = data.get("payload", data)

    summary = payload.get("Summary") or {}
    buybox_prices = summary.get("BuyBoxPrices") or []
    currency = None
    is_prime = None
    fulfillment_channel = None
    price_amount = None

    if buybox_prices:
        bb0 = buybox_prices[0] or {}
        lp = bb0.get("ListingPrice") or {}
        price_amount = lp.get("Amount")
        currency = lp.get("CurrencyCode")
        is_prime = bb0.get("IsPrime")
        fulfillment_channel = bb0.get("FulfillmentChannel")
    else:
        lowest_prices = summary.get("LowestPrices") or []
        if lowest_prices:
            lp0 = lowest_prices[0] or {}
            lp = lp0.get("ListingPrice") or {}
            price_amount = lp.get("Amount")
            currency = lp.get("CurrencyCode")
            is_prime = lp0.get("IsPrime")
            fulfillment_channel = lp0.get("FulfillmentChannel")

    if price_amount is None:
        return None

    try:
        price_value = float(price_amount)
    except (TypeError, ValueError):
        return None

    return {
        "asin": asin,
        "price": price_value,
        "currency": currency,
        "is_prime": is_prime,
        "fulfillment_channel": fulfillment_channel,
    }


# ---------------------------------------------------------------------------
# CLI de teste
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "catalog" and len(sys.argv) == 3:
        asin_arg = sys.argv[2]
        print(f"Buscando catálogo para ASIN={asin_arg} ...")
        data = get_catalog_item(asin_arg)
        print(json.dumps(data, indent=2))
    elif len(sys.argv) > 1 and sys.argv[1] == "gtin" and len(sys.argv) == 3:
        gtin_arg = sys.argv[2]
        print(f"Buscando item por GTIN={gtin_arg} ...")
        data = search_by_gtin(gtin_arg)
        print(json.dumps(data, indent=2))
    elif len(sys.argv) > 1 and sys.argv[1] == "offers" and len(sys.argv) == 3:
        asin_arg = sys.argv[2]
        print(f"Buscando BuyBox/offers para ASIN={asin_arg} ...")
        data = get_buybox_price(asin_arg)
        print(json.dumps(data, indent=2))
    else:
        print("Testando conexão com SP-API (sellers/v1/marketplaceParticipations)...")
        data = debug_ping()
        print(json.dumps(data, indent=2))

import os
import time
import hmac
import json
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple
from urllib.parse import quote
from pathlib import Path

import requests
from dotenv import load_dotenv
from rapidfuzz import fuzz


# ---------------------------------------------------------------------------
# Carregar .env da raiz do projeto
# ---------------------------------------------------------------------------

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)


# ---------------------------------------------------------------------------
# Excecoes e configuracao
# ---------------------------------------------------------------------------


class SellingPartnerAPIError(Exception):
    """Erro generico da Selling Partner API."""


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
        # Endpoints oficiais por regiao
        return f"sellingpartnerapi-{self.region}.amazon.com"

    @property
    def aws_region(self) -> str:
        # Mapeia a regiao logica da SP-API para a regiao AWS usada na assinatura
        if self.region == "na":
            return "us-east-1"
        if self.region == "eu":
            return "eu-west-1"
        if self.region == "fe":
            return "us-west-2"
        return "us-east-1"


def _load_config_from_env() -> SPAPIConfig:
    """Carrega configuracao da SP-API a partir das variaveis de ambiente (.env)."""
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
        raise RuntimeError(f"Variaveis SP-API ausentes no .env: {', '.join(missing)}")

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
    Obtem (ou reutiliza, se ainda valido) um access token da Login With Amazon (LWA),
    usando o refresh_token. O token costuma valer ~3600s.
    """
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


# ---------------------------------------------------------------------------
# Assinatura AWS SigV4 e chamada generica
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
    Monta cabecalhos de assinatura AWS Signature V4 para a SP-API.
    Service: execute-api
    """
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
    """
    Faz uma chamada generica a SP-API (autenticando e assinando).
    Retorna o JSON da resposta ou lanca SellingPartnerAPIError.
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
            f"Resposta SP-API nao eh JSON para {path}: {resp.text[:200]}"
        )


# ---------------------------------------------------------------------------
# Catalog helpers
# ---------------------------------------------------------------------------

def _extract_catalog_item(item: Dict[str, Any], marketplace_id: str, fallback_gtin: Optional[str] = None) -> Dict[str, Any]:
    asin = item.get("asin")

    summaries = item.get("summaries") or []
    summary = None
    for s in summaries:
        if s.get("marketplaceId") == marketplace_id:
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
        if ident.get("marketplaceId") == marketplace_id:
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
        if sr.get("marketplaceId") != marketplace_id:
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
        "marketplace_id": marketplace_id,
        "title": title,
        "brand": brand,
        "browse_node_id": browse_node_id,
        "browse_node_name": browse_node_name,
        "gtin": gtin_value or fallback_gtin,
        "gtin_type": gtin_type,
        "sales_rank": sales_rank,
        "sales_rank_category": sales_rank_category,
    }


def search_by_gtin(gtin: str) -> Optional[Dict[str, Any]]:
    """
    Busca item de catalogo por GTIN (UPC/EAN/ISBN) usando /catalog/2022-04-01/items.
    """
    cfg = _load_config_from_env()
    gtin_clean = gtin.strip()

    length = len(gtin_clean)
    if length == 12:
        candidates = ["UPC", "GTIN"]
    elif length == 13:
        candidates = ["EAN", "GTIN"]
    elif length in (10, 13):
        candidates = ["ISBN", "GTIN"]
    else:
        candidates = ["GTIN", "UPC", "EAN", "ISBN"]

    item: Optional[Dict[str, Any]] = None

    for ident_type in candidates:
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
                path="/catalog/2022-04-01/items",
                params=params,
            )
        except SellingPartnerAPIError as e:
            msg = str(e)
            if "404" in msg or "NOT_FOUND" in msg:
                continue
            raise

        items = data.get("items") or []
        if items:
            item = items[0]
            break

    if not item:
        return None

    return _extract_catalog_item(item, cfg.marketplace_id, fallback_gtin=gtin_clean)


def search_by_title(title: str, original_title: Optional[str] = None, page_size: int = 3) -> Optional[Dict[str, Any]]:
    """
    Fallback: busca item de catalogo por titulo/keywords.
    Retorna o item com melhor similaridade de titulo (se original_title fornecido).
    """
    cfg = _load_config_from_env()
    title_clean = (title or "").strip()
    if not title_clean:
        return None

    params = {
        "marketplaceIds": cfg.marketplace_id,
        "keywords": title_clean[:200],
        "includedData": "summaries,identifiers,salesRanks",
        "pageSize": max(1, min(page_size, 10)),
    }

    data = _request_sp_api(
        cfg=cfg,
        method="GET",
        path="/catalog/2022-04-01/items",
        params=params,
    )

    items = data.get("items") or []
    if not items:
        return None

    if original_title:
        # escolhe o item com maior similaridade de titulo
        scores: List[Tuple[int, Dict[str, Any]]] = []
        for it in items:
            summary = (it.get("summaries") or [{}])[0]
            cand_title = summary.get("itemName") or ""
            score = fuzz.token_sort_ratio(original_title.lower(), str(cand_title).lower())
            scores.append((score, it))
        best = max(scores, key=lambda x: x[0])[1] if scores else items[0]
        return _extract_catalog_item(best, cfg.marketplace_id)

    return _extract_catalog_item(items[0], cfg.marketplace_id)


# ---------------------------------------------------------------------------
# Funcoes de alto nivel - Catalog existentes
# ---------------------------------------------------------------------------

def get_catalog_item(asin: str) -> Dict[str, Any]:
    """
    Consulta de catalogo para um ASIN, usando a versao 2022-04-01
    do Catalog Items API, ja trazendo identifiers e salesRanks.
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
    Chamada simples a SP-API para teste.
    Usa /sellers/v1/marketplaceParticipations para verificar se a conta
    esta correta e se o seller participa do marketplace.
    """
    cfg = _load_config_from_env()
    path = "/sellers/v1/marketplaceParticipations"

    return _request_sp_api(
        cfg=cfg,
        method="GET",
        path=path,
        params=None,
    )


# ---------------------------------------------------------------------------
# Product Pricing - BuyBox / Lowest price
# ---------------------------------------------------------------------------

def get_buybox_price(asin: str, item_condition: str = "New") -> Optional[Dict[str, Any]]:
    """
    Usa /products/pricing/v0/items/{asin}/offers para tentar obter:
    - preco da BuyBox (se existir)
    - fallback para LowestPrices se nao houver BuyBox
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
        print(f"Buscando catalogo para ASIN={asin_arg} ...")
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
        print("Testando conexao com SP-API (sellers/v1/marketplaceParticipations)...")
        data = debug_ping()
        print(json.dumps(data, indent=2))

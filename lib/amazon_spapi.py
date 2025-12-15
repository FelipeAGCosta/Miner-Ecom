"""
Integração com a Amazon Selling Partner API (SP-API).

- Autenticação via LWA (Login With Amazon) + assinatura AWS SigV4.
- Endpoints usados:
    - Catalog Items 2022-04-01  (/catalog/2022-04-01/items)
    - Sellers                    (/sellers/v1/marketplaceParticipations)
    - Product Pricing            (/products/pricing/v0/items/{asin}/offers)

Exposto para o restante do projeto principalmente via:
    - search_by_gtin
    - search_by_title
    - search_catalog_items
    - get_catalog_item
    - get_buybox_price

Além de helpers internos reutilizados por outros módulos:
    - _extract_catalog_item
    - _load_config_from_env
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

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
# Exceções e configuração
# ---------------------------------------------------------------------------


class SellingPartnerAPIError(Exception):
    """Erro genérico da Selling Partner API."""
    pass


class SellingPartnerAuthError(SellingPartnerAPIError):
    """Erro ao obter token de acesso da LWA."""
    pass


@dataclass
class SPAPIConfig:
    lwa_client_id: str
    lwa_client_secret: str
    refresh_token: str

    aws_access_key: str
    aws_secret_key: str

    region: str          # "na", "eu", "fe"
    marketplace_id: str  # e.g. "ATVPDKIKX0DER"
    seller_id: Optional[str] = None  # não é obrigatório para os endpoints que usamos

    @property
    def endpoint_host(self) -> str:
        """Host da SP-API para a região lógica (na, eu, fe)."""
        return f"sellingpartnerapi-{self.region}.amazon.com"

    @property
    def aws_region(self) -> str:
        """Região AWS usada na assinatura SigV4 (mapeada a partir de `region`)."""
        if self.region == "na":
            return "us-east-1"
        if self.region == "eu":
            return "eu-west-1"
        if self.region == "fe":
            return "us-west-2"
        return "us-east-1"


def _load_config_from_env() -> SPAPIConfig:
    """
    Carrega configuração da SP-API a partir das variáveis de ambiente (.env).

    Levanta RuntimeError se alguma variável essencial estiver ausente.
    """
    missing: List[str] = []

    def required(name: str) -> str:
        v = os.getenv(name, "")
        if not v:
            missing.append(name)
        return v

    def optional(name: str, default: str = "") -> str:
        return os.getenv(name, default) or default

    cfg = SPAPIConfig(
        lwa_client_id=required("SPAPI_CLIENT_ID"),
        lwa_client_secret=required("SPAPI_CLIENT_SECRET"),
        refresh_token=required("SPAPI_REFRESH_TOKEN"),
        aws_access_key=required("SPAPI_AWS_ACCESS_KEY_ID"),
        aws_secret_key=required("SPAPI_AWS_SECRET_ACCESS_KEY"),
        region=optional("SPAPI_REGION", "na"),
        marketplace_id=optional("SPAPI_MARKETPLACE_ID", "ATVPDKIKX0DER"),
        seller_id=os.getenv("SPAPI_SELLER_ID") or None,
    )

    if missing:
        raise RuntimeError(f"Variáveis SP-API ausentes no .env: {', '.join(missing)}")

    return cfg


# ---------------------------------------------------------------------------
# Cache de access token LWA + controls de rate
# ---------------------------------------------------------------------------

_access_token_cache: Dict[str, Any] = {"token": None, "expires_at": 0.0}

# timestamp da última chamada de PRICING (para impor delay mínimo)
_last_pricing_call_ts: float = 0.0

# intervalo mínimo entre chamadas de pricing (segundos) - configurável via .env
PRICING_MIN_INTERVAL = float(os.getenv("SPAPI_PRICING_MIN_INTERVAL", "2.2"))

# Session HTTP (reuso de conexões)
_SESSION = requests.Session()

# Cache simples de paginação do Catalog Items:
# chave -> dict com:
#   page_items: {page:int -> items:list}
#   page_tokens: {page:int -> token:str|None}  (token usado para buscar aquela página)
# Observação: page_tokens[1] é sempre None
_catalog_pagination_cache: Dict[Tuple[str, int, str, Optional[int], str], Dict[str, Any]] = {}


def _get_lwa_access_token(cfg: SPAPIConfig) -> str:
    """
    Obtém (ou reutiliza, se ainda válido) um access token da Login With Amazon (LWA).
    """
    now = time.time()
    if _access_token_cache["token"] and now < _access_token_cache["expires_at"]:
        return _access_token_cache["token"]  # type: ignore[return-value]

    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": cfg.refresh_token,
        "client_id": cfg.lwa_client_id,
        "client_secret": cfg.lwa_client_secret,
    }

    resp = _SESSION.post(url, data=data, timeout=15)
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
# Assinatura AWS SigV4 e chamada genérica
# ---------------------------------------------------------------------------

def _normalize_query_params(params: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """
    Garante que params sejam strings (e listas sejam CSV),
    para manter coerência entre:
      - canonical querystring (assinatura)
      - params passados ao requests
    """
    if not params:
        return {}
    out: Dict[str, str] = {}
    for k, v in params.items():
        if v is None:
            continue
        if isinstance(v, (list, tuple, set)):
            vals = [str(x).strip() for x in v if x is not None and str(x).strip() != ""]
            if not vals:
                continue
            out[k] = ",".join(vals)
        else:
            out[k] = str(v)
    return out


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

    qp = _normalize_query_params(query_params)

    # Query string canônica (chaves ordenadas, encoding RFC 3986)
    if qp:
        qp_items: List[str] = []
        for key in sorted(qp.keys()):
            value = qp[key]
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
        [method, path, canonical_querystring, canonical_headers, signed_headers, payload_hash]
    )
    canonical_request_hash = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()

    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = f"{datestamp}/{aws_region}/{service}/aws4_request"
    string_to_sign = "\n".join([algorithm, amzdate, credential_scope, canonical_request_hash])

    def _sign(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    k_date = _sign(("AWS4" + cfg.aws_secret_key).encode("utf-8"), datestamp)
    k_region = _sign(k_date, aws_region)
    k_service = _sign(k_region, service)
    k_signing = _sign(k_service, "aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization_header = (
        f"{algorithm} "
        f"Credential={cfg.aws_access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, "
        f"Signature={signature}"
    )

    return {
        "host": host,
        "x-amz-date": amzdate,
        "x-amz-access-token": access_token,
        "Authorization": authorization_header,
        "content-type": "application/json",
        "user-agent": "miner-ecom/1.0",
    }


def _request_sp_api(
    cfg: SPAPIConfig,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Faz uma chamada genérica à SP-API (autenticando e assinando).
    Retorna o JSON da resposta ou lança SellingPartnerAPIError.
    """
    access_token = _get_lwa_access_token(cfg)

    body_str = json.dumps(json_body) if json_body is not None else ""
    params_norm = _normalize_query_params(params)

    headers = _sign_sp_api_request(
        cfg=cfg,
        method=method,
        path=path,
        query_params=params_norm,
        body=body_str,
        access_token=access_token,
    )

    url = f"https://{cfg.endpoint_host}{path}"

    resp = _SESSION.request(
        method=method,
        url=url,
        params=params_norm if params_norm else None,
        data=body_str if body_str else None,
        headers=headers,
        timeout=timeout,
    )

    if resp.status_code >= 400:
        # tenta enriquecer um pouco a msg, mas sem depender de formato
        req_id = resp.headers.get("x-amzn-RequestId") or resp.headers.get("x-amz-request-id") or ""
        suffix = f" | requestId={req_id}" if req_id else ""
        raise SellingPartnerAPIError(f"Erro SP-API {resp.status_code} para {path}: {resp.text}{suffix}")

    if not resp.text:
        return {}

    try:
        return resp.json()
    except json.JSONDecodeError:
        raise SellingPartnerAPIError(f"Resposta SP-API não é JSON para {path}: {resp.text[:200]}")


# ---------------------------------------------------------------------------
# Catalog helpers
# ---------------------------------------------------------------------------

def _extract_catalog_item(
    item: Dict[str, Any],
    marketplace_id: str,
    fallback_gtin: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Normaliza o payload de um item do Catalog Items em um dict
    com os campos mínimos usados no projeto.
    """
    asin = item.get("asin")

    # summary para o marketplace desejado
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

    # GTIN / identifiers (prioridade por tipo)
    gtin_value = None
    gtin_type = None

    identifiers = item.get("identifiers") or []
    ids_list: List[Dict[str, Any]] = []

    # pega o bloco do marketplace, senão cai no primeiro
    chosen_block = None
    for ident in identifiers:
        if ident.get("marketplaceId") == marketplace_id:
            chosen_block = ident
            break
    if chosen_block is None and identifiers:
        chosen_block = identifiers[0]

    if chosen_block:
        ids_list = chosen_block.get("identifiers") or []

    # escolhe o melhor identifier disponível
    preferred = ("GTIN", "EAN", "UPC", "ISBN")
    chosen_identifier = None
    for pref in preferred:
        for x in ids_list:
            if x.get("identifierType") == pref and x.get("identifier"):
                chosen_identifier = x
                break
        if chosen_identifier:
            break
    if chosen_identifier is None and ids_list:
        chosen_identifier = ids_list[0]

    if chosen_identifier:
        gtin_value = chosen_identifier.get("identifier")
        gtin_type = chosen_identifier.get("identifierType")

    # Sales rank (melhor rank do marketplace)
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
            title_val = cr.get("title") or sr.get("displayGroup")
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
    Busca item de catálogo por GTIN (UPC/EAN/ISBN) usando /catalog/2022-04-01/items.
    """
    cfg = _load_config_from_env()
    gtin_clean = gtin.strip()

    length = len(gtin_clean)
    if length == 12:
        candidates = ["UPC", "GTIN"]
    elif length == 13:
        candidates = ["EAN", "GTIN"]
    elif length == 10:
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
            data = _request_sp_api(cfg=cfg, method="GET", path="/catalog/2022-04-01/items", params=params)
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


def search_by_title(
    title: str,
    original_title: Optional[str] = None,
    page_size: int = 3,
) -> Optional[Dict[str, Any]]:
    """
    Fallback: busca item de catálogo por título/keywords.

    Se `original_title` for fornecido, retorna o item com maior similaridade
    de título (via rapidfuzz). Caso contrário, retorna o primeiro item.
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

    data = _request_sp_api(cfg=cfg, method="GET", path="/catalog/2022-04-01/items", params=params)

    items = data.get("items") or []
    if not items:
        return None

    if original_title:
        scores: List[Tuple[int, Dict[str, Any]]] = []
        for it in items:
            summary = (it.get("summaries") or [{}])[0]
            cand_title = summary.get("itemName") or ""
            score = fuzz.token_sort_ratio(original_title.lower(), str(cand_title).lower())
            scores.append((score, it))
        best = max(scores, key=lambda x: x[0])[1] if scores else items[0]
        return _extract_catalog_item(best, cfg.marketplace_id)

    return _extract_catalog_item(items[0], cfg.marketplace_id)


def _extract_next_token(data: Dict[str, Any]) -> Optional[str]:
    """
    O Catalog Items search retorna paginação com next token.
    Como o formato pode variar (pagination/nextToken etc), tentamos alguns caminhos.
    """
    if not isinstance(data, dict):
        return None

    pag = data.get("pagination") or data.get("Pagination") or {}
    if isinstance(pag, dict):
        nt = pag.get("nextToken") or pag.get("NextToken") or pag.get("nextPageToken")
        if nt:
            return str(nt)

    nt2 = data.get("nextToken") or data.get("NextToken") or data.get("nextPageToken")
    if nt2:
        return str(nt2)

    return None


def search_catalog_items_with_pagination(
    keywords: str,
    page_size: int = 20,
    page_token: Optional[str] = None,
    included_data: str = "summaries,identifiers,salesRanks",
    browse_node_id: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Busca UMA página no Catalog Items API por keywords e retorna:
      (items, next_token)

    - page_token: token retornado na página anterior (para buscar a próxima)
    - browse_node_id: quando fornecido, tentamos filtrar usando classificationIds (se suportado)
    """
    cfg = _load_config_from_env()
    if not keywords:
        return [], None

    page_size = max(1, min(int(page_size), 20))

    params: Dict[str, Any] = {
        "marketplaceIds": cfg.marketplace_id,
        "keywords": keywords[:200],
        "includedData": included_data,
        "pageSize": page_size,
    }

    if page_token:
        params["pageToken"] = page_token

    # tentativa de filtro por browse/classification (se a API aceitar)
    if browse_node_id is not None:
        try:
            params["classificationIds"] = str(int(browse_node_id))
        except Exception:
            # se vier algo inválido, só ignora
            pass

    try:
        data = _request_sp_api(cfg=cfg, method="GET", path="/catalog/2022-04-01/items", params=params)
    except SellingPartnerAPIError as e:
        # fallback: se classificationIds não for aceito, tenta sem ele
        msg = str(e)
        if browse_node_id is not None and ("classification" in msg.lower() or "invalidinput" in msg.lower()):
            params.pop("classificationIds", None)
            data = _request_sp_api(cfg=cfg, method="GET", path="/catalog/2022-04-01/items", params=params)
        else:
            raise

    items = data.get("items") or []
    next_token = _extract_next_token(data)
    return items, next_token


def search_catalog_items(
    keywords: str,
    page_size: int = 20,
    page: int = 1,
    included_data: str = "summaries,identifiers,salesRanks",
    browse_node_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Busca itens no Catalog Items API por palavra-chave, retornando a lista bruta de itens.

    IMPORTANTE:
      - A paginação real do Catalog Items usa pageToken.
      - Para manter compatibilidade com o projeto (que chama com `page=1..N`),
        esta função usa pageToken internamente e "salta" até a página desejada.
      - Há cache leve por (keywords, page_size, included_data, browse_node_id, marketplace_id)
        para evitar custo quadrático quando o chamador itera page=1..N.
    """
    cfg = _load_config_from_env()
    if not keywords:
        return []

    page_size = max(1, min(int(page_size), 20))
    page = max(1, int(page))

    cache_key = (keywords[:200], page_size, included_data, browse_node_id, cfg.marketplace_id)
    cache = _catalog_pagination_cache.get(cache_key)
    if cache is None:
        cache = {"page_items": {}, "page_tokens": {1: None}, "exhausted": False}
        _catalog_pagination_cache[cache_key] = cache

    # se já temos essa página em cache, devolve direto
    if page in cache["page_items"]:
        return cache["page_items"][page]

    # se já sabemos que acabou (sem next_token) e pediram página além, retorna vazio
    if cache.get("exhausted") and page not in cache["page_items"]:
        return []

    # encontra a maior página conhecida <= page, para começar de lá
    known_pages = [p for p in cache["page_tokens"].keys() if isinstance(p, int) and p <= page]
    start_page = max(known_pages) if known_pages else 1

    # token para buscar start_page
    token = cache["page_tokens"].get(start_page, None)

    # se start_page já tem items mas pediram uma página maior, avançamos a partir dela
    current_page = start_page

    # se start_page tem items e start_page == page, já teria sido retornado acima
    # então aqui avançamos até atingir page
    while current_page <= page:
        if current_page in cache["page_items"]:
            # já temos essa página, pega o próximo token e segue
            token = cache["page_tokens"].get(current_page + 1, token)
            current_page += 1
            continue

        items, next_token = search_catalog_items_with_pagination(
            keywords=keywords,
            page_size=page_size,
            page_token=token,
            included_data=included_data,
            browse_node_id=browse_node_id,
        )

        cache["page_items"][current_page] = items
        cache["page_tokens"][current_page + 1] = next_token

        if not next_token:
            cache["exhausted"] = True

        if current_page == page:
            return items

        if not next_token:
            # acabou antes de chegar na página solicitada
            return []

        token = next_token
        current_page += 1

    return []


# ---------------------------------------------------------------------------
# Funções de alto nível - Catalog existentes
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

    return _request_sp_api(cfg=cfg, method="GET", path=path, params=params)


def debug_ping() -> Dict[str, Any]:
    """
    Chamada simples à SP-API para teste.
    Usa /sellers/v1/marketplaceParticipations.
    """
    cfg = _load_config_from_env()
    path = "/sellers/v1/marketplaceParticipations"
    return _request_sp_api(cfg=cfg, method="GET", path=path, params=None)


# ---------------------------------------------------------------------------
# Product Pricing - BuyBox / Lowest price (com delay + retry)
# ---------------------------------------------------------------------------

def get_buybox_price(asin: str, item_condition: str = "New") -> Optional[Dict[str, Any]]:
    """
    Usa /products/pricing/v0/items/{asin}/offers para tentar obter:
    - preço da BuyBox (se existir)
    - fallback para LowestPrices se não houver BuyBox

    Respeita:
      - intervalo mínimo entre chamadas (PRICING_MIN_INTERVAL, configurável via .env)
      - retry em caso de QuotaExceeded

    Retorna dict com:
      asin, price, currency, is_prime, fulfillment_channel, condition
    """
    global _last_pricing_call_ts

    cfg = _load_config_from_env()
    path = f"/products/pricing/v0/items/{asin}/offers"
    params = {
        "MarketplaceId": cfg.marketplace_id,
        "ItemCondition": item_condition,
        "CustomerType": "Consumer",
    }

    # 1) Respeita intervalo mínimo entre chamadas
    min_interval = PRICING_MIN_INTERVAL
    now = time.time()
    delta = now - _last_pricing_call_ts
    if delta < min_interval:
        time.sleep(min_interval - delta)

    # 2) Retry simples em caso de QuotaExceeded
    max_attempts = 3
    data: Optional[Dict[str, Any]] = None

    for attempt in range(max_attempts):
        try:
            data = _request_sp_api(cfg=cfg, method="GET", path=path, params=params)
            _last_pricing_call_ts = time.time()
            break
        except SellingPartnerAPIError as e:
            msg = str(e)
            if "QuotaExceeded" in msg and attempt < max_attempts - 1:
                time.sleep(3.0)
                continue
            raise

    if data is None:
        return None

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
        "condition": item_condition,
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
    elif len(sys.argv) > 1 and sys.argv[1] == "offers" and len(sys.argv) in (3, 4):
        asin_arg = sys.argv[2]
        cond = sys.argv[3] if len(sys.argv) == 4 else "New"
        print(f"Buscando BuyBox/offers para ASIN={asin_arg} (cond={cond}) ...")
        data = get_buybox_price(asin_arg, item_condition=cond)
        print(json.dumps(data, indent=2))
    else:
        print("Testando conexao com SP-API (sellers/v1/marketplaceParticipations)...")
        data = debug_ping()
        print(json.dumps(data, indent=2))

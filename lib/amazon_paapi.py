import os
import time
import logging
from typing import Any, Dict, Optional

from amazon_paapi import AmazonApi  # pip install python-amazon-paapi

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Config via .env
# -------------------------------------------------------------------
PAAPI_ACCESS_KEY = os.getenv("PAAPI_ACCESS_KEY")
PAAPI_SECRET_KEY = os.getenv("PAAPI_SECRET_KEY")
PAAPI_PARTNER_TAG = os.getenv("PAAPI_PARTNER_TAG")

# Observação:
# A lib python-amazon-paapi usa "COUNTRY" (US, BR, DE, etc.).
# Vamos reaproveitar PAAPI_REGION como esse "COUNTRY".
# Ex.: PAAPI_REGION=US  ou  PAAPI_REGION=BR
PAAPI_COUNTRY = os.getenv("PAAPI_REGION", "US") or "US"

# Throttling padrão: 1 requisição/segundo (ajustável por env)
PAAPI_THROTTLING = float(os.getenv("PAAPI_THROTTLING", "1.0"))

# Cache em memória por GTIN (para não bater na PA-API toda hora)
_GTIN_CACHE: Dict[str, tuple[float, Dict[str, Any]]] = {}
_GTIN_CACHE_TTL = int(os.getenv("PAAPI_GTIN_CACHE_TTL", "3600"))  # segundos

_AMAZON_CLIENT: Optional[AmazonApi] = None


# -------------------------------------------------------------------
# Helpers internos
# -------------------------------------------------------------------
def _normalize_gtin(gtin: str | None) -> str:
    """
    Normaliza GTIN (UPC/EAN/ISBN): mantém só dígitos.
    """
    if gtin is None:
        return ""
    s = "".join(ch for ch in str(gtin).strip() if ch.isdigit())
    return s


def _get_cached_gtin(gtin: str) -> Optional[Dict[str, Any]]:
    if not gtin:
        return None

    entry = _GTIN_CACHE.get(gtin)
    if not entry:
        return None

    ts, data = entry
    if time.time() - ts > _GTIN_CACHE_TTL:
        # Expirou
        _GTIN_CACHE.pop(gtin, None)
        return None

    return data


def _set_cached_gtin(gtin: str, data: Optional[Dict[str, Any]]) -> None:
    if not gtin or data is None:
        return
    _GTIN_CACHE[gtin] = (time.time(), data)


def _get_client() -> AmazonApi:
    """
    Cria (lazy) o cliente AmazonApi da lib python-amazon-paapi.
    """
    global _AMAZON_CLIENT

    if _AMAZON_CLIENT is not None:
        return _AMAZON_CLIENT

    if not PAAPI_ACCESS_KEY or not PAAPI_SECRET_KEY or not PAAPI_PARTNER_TAG:
        raise RuntimeError(
            "Credenciais da Amazon PA-API não configuradas. "
            "Verifique PAAPI_ACCESS_KEY, PAAPI_SECRET_KEY e PAAPI_PARTNER_TAG no .env."
        )

    logger.info(
        "Inicializando AmazonApi (country=%s, throttling=%.2f)",
        PAAPI_COUNTRY,
        PAAPI_THROTTLING,
    )

    _AMAZON_CLIENT = AmazonApi(
        PAAPI_ACCESS_KEY,
        PAAPI_SECRET_KEY,
        PAAPI_PARTNER_TAG,
        PAAPI_COUNTRY,
        throttling=PAAPI_THROTTLING,
    )
    return _AMAZON_CLIENT


# -------------------------------------------------------------------
# Função pública: search_by_gtin
# -------------------------------------------------------------------
def search_by_gtin(gtin: str) -> Optional[Dict[str, Any]]:
    """
    Busca um produto na Amazon por GTIN (UPC/EAN/ISBN) usando PA-API 5.0,
    retornando um dict flatten com info mínima para o app:

    Retorno (dict) possível:
        {
            "asin": str | None,
            "title": str | None,
            "price": float | None,
            "currency": str | None,
            "is_prime_eligible": bool | None,
            "is_amazon_fulfilled": bool | None,
            "offer_type": str,           # "PRIME" / "AMAZON_FULFILLED" / "OTHER" / "UNKNOWN"
            "detail_page_url": str | None,
            "gtin_searched": str,
        }

    Se não encontrar nada ou der erro "não crítico", retorna None.
    """
    norm_gtin = _normalize_gtin(gtin)
    if not norm_gtin:
        logger.debug("search_by_gtin chamado com GTIN vazio/ inválido: %r", gtin)
        return None

    # 1) Verificar cache em memória
    cached = _get_cached_gtin(norm_gtin)
    if cached is not None:
        logger.debug("search_by_gtin(%s) atendido via cache em memória", norm_gtin)
        return cached

    # 2) Criar cliente Amazon
    try:
        client = _get_client()
    except Exception as exc:
        logger.error("Falha ao inicializar AmazonApi: %s", exc)
        return None

    # 3) Chamar a PA-API usando o GTIN como keywords
    #    Pedimos só 1 item e os recursos mínimos (titulo, preço, info Prime/FBA).
    try:
        search_result = client.search_items(
            keywords=norm_gtin,
            item_count=1,
            resources=[
                "ItemInfo.Title",
                "Offers.Listings.Price",
                "Offers.Listings.DeliveryInfo.IsPrimeEligible",
                "Offers.Listings.DeliveryInfo.IsAmazonFulfilled",
            ],
        )
    except Exception as exc:
        # Qualquer erro aqui não deve derrubar o app – só loga e devolve None.
        logger.warning("Amazon PA-API search_items falhou para GTIN %s: %s", norm_gtin, exc)
        return None

    items = getattr(search_result, "items", None)
    if not items:
        logger.info("Nenhum item retornado pela Amazon para GTIN %s", norm_gtin)
        return None

    # Pegamos o primeiro item da lista
    item = items[0]

    # ASIN
    asin = getattr(item, "asin", None)

    # Título
    title = None
    try:
        # python-amazon-paapi segue o mesmo nome da PA-API:
        # ItemInfo.Title.DisplayValue -> item.item_info.title.display_value
        title = item.item_info.title.display_value
    except AttributeError:
        pass

    # Ofertas / preço / Prime / FBA
    price_amount: Optional[float] = None
    currency: Optional[str] = None
    is_prime_eligible: Optional[bool] = None
    is_amazon_fulfilled: Optional[bool] = None

    try:
        listings = item.offers.listings  # type: ignore[attr-defined]
        if listings:
            listing = listings[0]
            # Preço
            try:
                price_amount = float(listing.price.amount)  # type: ignore[attr-defined]
                currency = str(listing.price.currency)      # type: ignore[attr-defined]
            except Exception:
                pass

            # Prime / FBA (DeliveryInfo)
            delivery_info = getattr(listing, "delivery_info", None)
            if delivery_info is not None:
                is_prime_eligible = getattr(delivery_info, "is_prime_eligible", None)
                is_amazon_fulfilled = getattr(delivery_info, "is_amazon_fulfilled", None)
    except AttributeError:
        # Se não tiver Offers/Listings, seguimos com None
        pass

    # Classificar tipo de oferta (para futuro filtro Prime/FBA)
    if is_prime_eligible:
        offer_type = "PRIME"
    elif is_amazon_fulfilled:
        offer_type = "AMAZON_FULFILLED"
    elif price_amount is not None:
        offer_type = "OTHER"
    else:
        offer_type = "UNKNOWN"

    detail_page_url = getattr(item, "detail_page_url", None)

    result: Dict[str, Any] = {
        "asin": asin,
        "title": title,
        "price": price_amount,
        "currency": currency,
        "is_prime_eligible": bool(is_prime_eligible) if is_prime_eligible is not None else None,
        "is_amazon_fulfilled": bool(is_amazon_fulfilled) if is_amazon_fulfilled is not None else None,
        "offer_type": offer_type,
        "detail_page_url": detail_page_url,
        "gtin_searched": norm_gtin,
    }

    # Salvar em cache
    _set_cached_gtin(norm_gtin, result)

    return result


# -------------------------------------------------------------------
# Função utilitária opcional (para debug/manual)
# -------------------------------------------------------------------
def is_configured() -> bool:
    """
    Retorna True se as variáveis mínimas da PA-API estiverem setadas no .env.
    Útil para a UI decidir se mostra/oculta filtros Amazon.
    """
    return bool(PAAPI_ACCESS_KEY and PAAPI_SECRET_KEY and PAAPI_PARTNER_TAG)

import pandas as pd
from typing import Dict, Any

from sqlalchemy.engine import Engine

from lib.config import make_engine
from lib.amazon_spapi import _load_config_from_env


def _prepare_amazon_products_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara o DataFrame vindo da mineração Amazon (amazon_matching/_discover_amazon_products)
    para o formato da tabela `amazon_products`.

    Espera colunas como:
      - amazon_asin
      - amazon_title
      - amazon_brand
      - amazon_browse_node_id
      - amazon_browse_node_name
      - amazon_sales_rank
      - amazon_sales_rank_category
      - amazon_price
      - amazon_currency
      - amazon_is_prime
      - amazon_fulfillment_channel
      - gtin
      - gtin_type
    """
    if df is None or df.empty:
        return df.iloc[0:0].copy()

    cfg = _load_config_from_env()
    marketplace_id = cfg.marketplace_id or "ATVPDKIKX0DER"

    # Garante que temos a coluna de ASIN
    if "amazon_asin" not in df.columns and "asin" not in df.columns:
        return df.iloc[0:0].copy()

    if "amazon_asin" in df.columns:
        asin_series = df["amazon_asin"]
    else:
        asin_series = df["asin"]

    out = pd.DataFrame()
    out["asin"] = asin_series.astype(str).str.strip()
    out["marketplace_id"] = marketplace_id

    # Campos de catálogo
    out["title"] = df.get("amazon_title", df.get("title"))
    out["brand"] = df.get("amazon_brand", df.get("brand"))
    out["browse_node_id"] = df.get("amazon_browse_node_id")
    out["browse_node_name"] = df.get("amazon_browse_node_name")

    # GTIN / UPC
    out["gtin"] = df.get("gtin")
    out["gtin_type"] = df.get("gtin_type")

    # BSR
    out["sales_rank"] = df.get("amazon_sales_rank")
    out["sales_rank_category"] = df.get("amazon_sales_rank_category")

    # Preço / oferta
    out["price"] = df.get("amazon_price")
    out["currency"] = df.get("amazon_currency")

    is_prime_series = df.get("amazon_is_prime")
    if is_prime_series is not None:
        out["is_prime"] = is_prime_series.fillna(False).astype(bool).astype(int)
    else:
        out["is_prime"] = 0

    out["fulfillment_channel"] = df.get("amazon_fulfillment_channel")

    # Remove ASIN vazio
    out = out[out["asin"].notna() & (out["asin"].str.strip() != "")]
    if out.empty:
        return out

    # Converte NaN -> None para o driver MySQL
    out = out.where(pd.notnull(out), None)

    return out


def _get_engine() -> Engine:
    return make_engine()


def upsert_amazon_products(df: pd.DataFrame) -> int:
    """
    Insere / atualiza produtos na tabela `amazon_products`.

    - INSERT com ON DUPLICATE KEY UPDATE (chave única = asin)
    - Atualiza campos de título, preço, BSR, etc.
    - first_seen_at / last_seen_at são controlados pelo próprio MySQL
      (DEFAULT CURRENT_TIMESTAMP / ON UPDATE CURRENT_TIMESTAMP).
    """
    store_df = _prepare_amazon_products_df(df)
    if store_df.empty:
        return 0

    cols = [
        "asin",
        "marketplace_id",
        "title",
        "brand",
        "browse_node_id",
        "browse_node_name",
        "gtin",
        "gtin_type",
        "sales_rank",
        "sales_rank_category",
        "price",
        "currency",
        "is_prime",
        "fulfillment_channel",
    ]

    # Garante que todas as colunas existam (mesmo que cheias de None)
    for c in cols:
        if c not in store_df.columns:
            store_df[c] = None

    values = [tuple(row[c] for c in cols) for _, row in store_df[cols].iterrows()]
    if not values:
        return 0

    sql = """
    INSERT INTO amazon_products (
        asin,
        marketplace_id,
        title,
        brand,
        browse_node_id,
        browse_node_name,
        gtin,
        gtin_type,
        sales_rank,
        sales_rank_category,
        price,
        currency,
        is_prime,
        fulfillment_channel
    ) VALUES (
        %s, %s, %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s
    )
    ON DUPLICATE KEY UPDATE
        marketplace_id       = VALUES(marketplace_id),
        title                = VALUES(title),
        brand                = VALUES(brand),
        browse_node_id       = VALUES(browse_node_id),
        browse_node_name     = VALUES(browse_node_name),
        gtin                 = VALUES(gtin),
        gtin_type            = VALUES(gtin_type),
        sales_rank           = VALUES(sales_rank),
        sales_rank_category  = VALUES(sales_rank_category),
        price                = VALUES(price),
        currency             = VALUES(currency),
        is_prime             = VALUES(is_prime),
        fulfillment_channel  = VALUES(fulfillment_channel);
    """

    engine = _get_engine()
    with engine.begin() as conn:
        # executa em modo "executemany"
        conn.exec_driver_sql(sql, values)

    return len(values)

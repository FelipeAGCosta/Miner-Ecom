# lib/db.py
import pandas as pd
import numpy as np
from sqlalchemy import text

# ---------------------------------------------------------------------------
# eBay: normalização e upsert
# ---------------------------------------------------------------------------

def sql_safe_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte NaN/NA para None, normaliza tipos e garante defaults (ex.: currency='USD').
    Usado para salvar listings do eBay.
    """
    df = df.copy()

    expected = [
        "item_id", "title", "brand", "mpn", "gtin", "price", "currency",
        "available_qty", "qty_flag", "condition", "seller", "category_id",
        "item_url",
    ]
    for col in expected:
        if col not in df.columns:
            df[col] = None

    # Numéricos
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    qty = pd.to_numeric(df["available_qty"], errors="coerce")
    df["available_qty"] = qty.where(qty.notna(), None)

    # category_id inteiro quando houver
    cat = pd.to_numeric(df["category_id"], errors="coerce").astype("Int64")
    df["category_id"] = cat.where(cat.notna(), None)

    # Normaliza condition
    df["condition"] = df["condition"].astype(str).str.title()

    # currency: se vier vazio/None/NaN, define USD
    cur = df["currency"].astype(str).str.upper()
    df["currency"] = cur.where(~cur.isin(["", "NONE", "NAN"]), "USD")
    df["currency"] = df["currency"].fillna("USD")

    # Converte NaN/NA restantes para None
    df = df.replace({np.nan: None, pd.NA: None})

    # Tipos Python "object" para o execute do SQLAlchemy/PyMySQL
    df = df.astype(
        {
            "item_id": object,
            "title": object,
            "brand": object,
            "mpn": object,
            "gtin": object,
            "price": object,
            "currency": object,
            "available_qty": object,
            "qty_flag": object,
            "condition": object,
            "seller": object,
            "category_id": object,
            "item_url": object,
        }
    )

    return df[expected]


def upsert_ebay_listings(engine, rows: pd.DataFrame) -> int:
    if rows.empty:
        return 0
    rows = sql_safe_frame(rows)
    sql = text(
        """
        INSERT INTO ebay_listing
        (item_id, title, brand, mpn, gtin, price, currency,
         available_qty, qty_flag, `condition`, seller, category_id,
         item_url, fetched_at)
        VALUES
        (:item_id, :title, :brand, :mpn, :gtin, :price, :currency,
         :available_qty, :qty_flag, :condition, :seller, :category_id,
         :item_url, NOW())
        ON DUPLICATE KEY UPDATE
          title        = VALUES(title),
          brand        = VALUES(brand),
          mpn          = VALUES(mpn),
          gtin         = VALUES(gtin),
          price        = VALUES(price),
          currency     = VALUES(currency),
          available_qty= VALUES(available_qty),
          qty_flag     = VALUES(qty_flag),
          `condition`  = VALUES(`condition`),
          seller       = VALUES(seller),
          category_id  = VALUES(category_id),
          item_url     = VALUES(item_url),
          fetched_at   = NOW();
    """
    )
    with engine.begin() as conn:
        conn.execute(sql, rows.to_dict(orient="records"))
    return len(rows)


# ---------------------------------------------------------------------------
# Amazon: normalização e upsert
# ---------------------------------------------------------------------------

def sql_safe_amazon_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza DataFrame de produtos Amazon para inserção na tabela amazon_products.
    Garante que todas as colunas esperadas existam e estejam em tipos compatíveis.
    """
    df = df.copy()

    expected = [
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
        "source_root_name",
        "source_child_name",
        "search_kw",
    ]

    # Garante todas as colunas
    for col in expected:
        if col not in df.columns:
            df[col] = None

    # Numéricos
    df["browse_node_id"] = pd.to_numeric(df["browse_node_id"], errors="coerce").astype(
        "Int64"
    )
    df["sales_rank"] = pd.to_numeric(df["sales_rank"], errors="coerce").astype("Int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Booleano -> int (0/1)
    df["is_prime"] = df["is_prime"].fillna(False).astype(bool).astype(int)

    # currency: fallback para USD
    cur = df["currency"].astype(str).str.upper()
    df["currency"] = cur.where(~cur.isin(["", "NONE", "NAN"]), "USD")
    df["currency"] = df["currency"].fillna("USD")

    # Converte NaN/NA restantes para None
    df = df.replace({np.nan: None, pd.NA: None})

    # Converte tudo para object (Python) para o driver do MySQL
    df = df.astype(
        {
            "asin": object,
            "marketplace_id": object,
            "title": object,
            "brand": object,
            "browse_node_id": object,
            "browse_node_name": object,
            "gtin": object,
            "gtin_type": object,
            "sales_rank": object,
            "sales_rank_category": object,
            "price": object,
            "currency": object,
            "is_prime": object,
            "fulfillment_channel": object,
            "source_root_name": object,
            "source_child_name": object,
            "search_kw": object,
        }
    )

    return df[expected]


def upsert_amazon_products(engine, df: pd.DataFrame) -> int:
    """
    Insere/atualiza produtos na tabela amazon_products.

    Campos esperados (df):
      asin, marketplace_id, title, brand,
      browse_node_id, browse_node_name,
      gtin, gtin_type,
      sales_rank, sales_rank_category,
      price, currency,
      is_prime, fulfillment_channel,
      source_root_name, source_child_name, search_kw
    """
    if df.empty:
        return 0

    rows = sql_safe_amazon_frame(df)

    sql = text(
        """
        INSERT INTO amazon_products
        (asin, marketplace_id, title, brand,
         browse_node_id, browse_node_name,
         gtin, gtin_type,
         sales_rank, sales_rank_category,
         price, currency,
         is_prime, fulfillment_channel,
         source_root_name, source_child_name, search_kw,
         fetched_at)
        VALUES
        (:asin, :marketplace_id, :title, :brand,
         :browse_node_id, :browse_node_name,
         :gtin, :gtin_type,
         :sales_rank, :sales_rank_category,
         :price, :currency,
         :is_prime, :fulfillment_channel,
         :source_root_name, :source_child_name, :search_kw,
         NOW())
        ON DUPLICATE KEY UPDATE
          marketplace_id      = VALUES(marketplace_id),
          title               = VALUES(title),
          brand               = VALUES(brand),
          browse_node_id      = VALUES(browse_node_id),
          browse_node_name    = VALUES(browse_node_name),
          gtin                = VALUES(gtin),
          gtin_type           = VALUES(gtin_type),
          sales_rank          = VALUES(sales_rank),
          sales_rank_category = VALUES(sales_rank_category),
          price               = VALUES(price),
          currency            = VALUES(currency),
          is_prime            = VALUES(is_prime),
          fulfillment_channel = VALUES(fulfillment_channel),
          source_root_name    = VALUES(source_root_name),
          source_child_name   = VALUES(source_child_name),
          search_kw           = VALUES(search_kw),
          fetched_at          = NOW();
    """
    )

    with engine.begin() as conn:
        conn.execute(sql, rows.to_dict(orient="records"))

    return len(rows)

"""
Funções de normalização e upsert para as tabelas principais:

- ebay_listing
- amazon_products

Centraliza a conversão de DataFrame (pandas) em linhas prontas para INSERT/UPDATE
via SQLAlchemy, garantindo tipos e valores padrão coerentes.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional, Set, Dict
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import text, bindparam


# ---------------------------------------------------------------------------
# eBay: normalização e upsert
# ---------------------------------------------------------------------------


def sql_safe_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza DataFrame de listings do eBay para inserção na tabela ebay_listing.

    - Garante que todas as colunas esperadas existam.
    - Converte valores numéricos (price, available_qty, category_id).
    - Normaliza `condition` e `currency` (fallback USD).
    - Converte NaN/NA para None (compatível com drivers MySQL).
    """
    df = df.copy()

    expected = [
        "item_id",
        "title",
        "brand",
        "mpn",
        "gtin",
        "price",
        "currency",
        "available_qty",
        "qty_flag",
        "condition",
        "seller",
        "category_id",
        "item_url",
    ]

    # Garante todas as colunas mínimas
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

    # Normaliza condição (apenas cosmeticamente)
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


def upsert_ebay_listings(engine: Any, rows: pd.DataFrame) -> int:
    """
    Insere/atualiza listings na tabela ebay_listing.

    - Usa item_id como chave única (definido no schema do MySQL).
    - Atualiza campos principais e fetched_at a cada execução.
    """
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
          title         = VALUES(title),
          brand         = VALUES(brand),
          mpn           = VALUES(mpn),
          gtin          = VALUES(gtin),
          price         = VALUES(price),
          currency      = VALUES(currency),
          available_qty = VALUES(available_qty),
          qty_flag      = VALUES(qty_flag),
          `condition`   = VALUES(`condition`),
          seller        = VALUES(seller),
          category_id   = VALUES(category_id),
          item_url      = VALUES(item_url),
          fetched_at    = NOW();
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

    Importante:
    - Mantém is_prime como TRISTATE: 1 (Prime), 0 (Não Prime), NULL (Desconhecido)
    - Evita apagar fulfillment_channel quando vier vazio/None
    - Normaliza item_condition (New/Used/Refurbished...) e respeita VARCHAR(16)
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
        "item_condition",  # <-- NOVO
        "source_root_name",
        "source_child_name",
        "search_kw",
    ]

    # Garante todas as colunas
    for col in expected:
        if col not in df.columns:
            df[col] = None

    # Numéricos
    df["browse_node_id"] = pd.to_numeric(df["browse_node_id"], errors="coerce").astype("Int64")
    df["sales_rank"] = pd.to_numeric(df["sales_rank"], errors="coerce").astype("Int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # marketplace_id: evita NULL/'' (sua coluna é NOT NULL)
    def _norm_marketplace(v: Any) -> str:
        s = "" if v is None or v is pd.NA else str(v).strip()
        return s if s else "ATVPDKIKX0DER"

    df["marketplace_id"] = df["marketplace_id"].apply(_norm_marketplace)

    # currency: fallback para USD
    cur = df["currency"].astype(str).str.upper()
    df["currency"] = cur.where(~cur.isin(["", "NONE", "NAN"]), "USD")
    df["currency"] = df["currency"].fillna("USD")

    # is_prime: 1/0/NULL (não forçar 0 quando desconhecido)
    def _prime_to_int01_or_none(v: Any) -> Optional[int]:
        if v is None or v is pd.NA:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        if isinstance(v, (bool, np.bool_)):
            return int(bool(v))
        # aceita strings "1"/"0"/"true"/"false"
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("", "none", "nan"):
                return None
            if s in ("1", "true", "t", "yes", "y"):
                return 1
            if s in ("0", "false", "f", "no", "n"):
                return 0
            try:
                iv = int(s)
                return iv if iv in (0, 1) else None
            except Exception:
                return None
        try:
            iv = int(v)
            return iv if iv in (0, 1) else None
        except Exception:
            return None

    df["is_prime"] = df["is_prime"].apply(_prime_to_int01_or_none)

    # fulfillment_channel: normaliza para FBA/FBM/NULL (sem apagar com vazio)
    def _norm_fulfillment(v: Any) -> Optional[str]:
        if v is None or v is pd.NA:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        s = str(v).strip().upper()
        if s in ("", "NONE", "NAN"):
            return None
        # aceita padrões antigos e normaliza
        if s in ("AMAZON", "FBA"):
            return "FBA"
        if s in ("MFN", "MERCHANT", "FBM"):
            return "FBM"
        # mantém outros valores, mas garante limite de tamanho
        return s[:20]

    df["fulfillment_channel"] = df["fulfillment_channel"].apply(_norm_fulfillment)

    # item_condition: normaliza e respeita VARCHAR(16)
    def _norm_item_condition(v: Any) -> Optional[str]:
        if v is None or v is pd.NA:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        s = str(v).strip()
        if not s or s.lower() in ("none", "nan"):
            return None
        # padrão esperado: New, Used, Refurbished, Collectible...
        return s.title()[:16]

    df["item_condition"] = df["item_condition"].apply(_norm_item_condition)

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
            "item_condition": object,  # <-- NOVO
            "source_root_name": object,
            "source_child_name": object,
            "search_kw": object,
        }
    )

    return df[expected]


def upsert_amazon_products(engine: Any, df: pd.DataFrame) -> int:
    """
    Insere/atualiza produtos na tabela amazon_products.

    Campos esperados (df):
      asin, marketplace_id, title, brand,
      browse_node_id, browse_node_name,
      gtin, gtin_type,
      sales_rank, sales_rank_category,
      price, currency,
      is_prime, fulfillment_channel,
      item_condition,
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
         is_prime, fulfillment_channel, item_condition,
         source_root_name, source_child_name, search_kw,
         fetched_at)
        VALUES
        (:asin, :marketplace_id, :title, :brand,
         :browse_node_id, :browse_node_name,
         :gtin, :gtin_type,
         :sales_rank, :sales_rank_category,
         :price, :currency,
         :is_prime, :fulfillment_channel, :item_condition,
         :source_root_name, :source_child_name, :search_kw,
         NOW())
        ON DUPLICATE KEY UPDATE
          marketplace_id      = COALESCE(NULLIF(VALUES(marketplace_id), ''), marketplace_id),
          title               = COALESCE(NULLIF(VALUES(title), ''), title),
          brand               = COALESCE(NULLIF(VALUES(brand), ''), brand),
          browse_node_id      = COALESCE(VALUES(browse_node_id), browse_node_id),
          browse_node_name    = COALESCE(NULLIF(VALUES(browse_node_name), ''), browse_node_name),
          gtin                = COALESCE(NULLIF(VALUES(gtin), ''), gtin),
          gtin_type           = COALESCE(NULLIF(VALUES(gtin_type), ''), gtin_type),
          sales_rank          = COALESCE(VALUES(sales_rank), sales_rank),
          sales_rank_category = COALESCE(NULLIF(VALUES(sales_rank_category), ''), sales_rank_category),
          price               = COALESCE(VALUES(price), price),
          currency            = COALESCE(NULLIF(VALUES(currency), ''), currency),

          -- tri-state: se vier NULL, preserva o antigo
          is_prime            = COALESCE(VALUES(is_prime), is_prime),
          fulfillment_channel = COALESCE(NULLIF(VALUES(fulfillment_channel), ''), fulfillment_channel),

          -- NOVO: condição (se vier NULL/'' não apaga o antigo)
          item_condition      = COALESCE(NULLIF(VALUES(item_condition), ''), item_condition),

          source_root_name    = COALESCE(NULLIF(VALUES(source_root_name), ''), source_root_name),
          source_child_name   = COALESCE(NULLIF(VALUES(source_child_name), ''), source_child_name),
          search_kw           = COALESCE(NULLIF(VALUES(search_kw), ''), search_kw),
          fetched_at          = NOW();
        """
    )

    with engine.begin() as conn:
        conn.execute(sql, rows.to_dict(orient="records"))

    return len(rows)


# ---------------------------------------------------------------------------
# Amazon: helpers para o crawler (evitar update dentro de X dias)
# ---------------------------------------------------------------------------


def _normalize_asins(asins: Iterable[str]) -> list[str]:
    out: list[str] = []
    for a in asins:
        s = str(a).strip()
        if s:
            out.append(s)
    # dedupe mantendo ordem
    return list(dict.fromkeys(out))


def get_existing_amazon_asins(
    engine: Any,
    asins: Iterable[str],
    marketplace_id: Optional[str],
) -> Set[str]:
    """
    Retorna o conjunto de ASINs que já existem no banco (para o marketplace_id informado).
    """
    asins_list = _normalize_asins(asins)
    if not asins_list:
        return set()

    if marketplace_id:
        sql = text(
            """
            SELECT asin
            FROM amazon_products
            WHERE marketplace_id = :marketplace_id
              AND asin IN :asins
            """
        ).bindparams(bindparam("asins", expanding=True))
        params = {"marketplace_id": marketplace_id, "asins": asins_list}
    else:
        sql = text(
            """
            SELECT asin
            FROM amazon_products
            WHERE asin IN :asins
            """
        ).bindparams(bindparam("asins", expanding=True))
        params = {"asins": asins_list}

    with engine.begin() as conn:
        rows = conn.execute(sql, params).fetchall()

    return {str(r[0]) for r in rows if r and r[0] is not None}


def get_recent_amazon_asins(
    engine: Any,
    asins: Iterable[str],
    marketplace_id: Optional[str],
    cutoff: datetime,
) -> Set[str]:
    """
    Retorna o conjunto de ASINs cujo fetched_at é >= cutoff
    (ou seja, "recentes" e NÃO devem ser atualizados).
    """
    asins_list = _normalize_asins(asins)
    if not asins_list:
        return set()

    if marketplace_id:
        sql = text(
            """
            SELECT asin
            FROM amazon_products
            WHERE marketplace_id = :marketplace_id
              AND asin IN :asins
              AND fetched_at >= :cutoff
            """
        ).bindparams(bindparam("asins", expanding=True))
        params = {
            "marketplace_id": marketplace_id,
            "asins": asins_list,
            "cutoff": cutoff,
        }
    else:
        sql = text(
            """
            SELECT asin
            FROM amazon_products
            WHERE asin IN :asins
              AND fetched_at >= :cutoff
            """
        ).bindparams(bindparam("asins", expanding=True))
        params = {"asins": asins_list, "cutoff": cutoff}

    with engine.begin() as conn:
        rows = conn.execute(sql, params).fetchall()

    return {str(r[0]) for r in rows if r and r[0] is not None}


def get_amazon_fetched_at_map(
    engine: Any,
    asins: Iterable[str],
    marketplace_id: Optional[str],
) -> Dict[str, Optional[datetime]]:
    """
    Retorna um dict {asin: fetched_at} para os ASINs informados.
    Útil para debug/log e decisões sem precisar de várias queries.

    Observação:
    - Se marketplace_id for None, busca por asin (sem filtrar marketplace).
    """
    asins_list = _normalize_asins(asins)
    if not asins_list:
        return {}

    if marketplace_id:
        sql = text(
            """
            SELECT asin, fetched_at
            FROM amazon_products
            WHERE marketplace_id = :marketplace_id
              AND asin IN :asins
            """
        ).bindparams(bindparam("asins", expanding=True))
        params = {"marketplace_id": marketplace_id, "asins": asins_list}
    else:
        sql = text(
            """
            SELECT asin, fetched_at
            FROM amazon_products
            WHERE asin IN :asins
            """
        ).bindparams(bindparam("asins", expanding=True))
        params = {"asins": asins_list}

    with engine.begin() as conn:
        rows = conn.execute(sql, params).fetchall()

    out: Dict[str, Optional[datetime]] = {}
    for r in rows:
        if not r:
            continue
        asin = r[0]
        fetched_at = r[1] if len(r) > 1 else None
        if asin is None:
            continue
        out[str(asin)] = fetched_at
    return out

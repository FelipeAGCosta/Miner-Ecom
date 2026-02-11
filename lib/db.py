"""
Funções de normalização e upsert para as tabelas principais:

- ebay_listing
- amazon_products

Centraliza a conversão de DataFrame (pandas) em linhas prontas para INSERT/UPDATE
via SQLAlchemy, garantindo tipos e valores padrão coerentes.
"""

from __future__ import annotations

from typing import List, Any, Iterable, Optional, Set, Dict
from datetime import datetime

import math
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

    # Normaliza condition sem transformar None em "None"
    def _norm_condition(v: Any) -> Optional[str]:
        if v is None or v is pd.NA:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        s = str(v).strip()
        if not s:
            return None
        low = s.lower()
        if low in ("none", "nan", "<na>", "null"):
            return None
        return s.title()

    df["condition"] = df["condition"].apply(_norm_condition)

    # currency: se vier vazio/None/NaN, define USD (sem virar "NONE"/"NAN")
    def _norm_currency(v: Any) -> str:
        if v is None or v is pd.NA:
            return "USD"
        if isinstance(v, float) and np.isnan(v):
            return "USD"
        s = str(v).strip().upper()
        if s in ("", "NONE", "NAN", "<NA>", "NULL"):
            return "USD"
        return s

    df["currency"] = df["currency"].apply(_norm_currency)

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


def _get_table_columns(engine, table_name: str, schema: Optional[str] = None) -> List[str]:
    if schema is None:
        schema = engine.url.database
    sql = text("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema
          AND TABLE_NAME = :table
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"schema": schema, "table": table_name}).fetchall()
    return [r[0] for r in rows]


def upsert_ebay_listings(engine, df: pd.DataFrame, chunk_size: int = 500) -> int:
    """
    Upsert em ebay_listing preservando dados antigos quando o novo vier NULL/vazio.
    - first_seen_at: mantém o primeiro (não sobrescreve)
    - fetched_at: atualiza sempre que vier preenchido
    - image_url/item_url/title/price/...: só atualiza se vier valor válido
    """
    if df is None or df.empty:
        return 0

    # normaliza NaN -> None
    def _clean(v):
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        return v

    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    table = "ebay_listing"
    cols_exist = set(_get_table_columns(engine, table))

    # colunas candidatas (só usa as que existirem no schema)
    wanted = [
        "item_id", "title", "brand", "mpn", "gtin",
        "price", "currency", "available_qty", "qty_flag",
        "condition", "seller", "category_id",
        "item_url", "image_url",
        "first_seen_at", "fetched_at",
    ]
    cols = [c for c in wanted if c in df.columns and c in cols_exist]
    if "item_id" not in cols:
        raise RuntimeError("upsert_ebay_listings: preciso de item_id (coluna e/ou DF não tem).")

    df = df[cols].copy()

    records: List[Dict[str, Any]] = []
    for rec in df.to_dict(orient="records"):
        records.append({k: _clean(v) for k, v in rec.items()})

    insert_cols_sql = ", ".join([f"`{c}`" for c in cols])
    values_sql = ", ".join([f":{c}" for c in cols])

    # regra: não apagar com vazio/NULL
    def _upd_keep(col: str) -> str:
        # COALESCE(NULLIF(VALUES(col), ''), col)
        return f"`{col}` = COALESCE(NULLIF(VALUES(`{col}`), ''), `{col}`)"

    update_parts: List[str] = []

    # first_seen_at: só preenche se ainda estiver NULL
    if "first_seen_at" in cols_exist and "first_seen_at" in cols:
        update_parts.append("`first_seen_at` = COALESCE(`first_seen_at`, VALUES(`first_seen_at`))")

    # fetched_at: atualiza se vier preenchido
    if "fetched_at" in cols_exist and "fetched_at" in cols:
        update_parts.append("`fetched_at` = COALESCE(VALUES(`fetched_at`), `fetched_at`)")

    # demais campos: só atualiza se vier valor válido
    for c in cols:
        if c in ("item_id", "first_seen_at", "fetched_at"):
            continue
        update_parts.append(_upd_keep(c))

    if not update_parts:
        # sem colunas pra update além de PK (raro), mas evita SQL inválido
        update_parts.append("`item_id` = `item_id`")

    update_sql = ", ".join(update_parts)

    sql = text(f"""
        INSERT INTO `{table}` ({insert_cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE
          {update_sql}
    """)

    total = 0
    with engine.begin() as conn:
        for i in range(0, len(records), chunk_size):
            batch = records[i:i + chunk_size]
            conn.execute(sql, batch)
            total += len(batch)

    return total


def upsert_match_map(engine, rows: List[Dict[str, Any]], chunk_size: int = 500) -> int:
    """
    Upsert em match_map com proteção:
    - Se existir match_score na tabela, só substitui um match quando o score novo >= score atual.
    - Caso não exista match_score, faz COALESCE/NULLIF padrão (não apaga com vazio).
    """
    if not rows:
        return 0

    table = "match_map"
    cols_exist = set(_get_table_columns(engine, table))

    # tenta usar o máximo, mas só se existir na tabela
    wanted = [
        "asin", "item_id",
        "match_method", "match_score", "image_distance", "notes",
        "created_at", "updated_at", "last_validated_at",
    ]
    cols = [c for c in wanted if c in cols_exist]

    if "asin" not in cols_exist:
        raise RuntimeError("upsert_match_map: tabela match_map não tem coluna asin.")
    if "asin" not in cols:
        cols.insert(0, "asin")
    if "item_id" in cols_exist and "item_id" not in cols:
        cols.insert(1, "item_id")

    def _clean(v):
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        return v

    # garante timestamps se existirem
    now = datetime.utcnow()
    prepared: List[Dict[str, Any]] = []
    for r in rows:
        rr = {k: _clean(v) for k, v in r.items() if k in cols_exist}
        if "created_at" in cols_exist and rr.get("created_at") is None:
            rr["created_at"] = now
        if "updated_at" in cols_exist:
            rr["updated_at"] = now
        if "last_validated_at" in cols_exist and rr.get("last_validated_at") is None:
            rr["last_validated_at"] = now
        prepared.append(rr)

    # usa apenas colunas presentes no batch
    cols_in_batch = [c for c in cols if any(c in r for r in prepared)]
    if "asin" not in cols_in_batch:
        cols_in_batch.insert(0, "asin")
    if "item_id" in cols_exist and "item_id" not in cols_in_batch:
        cols_in_batch.insert(1, "item_id")

    insert_cols_sql = ", ".join([f"`{c}`" for c in cols_in_batch])
    values_sql = ", ".join([f":{c}" for c in cols_in_batch])

    update_parts: List[str] = []

    has_score = ("match_score" in cols_exist) and ("match_score" in cols_in_batch)

    if has_score:
        # só troca se o score novo >= score atual (ou atual é NULL)
        cond = "(VALUES(`match_score`) IS NOT NULL AND (`match_score` IS NULL OR VALUES(`match_score`) >= `match_score`))"

        for c in cols_exist:
            if c in ("asin", "created_at"):
                continue
            if c not in cols_in_batch:
                continue

            if c == "updated_at":
                # só atualiza updated_at quando ocorrer upgrade real (senão mantém)
                update_parts.append(f"`updated_at` = CASE WHEN {cond} THEN VALUES(`updated_at`) ELSE `updated_at` END")
                continue

            if c == "last_validated_at":
                update_parts.append(f"`last_validated_at` = CASE WHEN {cond} THEN VALUES(`last_validated_at`) ELSE `last_validated_at` END")
                continue

            # campos do match: só atualiza quando cond for true
            update_parts.append(f"`{c}` = CASE WHEN {cond} THEN VALUES(`{c}`) ELSE `{c}` END")

        # match_score em si: guarda o maior
        update_parts.append("`match_score` = GREATEST(COALESCE(`match_score`, 0), COALESCE(VALUES(`match_score`), 0))")

    else:
        # fallback simples: não apagar com vazio/NULL
        def _upd_keep(col: str) -> str:
            return f"`{col}` = COALESCE(NULLIF(VALUES(`{col}`), ''), `{col}`)"

        for c in cols_in_batch:
            if c in ("asin", "created_at"):
                continue
            if c == "updated_at":
                update_parts.append("`updated_at` = VALUES(`updated_at`)")
                continue
            update_parts.append(_upd_keep(c))

    if not update_parts:
        update_parts.append("`asin` = `asin`")

    update_sql = ", ".join(update_parts)

    sql = text(f"""
        INSERT INTO `{table}` ({insert_cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE
          {update_sql}
    """)

    total = 0
    with engine.begin() as conn:
        for i in range(0, len(prepared), chunk_size):
            batch = prepared[i:i + chunk_size]
            conn.execute(sql, batch)
            total += len(batch)

    return total

# ---------------------------------------------------------------------------
# Amazon: normalização e upsert
# ---------------------------------------------------------------------------

_AMAZON_PRODUCTS_COLS_CACHE: Optional[Set[str]] = None


def _get_table_columns(engine: Any, table_name: str) -> Optional[Set[str]]:
    """
    Retorna set com nomes de colunas da tabela (schema atual).
    Se falhar por qualquer motivo, retorna None (não quebra).
    """
    try:
        sql = text(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :t
            """
        )
        with engine.begin() as conn:
            rows = conn.execute(sql, {"t": table_name}).fetchall()
        cols = {str(r[0]) for r in rows if r and r[0]}
        return cols if cols else None
    except Exception:
        return None


def _amazon_products_columns(engine: Any) -> Set[str]:
    """
    Cache simples das colunas de amazon_products para decidir SQL (ex.: image_url).

    IMPORTANTE:
    - Não "cacheia vazio" quando dá erro ao ler schema, para não travar decisões erradas
      (ex.: coluna image_url existe, mas uma leitura falha e o processo fica achando que não existe).
    """
    global _AMAZON_PRODUCTS_COLS_CACHE
    if _AMAZON_PRODUCTS_COLS_CACHE is not None:
        return _AMAZON_PRODUCTS_COLS_CACHE

    cols = _get_table_columns(engine, "amazon_products")
    if cols is None:
        # falhou ler schema -> fallback seguro sem cache
        return set()

    _AMAZON_PRODUCTS_COLS_CACHE = cols
    return _AMAZON_PRODUCTS_COLS_CACHE


def sql_safe_amazon_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza DataFrame de produtos Amazon para inserção na tabela amazon_products.

    Importante:
    - Mantém is_prime como TRISTATE: 1 (Prime), 0 (Não Prime), NULL (Desconhecido)
    - Evita apagar fulfillment_channel quando vier vazio/None
    - Padroniza fulfillment_channel para valores CANÔNICOS no DB: AMAZON (FBA) / MFN (FBM)
    - Normaliza item_condition (New/Used/Refurbished...) e respeita VARCHAR(16)
    - Suporta image_url (thumbnail) quando existir no schema
    """
    df = df.copy()

    expected = [
        "asin",
        "marketplace_id",
        "title",
        "image_url",  # thumbnail
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
        "item_condition",
        "source_root_name",
        "source_child_name",
        "search_kw",
    ]

    # Garante todas as colunas
    for col in expected:
        if col not in df.columns:
            df[col] = None

    # -----------------------------
    # Helpers básicos
    # -----------------------------
    def _none_if_blank(v: Any) -> Any:
        if v is None or v is pd.NA:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            low = s.lower()
            if low in ("none", "nan", "<na>", "null"):
                return None
            return s
        return v

    # Sanitiza strings principais (evita '' indo pro INSERT)
    text_cols = [
        "asin",
        "title",
        "image_url",
        "brand",
        "browse_node_name",
        "gtin",
        "gtin_type",
        "sales_rank_category",
        "source_root_name",
        "source_child_name",
        "search_kw",
        "currency",
        "fulfillment_channel",
        "item_condition",
        "marketplace_id",
    ]
    for c in text_cols:
        df[c] = df[c].apply(_none_if_blank)

    # Numéricos
    df["browse_node_id"] = pd.to_numeric(df["browse_node_id"], errors="coerce").astype("Int64")
    df["sales_rank"] = pd.to_numeric(df["sales_rank"], errors="coerce").astype("Int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # marketplace_id: evita NULL/'' (sua coluna é NOT NULL)
    def _norm_marketplace(v: Any) -> str:
        if v is None or v is pd.NA:
            return "ATVPDKIKX0DER"
        if isinstance(v, float) and np.isnan(v):
            return "ATVPDKIKX0DER"
        s = str(v).strip()
        if not s or s.lower() in ("none", "nan", "<na>", "null"):
            return "ATVPDKIKX0DER"
        return s

    df["marketplace_id"] = df["marketplace_id"].apply(_norm_marketplace)

    # currency: fallback para USD
    def _norm_currency(v: Any) -> str:
        if v is None or v is pd.NA:
            return "USD"
        if isinstance(v, float) and np.isnan(v):
            return "USD"
        s = str(v).strip().upper()
        if s in ("", "NONE", "NAN", "<NA>", "NULL"):
            return "USD"
        return s

    df["currency"] = df["currency"].apply(_norm_currency)

    # is_prime: 1/0/NULL (não forçar 0 quando desconhecido)
    def _prime_to_int01_or_none(v: Any) -> Optional[int]:
        if v is None or v is pd.NA:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        if isinstance(v, (bool, np.bool_)):
            return 1 if bool(v) else 0
        if isinstance(v, (int, np.integer)):
            iv = int(v)
            return iv if iv in (0, 1) else None
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("", "none", "nan", "<na>", "null"):
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

    # fulfillment_channel: CANÔNICO no DB -> AMAZON (FBA) / MFN (FBM) / NULL
    def _norm_fulfillment(v: Any) -> Optional[str]:
        if v is None or v is pd.NA:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        s = str(v).strip().upper()
        if s in ("", "NONE", "NAN", "<NA>", "NULL"):
            return None

        # aceita variações e padroniza para o DB
        if s in ("AMAZON", "FBA", "AFN"):
            return "AMAZON"
        if s in ("MFN", "FBM", "MERCHANT", "SELLER"):
            return "MFN"

        # fallback: preserva, mas respeita VARCHAR(20)
        return s[:20]

    df["fulfillment_channel"] = df["fulfillment_channel"].apply(_norm_fulfillment)

    # item_condition: normaliza e respeita VARCHAR(16)
    def _norm_item_condition(v: Any) -> Optional[str]:
        if v is None or v is pd.NA:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        s = str(v).strip()
        if not s or s.lower() in ("none", "nan", "<na>", "null"):
            return None
        return s.title()[:16]

    df["item_condition"] = df["item_condition"].apply(_norm_item_condition)

    # image_url: limpa lixo e mantém None quando vazio
    def _norm_image_url(v: Any) -> Optional[str]:
        if v is None or v is pd.NA:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        s = str(v).strip()
        if not s or s.lower() in ("none", "nan", "<na>", "null"):
            return None
        return s

    df["image_url"] = df["image_url"].apply(_norm_image_url)

    # Converte NaN/NA restantes para None
    df = df.replace({np.nan: None, pd.NA: None})

    # Proteção extra: asin obrigatório e sem vazio
    df["asin"] = df["asin"].apply(lambda v: None if v is None else str(v).strip())
    df = df[df["asin"].notna() & (df["asin"] != "")].copy()

    # Dedup por asin (evita mandar duplicado pro executemany)
    df = df.drop_duplicates(subset=["asin"], keep="first").reset_index(drop=True)

    # Converte tudo para object (Python) para o driver do MySQL
    df = df.astype({c: object for c in expected})

    return df[expected]


def upsert_amazon_products(engine: Any, df: pd.DataFrame) -> int:
    """
    Insere/atualiza produtos na tabela amazon_products.

    Campos esperados (df):
      asin, marketplace_id, title, image_url, brand,
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

    # Decide se a tabela tem image_url (para não quebrar se schema ainda não foi alterado)
    cols = _amazon_products_columns(engine)
    has_image_url = "image_url" in cols

    if not has_image_url:
        # evita erro de bind param extra e/ou coluna inexistente no INSERT
        rows = rows.drop(columns=["image_url"], errors="ignore")

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

              -- condição (se vier NULL/'' não apaga o antigo)
              item_condition      = COALESCE(NULLIF(VALUES(item_condition), ''), item_condition),

              source_root_name    = COALESCE(NULLIF(VALUES(source_root_name), ''), source_root_name),
              source_child_name   = COALESCE(NULLIF(VALUES(source_child_name), ''), source_child_name),
              search_kw           = COALESCE(NULLIF(VALUES(search_kw), ''), search_kw),
              fetched_at          = NOW();
            """
        )
    else:
        sql = text(
            """
            INSERT INTO amazon_products
            (asin, marketplace_id, title, image_url, brand,
             browse_node_id, browse_node_name,
             gtin, gtin_type,
             sales_rank, sales_rank_category,
             price, currency,
             is_prime, fulfillment_channel, item_condition,
             source_root_name, source_child_name, search_kw,
             fetched_at)
            VALUES
            (:asin, :marketplace_id, :title, :image_url, :brand,
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

              -- thumbnail (se vier NULL/'' não apaga o antigo)
              image_url           = COALESCE(NULLIF(VALUES(image_url), ''), image_url),

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

              -- condição (se vier NULL/'' não apaga o antigo)
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

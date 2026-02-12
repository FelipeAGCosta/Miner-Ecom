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
# Helpers comuns (schema introspection)
# ---------------------------------------------------------------------------

def _get_table_columns(engine: Any, table_name: str) -> Set[str]:
    """
    Retorna set com nomes de colunas da tabela no schema atual (DATABASE()).
    Nunca retorna None (fallback seguro: set()).
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
        return cols
    except Exception:
        return set()


def _get_column_type(engine: Any, table_name: str, column_name: str) -> Optional[str]:
    """
    Retorna COLUMN_TYPE (ex.: "enum('GTIN','BRAND_MPN','TITLE')") ou None.
    """
    try:
        sql = text(
            """
            SELECT COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :t
              AND COLUMN_NAME = :c
            """
        )
        with engine.begin() as conn:
            row = conn.execute(sql, {"t": table_name, "c": column_name}).fetchone()
        if not row or not row[0]:
            return None
        return str(row[0])
    except Exception:
        return None


def _parse_enum_values(column_type: str) -> List[str]:
    """
    Parse simples de enum('A','B','C') -> ['A','B','C']
    """
    s = column_type.strip()
    if not s.lower().startswith("enum("):
        return []
    inner = s[s.find("(") + 1 : s.rfind(")")]
    vals: List[str] = []
    cur = ""
    in_quote = False
    escape = False
    for ch in inner:
        if escape:
            cur += ch
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == "'":
            in_quote = not in_quote
            if not in_quote:
                vals.append(cur)
                cur = ""
            continue
        if in_quote:
            cur += ch
    return vals


def _clean_scalar(v: Any) -> Any:
    """
    Normaliza NaN/NA/blank -> None para inserir no MySQL.
    """
    if v is None or v is pd.NA:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, (np.floating,)) and np.isnan(v):
        return None
    if isinstance(v, str):
        s = v.strip()
        return None if s == "" else v
    return v


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
        # opcional (não quebra quem não usa):
        "image_url",
        "first_seen_at",
        "fetched_at",
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

    # image_url: limpa blanks
    df["image_url"] = df["image_url"].apply(lambda v: _clean_scalar(v))

    # timestamps (se vierem como string)
    for c in ("first_seen_at", "fetched_at"):
        df[c] = pd.to_datetime(df[c], errors="coerce")
        df[c] = df[c].where(df[c].notna(), None)

    # Converte NaN/NA restantes para None
    df = df.replace({np.nan: None, pd.NA: None})

    # Tipos Python "object" para o execute do SQLAlchemy/PyMySQL
    df = df.astype({c: object for c in expected})

    return df[expected]

def _get_table_columns_info(engine, table_name: str, schema: Optional[str] = None) -> Dict[str, str]:
    """
    Retorna dict {coluna: data_type} da tabela.
    Ex.: {"price": "decimal", "title": "varchar", ...}
    """
    if schema is None:
        schema = engine.url.database

    sql = text("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema
          AND TABLE_NAME = :table
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"schema": schema, "table": table_name}).fetchall()

    out: Dict[str, str] = {}
    for r in rows:
        if not r:
            continue
        out[str(r[0])] = str(r[1]).lower() if r[1] is not None else ""
    return out

def upsert_ebay_listings(engine, df: pd.DataFrame, chunk_size: int = 500) -> int:
    """
    Upsert em ebay_listing preservando dados antigos quando o novo vier NULL/vazio.

    Fix importante:
    - Para colunas NUMÉRICAS (DECIMAL/INT/etc), NÃO usar NULLIF(...,'') no UPDATE,
      pois MySQL pode dar "Truncated incorrect DECIMAL value: ''".
    """
    if df is None or df.empty:
        return 0

    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    table = "ebay_listing"

    cols_info = _get_table_columns_info(engine, table)  # {col: datatype}
    cols_exist = set(cols_info.keys())

    numeric_types = {
        "decimal", "numeric", "float", "double",
        "int", "bigint", "smallint", "mediumint", "tinyint",
        "year"
    }
    numeric_cols = {c for c, t in cols_info.items() if t in numeric_types}

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

    def _clean(v):
        if v is None:
            return None
        if v is pd.NA:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        # pandas Timestamp -> datetime python
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime().replace(tzinfo=None)
        # evita '' indo pro SQL (principalmente em numéricos)
        if isinstance(v, str):
            s = v.strip()
            return s if s != "" else None
        return v

    records: List[Dict[str, Any]] = []
    for rec in df.to_dict(orient="records"):
        records.append({k: _clean(v) for k, v in rec.items()})

    insert_cols_sql = ", ".join([f"`{c}`" for c in cols])
    values_sql = ", ".join([f":{c}" for c in cols])

    # update: strings não devem apagar com vazio/NULL
    def _upd_keep_text(col: str) -> str:
        return f"`{col}` = COALESCE(NULLIF(VALUES(`{col}`), ''), `{col}`)"

    # update: numéricos: não usar NULLIF com ''
    def _upd_keep_numeric(col: str) -> str:
        return f"`{col}` = COALESCE(VALUES(`{col}`), `{col}`)"

    update_parts: List[str] = []

    # first_seen_at: só preenche se ainda estiver NULL
    if "first_seen_at" in cols_exist and "first_seen_at" in cols:
        update_parts.append("`first_seen_at` = COALESCE(`first_seen_at`, VALUES(`first_seen_at`))")

    # fetched_at: atualiza se vier preenchido
    if "fetched_at" in cols_exist and "fetched_at" in cols:
        update_parts.append("`fetched_at` = COALESCE(VALUES(`fetched_at`), `fetched_at`)")

    # demais campos
    for c in cols:
        if c in ("item_id", "first_seen_at", "fetched_at"):
            continue

        if c in numeric_cols:
            update_parts.append(_upd_keep_numeric(c))
        else:
            update_parts.append(_upd_keep_text(c))

    if not update_parts:
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
    Upsert em match_map alinhado ao seu schema REAL (ENUM + DECIMAL NOT NULL).

    Seu schema (pelo que você mostrou):
      item_id varchar(32) NOT NULL
      asin varchar(10) NOT NULL
      match_method enum('GTIN','BRAND_MPN','TITLE') NOT NULL
      match_score decimal(4,2) NOT NULL
      notes varchar(255) NULL
      created_at datetime NOT NULL
      (possíveis colunas extras não assumidas aqui)

    Regra de segurança:
      - Só "troca" asin/method/notes quando o score novo >= score atual.
      - match_score sempre guarda o maior.
    """
    if not rows:
        return 0

    table = "match_map"
    cols_exist = _get_table_columns(engine, table)

    required = {"item_id", "asin", "match_method", "match_score", "created_at"}
    missing = [c for c in required if c not in cols_exist]
    if missing:
        raise RuntimeError(f"upsert_match_map: tabela {table} sem colunas esperadas: {missing}")

    # Descobre valores permitidos do ENUM (fallback seguro)
    allowed_methods = {"GTIN", "BRAND_MPN", "TITLE"}
    ctype = _get_column_type(engine, table, "match_method")
    if ctype:
        enum_vals = _parse_enum_values(ctype)
        if enum_vals:
            allowed_methods = {v.upper() for v in enum_vals}

    now = datetime.utcnow()
    prepared: List[Dict[str, Any]] = []

    for r in rows:
        item_id = _clean_scalar(r.get("item_id"))
        asin = _clean_scalar(r.get("asin"))
        if item_id is None or asin is None:
            continue

        method = _clean_scalar(r.get("match_method"))
        method = str(method).strip().upper() if method is not None else "TITLE"
        if method not in allowed_methods:
            method = "TITLE"

        score = _clean_scalar(r.get("match_score"))
        try:
            score_f = float(score) if score is not None else 0.0
        except Exception:
            score_f = 0.0

        # DECIMAL(4,2): 0.00 a 99.99
        if score_f < 0:
            score_f = 0.0
        if score_f > 99.99:
            score_f = 99.99
        score_f = round(score_f, 2)

        notes = _clean_scalar(r.get("notes"))
        notes_s = str(notes)[:255] if notes is not None else None

        created_at = r.get("created_at") if r.get("created_at") is not None else now

        prepared.append(
            {
                "item_id": str(item_id),
                "asin": str(asin)[:10],
                "match_method": method,
                "match_score": score_f,
                "notes": notes_s,
                "created_at": created_at,
            }
        )

    if not prepared:
        return 0

    insert_cols = ["item_id", "asin", "match_method", "match_score", "notes", "created_at"]
    insert_cols_sql = ", ".join([f"`{c}`" for c in insert_cols])
    values_sql = ", ".join([f":{c}" for c in insert_cols])

    # Só atualiza campos "sensíveis" quando o score novo >= score atual
    cond = "(VALUES(`match_score`) >= `match_score`)"

    sql = text(f"""
        INSERT INTO `{table}` ({insert_cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE
          `match_score`  = GREATEST(`match_score`, VALUES(`match_score`)),
          `asin`         = CASE WHEN {cond} THEN VALUES(`asin`) ELSE `asin` END,
          `match_method` = CASE WHEN {cond} THEN VALUES(`match_method`) ELSE `match_method` END,
          `notes`        = CASE
                             WHEN {cond} THEN COALESCE(NULLIF(VALUES(`notes`), ''), `notes`)
                             ELSE `notes`
                           END,
          `created_at`   = COALESCE(`created_at`, VALUES(`created_at`))
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


def _amazon_products_columns(engine: Any) -> Set[str]:
    """
    Cache simples das colunas de amazon_products para decidir SQL (ex.: image_url).
    Não cacheia em caso de falha.
    """
    global _AMAZON_PRODUCTS_COLS_CACHE
    if _AMAZON_PRODUCTS_COLS_CACHE is not None:
        return _AMAZON_PRODUCTS_COLS_CACHE

    cols = _get_table_columns(engine, "amazon_products")
    if not cols:
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

    for col in expected:
        if col not in df.columns:
            df[col] = None

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

    df["browse_node_id"] = pd.to_numeric(df["browse_node_id"], errors="coerce").astype("Int64")
    df["sales_rank"] = pd.to_numeric(df["sales_rank"], errors="coerce").astype("Int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

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

    def _norm_fulfillment(v: Any) -> Optional[str]:
        if v is None or v is pd.NA:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        s = str(v).strip().upper()
        if s in ("", "NONE", "NAN", "<NA>", "NULL"):
            return None
        if s in ("AMAZON", "FBA", "AFN"):
            return "AMAZON"
        if s in ("MFN", "FBM", "MERCHANT", "SELLER"):
            return "MFN"
        return s[:20]

    df["fulfillment_channel"] = df["fulfillment_channel"].apply(_norm_fulfillment)

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

    df = df.replace({np.nan: None, pd.NA: None})

    df["asin"] = df["asin"].apply(lambda v: None if v is None else str(v).strip())
    df = df[df["asin"].notna() & (df["asin"] != "")].copy()

    df = df.drop_duplicates(subset=["asin"], keep="first").reset_index(drop=True)
    df = df.astype({c: object for c in expected})

    return df[expected]


def upsert_amazon_products(engine: Any, df: pd.DataFrame) -> int:
    """
    Insere/atualiza produtos na tabela amazon_products.
    """
    if df.empty:
        return 0

    rows = sql_safe_amazon_frame(df)

    cols = _amazon_products_columns(engine)
    has_image_url = "image_url" in cols

    if not has_image_url:
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
              is_prime            = COALESCE(VALUES(is_prime), is_prime),
              fulfillment_channel = COALESCE(NULLIF(VALUES(fulfillment_channel), ''), fulfillment_channel),
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
              is_prime            = COALESCE(VALUES(is_prime), is_prime),
              fulfillment_channel = COALESCE(NULLIF(VALUES(fulfillment_channel), ''), fulfillment_channel),
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
    return list(dict.fromkeys(out))


def get_existing_amazon_asins(
    engine: Any,
    asins: Iterable[str],
    marketplace_id: Optional[str],
) -> Set[str]:
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

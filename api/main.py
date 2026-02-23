from __future__ import annotations

from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Query
from sqlalchemy import text

from lib.config import make_engine
from api.models import MatchListResponse, MatchItem

app = FastAPI(title="miner-ecom API", version="0.4.5")

DEFAULT_MEDIA_REGEX = r"(movie|movies|dvd|blu(\s|-)?ray|book|books|kindle|music|cd|vinyl|video\s?game|video\s?games|tv)"


def _clamp_int(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(v)))


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


@app.get("/filters/categories")
def list_categories() -> Dict[str, Any]:
    """
    Retorna categorias/subcategorias existentes na amazon_products:
    {
      "categories": [
        {"name": "Casa & Cozinha", "children": ["Utensílios ...", ...]},
        ...
      ]
    }

    Remove AUTO - Browse Nodes.
    """
    sql = text("""
        SELECT source_root_name AS root, source_child_name AS child
        FROM amazon_products
        WHERE source_root_name IS NOT NULL AND source_root_name <> ''
    """)
    engine = make_engine()
    with engine.begin() as conn:
        rows = conn.execute(sql).fetchall()

    IGNORE = {"AUTO - BROWSE NODES", "AUTO - BROWSE NODE", "AUTO - BROWSE", "AUTO"}
    tree: Dict[str, set] = {}

    for r in rows:
        root = (r[0] or "").strip()
        child = (r[1] or "").strip()
        if not root:
            continue

        root_up = root.upper()
        if root_up in IGNORE or "BROWSE NODE" in root_up or root_up.startswith("AUTO"):
            continue

        tree.setdefault(root, set())
        if child:
            tree[root].add(child)

    categories = []
    for root in sorted(tree.keys(), key=lambda x: x.lower()):
        children = sorted(tree[root], key=lambda x: x.lower())
        categories.append({"name": root, "children": children})

    return {"categories": categories}


@app.get("/matches", response_model=MatchListResponse)
def list_matches(
    # paginação
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),

    # regras (UI não expõe, mas a API suporta)
    max_image_distance: int = Query(8, ge=0, le=20),
    gtin_max_dist: int = Query(15, ge=0, le=40),

    # filtros Amazon
    keyword: Optional[str] = None,
    source_root_name: Optional[str] = None,
    source_child_name: Optional[str] = None,
    amazon_price_min: Optional[float] = None,
    amazon_price_max: Optional[float] = None,
    amazon_condition: Optional[str] = None,
    amazon_fulfillment: Optional[str] = None,
    prime_only: Optional[int] = Query(None, ge=0, le=1),

    # filtros eBay
    ebay_price_min: Optional[float] = None,
    ebay_price_max: Optional[float] = None,
    ebay_condition: Optional[str] = None,

    # estoque eBay (min_available_qty)
    ebay_stock_min: Optional[int] = Query(None, ge=0),

    # ordenação
    # default agora alinhado com o comportamento desejado da UI (Amazon menor -> maior)
    sort: str = Query("amazon_price_asc"),

    # mostrar mídia?
    include_media: int = Query(0, ge=0, le=1),
):
    page_size = _clamp_int(page_size, 1, 200)
    offset = (page - 1) * page_size

    where: List[str] = []
    params: Dict[str, Any] = {
        "lim": page_size,
        "off": offset,
        "max_dist": int(max_image_distance),
        "gtin_max_dist": int(gtin_max_dist),
        "media_re": DEFAULT_MEDIA_REGEX,
    }

    # Gate de validade (mesma lógica de antes)
    where.append(
        "("
        " ("
        "   ((mo.validated_method = 'GTIN' OR (mo.validated_method IS NULL AND ap.gtin IS NOT NULL))"
        "    AND (mo.image_distance IS NULL OR mo.image_distance <= :gtin_max_dist))"
        " )"
        " OR "
        " ("
        "   ((mo.validated_method <> 'GTIN' OR (mo.validated_method IS NULL AND ap.gtin IS NULL))"
        "    AND mo.image_distance IS NOT NULL AND mo.image_distance <= :max_dist)"
        " )"
        ")"
    )

    if include_media != 1:
        where.append("(ap.browse_node_name IS NULL OR LOWER(ap.browse_node_name) NOT REGEXP :media_re)")

    # -------- Amazon filters --------
    if keyword:
        kw = keyword.strip().lower()
        if kw:
            where.append("(LOWER(ap.title) LIKE :kw OR LOWER(ap.brand) LIKE :kw)")
            params["kw"] = f"%{kw}%"

    if source_root_name:
        where.append("ap.source_root_name = :root")
        params["root"] = str(source_root_name).strip()

    if source_child_name:
        where.append("ap.source_child_name = :child")
        params["child"] = str(source_child_name).strip()

    if amazon_price_min is not None:
        where.append("ap.price >= :ap_min")
        params["ap_min"] = float(amazon_price_min)

    if amazon_price_max is not None:
        where.append("ap.price <= :ap_max")
        params["ap_max"] = float(amazon_price_max)

    if prime_only == 1:
        where.append("ap.is_prime = 1")

    if amazon_fulfillment:
        fc = str(amazon_fulfillment).strip().upper()
        if fc in ("ANY", ""):
            pass
        elif fc == "FBA":
            where.append("(UPPER(ap.fulfillment_channel) IN ('FBA','AMAZON','AFN'))")
        elif fc == "FBM":
            where.append("(UPPER(ap.fulfillment_channel) IN ('FBM','MFN','MERCHANT','SELLER'))")
        else:
            where.append("ap.fulfillment_channel = :ap_fc")
            params["ap_fc"] = str(amazon_fulfillment).strip()

    if amazon_condition:
        ac = str(amazon_condition).strip().upper()
        if ac in ("ANY", ""):
            pass
        elif ac == "NEW":
            where.append("ap.item_condition = 'New'")
        elif ac == "USED":
            where.append("ap.item_condition = 'Used'")
        elif ac == "REFURB":
            where.append("ap.item_condition IN ('Refurbished','Renewed','Reconditioned')")
        elif ac == "UNKNOWN":
            where.append("(ap.item_condition IS NULL OR ap.item_condition = '')")
        else:
            where.append("ap.item_condition = :ap_cond")
            params["ap_cond"] = str(amazon_condition).strip()

    # -------- eBay filters --------
    if ebay_price_min is not None:
        where.append("el.price >= :eb_min")
        params["eb_min"] = float(ebay_price_min)

    if ebay_price_max is not None:
        where.append("el.price <= :eb_max")
        params["eb_max"] = float(ebay_price_max)

    if ebay_condition:
        ec = str(ebay_condition).strip().upper()
        if ec in ("ANY", ""):
            pass
        elif ec == "NEW":
            where.append("LOWER(el.`condition`) LIKE 'new%'")
        elif ec == "USED":
            where.append("LOWER(el.`condition`) LIKE 'used%'")
        elif ec == "REFURB":
            where.append("(LOWER(el.`condition`) LIKE '%refurb%' OR LOWER(el.`condition`) LIKE '%renew%')")
        else:
            where.append("el.`condition` = :eb_cond")
            params["eb_cond"] = str(ebay_condition).strip()

    # -------- eBay stock filter --------
    if ebay_stock_min is not None:
        where.append("el.min_available_qty >= :eb_stock_min")
        params["eb_stock_min"] = int(ebay_stock_min)

    where_sql = " AND ".join(where) if where else "1=1"

    # -----------------------------------------
    # Ordenação
    # - NULLs por último (campo IS NULL ASC => não-null primeiro)
    # - tie-breaker asin ASC pra ficar determinístico (evitar “embaralhar”)
    # -----------------------------------------
    sort_map = {
        # antigos
        "recent": "created_at DESC, asin ASC",
        "spread_desc": "spread IS NULL ASC, spread DESC, asin ASC",
        "spread_pct_desc": "spread_pct IS NULL ASC, spread_pct DESC, asin ASC",
        "ebay_price_asc": "ebay_price IS NULL ASC, ebay_price ASC, asin ASC",
        "amazon_bsr_asc": "amazon_bsr IS NULL ASC, amazon_bsr ASC, asin ASC",
        "match_score_desc": "match_score DESC, created_at DESC, asin ASC",

        # novos (necessários pro sort automático do front)
        "amazon_price_asc": "amazon_price IS NULL ASC, amazon_price ASC, asin ASC",
        "amazon_price_desc": "amazon_price IS NULL ASC, amazon_price DESC, asin ASC",
        "ebay_price_desc": "ebay_price IS NULL ASC, ebay_price DESC, asin ASC",
        "ebay_stock_asc": (
            "ebay_min_available_qty IS NULL ASC, ebay_min_available_qty ASC, "
            "amazon_price IS NULL ASC, amazon_price ASC, asin ASC"
        ),
    }

    sort_key = (sort or "").strip().lower()
    order_by = sort_map.get(sort_key, sort_map["amazon_price_asc"])

    sql_total = text(f"""
        SELECT COUNT(DISTINCT mo.asin)
        FROM match_offers mo
        JOIN amazon_products ap ON ap.asin = mo.asin
        JOIN ebay_listing el ON el.item_id = mo.item_id
        WHERE {where_sql}
    """)

    sql_items = text(f"""
        WITH filtered AS (
          SELECT
            mo.updated_at AS created_at,
            mo.validated_method AS match_method,
            COALESCE(mo.validated_score, 0) AS match_score,
            mo.image_distance AS image_distance,

            ap.asin AS asin,
            ap.title AS amazon_title,
            ap.brand AS amazon_brand,
            ap.item_condition AS amazon_condition,
            ap.price AS amazon_price,
            ap.currency AS amazon_currency,
            ap.sales_rank AS amazon_bsr,
            ap.gtin AS amazon_gtin,
            ap.is_prime AS amazon_is_prime,
            ap.fulfillment_channel AS amazon_fulfillment,
            ap.browse_node_name AS amazon_browse_node_name,
            ap.image_url AS amazon_image_url,
            CONCAT('https://www.amazon.com/dp/', ap.asin) AS amazon_url,

            ap.source_root_name AS amazon_category_root,
            ap.source_child_name AS amazon_category_child,

            el.item_id AS item_id,
            el.title AS ebay_title,
            el.price AS ebay_price,
            el.currency AS ebay_currency,
            el.`condition` AS ebay_condition,
            el.seller AS ebay_seller,
            el.item_url AS ebay_url,

            -- estoque
            el.available_qty AS ebay_available_qty,
            el.qty_flag AS ebay_qty_flag,
            el.min_available_qty AS ebay_min_available_qty,
            el.availability_status AS ebay_availability_status,
            el.availability_threshold_type AS ebay_availability_threshold_type,
            el.availability_threshold AS ebay_availability_threshold,
            el.availability_updated_at AS ebay_availability_updated_at,

            (ap.price - el.price) AS spread,
            CASE
              WHEN el.price IS NOT NULL AND el.price > 0 AND ap.price IS NOT NULL
              THEN ((ap.price - el.price) / el.price) * 100
              ELSE NULL
            END AS spread_pct
          FROM match_offers mo
          JOIN amazon_products ap ON ap.asin = mo.asin
          JOIN ebay_listing el ON el.item_id = mo.item_id
          WHERE {where_sql}
        ),
        ranked AS (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY asin ORDER BY ebay_price ASC, item_id ASC) AS rn
          FROM filtered
        )
        SELECT
          created_at,
          match_method,
          match_score,
          image_distance,

          asin,
          amazon_title,
          amazon_brand,
          amazon_condition,
          amazon_price,
          amazon_currency,
          amazon_bsr,
          amazon_gtin,
          amazon_is_prime,
          amazon_fulfillment,
          amazon_browse_node_name,
          amazon_image_url,
          amazon_url,

          amazon_category_root,
          amazon_category_child,

          item_id,
          ebay_title,
          ebay_price,
          ebay_currency,
          ebay_condition,
          ebay_seller,
          ebay_url,

          ebay_available_qty,
          ebay_qty_flag,
          ebay_min_available_qty,
          ebay_availability_status,
          ebay_availability_threshold_type,
          ebay_availability_threshold,
          ebay_availability_updated_at,

          spread,
          spread_pct
        FROM ranked
        WHERE rn = 1
        ORDER BY {order_by}
        LIMIT :lim OFFSET :off
    """)

    engine = make_engine()
    with engine.begin() as conn:
        total = int(conn.execute(sql_total, params).scalar() or 0)
        rows = conn.execute(sql_items, params).mappings().all()

    items = [MatchItem(**dict(r)) for r in rows]
    return MatchListResponse(page=page, page_size=page_size, total=total, items=items)
"""
Página Streamlit: Match Amazon (DB) → eBay (ao vivo)

Fluxo:
1) Usuário escolhe filtros Amazon (consulta em amazon_products no MySQL)
2) Usuário escolhe filtros eBay (faixa de preço/condição etc.)
3) Gerar tabela: para cada item Amazon retornado, busca candidatos no eBay e escolhe o melhor match
4) (Opcional) Consultar estoque: usa get_item_detail(item_id) e filtra por quantidade mínima

Obs.: NÃO minera Amazon aqui. Essa página usa a base local amazon_products.
"""

import os
import re
import time
import base64
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd
import requests
import streamlit as st
from sqlalchemy import text

from lib.config import make_engine
from lib.tasks import load_categories_tree, flatten_categories
from ebay_client import get_item_detail  # estoque (2ª etapa)

# ---------------------------------------------------------------------------
# Configs internas (não aparecem pro usuário)
# ---------------------------------------------------------------------------

AMAZON_DB_LIMIT = int(os.getenv("AMAZON_DB_LIMIT", "300"))  # limite padrão de candidatos no DB
EBAY_SEARCH_LIMIT = int(os.getenv("EBAY_SEARCH_LIMIT", "20"))  # resultados por item no eBay
EBAY_STOCK_MAX_ITEMS = int(os.getenv("EBAY_STOCK_MAX_ITEMS", "2000"))  # limite segurança (estoque)

# Regras internas de "match exato"
MIN_SCORE_TITLE_WITH_BRAND = float(os.getenv("MIN_SCORE_TITLE_WITH_BRAND", "92.0"))
MIN_SCORE_TITLE_NO_BRAND = float(os.getenv("MIN_SCORE_TITLE_NO_BRAND", "95.0"))
MIN_SCORE_GTIN = float(os.getenv("MIN_SCORE_GTIN", "85.0"))

# ---------------------------------------------------------------------------
# FORCE: Amazon-only (desativa eBay completamente nesta página)
# - default: ligado ("1") para você validar somente filtros do DB Amazon.
# - para reativar: adicione FORCE_AMAZON_ONLY=0 no .env (ou mude aqui).
# ---------------------------------------------------------------------------
FORCE_AMAZON_ONLY = (os.getenv("FORCE_AMAZON_ONLY", "1").strip() == "1")

# ---------------------------------------------------------------------------
# CSS global
# ---------------------------------------------------------------------------

CSS_PATH = Path(__file__).resolve().parent.parent / "assets" / "style.css"
if CSS_PATH.exists():
    st.markdown(f"<style>{CSS_PATH.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

st.markdown("<div class='page-shell'>", unsafe_allow_html=True)

st.markdown(
    """
    <div class="page-header">
      <div class="page-header-tag"></div>
      <h1 class="page-header-title">Match Amazon → eBay (ao vivo)</h1>
      <p class="page-header-subtitle">
        Filtra produtos da sua base <code>amazon_products</code> e encontra matches no eBay em tempo real.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)

# Bloquear digitação nos selectboxes (apenas seleção por clique)
st.markdown(
    """
    <style>
    /* Streamlit selectbox usa baseweb select com input interno para busca.
       Isso desabilita digitação, mas mantém seleção via dropdown. */
    div[data-baseweb="select"] input {
        pointer-events: none !important;
        caret-color: transparent !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

tree = load_categories_tree()
_ = flatten_categories(tree)

def _find_node_by_name(nodes: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    for n in nodes:
        if n.get("name") == name:
            return n
        for ch in n.get("children", []) or []:
            if ch.get("name") == name:
                return ch
    return None

def _norm_text(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _title_query_from_amazon(title: str, brand: Optional[str], max_words: int = 10) -> str:
    t = _norm_text(title)
    b = _norm_text(brand or "")
    words = t.split()
    parts = []
    if b:
        parts.extend(b.split())
    parts.extend(words)

    out: List[str] = []
    for w in parts:
        if w and w not in out:
            out.append(w)
        if len(out) >= max_words:
            break
    return " ".join(out) if out else "a"

def _similarity(a: str, b: str) -> float:
    from difflib import SequenceMatcher
    return SequenceMatcher(None, _norm_text(a), _norm_text(b)).ratio()

def _amazon_url(asin: Optional[str]) -> Optional[str]:
    asin = (asin or "").strip()
    return f"https://www.amazon.com/dp/{asin}" if asin else None

def _prime_status(v: Any) -> str:
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "❓ Desconhecido"
        iv = int(v)
        if iv == 1:
            return "✅ Prime"
        if iv == 0:
            return "❌ Não Prime"
        return "❓ Desconhecido"
    except Exception:
        return "❓ Desconhecido"

def _fulfillment_mode(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "Desconhecido"
    s = str(v).strip().upper()
    if not s:
        return "Desconhecido"
    # suporta base antiga e nova
    if s in ("FBA", "AMAZON", "AFN"):
        return "FBA"
    if s in ("FBM", "MFN", "MERCHANT", "SELLER"):
        return "FBM"
    return s

def _norm_condition(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "Desconhecida"
    s = str(v).strip()
    return s if s else "Desconhecida"

# ---------------------------------------------------------------------------
# eBay: token + search (Browse API)
# ---------------------------------------------------------------------------

def _ebay_base_url() -> str:
    env = (os.getenv("EBAY_ENV") or "").lower().strip()
    if "sand" in env:
        return "https://api.sandbox.ebay.com"
    return "https://api.ebay.com"

def _ebay_marketplace_id() -> str:
    return (os.getenv("EBAY_MARKETPLACE_ID") or "EBAY_US").strip()

def _ebay_currency() -> str:
    return (os.getenv("EBAY_CURRENCY") or "USD").strip()

@st.cache_data(ttl=7000)
def _ebay_get_app_token(client_id: str, client_secret: str) -> str:
    base = _ebay_base_url()
    token_url = f"{base}/identity/v1/oauth2/token"

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {basic}",
    }
    data = {"grant_type": "client_credentials", "scope": "https://api.ebay.com/oauth/api_scope"}
    resp = requests.post(token_url, headers=headers, data=data, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Falha ao obter token eBay ({resp.status_code}): {resp.text[:400]}")
    return resp.json()["access_token"]

def _ebay_search_item_summaries(
    token: str,
    q: Optional[str],
    gtin: Optional[str],
    price_min: Optional[float],
    price_max: Optional[float],
    condition_ids: Optional[List[int]],
    limit: int,
) -> List[Dict[str, Any]]:
    base = _ebay_base_url()
    url = f"{base}/buy/browse/v1/item_summary/search"

    params: Dict[str, str] = {}
    if gtin:
        params["gtin"] = gtin
    else:
        params["q"] = q or "a"

    filters = ["buyingOptions:{FIXED_PRICE}"]

    if condition_ids:
        joined = "|".join(str(x) for x in condition_ids)
        filters.append(f"conditionIds:{{{joined}}}")

    cur = _ebay_currency()
    if price_min is not None or price_max is not None:
        if price_min is None:
            price_expr = f"price:[..{price_max}]"
        elif price_max is None:
            price_expr = f"price:[{price_min}..]"
        else:
            price_expr = f"price:[{price_min}..{price_max}]"
        filters.append(price_expr)
        filters.append(f"priceCurrency:{cur}")

    params["filter"] = ",".join(filters)
    params["limit"] = str(max(1, min(int(limit), 50)))
    params["offset"] = "0"

    headers = {"Authorization": f"Bearer {token}", "X-EBAY-C-MARKETPLACE-ID": _ebay_marketplace_id()}

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    if resp.status_code == 429:
        time.sleep(1.0)
        resp = requests.get(url, headers=headers, params=params, timeout=30)

    if resp.status_code != 200:
        raise RuntimeError(f"eBay search falhou ({resp.status_code}): {resp.text[:400]}")

    data = resp.json() or {}
    return data.get("itemSummaries") or []

def _pick_best_match(
    amazon_title: str,
    amazon_brand: Optional[str],
    has_gtin: bool,
    ebay_items: List[Dict[str, Any]],
    amazon_price: Optional[float],
) -> Optional[Dict[str, Any]]:
    if not ebay_items:
        return None

    best = None
    best_score = -1.0

    for it in ebay_items:
        t = it.get("title") or ""
        score = _similarity(amazon_title, t)

        # bônus por brand aparecer no título
        if amazon_brand:
            b = _norm_text(amazon_brand)
            if b and b in _norm_text(t):
                score += 0.05

        if score > best_score:
            best_score = score
            best = it

    if not best:
        return None

    score_pct = best_score * 100.0

    # regra interna: "match exato"
    if has_gtin:
        if score_pct < MIN_SCORE_GTIN:
            return None
    else:
        if amazon_brand:
            if score_pct < MIN_SCORE_TITLE_WITH_BRAND:
                return None
        else:
            if score_pct < MIN_SCORE_TITLE_NO_BRAND:
                return None

    def _money_val(m: Any) -> Optional[float]:
        try:
            if not isinstance(m, dict):
                return None
            v = m.get("value")
            return float(v) if v is not None else None
        except Exception:
            return None

    price = _money_val(best.get("price"))

    ship_cost = None
    ship_opts = best.get("shippingOptions") or []
    if isinstance(ship_opts, list) and ship_opts:
        ship_cost = _money_val((ship_opts[0] or {}).get("shippingCost"))

    total = None
    if price is not None:
        total = price + (ship_cost or 0.0)

    # Keepa-like: Spread = Amazon - eBay (positivo é "bom" p/ arbitragem)
    spread = None
    spread_pct = None
    if amazon_price is not None and total is not None and total > 0:
        spread = amazon_price - total
        spread_pct = (spread / total) * 100.0

    return {
        "score": round(score_pct, 2),
        "item_id": best.get("itemId"),
        "ebay_title": best.get("title"),
        "ebay_price": price,
        "ebay_shipping": ship_cost,
        "ebay_total": total,
        "ebay_url": best.get("itemWebUrl") or best.get("itemAffiliateWebUrl"),
        "ebay_condition": best.get("condition"),
        "ebay_condition_id": best.get("conditionId"),
        "spread": spread,
        "spread_pct": spread_pct,
    }

# ---------------------------------------------------------------------------
# MySQL: carregar candidatos Amazon (do DB)
# ---------------------------------------------------------------------------

def _count_prime(engine) -> int:
    try:
        with engine.connect() as conn:
            r = conn.execute(text("SELECT COUNT(*) FROM amazon_products WHERE is_prime = 1"))
            return int(r.scalar() or 0)
    except Exception:
        return 0

def _load_amazon_from_db(
    engine,
    source_root_name: Optional[str],
    source_child_name: Optional[str],
    keyword: Optional[str],
    price_min: Optional[float],
    price_max: Optional[float],
    prime_only: bool,
    fulfillment_mode: str,      # ANY / FBA / FBM
    amazon_condition: str,      # ANY / NEW / USED / REFURB / UNKNOWN
    limit_rows: int,
) -> pd.DataFrame:
    where = ["1=1", "price IS NOT NULL"]
    params: Dict[str, Any] = {}

    if source_root_name:
        where.append("source_root_name = :root")
        params["root"] = source_root_name

    if source_child_name:
        where.append("source_child_name = :child")
        params["child"] = source_child_name

    if keyword:
        where.append("(title LIKE :kw OR brand LIKE :kw OR search_kw LIKE :kw)")
        params["kw"] = f"%{keyword.strip()}%"

    if price_min is not None:
        where.append("price >= :pmin")
        params["pmin"] = float(price_min)

    if price_max is not None:
        where.append("price <= :pmax")
        params["pmax"] = float(price_max)

    # Prime estrito: somente is_prime = 1
    if prime_only:
        where.append("is_prime = 1")

    # Fulfillment estrito, mas compatível com bases antigas e novas
    if fulfillment_mode == "FBA":
        where.append("(fulfillment_channel IN ('FBA','AMAZON','AFN'))")

    elif fulfillment_mode == "FBM":
        where.append("(fulfillment_channel IN ('FBM','MFN','MERCHANT'))")

    # Condição Amazon (DB) — estrita
    # Valores esperados: New / Used / Refurbished ... (ou NULL)
    if amazon_condition == "NEW":
        where.append("item_condition = 'New'")
    elif amazon_condition == "USED":
        where.append("item_condition = 'Used'")
    elif amazon_condition == "REFURB":
        where.append("item_condition IN ('Refurbished','Renewed','Reconditioned')")
    elif amazon_condition == "UNKNOWN":
        where.append("item_condition IS NULL")

    sql = f"""
        SELECT
            asin,
            title,
            brand,
            gtin,
            gtin_type,
            sales_rank,
            sales_rank_category,
            price,
            currency,
            is_prime,
            fulfillment_channel,
            item_condition,
            browse_node_name,
            source_root_name,
            source_child_name,
            fetched_at
        FROM amazon_products
        WHERE {" AND ".join(where)}
        ORDER BY fetched_at DESC
        LIMIT {int(limit_rows)}
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    return df

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

# Card Amazon
st.markdown(
    """
    <div class='card'>
      <div class='card-title'>
        <div class='card-title-icon'>📦</div>
        <div>Filtros Amazon (base local)</div>
      </div>
      <p class='card-caption'>
        Aqui buscamos no seu banco (<code>amazon_products</code>). O eBay é consultado em tempo real depois.
      </p>
    """,
    unsafe_allow_html=True,
)

user_kw = st.text_input("Palavra-chave (opcional)", value="").strip() or None

col_cat1, col_cat2 = st.columns([1.6, 1.6])
with col_cat1:
    root_names = ["Todas as categorias"] + [n.get("name") for n in tree if n.get("name")]
    sel_root = st.selectbox("Categoria", root_names, index=0)

with col_cat2:
    child_names = ["Todas as subcategorias"]
    parent_node = _find_node_by_name(tree, sel_root) if sel_root != "Todas as categorias" else None
    if parent_node and parent_node.get("children"):
        for ch in parent_node.get("children", []) or []:
            if ch.get("name"):
                child_names.append(ch["name"])
    sel_child = st.selectbox("Subcategoria (opcional)", child_names, index=0)

source_root_name = sel_root if sel_root != "Todas as categorias" else None
source_child_name = sel_child if sel_child != "Todas as subcategorias" else None

cA, cB, cC, cD = st.columns(4)
with cA:
    amazon_price_min = st.number_input("Preço mínimo (Amazon)", min_value=0.0, value=0.0, step=1.0)
with cB:
    amazon_price_max = st.number_input("Preço máximo (Amazon)", min_value=0.0, value=0.0, step=1.0)
with cC:
    prime_only = st.checkbox("Somente Prime", value=False)
with cD:
    fulfillment_pt = st.selectbox(
        "Logística (Amazon)",
        ["Qualquer", "Enviado pela Amazon (FBA)", "Enviado pelo vendedor (FBM)"],
        index=0,
    )

amazon_price_min = None if amazon_price_min <= 0 else float(amazon_price_min)
amazon_price_max = None if amazon_price_max <= 0 else float(amazon_price_max)

fulfillment_mode = "ANY"
if fulfillment_pt == "Enviado pela Amazon (FBA)":
    fulfillment_mode = "FBA"
elif fulfillment_pt == "Enviado pelo vendedor (FBM)":
    fulfillment_mode = "FBM"

# Condição Amazon (do DB)
condA = st.selectbox(
    "Condição (Amazon - DB)",
    ["Qualquer", "Novo", "Usado", "Recondicionado", "Desconhecida"],
    index=0,
)

amazon_condition = "ANY"
if condA == "Novo":
    amazon_condition = "NEW"
elif condA == "Usado":
    amazon_condition = "USED"
elif condA == "Recondicionado":
    amazon_condition = "REFURB"
elif condA == "Desconhecida":
    amazon_condition = "UNKNOWN"

st.markdown("</div>", unsafe_allow_html=True)

# Card eBay
st.markdown(
    """
    <div class='card'>
      <div class='card-title'>
        <div class='card-title-icon'>🛒</div>
        <div>Filtros eBay (ao vivo)</div>
      </div>
      <p class='card-caption'>
        Procuramos o match mais exato possível. Se não for match forte, o item fica sem match.
      </p>
    """,
    unsafe_allow_html=True,
)

e1, e2, e3 = st.columns(3)
with e1:
    ebay_price_min = st.number_input("Preço mínimo (eBay)", min_value=0.0, value=0.0, step=1.0)
with e2:
    ebay_price_max = st.number_input("Preço máximo (eBay)", min_value=0.0, value=0.0, step=1.0)
with e3:
    cond_sel = st.selectbox("Condição (eBay)", ["Qualquer", "Novo", "Usado", "Recondicionado"], index=0)

ebay_price_min = None if ebay_price_min <= 0 else float(ebay_price_min)
ebay_price_max = None if ebay_price_max <= 0 else float(ebay_price_max)

condition_ids = None
if cond_sel == "Novo":
    condition_ids = [1000]
elif cond_sel == "Usado":
    condition_ids = [3000]
elif cond_sel == "Recondicionado":
    condition_ids = [2000, 2010, 2020, 2030]

st.markdown("</div>", unsafe_allow_html=True)

st.markdown("### ⚡ Gerar tabela final (Amazon DB → eBay ao vivo)")
btn_run = st.button("Gerar tabela", use_container_width=True)

# ---------------------------------------------------------------------------
# Render tabela Keepa-like
# ---------------------------------------------------------------------------

def _render_keepa_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.warning("Nenhum resultado para exibir.")
        return

    show = df.copy()

    # números
    for c in ["amazon_price", "ebay_total", "spread", "spread_pct", "score", "amazon_sales_rank", "available_qty"]:
        if c in show.columns:
            show[c] = pd.to_numeric(show[c], errors="coerce")

    # ordenar por maior spread (oportunidade) desc
    show = show.sort_values(by=["spread", "score"], ascending=[False, False], na_position="last")

    keep_cols = [
        "amazon_title",
        "amazon_brand",
        "amazon_item_condition",
        "amazon_price",
        "amazon_sales_rank",
        "amazon_url",
        "ebay_total",
        "spread",
        "spread_pct",
        "ebay_url",
        "score",
        "available_qty",
    ]
    keep_cols = [c for c in keep_cols if c in show.columns]
    show = show[keep_cols].copy()

    st.dataframe(
        show,
        use_container_width=True,
        hide_index=True,
        height=560,
        column_config={
            "amazon_title": "Produto (Amazon)",
            "amazon_brand": "Marca",
            "amazon_item_condition": "Condição (Amazon)",
            "amazon_price": st.column_config.NumberColumn("Preço Amazon", format="$%.2f"),
            "amazon_sales_rank": st.column_config.NumberColumn("BSR", format="%d"),
            "amazon_url": st.column_config.LinkColumn("Link Amazon", display_text="Abrir"),
            "ebay_total": st.column_config.NumberColumn("Total eBay", format="$%.2f"),
            "spread": st.column_config.NumberColumn("Spread (Amazon - eBay)", format="$%.2f"),
            "spread_pct": st.column_config.NumberColumn("Spread %", format="%.2f"),
            "ebay_url": st.column_config.LinkColumn("Link eBay", display_text="Abrir"),
            "score": st.column_config.NumberColumn("Score match", format="%.2f"),
            "available_qty": st.column_config.NumberColumn("Estoque eBay", format="%d"),
        },
    )

# ---------------------------------------------------------------------------
# Execução do match
# ---------------------------------------------------------------------------

if btn_run:
    try:
        engine = make_engine()
    except Exception as e:
        st.error(f"Falha ao conectar no MySQL: {e}")
        st.stop()

    # Prime: desabilita automaticamente se banco não tiver nada
    prime_count = _count_prime(engine)
    if prime_only and prime_count == 0:
        st.warning("Sua base não tem itens Prime (is_prime=1). Desmarcando filtro 'Somente Prime'.")
        prime_only = False

    with st.spinner("Carregando produtos da Amazon (do banco)..."):
        am_df = _load_amazon_from_db(
            engine=engine,
            source_root_name=source_root_name,
            source_child_name=source_child_name,
            keyword=user_kw,
            price_min=amazon_price_min,
            price_max=amazon_price_max,
            prime_only=prime_only,
            fulfillment_mode=fulfillment_mode,
            amazon_condition=amazon_condition,
            limit_rows=AMAZON_DB_LIMIT,
        )

    if am_df.empty:
        st.warning("Nenhum produto da Amazon encontrado com esses filtros.")
        st.stop()

    if len(am_df) >= AMAZON_DB_LIMIT:
        st.info(
            f"Encontramos muitos resultados. Mostrando até {AMAZON_DB_LIMIT} itens (limite padrão do app). "
            f"Se quiser aumentar nos testes, ajuste AMAZON_DB_LIMIT no .env."
        )

    # -----------------------------------------------------------------------
    # FORCE Amazon-only: mostra apenas resultados do DB e interrompe antes do eBay
    # -----------------------------------------------------------------------
    if FORCE_AMAZON_ONLY:
        st.success("Modo Amazon-only ativo: eBay desativado. Exibindo apenas resultados filtrados do DB Amazon.")

        show = am_df.copy()
        show["amazon_url"] = show["asin"].apply(_amazon_url)

        # colunas “amigáveis” (tri-state + fulfillment normalizado)
        show["prime_status"] = show["is_prime"].apply(_prime_status)
        show["fulfillment_mode"] = show["fulfillment_channel"].apply(_fulfillment_mode)
        show["item_condition_view"] = show["item_condition"].apply(_norm_condition)

        preferred_cols = [
            "title",
            "brand",
            "item_condition_view",
            "price",
            "sales_rank",
            "amazon_url",
            "gtin",
            "prime_status",
            "fulfillment_mode",
            "browse_node_name",
            "source_root_name",
            "source_child_name",
            "fetched_at",
        ]
        cols = [c for c in preferred_cols if c in show.columns]
        show_view = show[cols].copy()

        st.metric("Itens Amazon retornados", len(show_view))

        st.dataframe(
            show_view,
            use_container_width=True,
            hide_index=True,
            height=560,
            column_config={
                "title": "Produto (Amazon)",
                "brand": "Marca",
                "item_condition_view": "Condição (Amazon)",
                "price": st.column_config.NumberColumn("Preço Amazon", format="$%.2f"),
                "sales_rank": st.column_config.NumberColumn("BSR", format="%d"),
                "amazon_url": st.column_config.LinkColumn("Link Amazon", display_text="Abrir"),
                "gtin": "GTIN",
                "prime_status": "Prime (tri-state)",
                "fulfillment_mode": "Fulfillment",
                "browse_node_name": "Browse node",
                "source_root_name": "Categoria (root)",
                "source_child_name": "Subcategoria (child)",
                "fetched_at": "Fetched at",
            },
        )

        # Export CSV (do resultado filtrado da Amazon)
        csv_bytes = show.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Exportar CSV (Amazon DB filtrado)",
            data=csv_bytes,
            file_name="amazon_db_filtrado.csv",
            mime="text/csv",
            use_container_width=False,
        )

        # Debug (opcional)
        with st.expander("Detalhes (debug) - Amazon DB filtrado"):
            st.dataframe(show, use_container_width=True, hide_index=True)

        st.stop()

    client_id = (os.getenv("EBAY_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("EBAY_CLIENT_SECRET") or "").strip()
    if not client_id or not client_secret:
        st.error("Faltou EBAY_CLIENT_ID e/ou EBAY_CLIENT_SECRET no .env válido.")
        st.stop()

    try:
        token = _ebay_get_app_token(client_id, client_secret)
    except Exception as e:
        st.error(f"Falha ao obter token eBay: {e}")
        st.stop()

    progress = st.progress(0.0, text="Rodando match no eBay...")
    out_rows: List[Dict[str, Any]] = []
    errors = 0
    matched = 0

    total = len(am_df)
    for _, row in am_df.iterrows():
        idx = len(out_rows) + 1
        progress.progress(idx / max(1, total), text=f"Match no eBay... {idx}/{total}")

        asin = row.get("asin")
        title = row.get("title") or ""
        brand = row.get("brand")

        gtin = row.get("gtin")
        gtin = gtin.strip() if isinstance(gtin, str) else None
        has_gtin = bool(gtin)

        amazon_price = None
        try:
            amazon_price = float(row.get("price")) if row.get("price") is not None else None
        except Exception:
            amazon_price = None

        match = None
        try:
            q = None if has_gtin else _title_query_from_amazon(title, brand, max_words=10)
            ebay_items = _ebay_search_item_summaries(
                token=token,
                q=q,
                gtin=gtin if has_gtin else None,
                price_min=ebay_price_min,
                price_max=ebay_price_max,
                condition_ids=condition_ids,
                limit=int(EBAY_SEARCH_LIMIT),
            )
            match = _pick_best_match(
                amazon_title=title,
                amazon_brand=brand,
                has_gtin=has_gtin,
                ebay_items=ebay_items,
                amazon_price=amazon_price,
            )
        except Exception:
            errors += 1
            match = None

        base = {
            "asin": asin,
            "amazon_title": title,
            "amazon_brand": brand,
            "amazon_item_condition": row.get("item_condition"),
            "amazon_price": amazon_price,
            "amazon_sales_rank": row.get("sales_rank"),
            "amazon_sales_rank_category": row.get("sales_rank_category"),
            "amazon_url": _amazon_url(asin),
            "amazon_gtin": row.get("gtin"),
            "amazon_prime": _prime_status(row.get("is_prime")),
            "amazon_fulfillment": _fulfillment_mode(row.get("fulfillment_channel")),
            "fetched_at": row.get("fetched_at"),
            "source_root_name": row.get("source_root_name"),
            "source_child_name": row.get("source_child_name"),
        }

        if match:
            matched += 1
            base.update(match)
        else:
            base.update({
                "score": None,
                "item_id": None,
                "ebay_title": None,
                "ebay_total": None,
                "ebay_url": None,
                "spread": None,
                "spread_pct": None,
                "available_qty": None,
            })

        out_rows.append(base)

    progress.empty()

    res_df = pd.DataFrame(out_rows)

    st.session_state["_match_df"] = res_df.copy()
    st.session_state["_match_stage"] = "results"

    st.metric("Itens Amazon processados", len(am_df))
    st.metric("Matches encontrados", matched)
    st.metric("Sem match", max(0, len(am_df) - matched))
    st.metric("Erros eBay", errors)

    st.success("Tabela gerada.")
    _render_keepa_table(res_df)

    # Export CSV
    csv_bytes = res_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Exportar CSV",
        data=csv_bytes,
        file_name="match_amazon_ebay.csv",
        mime="text/csv",
        use_container_width=False,
    )

    # Debug (opcional)
    with st.expander("Detalhes (debug)"):
        st.dataframe(res_df, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Etapa 2: consultar estoque e filtrar por quantidade
# ---------------------------------------------------------------------------

if st.session_state.get("_match_stage") == "results" and isinstance(st.session_state.get("_match_df"), pd.DataFrame):
    df = st.session_state["_match_df"].copy()

    st.markdown("---")
    st.subheader("Consultar quantidade em estoque (opcional)")

    q1, q2 = st.columns([1, 2])
    with q1:
        min_qty = st.number_input("Quantidade mínima", min_value=0, value=0, step=1)
    with q2:
        keep_unknown = st.checkbox("Manter itens sem estoque conhecido", value=True)

    btn_qty = st.button("Consultar estoque e aplicar filtro", use_container_width=False, disabled=(min_qty <= 0))

    if btn_qty:
        if min_qty <= 0:
            st.info("Informe uma quantidade mínima maior que zero.")
        else:
            work = df[df["item_id"].notna()].copy()
            ids = work["item_id"].astype(str).unique().tolist()

            # consulta "todos" dentro de um limite de segurança interno
            if len(ids) > EBAY_STOCK_MAX_ITEMS:
                st.info(
                    f"A tabela tem {len(ids)} itens com item_id. "
                    f"Para proteger cota/tempo, consultaremos {EBAY_STOCK_MAX_ITEMS} itens agora. "
                    f"(Ajuste EBAY_STOCK_MAX_ITEMS no .env se quiser aumentar.)"
                )
                ids = ids[:EBAY_STOCK_MAX_ITEMS]

            st.info(f"Consultando detalhes de {len(ids)} itens no eBay...")
            prog2 = st.progress(0.0, text="Consultando estoque...")

            enr: List[Dict[str, Any]] = []
            for i, iid in enumerate(ids, start=1):
                prog2.progress(i / max(1, len(ids)), text=f"Consultando estoque... {i}/{len(ids)}")
                try:
                    d = get_item_detail(iid)
                except Exception as e:
                    d = {"item_id": iid, "available_qty": None, "qty_flag": f"ERROR:{type(e).__name__}"}
                enr.append(d)

            prog2.empty()

            enr_df = pd.DataFrame(enr)
            if "item_id" not in enr_df.columns or "available_qty" not in enr_df.columns:
                st.error("get_item_detail não retornou item_id/available_qty. Verifique ebay_client.py.")
                st.stop()

            df = df.merge(enr_df[["item_id", "available_qty"]], on="item_id", how="left", suffixes=("", "_enr"))
            if "available_qty_enr" in df.columns:
                df["available_qty"] = df["available_qty"].where(df["available_qty"].notna(), df["available_qty_enr"])
                df = df.drop(columns=["available_qty_enr"])

            qty_num = pd.to_numeric(df["available_qty"], errors="coerce")
            mask = qty_num.notna() & (qty_num >= int(min_qty))
            if keep_unknown:
                mask = mask | qty_num.isna()

            filtered = df[mask].copy()
            st.session_state["_match_df"] = filtered.copy()

            st.success(f"Após filtro de estoque: {len(filtered)} itens.")
            _render_keepa_table(filtered)

st.markdown("</div>", unsafe_allow_html=True)

"""
Página Streamlit: Match Amazon (DB) → eBay (ao vivo)

Fluxo:
1) Usuário escolhe filtros Amazon (consulta em amazon_products no MySQL)
2) Usuário escolhe filtros eBay (faixa de preço/condição etc.)
3) Gerar tabela: para cada item Amazon, busca candidatos no eBay Browse API e escolhe melhor match
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
from ebay_client import get_item_detail  # para "Consultar estoque" (2ª etapa)

# ---------------------------------------------------------------------------
# Configs internas (sem aparecer pro usuário)
# ---------------------------------------------------------------------------

AMAZON_DB_MAX_ROWS = int(os.getenv("AMAZON_DB_MAX_ROWS", "300"))          # quantos candidatos puxar do DB
MATCH_MAX_ITEMS = int(os.getenv("MATCH_MAX_ITEMS", "80"))                # quantos itens no máximo processar no eBay por clique
EBAY_SEARCH_LIMIT_DEFAULT = int(os.getenv("EBAY_SEARCH_LIMIT", "20"))     # quantos resultados pedir no eBay por item Amazon
EBAY_STOCK_MAX_ITEMS = int(os.getenv("EBAY_STOCK_MAX_ITEMS", "300"))     # limite de segurança para "Consultar estoque"

# Regras internas de "match exato"
# - Se não bater, NÃO retorna match (fica vazio)
MIN_SCORE_TITLE_WITH_BRAND = float(os.getenv("MIN_SCORE_TITLE_WITH_BRAND", "92.0"))
MIN_SCORE_TITLE_NO_BRAND = float(os.getenv("MIN_SCORE_TITLE_NO_BRAND", "95.0"))
MIN_SCORE_GTIN = float(os.getenv("MIN_SCORE_GTIN", "85.0"))

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

# ---------------------------------------------------------------------------
# Helpers: categorias e normalização
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

    out = []
    for w in parts:
        if w and w not in out:
            out.append(w)
        if len(out) >= max_words:
            break
    return " ".join(out) if out else "a"

def _similarity(a: str, b: str) -> float:
    from difflib import SequenceMatcher
    return SequenceMatcher(None, _norm_text(a), _norm_text(b)).ratio()

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
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }
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
    limit: int = 20,
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
            price_expr = f"price:[{price_min}]"
        else:
            price_expr = f"price:[{price_min}..{price_max}]"
        filters.append(price_expr)
        filters.append(f"priceCurrency:{cur}")

    params["filter"] = ",".join(filters)
    params["limit"] = str(max(1, min(int(limit), 50)))
    params["offset"] = "0"

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": _ebay_marketplace_id(),
    }

    resp = requests.get(url, headers=headers, params=params, timeout=30)

    if resp.status_code == 429:
        time.sleep(1.0)
        resp = requests.get(url, headers=headers, params=params, timeout=30)

    if resp.status_code != 200:
        raise RuntimeError(f"eBay search falhou ({resp.status_code}): {resp.text[:400]}")

    data = resp.json()
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

    # regra interna: "match exato" → se score baixo, retorna None
    if has_gtin:
        if (best_score * 100.0) < MIN_SCORE_GTIN:
            return None
    else:
        if amazon_brand and (best_score * 100.0) < MIN_SCORE_TITLE_WITH_BRAND:
            return None
        if (not amazon_brand) and (best_score * 100.0) < MIN_SCORE_TITLE_NO_BRAND:
            return None

    def _money_val(m: Any) -> Optional[float]:
        try:
            if not isinstance(m, dict):
                return None
            return float(m.get("value")) if m.get("value") is not None else None
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

    diff = None
    if amazon_price is not None and total is not None:
        diff = total - amazon_price

    return {
        "score": round(best_score * 100, 2),
        "item_id": best.get("itemId"),
        "ebay_title": best.get("title"),
        "ebay_price": price,
        "ebay_shipping": ship_cost,
        "ebay_total": total,
        "ebay_url": best.get("itemWebUrl") or best.get("itemAffiliateWebUrl"),
        "ebay_condition": best.get("condition"),
        "ebay_condition_id": best.get("conditionId"),
        "diff_total": diff,
    }

# ---------------------------------------------------------------------------
# MySQL: carregar candidatos Amazon (do DB)
# ---------------------------------------------------------------------------

def _load_amazon_from_db(
    engine,
    source_root_name: Optional[str],
    source_child_name: Optional[str],
    keyword: Optional[str],
    price_min: Optional[float],
    price_max: Optional[float],
    prime_only: bool,
    fulfillment_mode: str,
    max_rows: int,
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

    if prime_only:
        where.append("is_prime = 1")

    # Mapeamento para valores do banco (sem input do usuário)
    if fulfillment_mode == "FBA":
        where.append("fulfillment_channel = 'AMAZON'")
    elif fulfillment_mode == "FBM":
        where.append("(fulfillment_channel = 'MFN' OR fulfillment_channel = 'MERCHANT')")

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
            browse_node_id,
            browse_node_name,
            source_root_name,
            source_child_name,
            search_kw,
            fetched_at
        FROM amazon_products
        WHERE {" AND ".join(where)}
        ORDER BY fetched_at DESC
        LIMIT {int(max_rows)}
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    return df

# ---------------------------------------------------------------------------
# UI: filtros
# ---------------------------------------------------------------------------

st.markdown(
    """
    <div class='card'>
      <div class='card-title'>
        <div class='card-title-icon'>📦</div>
        <div>Filtros Amazon (base local)</div>
      </div>
      <p class='card-caption'>
        Aqui buscamos no seu banco (<code>amazon_products</code>), não chamamos a API da Amazon.
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
    sel_child = st.selectbox("Subcategoria (Opcional)", child_names, index=0)

source_root_name = sel_root if sel_root != "Todas as categorias" else None
source_child_name = sel_child if sel_child != "Todas as subcategorias" else None

cA, cB, cC, cD = st.columns(4)
with cA:
    amazon_price_min = st.number_input("Preço mínimo (Amazon)", min_value=0.0, value=0.0, step=1.0)
with cB:
    amazon_price_max = st.number_input("Preço máximo (Amazon)", min_value=0.0, value=0.0, step=1.0)
with cC:
    prime_only = st.checkbox(
        "Somente Prime",
        value=False,
        help="Filtra itens marcados como Prime na sua base (is_prime=1).",
    )
with cD:
    fulfillment_pt = st.selectbox(
        "Logística (Amazon)",
        ["Qualquer", "Enviado pela Amazon (FBA)", "Enviado pelo vendedor (FBM)"],
        index=0,
        help="FBA = entregue pela Amazon (fulfillment_channel='AMAZON'). FBM = entregue pelo vendedor (MFN/MERCHANT).",
    )

amazon_price_min = None if amazon_price_min <= 0 else float(amazon_price_min)
amazon_price_max = None if amazon_price_max <= 0 else float(amazon_price_max)

fulfillment_mode = "ANY"
if fulfillment_pt == "Enviado pela Amazon (FBA)":
    fulfillment_mode = "FBA"
elif fulfillment_pt == "Enviado pelo vendedor (FBM)":
    fulfillment_mode = "FBM"

st.markdown("</div>", unsafe_allow_html=True)

st.markdown(
    """
    <div class='card'>
      <div class='card-title'>
        <div class='card-title-icon'>🛒</div>
        <div>Filtros eBay (ao vivo)</div>
      </div>
      <p class='card-caption'>
        Aplicamos os filtros e procuramos o melhor match para cada item Amazon
        (GTIN quando houver, senão título+marca). Se não for match "exato", não retorna.
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

ebay_limit = EBAY_SEARCH_LIMIT_DEFAULT  # fixo (interno)

st.markdown("</div>", unsafe_allow_html=True)

st.markdown("### ⚡ Gerar tabela final (Amazon DB → eBay ao vivo)")
btn_run = st.button("Gerar tabela", use_container_width=True)

# ---------------------------------------------------------------------------
# Execução do match
# ---------------------------------------------------------------------------

def _render_results_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.warning("Nenhum resultado para exibir.")
        return

    show = df.copy()

    for c in ["amazon_price", "ebay_price", "ebay_shipping", "ebay_total", "diff_total", "score"]:
        if c in show.columns:
            show[c] = pd.to_numeric(show[c], errors="coerce")

    # aqui você pode escolher a ordenação que quiser
    show = show.sort_values(by=["diff_total", "score"], ascending=[True, False], na_position="last")

    st.dataframe(
        show,
        use_container_width=True,
        hide_index=True,
        height=520,
        column_config={
            "amazon_price": st.column_config.NumberColumn("Preço Amazon", format="$%.2f"),
            "ebay_total": st.column_config.NumberColumn("Total eBay (preço+frete)", format="$%.2f"),
            "diff_total": st.column_config.NumberColumn("Diferença (eBay - Amazon)", format="$%.2f"),
            "score": st.column_config.NumberColumn("Score match", format="%.2f"),
            "ebay_url": st.column_config.LinkColumn("Link eBay", display_text="Abrir"),
        },
    )

if btn_run:
    try:
        engine = make_engine()
    except Exception as e:
        st.error(f"Falha ao conectar no MySQL: {e}")
        st.stop()

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
            max_rows=int(AMAZON_DB_MAX_ROWS),
        )

    if am_df.empty:
        st.warning("Nenhum produto da Amazon encontrado com esses filtros.")
        st.stop()

    # processa "todos os resultados" dentro de um limite de segurança (interno)
    total_found = len(am_df)
    if total_found > MATCH_MAX_ITEMS:
        st.info(
            f"Foram encontrados {total_found} itens na Amazon (DB). "
            f"Para proteger a cota/tempo do eBay, processaremos {MATCH_MAX_ITEMS} itens neste clique."
        )
        am_df = am_df.head(int(MATCH_MAX_ITEMS)).copy()
    else:
        am_df = am_df.copy()

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
                limit=int(ebay_limit),
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
            "amazon_price": amazon_price,
            "amazon_currency": row.get("currency"),
            "amazon_gtin": row.get("gtin"),
            "amazon_sales_rank": row.get("sales_rank"),
            "amazon_sales_rank_category": row.get("sales_rank_category"),
            "amazon_browse_node": row.get("browse_node_name"),
            "source_root_name": row.get("source_root_name"),
            "source_child_name": row.get("source_child_name"),
            "fetched_at": row.get("fetched_at"),
        }

        if match:
            base.update(match)
        else:
            base.update({
                "score": None,
                "item_id": None,
                "ebay_title": None,
                "ebay_price": None,
                "ebay_shipping": None,
                "ebay_total": None,
                "diff_total": None,
                "ebay_url": None,
                "ebay_condition": None,
                "ebay_condition_id": None,
            })

        out_rows.append(base)

    progress.empty()

    res_df = pd.DataFrame(out_rows)

    st.session_state["_match_df"] = res_df.copy()
    st.session_state["_match_stage"] = "results"

    st.success(f"Match finalizado. Itens processados: {len(am_df)} | Erros eBay: {errors}")
    _render_results_table(res_df)

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
            total_ids = len(ids)
            if total_ids > EBAY_STOCK_MAX_ITEMS:
                st.info(
                    f"A tabela tem {total_ids} itens com item_id. "
                    f"Para proteger cota/tempo, consultaremos {EBAY_STOCK_MAX_ITEMS} itens agora."
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
            if "item_id" not in enr_df.columns:
                st.error("O retorno do get_item_detail não trouxe item_id. Verifique ebay_client.py.")
                st.stop()

            if "available_qty" not in enr_df.columns:
                st.error("O retorno do get_item_detail não trouxe available_qty. Verifique ebay_client.py.")
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
            _render_results_table(filtered)

st.markdown("</div>", unsafe_allow_html=True)

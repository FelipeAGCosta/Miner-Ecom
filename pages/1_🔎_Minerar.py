import math
import os
import urllib.parse as _url
from datetime import timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd
import streamlit as st

from integrations.amazon_matching import discover_amazon_products
from lib.tasks import flatten_categories, load_categories_tree
from ebay_client import get_item_detail  # ainda usado no filtro de quantidade eBay

# ---------------------------------------------------------------------------
# CSS global
# ---------------------------------------------------------------------------
CSS_PATH = Path(__file__).resolve().parent.parent / "assets" / "style.css"
if CSS_PATH.exists():
    st.markdown(f"<style>{CSS_PATH.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

st.markdown("<div class='page-shell'>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Cabeçalho da página
# ---------------------------------------------------------------------------
st.markdown(
    """
    <div class="page-header">
      <div class="page-header-tag"></div>
      <h1 class="page-header-title">Minerar produtos</h1>
      <p class="page-header-subtitle">
        Defina categoria Amazon e deixe o app trazer até 500 produtos distintos com preço na Amazon.com.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Stepper visual
# ---------------------------------------------------------------------------
stage = st.session_state.get("_stage", "filters")
step2_active = stage in ("running", "results")
step3_active = stage == "results"

st.markdown(
    f"""
    <div class="flow-steps">
      <div class="flow-step flow-step--active">
        <div class="flow-step-index">1</div>
        <div class="flow-step-text">
          <div class="flow-step-title">Filtros</div>
          <div class="flow-step-subtitle">Escolha categoria e palavra-chave</div>
        </div>
      </div>
      <div class="flow-step {'flow-step--active' if step2_active else ''}">
        <div class="flow-step-index">2</div>
        <div class="flow-step-text">
          <div class="flow-step-title">Minerar</div>
          <div class="flow-step-subtitle">Buscamos na Amazon</div>
        </div>
      </div>
      <div class="flow-step {'flow-step--active' if step3_active else ''}">
        <div class="flow-step-index">3</div>
        <div class="flow-step-text">
          <div class="flow-step-title">Resultados</div>
          <div class="flow-step-subtitle">Analise os produtos</div>
        </div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Carrega árvore de categorias (vem do search_tasks.yaml)
# ---------------------------------------------------------------------------
tree = load_categories_tree()
flat = flatten_categories(tree)  # ainda útil em outros fluxos / futuro

# ---------------------------------------------------------------------------
# Helpers de categoria / palavra-chave / browse_node_id
# ---------------------------------------------------------------------------
def _find_node_by_name(nodes: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    for n in nodes:
        if n.get("name") == name:
            return n
        for ch in n.get("children", []) or []:
            if ch.get("name") == name:
                return ch
    return None


def _kw_for_node(node: Optional[Dict[str, Any]]) -> str:
    if not node:
        return ""
    return (node.get("amazon_kw") or node.get("name") or "").strip()


# ---------------------------------------------------------------------------
# Card de filtros Amazon
# ---------------------------------------------------------------------------
st.markdown(
    """
    <div class='card'>
      <div class='card-title'>
        <div class='card-title-icon'>📦</div>
        <div>Filtros Amazon</div>
      </div>
      <p class='card-caption'>
        Escolha categoria/subcategoria em PT (usamos <code>amazon_kw</code> em EN na SP-API).
        Palavra-chave extra é opcional.
      </p>
    """,
    unsafe_allow_html=True,
)

# Palavra-chave livre
user_kw = st.text_input("Palavra-chave (opcional)", value="").strip()

# Categoria / subcategoria
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

# Resolve nós selecionados
selected_parent = parent_node if sel_root != "Todas as categorias" else None
selected_child = (
    _find_node_by_name(parent_node.get("children", []) if parent_node else [], sel_child)
    if sel_child != "Todas as subcategorias"
    else None
)

# Monta keyword final (user_kw + amazon_kw da categoria/subcategoria)
kw_parts: List[str] = []
if user_kw:
    kw_parts.append(user_kw)
if selected_child:
    kw_parts.append(_kw_for_node(selected_child))
elif selected_parent:
    kw_parts.append(_kw_for_node(selected_parent))

kw = " ".join(p for p in kw_parts if p).strip() or "a"
st.session_state["_kw"] = kw

# resolve browse_node_id a partir do category_id da subcategoria/categoria
browse_node_id: Optional[int] = None
if selected_child and selected_child.get("category_id") is not None:
    try:
        browse_node_id = int(selected_child["category_id"])
    except (TypeError, ValueError):
        browse_node_id = None
elif selected_parent and selected_parent.get("category_id") is not None:
    try:
        browse_node_id = int(selected_parent["category_id"])
    except (TypeError, ValueError):
        browse_node_id = None

st.session_state["_browse_node_id"] = browse_node_id

st.markdown("</div>", unsafe_allow_html=True)
st.caption(
    "Quanto mais ampla a busca, maior o tempo. "
    "Por enquanto focamos em trazer produtos \"brutos\" com preço na Amazon.com."
)

# ---------------------------------------------------------------------------
# Helpers de quantidade e tabela (eBay ainda mantido para uso futuro)
# ---------------------------------------------------------------------------
def _dedup(df: pd.DataFrame) -> pd.DataFrame:
    if "item_id" not in df.columns:
        return df
    return df.dropna(subset=["item_id"]).drop_duplicates(subset=["item_id"], keep="first").copy()


def _apply_condition_filter(df: pd.DataFrame, cond_pt: str) -> pd.DataFrame:
    if "condition" not in df.columns:
        return df
    cond = df["condition"].astype(str).str.lower()
    if cond_pt == "Novo":
        mask = cond.str.contains("new")
    elif cond_pt == "Usado":
        mask = cond.str.contains("used")
    elif cond_pt == "Recondicionado":
        mask = cond.str.contains("refurb")
    else:
        mask = cond.str.contains("new") | cond.str.contains("used")
    return df[mask].copy()


def _apply_qty_filter(df: pd.DataFrame, qmin: int | None, include_unknown: bool = False) -> pd.DataFrame:
    if qmin is None:
        return df
    if "available_qty" not in df.columns:
        return df.iloc[0:0].copy()
    qty = pd.to_numeric(df["available_qty"], errors="coerce")
    mask = qty.notna() & (qty >= qmin)
    if include_unknown:
        mask = mask | qty.isna()
    return df[mask].copy()


def _enrich_and_filter_qty(df: pd.DataFrame, qmin: int, cond_pt: str) -> tuple[pd.DataFrame, int, int, int]:
    """
    Enriquecimento tardio: busca detalhes no eBay para preencher estoque e filtra por quantidade mínima.
    Mantido aqui para uso futuro. Não interfere na fase Amazon-only.
    """
    if qmin <= 0 or df.empty:
        return df.copy(), 0, 0, 0

    base = df.copy()
    if "available_qty" in base.columns:
        no_qty_mask = pd.isna(base["available_qty"])
    else:
        no_qty_mask = pd.Series(True, index=base.index)

    ids = base.loc[no_qty_mask, "item_id"].dropna().astype(str).unique().tolist()
    to_enrich = ids[: int(os.getenv("MAX_ENRICH", 500))]
    enr: List[Dict[str, Any]] = []

    for iid in to_enrich:
        try:
            d = get_item_detail(iid)
        except Exception as e:
            d = {
                "item_id": iid,
                "available_qty": None,
                "qty_flag": f"ERROR:{type(e).__name__}",
                "brand": None,
                "mpn": None,
                "gtin": None,
                "category_id": None,
            }
        enr.append(d)

    if enr:
        df_enr = _dedup(pd.DataFrame(enr))
        if not df_enr.empty and "item_id" in df_enr.columns:
            cols = [c for c in ["item_id", "available_qty", "qty_flag", "brand", "mpn", "gtin", "category_id"] if c in df_enr.columns]
            df = df.merge(df_enr[cols], on="item_id", how="left", suffixes=("", "_enr"))
            for col in ["available_qty", "qty_flag", "brand", "mpn", "gtin", "category_id"]:
                alt = f"{col}_enr"
                if alt in df.columns:
                    df[col] = df[col].where(df[col].notna(), df[alt])
            df = df.drop(columns=[c for c in df.columns if c.endswith("_enr")])

    view = _apply_condition_filter(df, cond_pt)
    view = _apply_qty_filter(view, qmin, include_unknown=True)
    qty_non_null = pd.to_numeric(df.get("available_qty"), errors="coerce").notna().sum()
    return view, len(enr), len(to_enrich), qty_non_null


def _make_search_url(row) -> Optional[str]:
    q = None
    for key in ["gtin", "UPC/EAN/ISBN", "upc", "ean"]:
        if key in row and pd.notna(row[key]):
            q = str(row[key]).strip()
            break
    if not q and "title" in row:
        q = str(row["title"]).strip()
    return f"https://www.ebay.com/sch/i.html?_nkw={_url.quote_plus(q)}" if q else None


def _render_table(df: pd.DataFrame):
    """
    Renderiza a tabela 100% Amazon-first, com foco em:
      - Título
      - Preço (BuyBox/Lowest)
      - Sales Rank
      - Categoria do BSR
      - Tipo de oferta (FBA/FBM)
      - Prime?
      - ASIN
      - UPC (se conhecido)
      - Link Amazon
    """
    if "amazon_price" in df.columns:
        df["amazon_price_num"] = pd.to_numeric(df["amazon_price"], errors="coerce")

    if "amazon_sales_rank" in df.columns:
        df["amazon_sales_rank"] = pd.to_numeric(df["amazon_sales_rank"], errors="coerce").round(0)

    # Prime icon
    if "amazon_is_prime" in df.columns:
        df["prime_icon"] = df["amazon_is_prime"].apply(lambda x: "✅" if bool(x) else "❌")
    else:
        df["prime_icon"] = "❌"

    # Modo de fulfillment: FBA/FBM/? baseado em amazon_fulfillment_channel
    def _mode(row):
        ch = str(row.get("amazon_fulfillment_channel") or "").upper()
        if ch == "AMAZON":
            return "FBA"
        if ch:
            return "FBM"
        return "?"

    if "amazon_fulfillment_channel" in df.columns:
        df["amazon_fulfillment_mode"] = df.apply(_mode, axis=1)
    else:
        df["amazon_fulfillment_mode"] = ""

    # Garante colunas básicas
    if "upc" not in df.columns:
        df["upc"] = ""

    show_cols = [
        "amazon_title",
        "amazon_price_num",
        "amazon_sales_rank",
        "amazon_sales_rank_category",
        "amazon_fulfillment_mode",
        "prime_icon",
        "amazon_asin",
        "upc",
        "amazon_product_url",
    ]

    exist = [c for c in show_cols if c in df.columns]
    if not exist:
        return

    display_df = df[exist].copy().fillna("")

    # Estilo: título alinhado à esquerda, resto centralizado
    left_cols = [c for c in ["amazon_title"] if c in display_df.columns]

    styler = (
        display_df.style.set_properties(**{"text-align": "center"})
        .set_table_styles(
            [
                {"selector": "th", "props": [("text-align", "center")]},
                {"selector": "td", "props": [("text-align", "center"), ("vertical-align", "middle")]},
            ]
        )
    )
    if left_cols:
        styler = styler.set_properties(subset=left_cols, **{"text-align": "left"})

    st.dataframe(
        styler,
        use_container_width=True,
        hide_index=True,
        height=500,
        column_config={
            "amazon_title": "Título (Amazon)",
            "amazon_price_num": st.column_config.NumberColumn("Preço (Amazon)", format="$%.2f"),
            "amazon_sales_rank": st.column_config.NumberColumn("BSR Amazon", format="%d"),
            "amazon_sales_rank_category": "Categoria BSR (Amazon)",
            "amazon_fulfillment_mode": "Oferta (FBA/FBM)",
            "prime_icon": "Prime Amazon",
            "amazon_asin": "ASIN",
            "upc": "UPC",
            "amazon_product_url": st.column_config.LinkColumn("Produto (Amazon)", display_text="Abrir"),
        },
    )


# ---------------------------------------------------------------------------
# Botão principal: Buscar Amazon
# ---------------------------------------------------------------------------
st.markdown("### 🚀 Passo 1: Buscar produtos na Amazon")

if st.button("Buscar Amazon", key="run_amazon"):
    st.session_state["_stage"] = "running"
    st.session_state["_page_num"] = 1
    st.session_state["_show_qty"] = False
    prog = st.progress(0.0, text="Buscando produtos na Amazon...")

    def _update_amz(done: int, total: int, phase: str):
        frac = done / max(1, total)
        txt = f"Buscando produtos na Amazon... {done}/{total}" if phase == "amazon" else "Processando..."
        prog.progress(frac, text=txt)

    try:
        am_items, stats = discover_amazon_products(
            kw=st.session_state.get("_kw", "") or None,
            amazon_price_min=None,
            amazon_price_max=None,
            amazon_offer_type="any",
            min_monthly_sales_est=0,
            browse_node_id=st.session_state.get("_browse_node_id"),
            max_items=500,  # alvo de 500 distintos com preço
            progress_cb=_update_amz,
        )
        prog.empty()
        am_df = pd.DataFrame(am_items)

        st.session_state["_amazon_items_df"] = am_df.copy()
        st.session_state["_results_df"] = pd.DataFrame()
        st.session_state["_stage"] = "amazon"

        if am_df.empty:
            msg = (
                f"Nenhum produto encontrado na Amazon. "
                f"Catálogo visto: {stats.get('catalog_seen')}, "
                f"com preço conhecido: {stats.get('with_price')}, "
                f"sem preço conhecido: {stats.get('skipped_no_price')}, "
                f"mantidos: {stats.get('kept')}, "
                f"duplicados ignorados: {stats.get('dup_asins')}, "
                f"erros de API: {stats.get('errors_api')}."
            )
            last_err = stats.get("last_error")
            if last_err:
                msg += f"<br><br><small>Detalhe técnico do último erro de API: {last_err}</small>"
            st.warning(msg, icon="⚠️")
        else:
            msg = (
                f"{len(am_df)} produtos distintos encontrados na Amazon "
                f"(catálogo visto: {stats.get('catalog_seen')}, "
                f"com preço: {stats.get('with_price')}, "
                f"sem preço conhecido: {stats.get('skipped_no_price')}, "
                f"mantidos: {stats.get('kept')}, "
                f"duplicados ignorados: {stats.get('dup_asins')}, "
                f"erros de API: {stats.get('errors_api')}). "
                "A tabela abaixo mostra os produtos encontrados."
            )
            last_err = stats.get("last_error")
            if last_err:
                msg += f"<br><br><small>Detalhe técnico do último erro de API: {last_err}</small>"

            st.success(msg)
            st.session_state["_amazon_stats"] = stats
            st.session_state["_results_df"] = am_df.reset_index(drop=True)
            st.session_state["_results_source"] = "amazon_only"
            st.session_state["_page_num"] = 1
            st.session_state["_stage"] = "results"
    except Exception as e:
        prog.empty()
        st.error(f"Falha na busca Amazon: {e}")
        st.session_state["_stage"] = "filters"

st.markdown("---")

# ---------------------------------------------------------------------------
# Tabela + paginação
# ---------------------------------------------------------------------------
if "_results_df" in st.session_state and not st.session_state["_results_df"].empty:
    df = st.session_state["_results_df"]
    PAGE_SIZE = 50
    total_pages = max(1, math.ceil(len(df) / PAGE_SIZE))
    page = st.session_state.get("_page_num", 1)

    col_jump_back, col_prev, col_info, col_next, col_jump_forward = st.columns([0.1, 0.1, 0.6, 0.1, 0.1])

    with col_jump_back:
        if st.button("◀◀", use_container_width=True, disabled=(page <= 1), key="jump_back_10"):
            st.session_state["_page_num"] = max(1, page - 10)
            st.rerun()

    with col_prev:
        if st.button("◀", use_container_width=True, disabled=(page <= 1), key="prev_page"):
            st.session_state["_page_num"] = max(1, page - 1)
            st.rerun()

    with col_info:
        st.markdown(
            f"<div style='text-align:center; font-weight:700;'>Total: {len(df)} itens | Página {page}/{total_pages}</div>",
            unsafe_allow_html=True,
        )

    with col_next:
        if st.button("▶", use_container_width=True, disabled=(page >= total_pages), key="next_page"):
            st.session_state["_page_num"] = min(total_pages, page + 1)
            st.rerun()

    with col_jump_forward:
        if st.button("▶▶", use_container_width=True, disabled=(page >= total_pages), key="jump_forward_10"):
            st.session_state["_page_num"] = min(total_pages, page + 10)
            st.rerun()

    start, end = (page - 1) * PAGE_SIZE, (page - 1) * PAGE_SIZE + PAGE_SIZE
    _render_table(df.iloc[start:end].copy())
    st.caption(f"Página {page}/{total_pages} - exibindo {len(df.iloc[start:end])} itens.")

    # Bloco de quantidade eBay (mantido, mas opcional / futuro)
    st.subheader("Quantidade mínima do produto em estoque eBay (opcional, fluxo futuro)")
    qty_after = st.number_input(
        "Inserir quantidade mínima desejada (opcional)",
        min_value=0,
        value=0,
        step=1,
        help="Enriquece estoque no eBay e filtra pela quantidade desejada. (usa item_id, se houver)",
    )
    if st.button("Ok!", use_container_width=False, disabled=df.empty):
        if qty_after <= 0:
            st.info("Informe uma quantidade mínima maior que zero para aplicar o filtro.")
        else:
            with st.spinner("Enriquecendo e filtrando por quantidade..."):
                filtered, enr_cnt, proc_cnt, qty_non_null = _enrich_and_filter_qty(df, int(qty_after), "Novo")
            st.info(
                f"Detalhes consultados para {proc_cnt} itens (enriquecidos: {enr_cnt}). "
                f"Itens com quantidade conhecida: {qty_non_null}."
            )
            if filtered.empty:
                st.warning("Nenhum item com a quantidade mínima informada.")
            else:
                st.success(f"Itens após filtro de quantidade: {len(filtered)}.")
                st.session_state["_results_df"] = filtered.reset_index(drop=True)
            st.session_state["_results_source"] = "amazon_only"
            st.session_state["_show_qty"] = True
            st.session_state["_page_num"] = 1
            st.session_state["_stage"] = "results"
            st.rerun()

st.markdown("</div>", unsafe_allow_html=True)

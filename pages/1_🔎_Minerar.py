import math
from pathlib import Path

import pandas as pd
import streamlit as st

from integrations.amazon_matching import discover_amazon_products
from lib.tasks import load_categories_tree

# -------------------------------------------------------
# CSS global
# -------------------------------------------------------
CSS_PATH = Path(__file__).resolve().parent.parent / "assets" / "style.css"
if CSS_PATH.exists():
    st.markdown(f"<style>{CSS_PATH.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

st.markdown("<div class='page-shell'>", unsafe_allow_html=True)

# -------------------------------------------------------
# Cabeçalho
# -------------------------------------------------------
st.markdown(
    """
    <div class="page-header">
      <div class="page-header-tag"></div>
      <h1 class="page-header-title">Minerar produtos (Amazon)</h1>
      <p class="page-header-subtitle">
        Selecione categoria/subcategoria e busque até 500 produtos da Amazon
        (Título, preço, ASIN e link da oferta).
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)

# -------------------------------------------------------
# Stepper visual
# -------------------------------------------------------
if "_stage" not in st.session_state:
    st.session_state["_stage"] = "filters"

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
          <div class="flow-step-subtitle">Buscamos produtos na Amazon</div>
        </div>
      </div>
      <div class="flow-step {'flow-step--active' if step3_active else ''}">
        <div class="flow-step-index">3</div>
        <div class="flow-step-text">
          <div class="flow-step-title">Resultados</div>
          <div class="flow-step-subtitle">Analise os produtos encontrados</div>
        </div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# -------------------------------------------------------
# Carrega árvore de categorias (YAML) com amazon_kw
# -------------------------------------------------------
tree = load_categories_tree()


def _find_node_by_name(nodes: list[dict], name: str) -> dict | None:
    for n in nodes:
        if n.get("name") == name:
            return n
        for ch in n.get("children", []) or []:
            if ch.get("name") == name:
                return ch
    return None


def _kw_for_node(node: dict | None) -> str:
    """
    Retorna a keyword em inglês para a Amazon:
      - amazon_kw (se existir)
      - senão, o próprio name.
    """
    if not node:
        return ""
    kw_val = (node.get("amazon_kw") or node.get("name") or "").strip()
    return kw_val or ""


# -------------------------------------------------------
# Card de filtros Amazon
# -------------------------------------------------------
st.markdown(
    """
    <div class='card'>
      <div class='card-title'>
        <div class='card-title-icon'>📦</div>
        <div>Filtros Amazon</div>
      </div>
      <p class='card-caption'>
        Escolha categoria/subcategoria em PT (usamos o campo <code>amazon_kw</code> em EN para buscar na Amazon).
        Você também pode digitar uma palavra-chave livre para refinar.
      </p>
    """,
    unsafe_allow_html=True,
)

user_kw = st.text_input("Palavra-chave adicional (opcional)", value="").strip()

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

selected_parent = parent_node if sel_root != "Todas as categorias" else None
selected_child = (
    _find_node_by_name(parent_node.get("children", []) if parent_node else [], sel_child)
    if sel_child != "Todas as subcategorias"
    else None
)

# ID da categoria Amazon (browse node) escolhido na UI
browse_node_id = None
if selected_child and selected_child.get("category_id"):
    browse_node_id = int(selected_child["category_id"])
elif selected_parent and selected_parent.get("category_id"):
    browse_node_id = int(selected_parent["category_id"])


kw_parts: list[str] = []
if user_kw:
    kw_parts.append(user_kw)
if selected_child:
    kw_parts.append(_kw_for_node(selected_child))
elif selected_parent:
    kw_parts.append(_kw_for_node(selected_parent))

kw = " ".join(p for p in kw_parts if p).strip()
if not kw:
    kw = "a"  # fallback super amplo
st.session_state["_kw"] = kw

st.markdown("</div>", unsafe_allow_html=True)
st.caption(
    "A busca usa apenas a API da Amazon (SP-API). "
    "Neste momento não há filtro de demanda/BSR: o objetivo é listar produtos 'brutos'."
)

# -------------------------------------------------------
# Helper para exibir tabela
# -------------------------------------------------------
def _render_table(df: pd.DataFrame) -> None:
    """
    Exibe somente:
      - amazon_title
      - amazon_price (como número formatado)
      - amazon_asin
      - amazon_product_url (como link)
    """
    if df.empty:
        st.info("Nenhum produto para exibir.")
        return

    df = df.copy()

    # Normaliza preço numérico
    if "amazon_price" in df.columns:
        df["amazon_price_num"] = pd.to_numeric(df["amazon_price"], errors="coerce")
    else:
        df["amazon_price_num"] = None

    show_cols = [
        "amazon_title",
        "amazon_price_num",
        "amazon_asin",
        "amazon_product_url",
    ]
    exist = [c for c in show_cols if c in df.columns]
    if not exist:
        st.warning("Os dados retornados não possuem as colunas esperadas da Amazon.")
        return

    display_df = df[exist].copy().fillna("")

    # Estilo: título à esquerda, resto centralizado
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
            "amazon_asin": "ASIN",
            "amazon_product_url": st.column_config.LinkColumn("Oferta na Amazon", display_text="Abrir"),
        },
    )


# -------------------------------------------------------
# Botão principal: Buscar produtos na Amazon
# -------------------------------------------------------
st.markdown("### 🚀 Passo 1: Buscar produtos na Amazon")

MAX_RESULTS = 500  # quantidade máxima de itens distintos na tabela

if st.button("Buscar Amazon", key="run_amazon"):
    st.session_state["_stage"] = "running"
    st.session_state["_page_num"] = 1

    prog = st.progress(0.0, text="Buscando produtos na Amazon...")

    def _update_amz(done: int, total: int, phase: str) -> None:
        frac = done / max(1, total)
        txt = f"Buscando produtos na Amazon... {done}/{total}" if phase == "amazon" else "Processando..."
        prog.progress(frac, text=txt)

    try:
        am_items, stats = discover_amazon_products(
            kw=st.session_state.get("_kw", "") or None,
            amazon_price_min=None,
            amazon_price_max=None,
            amazon_offer_type="any",
            min_monthly_sales_est=0,  # BSR NÃO influencia nada
            browse_node_id=browse_node_id,
            max_pages=150,                 # pode ajustar se quiser
            page_size=20,
            max_items=500,
            progress_cb=_update_amz,
        )
        prog.empty()

        am_df = pd.DataFrame(am_items or [])
        st.session_state["_results_df"] = am_df.reset_index(drop=True)
        st.session_state["_amazon_stats"] = stats
        st.session_state["_page_num"] = 1
        st.session_state["_stage"] = "results"

        if am_df.empty:
            st.warning(
                f"Nenhum produto encontrado na Amazon. "
                f"Catálogo visto: {stats.get('catalog_seen')}, "
                f"com preço: {stats.get('with_price')}, "
                f"sem preço: {stats.get('skipped_no_price')}, "
                f"mantidos: {stats.get('kept')}, "
                f"duplicados: {stats.get('skipped_duplicate_asin')}, "
                f"erros de API: {stats.get('api_errors')}."
            )
        else:
            st.success(
                f"{len(am_df)} produtos distintos encontrados na Amazon "
                f"(catálogo visto: {stats.get('catalog_seen')}, "
                f"com preço: {stats.get('with_price')}, "
                f"sem preço: {stats.get('skipped_no_price')}, "
                f"mantidos: {stats.get('kept')}, "
                f"duplicados ignorados: {stats.get('skipped_duplicate_asin')}, "
                f"erros de API: {stats.get('api_errors')}). "
                "A tabela abaixo mostra os produtos encontrados."
            )


    except Exception as e:
        prog.empty()
        st.session_state["_stage"] = "filters"
        st.error(f"Falha na busca Amazon: {e}")

st.markdown("---")

# -------------------------------------------------------
# Tabela + paginação
# -------------------------------------------------------
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

st.markdown("</div>", unsafe_allow_html=True)

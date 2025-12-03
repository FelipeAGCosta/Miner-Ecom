import math
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
import streamlit as st

from lib.tasks import load_categories_tree, flatten_categories
from integrations.amazon_spapi import (
    search_catalog_items,
    get_buybox_price,
    _extract_catalog_item,
    _load_config_from_env,
)


# ---------------------------------------------------------------------------
# Config Amazon / helpers basicos
# ---------------------------------------------------------------------------

_cfg = _load_config_from_env()
_MARKETPLACE_ID = _cfg.marketplace_id

# seeds amplas pra variar a busca
_KEYWORD_SEEDS = ["a", "e", "i", "o", "u", "aa", "zz", "10", "2024", "set", "kit"]
_MAX_ITEMS = 500          # objetivo: 500 produtos distintos
_PAGE_SIZE = 20           # limite da API
_MAX_PAGES_PER_COMBO = 60  # 60 * 20 = 1200 itens por combinacao (antes de deduplicar)


# ---------------------------------------------------------------------------
# CSS / layout
# ---------------------------------------------------------------------------

CSS_PATH = Path(__file__).resolve().parent.parent / "assets" / "style.css"
if CSS_PATH.exists():
    st.markdown(f"<style>{CSS_PATH.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

st.markdown("<div class='page-shell'>", unsafe_allow_html=True)

st.markdown(
    """
    <div class="page-header">
      <div class="page-header-tag"></div>
      <h1 class="page-header-title">Minerar produtos (Amazon)</h1>
      <p class="page-header-subtitle">
        Busca <b>direta no catálogo da Amazon</b>, retornando até 500 produtos diferentes,
        sem filtrar por BSR ou demanda. Somente Amazon por enquanto.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)

# Stepper visual
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
          <div class="flow-step-subtitle">Categoria / palavra-chave</div>
        </div>
      </div>
      <div class="flow-step {'flow-step--active' if step2_active else ''}">
        <div class="flow-step-index">2</div>
        <div class="flow-step-text">
          <div class="flow-step-title">Minerar</div>
          <div class="flow-step-subtitle">Chama SP-API</div>
        </div>
      </div>
      <div class="flow-step {'flow-step--active' if step3_active else ''}">
        <div class="flow-step-index">3</div>
        <div class="flow-step-text">
          <div class="flow-step-title">Resultados</div>
          <div class="flow-step-subtitle">Tabela bruta (500 itens máx.)</div>
        </div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Categorias (árvore YAML)
# ---------------------------------------------------------------------------

tree = load_categories_tree()
flat = flatten_categories(tree)


def _find_node_by_name(nodes: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    for n in nodes:
        if n.get("name") == name:
            return n
        for ch in n.get("children") or []:
            if ch.get("name") == name:
                return ch
    return None


def _kw_for_node(node: Optional[Dict[str, Any]]) -> str:
    if not node:
        return ""
    return (node.get("amazon_kw") or node.get("name") or "").strip()


def _collect_classification_ids(sel_root: str, sel_child: str) -> List[str]:
    """
    Usa a árvore de categorias para montar uma lista de classificationIds Amazon.

    - Se só root escolhida: inclui o classificationId da root + de todas as subcategorias.
    - Se subcategoria escolhida: inclui só o classificationId da subcategoria.
    - Se "Todas as categorias": não retorna nada (sem filtro de classificação).
    """
    ids: List[str] = []
    if sel_root == "Todas as categorias":
        return ids

    for node in tree:
        if node.get("name") != sel_root:
            continue

        # Subcategoria específica selecionada
        if sel_child and sel_child != "Todas as subcategorias":
            for ch in node.get("children") or []:
                if ch.get("name") == sel_child and ch.get("category_id"):
                    ids.append(str(ch["category_id"]))
                    return ids
            return ids  # não achou subcategoria, volta vazio mesmo

        # Somente categoria raiz -> pega raiz + todos os filhos que tiverem category_id
        if node.get("category_id"):
            ids.append(str(node["category_id"]))
        for ch in node.get("children") or []:
            cid = ch.get("category_id")
            if cid:
                ids.append(str(cid))
        return ids

    return ids


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
        Escolha categoria/subcategoria (para usarmos <code>classificationIds</code>) e, se quiser,
        uma palavra-chave extra. A busca não usa BSR nem filtros de demanda.
      </p>
    """,
    unsafe_allow_html=True,
)

user_kw = st.text_input("Palavra-chave (opcional)", value="").strip()

col_cat1, col_cat2 = st.columns([1.6, 1.6])
with col_cat1:
    root_names = ["Todas as categorias"] + [n.get("name") for n in tree if n.get("name")]
    sel_root = st.selectbox("Categoria", root_names, index=0)
with col_cat2:
    child_names = ["Todas as subcategorias"]
    parent_node = _find_node_by_name(tree, sel_root) if sel_root != "Todas as categorias" else None
    if parent_node and parent_node.get("children"):
        for ch in parent_node.get("children") or []:
            if ch.get("name"):
                child_names.append(ch["name"])
    sel_child = st.selectbox("Subcategoria (Opcional)", child_names, index=0)

selected_parent = parent_node if sel_root != "Todas as categorias" else None
selected_child = (
    _find_node_by_name(parent_node.get("children") or [], sel_child)
    if (parent_node and sel_child != "Todas as subcategorias")
    else None
)

# Monta keyword base (ampla) para jogar na SP-API
kw_parts: List[str] = []
if user_kw:
    kw_parts.append(user_kw)
if selected_child:
    kw_parts.append(_kw_for_node(selected_child))
elif selected_parent:
    kw_parts.append(_kw_for_node(selected_parent))

kw = " ".join(p for p in kw_parts if p).strip() or "a"
st.session_state["_kw"] = kw

# classificationIds baseados na seleção da árvore
classification_ids = _collect_classification_ids(sel_root, sel_child)
st.session_state["_classification_ids"] = classification_ids

st.markdown("</div>", unsafe_allow_html=True)
st.caption(
    "Quanto mais ampla a busca, maior o tempo. "
    "A SP-API retorna blocos de até 20 itens por página; aqui buscamos no máximo 500 produtos distintos."
)


# ---------------------------------------------------------------------------
# Descoberta Amazon bruta (sem BSR)
# ---------------------------------------------------------------------------

def _discover_amazon_random(
    kw: Optional[str],
    classification_ids: Optional[List[str]],
    max_items: int,
    progress_cb=None,
) -> tuple[list[dict], dict]:
    """
    Buscador bruto na Amazon:

    - Usa classificationIds (categoria/subcategoria) quando fornecidos.
    - Usa conjunto de 'keyword seeds' para variar a busca.
    - Paginação via pageToken até atingir max_items ou acabar os resultados.
    - NÃO filtra por BSR, nem por demanda, nem por tipo de oferta.
    """
    seen_asins: set[str] = set()
    out_items: list[dict] = []

    stats = {
        "catalog_seen": 0,
        "with_price": 0,
        "skipped_no_price": 0,
        "kept": 0,
        "duplicates": 0,
        "api_errors": 0,
    }

    base_kw = (kw or "").strip()

    seeds: list[str] = []
    if base_kw:
        seeds.append(base_kw)
        for s in _KEYWORD_SEEDS:
            seeds.append((base_kw + " " + s).strip())
    else:
        seeds.extend(_KEYWORD_SEEDS)

    # Remove vazios/duplicados em seeds
    seen_seed = set()
    final_seeds: list[str] = []
    for s in seeds:
        s_norm = s.strip()
        if not s_norm:
            continue
        if s_norm.lower() in seen_seed:
            continue
        seen_seed.add(s_norm.lower())
        final_seeds.append(s_norm)

    def _progress():
        if progress_cb:
            progress_cb(len(out_items), max_items, "amazon")

    # Itera classificacoes e seeds
    if classification_ids:
        combos = [(classification_ids, seed) for seed in final_seeds]
    else:
        combos = [(None, seed) for seed in final_seeds]

    for class_ids, seed in combos:
        if len(out_items) >= max_items:
            break

        page_token: Optional[str] = None
        pages_used = 0

        while True:
            if len(out_items) >= max_items:
                break

            try:
                raw_items, next_token = search_catalog_items(
                    keywords=seed,
                    classification_ids=class_ids,
                    page_size=_PAGE_SIZE,
                    page_token=page_token,
                    included_data="summaries,identifiers,salesRanks",
                )
            except Exception:
                stats["api_errors"] += 1
                break

            if not raw_items:
                break

            pages_used += 1

            for raw in raw_items:
                stats["catalog_seen"] += 1
                extracted = _extract_catalog_item(raw, _MARKETPLACE_ID)
                asin = extracted.get("asin")
                if not asin:
                    continue
                if asin in seen_asins:
                    stats["duplicates"] += 1
                    continue
                seen_asins.add(asin)

                # Preço (BuyBox / LowestPrice). Se não achar, mantemos item mesmo assim.
                price_value = None
                currency = None
                try:
                    price_info = get_buybox_price(asin)
                except Exception:
                    price_info = None
                    stats["api_errors"] += 1

                if price_info and price_info.get("price") is not None:
                    try:
                        price_value = float(price_info["price"])
                    except (TypeError, ValueError):
                        price_value = None
                    currency = price_info.get("currency")
                    stats["with_price"] += 1
                else:
                    stats["skipped_no_price"] += 1

                item_out = {
                    "amazon_asin": asin,
                    "amazon_title": extracted.get("title"),
                    "amazon_brand": extracted.get("brand"),
                    "amazon_browse_node_id": extracted.get("browse_node_id"),
                    "amazon_browse_node_name": extracted.get("browse_node_name"),
                    "amazon_sales_rank": extracted.get("sales_rank"),
                    "amazon_sales_rank_category": extracted.get("sales_rank_category"),
                    "amazon_price": price_value,
                    "amazon_currency": currency,
                    "amazon_product_url": f"https://www.amazon.com/dp/{asin}",
                }
                out_items.append(item_out)
                stats["kept"] += 1
                _progress()
                if len(out_items) >= max_items:
                    break

            if len(out_items) >= max_items:
                break

            if not next_token:
                break

            page_token = next_token
            if pages_used >= _MAX_PAGES_PER_COMBO:
                break

    return out_items, stats


# ---------------------------------------------------------------------------
# Render tabela simples (Amazon-only)
# ---------------------------------------------------------------------------

def _render_table(df: pd.DataFrame) -> None:
    if "amazon_price" in df.columns:
        df["amazon_price_num"] = pd.to_numeric(df["amazon_price"], errors="coerce")

    show_cols = [
        "amazon_title",
        "amazon_price_num",
        "amazon_asin",
        "amazon_product_url",
    ]

    existing_cols = [c for c in show_cols if c in df.columns]
    if not existing_cols:
        return

    display_df = df[existing_cols].copy()

    styler = (
        display_df.style.set_properties(**{"text-align": "center"})
        .set_table_styles(
            [
                {"selector": "th", "props": [("text-align", "center")]},
                {
                    "selector": "td",
                    "props": [("text-align", "center"), ("vertical-align", "middle")],
                },
            ]
        )
        .set_properties(subset=["amazon_title"], **{"text-align": "left"})
    )

    st.dataframe(
        styler,
        use_container_width=True,
        hide_index=True,
        height=500,
        column_config={
            "amazon_title": "Título (Amazon)",
            "amazon_price_num": st.column_config.NumberColumn(
                "Preço (BuyBox/lowest)", format="$%.2f"
            ),
            "amazon_asin": "ASIN",
            "amazon_product_url": st.column_config.LinkColumn(
                "Link Amazon", display_text="Abrir"
            ),
        },
    )


# ---------------------------------------------------------------------------
# Botão principal: Buscar Amazon
# ---------------------------------------------------------------------------

st.markdown("### 🚀 Passo 1: Buscar produtos na Amazon (sem BSR, até 500 itens distintos)")

if st.button("Buscar Amazon", key="run_amazon"):
    st.session_state["_stage"] = "running"
    st.session_state["_page_num"] = 1

    prog = st.progress(0.0, text="Buscando produtos na Amazon...")

    def _update_amz(done: int, total: int, phase: str):
        frac = min(1.0, done / max(1, total))
        txt = f"Buscando produtos na Amazon... {done}/{total}"
        prog.progress(frac, text=txt)

    try:
        items, stats = _discover_amazon_random(
            kw=st.session_state.get("_kw", "") or None,
            classification_ids=st.session_state.get("_classification_ids") or None,
            max_items=_MAX_ITEMS,
            progress_cb=_update_amz,
        )
        prog.empty()

        df = pd.DataFrame(items)
        st.session_state["_results_df"] = df.copy()
        st.session_state["_stage"] = "results"

        distinct_count = len(df)
        st.success(
            f"{distinct_count} produtos distintos encontrados na Amazon "
            f"(catálogo visto: {stats.get('catalog_seen')}, "
            f"com preço: {stats.get('with_price')}, "
            f"sem preço conhecido: {stats.get('skipped_no_price')}, "
            f"mantidos: {stats.get('kept')}, "
            f"duplicados ignorados: {stats.get('duplicates')}, "
            f"erros de API: {stats.get('api_errors')}). "
            "A tabela abaixo mostra os produtos encontrados."
        )

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

    col_jump_back, col_prev, col_info, col_next, col_jump_forward = st.columns(
        [0.1, 0.1, 0.6, 0.1, 0.1]
    )
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

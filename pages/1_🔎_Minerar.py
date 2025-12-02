import os
import math
import time
import urllib.parse as _url
from datetime import timedelta

import pandas as pd
import streamlit as st
from pathlib import Path

from lib.config import make_engine
from lib.tasks import load_categories_tree, flatten_categories
from lib.db import upsert_ebay_listings, sql_safe_frame
from lib.ebay_api import get_item_detail          # detalhes para enriquecimento
from lib.redis_cache import cache_get, cache_set
from integrations.amazon_matching import (
    match_ebay_to_amazon,  # legado (não removido)
    discover_amazon_and_match_ebay,  # fluxo unificado
    match_amazon_list_to_ebay,  # fluxo em duas etapas
    discover_amazon_products,  # descoberta Amazon (debug/etapa 1)
)

# --- carregar CSS global (tema aplicado também nesta página) ---
CSS_PATH = Path(__file__).resolve().parent.parent / "assets" / "style.css"
if CSS_PATH.exists():
    st.markdown(f"<style>{CSS_PATH.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

st.markdown("<div class='page-shell'>", unsafe_allow_html=True)

st.markdown(
    """
    <div class="page-header">
      <div class="page-header-tag"></div>
      <h1 class="page-header-title">Minerar produtos</h1>
      <p class="page-header-subtitle">
        Defina filtros de eBay e Amazon para encontrar oportunidades com preço e demanda interessantes.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)

# estágio visual do stepper
if "_stage" not in st.session_state and "_results_df" in st.session_state and not st.session_state["_results_df"].empty:
    st.session_state["_stage"] = "results"
if st.session_state.get("run_btn", False) or st.session_state.get("_start_run", False):
    st.session_state["_stage"] = "running"
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
          <div class="flow-step-subtitle">Defina categoria, preços e demanda</div>
        </div>
      </div>
      <div class="flow-step {'flow-step--active' if step2_active else ''}">
        <div class="flow-step-index">2</div>
        <div class="flow-step-text">
          <div class="flow-step-title">Minerar</div>
          <div class="flow-step-subtitle">Buscamos no eBay e Amazon</div>
        </div>
      </div>
      <div class="flow-step {'flow-step--active' if step3_active else ''}">
        <div class="flow-step-index">3</div>
        <div class="flow-step-text">
          <div class="flow-step-title">Resultados</div>
          <div class="flow-step-subtitle">Analise oportunidades</div>
        </div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

API_ITEMS_PER_PAGE = int(st.secrets.get("EBAY_LIMIT_PER_PAGE", os.getenv("EBAY_LIMIT_PER_PAGE", 200)))
API_MAX_PAGES = int(st.secrets.get("EBAY_MAX_PAGES", os.getenv("EBAY_MAX_PAGES", 25)))
MAX_ENRICH = int(st.secrets.get("MAX_ENRICH", os.getenv("MAX_ENRICH", 500)))
tree = load_categories_tree()
flat = flatten_categories(tree)

# --- filtros Amazon (opcionais) --------------------------------------------
st.markdown(
    """
    <div class='card'>
      <div class='card-title'>
        <div class='card-title-icon'>📦</div>
        <div>Filtros Amazon</div>
      </div>
      <p class='card-caption'>Escolha apenas a categoria/subcategoria para minerar produtos na Amazon.</p>
    """,
    unsafe_allow_html=True,
)

col_cat1, col_cat2 = st.columns([1.6, 1.6])
with col_cat1:
    root_names = ["Todas as categorias"] + [n["name"] for n in tree]
    sel_root = st.selectbox("Categoria", root_names, index=0)
with col_cat2:
    child_names = ["Todas as subcategorias"]
    if sel_root != "Todas as categorias":
        for n in tree:
            if n["name"] == sel_root:
                for ch in n.get("children", []) or []:
                    child_names.append(ch["name"])
                break
    sel_child = st.selectbox("Subcategoria (Opcional)", child_names, index=0)

kw_parts = []
if sel_child != "Todas as subcategorias":
    kw_parts.append(sel_child)
elif sel_root != "Todas as categorias":
    kw_parts.append(sel_root)
kw = " ".join(p for p in kw_parts if p).strip() or ""
st.session_state["_kw"] = kw

st.markdown("</div>", unsafe_allow_html=True)

st.caption(
    "Quanto mais ampla a busca, maior o tempo de pesquisa. Use os filtros para equilibrar velocidade e profundidade."
)
# helpers 
def _fmt_eta(seconds: float) -> str:
    return str(timedelta(seconds=int(max(0, seconds))))

def _dedup(df: pd.DataFrame) -> pd.DataFrame:
    if "item_id" not in df.columns:
        return df
    return df.dropna(subset=["item_id"]).drop_duplicates(subset=["item_id"], keep="first").copy()

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
    Retorna (df_filtrado, enriquecidos_feitos, candidatos_processados).
    """
    if qmin <= 0 or df.empty:
        return df.copy(), 0, 0

    base = df.copy()
    if "available_qty" in base.columns:
        no_qty_mask = pd.isna(base["available_qty"])
    else:
        no_qty_mask = pd.Series(True, index=base.index)

    ids = (
        base.loc[no_qty_mask, "item_id"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    to_enrich = ids[:MAX_ENRICH]
    enr: list[dict] = []

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
            cols = ["item_id", "available_qty", "qty_flag", "brand", "mpn", "gtin", "category_id"]
            cols = [c for c in cols if c in df_enr.columns]
            df = df.merge(
                df_enr[cols],
                on="item_id",
                how="left",
                suffixes=("", "_enr"),
            )
            for col in ["available_qty", "qty_flag", "brand", "mpn", "gtin", "category_id"]:
                alt = f"{col}_enr"
                if alt in df.columns:
                    df[col] = df[col].where(df[col].notna(), df[alt])
            drop_cols = [c for c in df.columns if c.endswith("_enr")]
            df = df.drop(columns=drop_cols)

    view = _apply_condition_filter(df, cond_pt)
    view = _apply_qty_filter(view, qmin, include_unknown=True)
    qty_non_null = pd.to_numeric(df.get("available_qty"), errors="coerce").notna().sum()
    return view, len(enr), len(to_enrich), qty_non_null

def _apply_price_filter(df: pd.DataFrame, pmin_v: float | None, pmax_v: float | None) -> pd.DataFrame:
    if "price" not in df.columns:
        if pmin_v is not None or pmax_v is not None:
            return df.iloc[0:0].copy()
        return df

    df = df.copy()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    mask = pd.Series(True, index=df.index)
    if pmin_v is not None:
        mask &= df["price"].fillna(float("inf")) >= (float(pmin_v) - 1e-9)
    if pmax_v is not None:
        mask &= df["price"].fillna(float("-inf")) <= (float(pmax_v) + 1e-9)

    return df[mask].copy()

def _apply_condition_filter(df: pd.DataFrame, cond_pt: str) -> pd.DataFrame:
    """
    Reforça o filtro de condição no lado do app,
    usando 'New', 'Used', 'Refurbished' e variações.
    """
    if "condition" not in df.columns:
        return df

    df = df.copy()
    cond = df["condition"].astype(str).str.lower()

    if cond_pt == "Novo":
        mask = cond.str.contains("new")
    elif cond_pt == "Usado":
        mask = cond.str.contains("used")
    elif cond_pt == "Recondicionado":
        mask = cond.str.contains("refurb")
    else:  # Novo & Usado
        mask = cond.str.contains("new") | cond.str.contains("used")

    return df[mask].copy()

def _resolve_category_ids() -> list[int]:
    ids: list[int] = []
    if sel_root == "Todas as categorias":
        if not flat.empty:
            ids = flat["category_id"].dropna().astype(int).unique().tolist()
    else:
        root_id = None
        for n in tree:
            if n["name"] == sel_root:
                root_id = int(n["category_id"])
                break
        if sel_child == "Todas as subcategorias":
            ids = [root_id]
            for n in tree:
                if n["name"] == sel_root:
                    for ch in n.get("children", []) or []:
                        ids.append(int(ch["category_id"]))
                    break
        else:
            for n in tree:
                if n["name"] == sel_root:
                    for ch in n.get("children", []) or []:
                        if ch["name"] == sel_child:
                            ids = [int(ch["category_id"])]
                            break
                    break
            if not ids and root_id:
                ids = [root_id]
    ids = [int(i) for i in ids if i is not None]
    ids = [i for i in ids if i > 0]
    return list(dict.fromkeys(ids))

def _make_search_url(row) -> str | None:
    q = None
    for key in ["gtin", "UPC/EAN/ISBN", "upc", "ean"]:
        if key in row and pd.notna(row[key]):
            q = str(row[key]).strip()
            break
    if not q and "title" in row:
        q = str(row["title"]).strip()
    # Busca precisa no eBay: usa parâmetro _nkw corretamente
    return f"https://www.ebay.com/sch/i.html?_nkw={_url.quote_plus(q)}" if q else None

def _fmt_price(x):
    try:
        f = float(x)
        return f"${f:.2f}"
    except Exception:
        return ""

def _render_table(df: pd.DataFrame):
    # normaliza colunas numericas para sort correto
    if "price" in df.columns:
        df["price_num"] = pd.to_numeric(df["price"], errors="coerce")
    if "amazon_price" in df.columns:
        df["amazon_price_num"] = pd.to_numeric(df["amazon_price"], errors="coerce")
    if "amazon_sales_rank" in df.columns:
        df["amazon_sales_rank"] = pd.to_numeric(df["amazon_sales_rank"], errors="coerce").round(0)

    show_qty = bool(st.session_state.get("_show_qty", False))
    if show_qty and "available_qty" in df.columns:
        df["available_qty_disp"] = df["available_qty"].apply(lambda x: int(x) if pd.notna(x) else "+10")

    if "amazon_is_prime" in df.columns:
        df["prime_icon"] = df["amazon_is_prime"].apply(lambda x: "✅" if bool(x) else "❌")
    else:
        df["prime_icon"] = "❌"

    show_cols = [
        "title",
        "price_num",
        "amazon_price_num",
        "amazon_est_monthly_sales",
        "amazon_sales_rank",
        "amazon_sales_rank_category",
        "seller",
        "amazon_demand_bucket",
        "brand",
        "amazon_brand",
        "amazon_title",
        "condition",
        "item_url",
        "amazon_product_url",
        "search_url",
        "amazon_asin",
        "prime_icon",
    ]
    if show_qty and "available_qty_disp" in df.columns:
        show_cols.insert(3, "available_qty_disp")

    exist = [c for c in show_cols if c in df.columns]
    if not exist:
        return

    display_df = df[exist].copy().fillna("")
    left_cols = [c for c in ["title", "amazon_title"] if c in display_df.columns]
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
            "title": "Título",
            "price_num": st.column_config.NumberColumn("Preço (eBay)", format="$%.2f"),
            "amazon_price_num": st.column_config.NumberColumn("Preço (Amazon)", format="$%.2f"),
            "amazon_est_monthly_sales": st.column_config.NumberColumn("Vendas aproximadas (último mês)", format="%d"),
            "amazon_sales_rank": st.column_config.NumberColumn("BSR Amazon", format="%d"),
            "amazon_sales_rank_category": "Categoria BSR (Amazon)",
            "seller": "Vendedor eBay",
            "amazon_demand_bucket": "Demanda (BSR)",
            "brand": "Marca (eBay)",
            "amazon_brand": "Marca (Amazon)",
            "amazon_title": "Título (Amazon)",
            "condition": "Condição",
            "item_url": st.column_config.LinkColumn("Produto (eBay)", display_text="Abrir"),
            "amazon_product_url": st.column_config.LinkColumn("Produto (Amazon)", display_text="Abrir"),
            "search_url": st.column_config.LinkColumn("Ver outros vendedores", display_text="Buscar"),
            "amazon_asin": "ASIN",
            "prime_icon": "Prime Amazon",
            **({"available_qty_disp": "Qtd (estim.) eBay"} if show_qty and "available_qty_disp" in df.columns else {}),
        },
    )

def _ensure_currency(df: pd.DataFrame) -> pd.DataFrame:
    if "currency" not in df.columns:
        df["currency"] = "USD"
    else:
        df["currency"] = df["currency"].fillna("USD").replace("", "USD")
    return df

st.markdown("### 🚀 Passo 1: Buscar produtos na Amazon")
if st.button("Buscar Amazon", key="run_amazon"):
    st.session_state["_stage"] = "running"
    st.session_state["_page_num"] = 1
    st.session_state["_show_qty"] = False
    prog = st.progress(0.0, text="Buscando produtos na Amazon...")

    amazon_pmin_v = None
    amazon_pmax_v = None
    amazon_offer_type = "any"

    def _update_amz(done: int, total: int, phase: str):
        frac = done / max(1, total)
        txt = f"Buscando produtos na Amazon... {done}/{total}" if phase == "amazon" else "Processando..."
        prog.progress(frac, text=txt)

    try:
        am_items, stats = discover_amazon_products(
            kw=st.session_state.get("_kw", "") or None,
            amazon_price_min=amazon_pmin_v,
            amazon_price_max=amazon_pmax_v,
            amazon_offer_type=amazon_offer_type,
            min_monthly_sales_est=None,
            progress_cb=_update_amz,
        )
        prog.empty()
        am_df = pd.DataFrame(am_items)
        # Dedup por ASIN para evitar itens repetidos
        if "amazon_asin" in am_df.columns:
            am_df = am_df.drop_duplicates(subset=["amazon_asin"], keep="first")
        st.session_state["_amazon_items_df"] = am_df.copy()
        st.session_state["_results_df"] = pd.DataFrame()  # limpa final
        st.session_state["_stage"] = "amazon"
        if am_df.empty:
            st.warning(
                f"Nenhum produto encontrado na Amazon. Stats: total catálogo {stats.get('catalog_seen')}, "
                f"com preço {stats.get('with_price')}, sem preço {stats.get('skipped_no_price')}, "
                f"ignorados por idioma {stats.get('skipped_lang')}."
            )
        else:
            st.success(
                f"{len(am_df)} produtos encontrados na Amazon "
                f"(catálogo visto: {stats.get('catalog_seen')}, com preço: {stats.get('with_price')}, "
                f"sem preço: {stats.get('skipped_no_price')}, ignorados por idioma: {stats.get('skipped_lang')}). "
                "A tabela abaixo mostra todos os produtos encontrados na Amazon."
            )
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
st.markdown("### 🔍 Passo 2: (temporariamente desativado) Buscar fornecedores no eBay")
st.info("Foco atual: mineração Amazon. A etapa eBay será reativada depois que a lista Amazon estiver validada.")

# Tabela + paginação
if "_results_df" in st.session_state and not st.session_state["_results_df"].empty:
    df = st.session_state["_results_df"]
    base_df = st.session_state.get("_ebay_df")
    source = st.session_state.get("_results_source", "ebay")

    amazon_price_min = globals().get("amazon_price_min", 0)
    amazon_price_max = globals().get("amazon_price_max", 0)
    amazon_offer_label = globals().get("amazon_offer_label", "Qualquer")

    amazon_pmin_v = amazon_price_min if amazon_price_min > 0 else None
    amazon_pmax_v = amazon_price_max if amazon_price_max > 0 else None
    if amazon_offer_label.startswith("Prime"):
        amazon_offer_type = "prime"
    elif amazon_offer_label.startswith("Terceiros"):
        amazon_offer_type = "fbm"
    else:
        amazon_offer_type = "any"

    if amazon_pmin_v is not None and amazon_pmax_v is not None and amazon_pmax_v < amazon_pmin_v:
        st.error("Na Amazon, o preço máximo não pode ser menor que o preço mínimo.")

    # mensagens internas removidas da interface para reduzir ruído

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

    st.subheader("Quantidade mínima do produto em estoque eBay")
    qty_after = st.number_input(
        "Inserir quantidade mínima desejada (opcional)",
        min_value=0,
        value=0,
        step=1,
        help="Enriquece estoque no eBay e filtra pela quantidade desejada.",
    )
    if st.button("Ok!", use_container_width=False, disabled=df.empty):
        if qty_after <= 0:
            st.info("Informe uma quantidade mínima maior que zero para aplicar o filtro.")
        else:
            with st.spinner("Enriquecendo e filtrando por quantidade..."):
                filtered, enr_cnt, proc_cnt, qty_non_null = _enrich_and_filter_qty(df, int(qty_after), cond_pt)
        st.info(
            f"Detalhes consultados para {proc_cnt} itens (enriquecidos: {enr_cnt}). "
            f"Itens com quantidade conhecida: {qty_non_null}."
        )
        if filtered.empty:
            st.warning("Nenhum item com a quantidade mínima informada.")
        else:
            st.success(f"Itens após filtro de quantidade: {len(filtered)}.")
            st.session_state["_results_df"] = filtered.reset_index(drop=True)
        st.session_state["_results_source"] = source
        st.session_state["_show_qty"] = True
        st.session_state["_page_num"] = 1
        st.session_state["_stage"] = "results"
        st.rerun()
st.markdown("</div>", unsafe_allow_html=True)

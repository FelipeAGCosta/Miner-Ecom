import math
import time
import urllib.parse as _url
import pandas as pd
import streamlit as st
from lib.config import make_engine
from lib.tasks import load_categories_tree, flatten_categories
from lib.db import upsert_ebay_listings, sql_safe_frame
from lib.ebay_api import search_category_safe, get_item_detail

st.header("🔎 Minerar")

# Limites fixos de coleta (máximos seguros para Browse API)
API_ITEMS_PER_PAGE = 200
API_MAX_PAGES      = 25
MAX_ENRICH         = 300   # só usado se houver qty_min

# Carrega categorias / subcategorias
tree = load_categories_tree()
flat = flatten_categories(tree)

# ----------------------------
# Filtros
# ----------------------------
col1, col2 = st.columns([1.6, 1.6])
with col1:
    root_names = ["Todas as categorias"] + [n["name"] for n in tree]
    sel_root = st.selectbox("Categoria (nível 1)", root_names, index=0)

with col2:
    child_names = ["Todas as subcategorias"]
    if sel_root != "Todas as categorias":
        for n in tree:
            if n["name"] == sel_root:
                for ch in n.get("children", []) or []:
                    child_names.append(ch["name"])
                break
    sel_child = st.selectbox("Subcategoria (nível 2)", child_names, index=0)

col3, col4, col5 = st.columns([1,1,1])
with col3:
    pmin = st.text_input("Preço mínimo (US$)", value="")
with col4:
    pmax = st.text_input("Preço máximo (US$)", value="")
with col5:
    cond = st.selectbox("Condição", ["NEW","USED","REFURBISHED"], index=0)

qty_min = st.text_input("Quantidade mínima (só enriquece se informar)", value="")

st.caption("Conforme mais ampla sua busca e mais filtros aplicar, maior o tempo de execução. (Mostramos tempo decorrido e uma ETA durante o carregamento.)")
st.divider()

# ----------------------------
# Helpers
# ----------------------------
def _num(x, to_int=False):
    try:
        v = float(x)
        return int(v) if to_int else v
    except Exception:
        return None

def _apply_filters_local(df: pd.DataFrame, pmin, pmax, qmin):
    out = df.copy()
    if "price" in out.columns:
        prices = pd.to_numeric(out["price"], errors="coerce")
        if pmin is not None: out = out[prices >= pmin]
        if pmax is not None: out = out[prices <= pmax]
    if qmin is not None and "available_qty" in out.columns:
        qty = pd.to_numeric(out["available_qty"], errors="coerce")
        out = out[qty.notna() & (qty >= qmin)]
    return out

def _dedup(df):
    if "item_id" not in df.columns:
        return df
    return df.dropna(subset=["item_id"]).drop_duplicates(subset=["item_id"], keep="first").copy()

def _resolve_category_ids():
    ids = []
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
    return ids

def _make_search_url(row):
    # Prioriza GTIN se existir, senão usa o título
    q = None
    for key in ["gtin", "UPC/EAN/ISBN", "upc", "ean"]:
        if key in row and pd.notna(row[key]):
            q = str(row[key]).strip()
            break
    if not q and "title" in row:
        q = str(row["title"]).strip()
    return f"https://www.ebay.com/sch/i.html?_nkw={_url.quote_plus(q)}" if q else None

def _fmt_price(x):
    try:
        f = float(x)
        return f"${f:.2f}"
    except Exception:
        return ""

def _render_table(df):
    # Monta colunas de exibição:
    # - remove "currency"
    # - "price_disp" formatado com $ e 2 casas
    show_cols = ["title","price_disp","available_qty","brand","mpn","gtin","item_url","search_url"]
    exist = [c for c in show_cols if c in df.columns]
    st.dataframe(
        df[exist],
        use_container_width=True,
        hide_index=True,
        column_config={
            "title": "Título",
            "price_disp": "Preço",
            "available_qty": "Qtd (estim.)",
            "brand": "Marca",
            "mpn": "MPN",
            "gtin": "UPC/EAN/ISBN",
            "item_url": st.column_config.LinkColumn("Produto (eBay)", display_text="Abrir"),
            "search_url": st.column_config.LinkColumn("Ver outros vendedores", display_text="Buscar"),
        },
    )

# ----------------------------
# Ação: Minerar
# ----------------------------
if st.button("🧲 Minerar eBay"):
    cat_ids = _resolve_category_ids()
    if not cat_ids:
        st.error("Nenhuma categoria resolvida. Revise `search_tasks.yaml`.")
        st.stop()

    pmin_v = _num(pmin)
    pmax_v = _num(pmax)
    qmin_v = _num(qty_min, to_int=True)

    try:
        # Timer / mensagens dinâmicas
        t0 = time.time()
        msg = st.empty()
        progress = st.progress(0, text="Iniciando...")

        # 1) Coleta
        all_rows = []
        total = len(cat_ids)
        for i, cat_id in enumerate(cat_ids, start=1):
            cat_t0 = time.time()
            results = search_category_safe(
                category_id=cat_id,
                source_price_min=pmin_v,
                condition=cond,
                limit_per_page=API_ITEMS_PER_PAGE,
                max_pages=API_MAX_PAGES,
            ) or []
            for r in results:
                if not r.get("category_id"):
                    r["category_id"] = cat_id
            all_rows.extend(results)

            # ETA simples por categoria concluída
            elapsed = time.time() - t0
            per_cat = elapsed / i
            rem = (total - i) * per_cat
            progress.progress(i/total)
            msg.markdown(
                f"⏳ Buscando… (categoria {i}/{total}) — decorrido **{elapsed:0.1f}s** · estimado restante **{rem:0.1f}s**"
            )

        df = pd.DataFrame(all_rows)
        if df.empty:
            st.warning("Sem resultados para os filtros.")
            st.stop()

        df = _dedup(df)

        # 2) Enriquecimento CONDICIONAL (só se qty mínima informada)
        if qmin_v is not None and "available_qty" in df.columns:
            missing_ids = df.loc[df["available_qty"].isna(), "item_id"].dropna().astype(str).tolist()
            to_enrich = missing_ids[:MAX_ENRICH]
            if to_enrich:
                enr = []
                for j, iid in enumerate(to_enrich, start=1):
                    d = get_item_detail(iid)
                    if d.get("item_id") and (not d.get("category_id")):
                        base_cat = df.loc[df["item_id"] == d["item_id"], "category_id"]
                        if not base_cat.empty:
                            d["category_id"] = int(base_cat.iloc[0])
                    enr.append(d)

                    # ETA simples no enriquecimento
                    elapsed = time.time() - t0
                    per_item = elapsed / max(1, (len(cat_ids) + j))  # aproximação
                    rem = (len(to_enrich) - j) * per_item
                    progress.progress(min(1.0, (i-1)/total + (j/len(to_enrich))/total))
                    msg.markdown(
                        f"🔎 Enriquecendo… ({j}/{len(to_enrich)}) — decorrido **{elapsed:0.1f}s** · estimado restante **{rem:0.1f}s**"
                    )

                if enr:
                    df_enr = _dedup(pd.DataFrame(enr))
                    if not df_enr.empty and "item_id" in df_enr.columns:
                        df = df.merge(
                            df_enr[["item_id","available_qty","qty_flag","brand","mpn","gtin","category_id"]],
                            on="item_id", how="left", suffixes=("", "_enr")
                        )
                        for col in ["available_qty","qty_flag","brand","mpn","gtin","category_id"]:
                            alt = f"{col}_enr"
                            if alt in df.columns:
                                df[col] = df[col].where(df[col].notna(), df[alt])
                        drop_cols = [c for c in df.columns if c.endswith("_enr")]
                        df = df.drop(columns=drop_cols)

        # 3) Filtros finais (preço/qty)
        view = _apply_filters_local(df, pmin_v, pmax_v, qmin_v)

        # 3.1) Ordenação global por preço crescente (antes de paginar)
        view["price_num"] = pd.to_numeric(view["price"], errors="coerce")
        view = view.sort_values(by=["price_num","title"], ascending=[True, True], kind="mergesort").reset_index(drop=True)

        # 3.2) Formatação do preço e remoção da coluna currency
        view["price_disp"] = view["price_num"].apply(_fmt_price)
        if "currency" in view.columns:
            view = view.drop(columns=["currency"])

        # 3.3) Link de “outros vendedores”
        if "search_url" not in view.columns:
            view["search_url"] = view.apply(_make_search_url, axis=1)

        # 4) Sanitiza e persiste
        engine = make_engine()
        safe_df = sql_safe_frame(view)  # evita NaN no MySQL
        n = upsert_ebay_listings(engine, safe_df)

        elapsed_total = time.time() - t0
        msg.markdown(f"✅ Concluído em **{elapsed_total:0.1f}s**.")
        st.success(f"Gravados/atualizados: **{n}**")

        # 5) Guarda na sessão para paginação e renderiza página 1
        st.session_state["_results_df"] = view.reset_index(drop=True)
        st.session_state["_page_num"] = 1

    except Exception as e:
        st.error(f"Falha na mineração/enriquecimento: {e}")

# ----------------------------
# Tabela + paginação (setas)
# ----------------------------
if "_results_df" in st.session_state and not st.session_state["_results_df"].empty:
    df = st.session_state["_results_df"]

    PAGE_SIZE = 50
    total_pages = max(1, math.ceil(len(df) / PAGE_SIZE))
    page = st.session_state.get("_page_num", 1)

    prev_col, info_col, next_col = st.columns([0.1, 0.8, 0.1])
    with prev_col:
        if st.button("◀", use_container_width=True, disabled=(page <= 1), key="prev_page"):
            st.session_state["_page_num"] = max(1, page - 1)
            st.rerun()
    with info_col:
        st.write(f"**Total: {len(df)} itens | Página {page}/{total_pages}**")
    with next_col:
        if st.button("▶", use_container_width=True, disabled=(page >= total_pages), key="next_page"):
            st.session_state["_page_num"] = min(total_pages, page + 1)
            st.rerun()

    start = (page - 1) * PAGE_SIZE
    end = start + PAGE_SIZE
    page_df = df.iloc[start:end].copy()

    # Render (sem “Moeda”, preço formatado com $ e 2 casas, links clicáveis)
    _render_table(page_df)

    st.caption(f"Página {page}/{total_pages} — exibindo {len(page_df)} itens.")

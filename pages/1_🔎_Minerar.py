import os, math, time, urllib.parse as _url
from datetime import timedelta

import pandas as pd
import streamlit as st

from lib.config import make_engine
from lib.tasks import load_categories_tree, flatten_categories
from lib.db import upsert_ebay_listings, sql_safe_frame
from lib.ebay_search import search_items          # cliente de busca oficial
from lib.ebay_api import get_item_detail          # detalhes para enriquecimento
from lib.redis_cache import cache_get, cache_set

st.header("🔎 Minerar")

API_ITEMS_PER_PAGE = int(st.secrets.get("EBAY_LIMIT_PER_PAGE", os.getenv("EBAY_LIMIT_PER_PAGE", 200)))
API_MAX_PAGES      = int(st.secrets.get("EBAY_MAX_PAGES", os.getenv("EBAY_MAX_PAGES", 25)))
MAX_ENRICH         = int(st.secrets.get("MAX_ENRICH", os.getenv("MAX_ENRICH", 500)))

tree = load_categories_tree()
flat = flatten_categories(tree)

# ── filtros ─────────────────────────────────────────────────────────────────────
col_kw, _ = st.columns([2, 1])
with col_kw:
    kw = st.text_input("Palavra-chave (opcional)", value="").strip()
    st.session_state["_kw"] = kw

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

col3, col4, col5 = st.columns([1, 1, 1])
with col3:
    pmin_input = st.number_input("Preço mínimo (US$)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
with col4:
    pmax_input = st.number_input("Preço máximo (US$)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
with col5:
    cond_pt = st.selectbox("Condição", ["Novo", "Usado", "Recondicionado", "Novo & Usado"], index=0)

qty_min_input = st.number_input("Quantidade mínima (só enriquece se informar)", min_value=0, value=0, step=1)

st.caption("Quanto mais ampla a busca, maior o tempo de pesquisa. Recomendamos adicionar mais filtros para que a busca seja mais rápida.")
st.divider()

# ── helpers ─────────────────────────────────────────────────────────────────────
def _fmt_eta(seconds: float) -> str:
    return str(timedelta(seconds=int(max(0, seconds))))

def _dedup(df: pd.DataFrame) -> pd.DataFrame:
    if "item_id" not in df.columns:
        return df
    return df.dropna(subset=["item_id"]).drop_duplicates(subset=["item_id"], keep="first").copy()

def _apply_qty_filter(df: pd.DataFrame, qmin: int | None) -> pd.DataFrame:
    if qmin is None:
        return df
    if "available_qty" not in df.columns:
        return df.iloc[0:0].copy()
    qty = pd.to_numeric(df["available_qty"], errors="coerce")
    return df[qty.notna() & (qty >= qmin)].copy()

def _apply_price_filter(df: pd.DataFrame, pmin_v: float | None, pmax_v: float | None) -> pd.DataFrame:
    """
    Filtro local de preço. Usa a coluna 'price' numérica.
    """
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
    Reforça o filtro de condição no lado do app.
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
    return ids

def _make_search_url(row) -> str | None:
    q = None
    for key in ["gtin", "UPC/EAN/ISBN", "upc", "ean"]:
        if key in row and pd.notna(row[key]):
            q = str(row[key]).strip()
            break
    if not q and "title" in row:
        q = str(row["title"]).strip()
    return f"https://www.ebay.com/sch/i.html?_nkw={_url.quote_plus(q)}" if q else None

def _render_table(df: pd.DataFrame) -> None:
    show_cols = ["title", "price", "available_qty", "brand", "mpn", "gtin", "condition", "item_url", "search_url"]
    exist = [c for c in show_cols if c in df.columns]
    st.dataframe(
        df[exist],
        use_container_width=True,
        hide_index=True,
        column_config={
            "title": "Título",
            "price": st.column_config.NumberColumn("Preço", format="$%.2f"),
            "available_qty": "Qtd (estim.)",
            "brand": "Marca",
            "mpn": "MPN",
            "gtin": "UPC/EAN/ISBN",
            "condition": "Condição",
            "item_url": st.column_config.LinkColumn("Produto (eBay)", display_text="Abrir"),
            "search_url": st.column_config.LinkColumn("Ver outros vendedores", display_text="Buscar"),
        },
    )

def _ensure_currency(df: pd.DataFrame) -> pd.DataFrame:
    if "currency" not in df.columns:
        df["currency"] = "USD"
    else:
        df["currency"] = df["currency"].fillna("USD").replace("", "USD")
    return df

# ── ação ────────────────────────────────────────────────────────────────────────
if st.button("🧲 Minerar eBay"):
    pmin_v = pmin_input if pmin_input > 0 else None
    pmax_v = pmax_input if pmax_input > 0 else None
    qmin_v = int(qty_min_input) if qty_min_input > 0 else None

    if pmin_v is not None and pmax_v is not None and pmax_v < pmin_v:
        st.error("Preço máximo não pode ser menor que o preço mínimo.")
        st.stop()

    if sel_root == "Todas as categorias" and st.session_state.get("_kw", "") == "":
        st.error("Escolha **uma categoria** ou informe uma **palavra-chave** para buscar.")
        st.stop()

    cat_ids = _resolve_category_ids()
    if not cat_ids:
        st.error("Nenhuma categoria resolvida.")
        st.stop()

    if cond_pt == "Novo":
        cond_list = ["NEW"]
    elif cond_pt == "Usado":
        cond_list = ["USED"]
    elif cond_pt == "Recondicionado":
        cond_list = ["REFURBISHED"]
    else:
        cond_list = ["NEW", "USED"]

    try:
        ns, payload = (
            "browse_results_v2",
            {
                "cat_ids": cat_ids,
                "pmin": pmin_v,
                "pmax": pmax_v,
                "cond_pt": cond_pt,
                "kw": st.session_state.get("_kw", ""),
                "items_per_page": API_ITEMS_PER_PAGE,
                "max_pages": API_MAX_PAGES,
            },
        )
        cached = cache_get(ns, payload)
        t0 = time.time()
        msg = st.empty()
        progress = st.progress(0.0, text="Preparando coleta…")

        if cached:
            df = _dedup(pd.DataFrame(cached))
            st.info(f"🧠 Cache usado: {len(df)} itens brutos antes de filtros.")
        else:
            all_rows: list[dict] = []
            total_steps = len(cat_ids) * len(cond_list)
            step = 0
            failures = 0

            for cond in cond_list:
                for cat_id in cat_ids:
                    step += 1
                    try:
                        items = search_items(
                            category_id=cat_id,
                            keyword=st.session_state.get("_kw", "") or None,
                            price_min=pmin_v,
                            price_max=pmax_v,
                            condition=cond,
                            limit_per_page=API_ITEMS_PER_PAGE,
                            max_pages=API_MAX_PAGES,
                        )
                        all_rows.extend(items)
                    except Exception as e:
                        failures += 1
                        st.warning(f"⚠️ Falha na categoria {cat_id} ({type(e).__name__}): {e}. Continuando…")

                    elapsed = time.time() - t0
                    per_step = elapsed / max(1, step)
                    rem = (total_steps - step) * per_step
                    progress.progress(
                        step / total_steps,
                        text=f"Consultando eBay… {step}/{total_steps} · decorrido {elapsed:.1f}s · restante ~{_fmt_eta(rem)}",
                    )
                    msg.markdown(
                        f"⏳ Buscando… **{step}/{total_steps}** — decorrido **{elapsed:0.1f}s** · estimado restante **{_fmt_eta(rem)}**"
                    )

            df = _dedup(pd.DataFrame(all_rows))
            st.info(f"🔎 API retornou {len(df)} itens brutos antes de filtros.")
            if failures:
                st.info(f"{failures} categoria(s) falharam por timeout/erro de rede. As demais foram processadas.")
            cache_set(ns, payload, df.to_dict(orient="records"), ttl_sec=1800)

        if df.empty:
            st.warning("Sem resultados para os filtros (antes dos filtros locais).")
            st.stop()

        # ── filtro local de preço ───────────────────────────────────────────────
        df = _apply_price_filter(df, pmin_v, pmax_v)
        st.info(f"💰 Após filtro de preço local: {len(df)} itens.")
        if df.empty:
            st.warning("Nenhum item dentro da faixa de preço informada.")
            st.stop()

        # ── filtro local de condição (reforço) ─────────────────────────────────
        df = _apply_condition_filter(df, cond_pt)
        st.info(f"🎯 Após filtro de condição local: {len(df)} itens.")
        if df.empty:
            st.warning("Nenhum item dentro da condição escolhida.")
            st.stop()

        # ── enriquecimento (estoque / brand / mpn / gtin) ──────────────────────
        if qmin_v is not None:
            ids = df["item_id"].dropna().astype(str).unique().tolist()
            to_enrich = ids[:MAX_ENRICH]
            if to_enrich:
                msg = st.empty()
                progress = st.progress(0.0, text="Enriquecendo…")
                t1 = time.time()
                enr: list[dict] = []
                for j, iid in enumerate(to_enrich, start=1):
                    d = get_item_detail(iid)
                    enr.append(d)
                    elapsed_e = time.time() - t1
                    rem_e = (len(to_enrich) - j) * (elapsed_e / max(1, j))
                    progress.progress(
                        j / len(to_enrich),
                        text=f"Enriquecendo… {j}/{len(to_enrich)} · restante ~{_fmt_eta(rem_e)}",
                    )
                    msg.markdown(
                        f"🔎 Enriquecendo… ({j}/{len(to_enrich)}) — decorrido **{elapsed_e:.1f}s** · estimado restante **{_fmt_eta(rem_e)}**"
                    )
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

        # ── filtro de quantidade ────────────────────────────────────────────────
        view = _apply_qty_filter(df, qmin_v)
        st.info(f"🧪 Depois dos filtros locais (preço + condição + quantidade): {len(view)} itens.")

        # ordenação inicial (padrão) — depois o usuário pode clicar no cabeçalho
        view["price"] = pd.to_numeric(view["price"], errors="coerce")
        view = (
            view.sort_values(by=["price", "title"], ascending=[True, True], kind="mergesort")
            .reset_index(drop=True)
        )

        # persistir (mantém currency) e exibir (sem currency extra)
        view_for_db = _ensure_currency(view.copy())
        n = upsert_ebay_listings(make_engine(), sql_safe_frame(view_for_db))
        st.success(f"✅ Gravados/atualizados: **{n}** registros.")

        if "search_url" not in view.columns:
            view["search_url"] = view.apply(_make_search_url, axis=1)
        if "currency" in view.columns:
            view = view.drop(columns=["currency"])

        st.session_state["_results_df"] = view.reset_index(drop=True)
        st.session_state["_page_num"] = 1

    except Exception as e:
        st.error(f"Falha na mineração/enriquecimento: {e}")

# ── tabela + paginação ─────────────────────────────────────────────────────────

if "_results_df" in st.session_state and not st.session_state["_results_df"].empty:
    df = st.session_state["_results_df"]
    PAGE_SIZE = 50
    total_pages = max(1, math.ceil(len(df) / PAGE_SIZE))
    page = st.session_state.get("_page_num", 1)

    # navegação: ⏮ -10  |  ◀ -1  | info | +1 ▶  |  +10 ⏭
    col_jump_back, col_prev, col_info, col_next, col_jump_forward = st.columns(
        [0.08, 0.08, 0.68, 0.08, 0.08]
    )

    with col_jump_back:
        if st.button("⏮", use_container_width=True, disabled=(page <= 1), key="jump_back_10"):
            st.session_state["_page_num"] = max(1, page - 10)
            st.rerun()

    with col_prev:
        if st.button("◀", use_container_width=True, disabled=(page <= 1), key="prev_page"):
            st.session_state["_page_num"] = max(1, page - 1)
            st.rerun()

    with col_info:
        st.write(f"**Total: {len(df)} itens | Página {page}/{total_pages}**")

    with col_next:
        if st.button("▶", use_container_width=True, disabled=(page >= total_pages), key="next_page"):
            st.session_state["_page_num"] = min(total_pages, page + 1)
            st.rerun()

    with col_jump_forward:
        if st.button("⏭", use_container_width=True, disabled=(page >= total_pages), key="jump_forward_10"):
            st.session_state["_page_num"] = min(total_pages, page + 10)
            st.rerun()

    start, end = (page - 1) * PAGE_SIZE, (page - 1) * PAGE_SIZE + PAGE_SIZE
    _render_table(df.iloc[start:end].copy())
    st.caption(f"Página {page}/{total_pages} — exibindo {len(df.iloc[start:end])} itens.")


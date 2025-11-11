import pandas as pd
import streamlit as st
from lib.config import make_engine
from lib.tasks import load_tasks
from lib.db import upsert_ebay_listings, fetch_recent_items
from lib.ebay_api import search_by_category

st.header("🔎 Minerar")

tasks_df = load_tasks()

col1, col2, col3, col4 = st.columns([1.2,1,1,1])
with col1:
    options = tasks_df["category_id"].astype(int).unique().tolist() if not tasks_df.empty and "category_id" in tasks_df.columns else []
    if options:
        st.session_state.filters["category_id"] = st.selectbox(
            "Categoria eBay (category_id)", options,
            index=options.index(st.session_state.filters["category_id"]) if st.session_state.filters["category_id"] in options else 0
        )
    else:
        st.session_state.filters["category_id"] = st.number_input(
            "Categoria eBay (category_id)", min_value=0, step=1,
            value=int(st.session_state.filters["category_id"])
        )
with col2:
    st.session_state.filters["source_price_min"] = st.number_input(
        "Preço mínimo (US$)", min_value=0.0, step=1.0,
        value=float(st.session_state.filters["source_price_min"]), format="%.2f"
    )
with col3:
    st.session_state.filters["min_qty"] = st.number_input(
        "Quantidade mínima", min_value=1, step=1, value=int(st.session_state.filters["min_qty"])
    )
with col4:
    st.session_state.filters["condition"] = st.selectbox(
        "Condição", ["NEW","USED","REFURBISHED"],
        index=["NEW","USED","REFURBISHED"].index(st.session_state.filters["condition"])
    )

st.caption("Ajuste e clique em **Minerar eBay**. Tudo é salvo no banco.")
st.divider()

if st.button("🧲 Minerar eBay (por categoria)"):
    try:
        st.info("Consultando eBay Browse API…")
        results = search_by_category(
            category_id=int(st.session_state.filters["category_id"]),
            source_price_min=float(st.session_state.filters["source_price_min"]),
            condition=str(st.session_state.filters["condition"]),
            limit_per_page=50,
            max_pages=2,
        )
        df = pd.DataFrame(results)
        st.write(f"Itens retornados: **{len(df)}**")

        if not df.empty:
            df["available_qty_int"] = pd.to_numeric(df["available_qty"], errors="coerce").fillna(-1).astype(int)
            st.write(f"Itens com qty **informada**: **{(df['available_qty'].notna()).sum()}**")

            df_qty_ok = df[df["available_qty_int"] >= int(st.session_state.filters["min_qty"])]
            st.write(f"Qtd ≥ {int(st.session_state.filters['min_qty'])}: **{len(df_qty_ok)}**")

            engine = make_engine()
            inserted_all = upsert_ebay_listings(engine, df.drop(columns=["available_qty_int"]))
            st.success(f"Gravados/atualizados (TODOS): **{inserted_all}** em `ebay_listing`.")

            if not df_qty_ok.empty:
                cols_map = {"title":"Título","price":"Preço","currency":"Moeda","available_qty":"Qtd (estim.)",
                            "brand":"Marca","mpn":"MPN","gtin":"UPC/EAN/ISBN","item_url":"Link"}
                view = df_qty_ok[list(cols_map.keys())].rename(columns=cols_map)
                st.dataframe(view.head(50), use_container_width=True, hide_index=True)
            else:
                st.warning("Nenhum item atingiu a quantidade mínima — ou a API não retornou quantidade.")
        else:
            st.warning("Sem resultados para esses filtros.")
    except Exception as e:
        st.error(f"Falha na mineração: {e}")

st.divider()
st.subheader("Resultados salvos no banco")
colf1, colf2, colf3, colf4 = st.columns([1,1,2,1])
with colf1:
    qty_filter = st.selectbox("Quantidade", ["Todos","Com qty","Sem qty"], index=0)
    has_qty = {"Todos": None, "Com qty": True, "Sem qty": False}[qty_filter]
with colf2:
    min_price_filter = st.number_input("Preço mínimo (US$)", min_value=0.0, step=1.0, value=0.0, format="%.2f")
    min_price_val = min_price_filter if min_price_filter > 0 else None
with colf3:
    q_title = st.text_input("Buscar no título", value="")
with colf4:
    limit_rows = st.number_input("Limite", min_value=10, max_value=2000, value=200, step=10)

if st.button("Atualizar tabela"):
    try:
        engine = make_engine()
        df_view = fetch_recent_items(engine, has_qty, min_price_val, q_title.strip() or None, limit=int(limit_rows))
        st.write(f"Registros exibidos: **{len(df_view)}**")
        if not df_view.empty:
            cols_map = {"title":"Título","price":"Preço","currency":"Moeda","available_qty":"Qtd (estim.)",
                        "brand":"Marca","mpn":"MPN","gtin":"UPC/EAN/ISBN","item_url":"Link","fetched_at":"Atualizado em"}
            view = df_view[list(cols_map.keys())].rename(columns=cols_map)
            st.dataframe(view, use_container_width=True, hide_index=True)
        else:
            st.info("Nada a exibir com os filtros atuais.")
    except Exception as e:
        st.error(f"Falha ao carregar dados: {e}")

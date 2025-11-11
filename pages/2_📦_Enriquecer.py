import pandas as pd
import streamlit as st
from sqlalchemy import text
from lib.config import make_engine
from lib.db import update_qty_batch
from lib.ebay_api import get_item_detail

st.header("📦 Enriquecer quantidade (detalhe por item)")
max_to_enrich = st.number_input("Máx. itens sem qty para enriquecer", min_value=10, max_value=500, value=100, step=10)

if st.button("🔄 Enriquecer qty para itens recentes sem quantidade"):
    try:
        engine = make_engine()
        sql_pick = text("""
            SELECT item_id
            FROM ebay_listing
            WHERE available_qty IS NULL
            ORDER BY fetched_at DESC
            LIMIT :lim
        """)
        with engine.begin() as conn:
            rows = conn.execute(sql_pick, {"lim": int(max_to_enrich)}).fetchall()

        if not rows:
            st.info("Não há itens sem quantidade para enriquecer.")
        else:
            item_ids = [r[0] for r in rows]
            enriched = []
            prog = st.progress(0, text="Consultando detalhes no eBay…")
            total = len(item_ids)

            for i, iid in enumerate(item_ids, start=1):
                try:
                    d = get_item_detail(iid)
                    enriched.append(d)
                except Exception as e:
                    st.warning(f"Falha ao enriquecer {iid}: {e}")
                finally:
                    prog.progress(min(i/total, 1.0))

            df_enriched = pd.DataFrame(enriched)
            df_enriched = df_enriched[df_enriched["available_qty"].notna()]

            if df_enriched.empty:
                st.warning("Nenhum item retornou quantidade no detalhe.")
            else:
                updated = update_qty_batch(engine, df_enriched)
                st.success(f"Atualizados {updated} itens com quantidade.")
                cols_map = {"item_id":"Item ID","available_qty":"Qtd (estim.)","qty_flag":"Origem",
                            "brand":"Marca","mpn":"MPN","gtin":"UPC/EAN/ISBN"}
                view = df_enriched[list(cols_map.keys())].rename(columns=cols_map)
                st.dataframe(view.head(50), use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Falha no enriquecimento: {e}")

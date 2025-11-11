import pandas as pd
import streamlit as st
from sqlalchemy import text
from pathlib import Path
from lib.config import make_engine, DB_HOST, DB_PORT, DB_USER, DB_NAME
from lib.tasks import load_tasks

st.header("⚙️ Avançado (debug, presets)")
tasks_df = load_tasks()
if tasks_df.empty:
    st.info("Nenhuma tarefa encontrada. Edite `search_tasks.yaml`.")
else:
    st.dataframe(tasks_df, use_container_width=True)

st.divider()
st.subheader("Diagnóstico rápido do ambiente")
colA, colB = st.columns(2)
with colA:
    st.write("**Variáveis de ambiente (DB)**")
    st.code(f"DB_HOST={DB_HOST}\nDB_PORT={DB_PORT}\nDB_USER={DB_USER}\nDB_NAME={DB_NAME}", language="bash")
with colB:
    st.write("**Arquivos**")
    root = Path(__file__).resolve().parents[1]
    status_items = {
        "Arquivo .env": (root / ".env").exists(),
        "search_tasks.yaml": (root / "search_tasks.yaml").exists(),
    }
    st.table(pd.DataFrame([status_items]))

st.divider()
if st.button("🔌 Testar conexão com MySQL"):
    try:
        engine = make_engine()
        with engine.connect() as conn:
            current_db = conn.execute(text("SELECT DATABASE();")).scalar()
            count_ebay = conn.execute(text("SELECT COUNT(*) FROM ebay_listing;")).scalar()
            st.success(f"Conexão OK! DATABASE() = {current_db} | ebay_listing registros: {count_ebay}")
    except Exception as e:
        st.error(f"Falha ao conectar: {e}")

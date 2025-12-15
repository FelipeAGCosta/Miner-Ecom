"""
Página Streamlit: Avançado (diagnóstico).

- Mostra variáveis básicas de conexão com o MySQL (DB_*).
- Verifica a existência de arquivos sensíveis (.env, search_tasks.yaml).
- Permite testar a conexão com o banco e contar registros em ebay_listing.
"""

from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import text

from lib.config import make_engine, DB_HOST, DB_PORT, DB_USER, DB_NAME

# ---------------------------------------------------------------------------
# CSS global
# ---------------------------------------------------------------------------
CSS_PATH = Path(__file__).resolve().parent.parent / "assets" / "style.css"
if CSS_PATH.exists():
    st.markdown(f"<style>{CSS_PATH.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Cabeçalho
# ---------------------------------------------------------------------------
st.markdown(
    "<h1 style='text-align:center; margin-top:8px;'>⚙️ Avançado (diagnóstico)</h1>",
    unsafe_allow_html=True,
)

st.subheader("Diagnóstico rápido do ambiente")

# ---------------------------------------------------------------------------
# Bloco: variáveis DB + arquivos importantes
# ---------------------------------------------------------------------------
colA, colB = st.columns(2)

with colA:
    st.write("**Variáveis de ambiente (DB)**")
    st.code(
        f"DB_HOST={DB_HOST}\nDB_PORT={DB_PORT}\nDB_USER={DB_USER}\nDB_NAME={DB_NAME}",
        language="bash",
    )

with colB:
    st.write("**Arquivos**")
    root = Path(__file__).resolve().parents[1]
    status_items = {
        "Arquivo .env": (root / ".env").exists(),
        "search_tasks.yaml": (root / "search_tasks.yaml").exists(),
    }
    st.table(pd.DataFrame([status_items]))

st.divider()

# ---------------------------------------------------------------------------
# Teste de conexão com MySQL
# ---------------------------------------------------------------------------
if st.button("🔌 Testar conexão com MySQL"):
    try:
        engine = make_engine()
        with engine.connect() as conn:
            current_db = conn.execute(text("SELECT DATABASE();")).scalar()
            count_ebay = conn.execute(text("SELECT COUNT(*) FROM ebay_listing;")).scalar()

        st.success(
            f"Conexão OK! DATABASE() = {current_db} | "
            f"ebay_listing registros: {count_ebay}"
        )
    except Exception as e:
        st.error(f"Falha ao conectar: {e}")

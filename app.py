import os
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import yaml

# ---------------------------------------------
# Carregar variáveis de ambiente (.env)
# ---------------------------------------------
ENV_PATH = Path(__file__).parent / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
else:
    st.warning(".env não encontrado na raiz. Crie a partir do .env.example.")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "")

# ---------------------------------------------
# Helper: criar engine do MySQL (SQLAlchemy)
# ---------------------------------------------
def make_engine():
    if not all([DB_HOST, DB_PORT, DB_USER, DB_NAME]) or DB_PASS is None:
        raise RuntimeError("Variáveis de DB ausentes no .env (DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME).")
    url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True)

# ---------------------------------------------
# Carregar configurações de busca (search_tasks.yaml)
# ---------------------------------------------
def load_tasks():
    cfg_path = Path(__file__).parent / "search_tasks.yaml"
    if not cfg_path.exists():
        return pd.DataFrame()
    with open(cfg_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    tasks = data.get("tasks", [])
    if not tasks:
        return pd.DataFrame()
    return pd.DataFrame(tasks)

# ---------------------------------------------
# UI
# ---------------------------------------------
st.set_page_config(page_title="Miner Ecom — eBay → Amazon", layout="wide")
st.title("🛒 Miner Ecom — eBay → Amazon (MVP UI)")

with st.sidebar:
    st.header("Configurações rápidas")
    st.caption("Preencha o .env com as credenciais do MySQL.")

    # Botão de teste de conexão
    if st.button("🔌 Testar conexão com MySQL"):
        try:
            engine = make_engine()
            with engine.connect() as conn:
                current_db = conn.execute(text("SELECT DATABASE();")).scalar()
                count_ebay = conn.execute(text("SELECT COUNT(*) FROM ebay_listing;")).scalar()
                st.success(f"Conexão OK! DATABASE() = {current_db} | ebay_listing registros: {count_ebay}")
        except Exception as e:
            st.error(f"Falha ao conectar: {e}")

st.subheader("Tarefas de busca (search_tasks.yaml)")
tasks_df = load_tasks()
if tasks_df.empty:
    st.info("Nenhuma tarefa encontrada. Edite `search_tasks.yaml` para adicionar pelo menos uma task.")
else:
    st.dataframe(tasks_df, use_container_width=True)

# Filtros manuais (você pode alterar antes de rodar a mineração)
st.subheader("Filtros da execução (pré-mineração)")
col1, col2, col3, col4 = st.columns([1.2, 1, 1, 1])

with col1:
    # Preferir categoria vinda do YAML; se não vier, permitir digitar.
    category_id_options = tasks_df["category_id"].astype("Int64").dropna().unique().tolist() if not tasks_df.empty else []
    if category_id_options:
        category_id = st.selectbox("Categoria eBay (category_id)", category_id_options, index=0)
    else:
        category_id = st.number_input("Categoria eBay (category_id)", min_value=0, step=1, value=11700)

with col2:
    source_price_min = st.number_input("Preço mínimo (US$)", min_value=0.0, step=1.0, value=15.0, format="%.2f")

with col3:
    min_qty = st.number_input("Quantidade mínima", min_value=1, step=1, value=10)

with col4:
    condition = st.selectbox("Condição", ["NEW", "USED", "REFURBISHED"], index=0)

st.caption("Esses filtros serão usados na mineração do eBay (por categoria), exatamente como combinamos.")

# Bloco de diagnóstico do ambiente
st.subheader("Diagnóstico rápido do ambiente")
colA, colB = st.columns(2)
with colA:
    st.write("**Variáveis de ambiente (DB)**")
    st.code(
        f"DB_HOST={DB_HOST}\nDB_PORT={DB_PORT}\nDB_USER={DB_USER}\nDB_NAME={DB_NAME}",
        language="bash"
    )
with colB:
    st.write("**Resumo**")
    status_items = {
        "Arquivo .env": ENV_PATH.exists(),
        "search_tasks.yaml": (Path(__file__).parent / "search_tasks.yaml").exists(),
    }
    st.table(pd.DataFrame([status_items]))

# Simulação do parâmetro de execução (por enquanto só mostra; na próxima etapa chamaremos a eBay API)
if st.button("✅ Salvar parâmetros da execução (teste)"):
    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "category_id": int(category_id),
        "source_price_min": float(source_price_min),
        "min_qty": int(min_qty),
        "condition": condition,
    }
    st.success("Parâmetros prontos para a mineração (exemplo).")
    st.json(payload)

import streamlit as st
from lib.tasks import load_tasks

st.set_page_config(page_title="Miner Ecom — eBay → Amazon", layout="wide")
st.title("🛒 Miner Ecom — eBay → Amazon")

# Estado default (zerado) — condição NEW permanece como padrão
if "filters" not in st.session_state:
    st.session_state.filters = {
        "category_id": None,             # passa a ser escolhido pelo nome (dropdown)
        "category_name": None,
        "source_price_min": None,        # vazio
        "min_qty": None,                 # vazio
        "condition": "NEW",              # mantém NEW
        "max_enrich": 100,               # limite para enriquecer
    }

tasks_df = load_tasks()
st.success("Use o menu lateral para navegar: Minerar e Avançado.")

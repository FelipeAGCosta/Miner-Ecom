import streamlit as st
from lib.tasks import load_tasks

st.set_page_config(page_title="Miner Ecom — eBay → Amazon", layout="wide")
st.title("🛒 Miner Ecom — eBay → Amazon")

# Estado inicial compartilhado entre páginas
tasks_df = load_tasks()
_default_category = (
    tasks_df["category_id"].astype("Int64").dropna().iloc[0]
    if not tasks_df.empty and "category_id" in tasks_df.columns and len(tasks_df["category_id"].dropna()) > 0
    else 11700
)
if "filters" not in st.session_state:
    st.session_state.filters = {
        "category_id": int(_default_category),
        "source_price_min": 15.0,
        "min_qty": 10,
        "condition": "NEW",
    }

st.success("Use o menu lateral para navegar: Minerar, Enriquecer, Avançado.")

from pathlib import Path
import streamlit as st
from lib.tasks import load_tasks

# --- Configuracao global da pagina ---
st.set_page_config(
    page_title="Miner Ecom - Arbitragem eBay -> Amazon",
    page_icon="??",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Carregar CSS global ---
CSS_PATH = Path(__file__).parent / "assets" / "style.css"
if CSS_PATH.exists():
    st.markdown(f"<style>{CSS_PATH.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

# --- Sidebar: logo + descricao ---
logo_path = Path(__file__).parent / "assets" / "logo_miner_ecom.png"
with st.sidebar:
    if logo_path.exists():
        st.image(str(logo_path), use_column_width=True)
    st.markdown("---")

st.markdown(
    "<h1 style='text-align:center; margin-top:0;'>Miner Ecom - eBay &amp; Amazon</h1>",
    unsafe_allow_html=True,
)
st.markdown(
    "<div style='text-align:center; color:#9CA3AF; margin-bottom:1.5rem;'>"
    "Ferramenta de mineracao para arbitragem entre eBay e Amazon (SP-API)."
    "</div>",
    unsafe_allow_html=True,
)

# Estado default (zerado) - condicao NEW permanece como padrao
if "filters" not in st.session_state:
    st.session_state.filters = {
        "category_id": None,
        "category_name": None,
        "source_price_min": None,
        "min_qty": None,
        "condition": "NEW",
        "max_enrich": 100,
    }

tasks_df = load_tasks()
st.success("Use o menu lateral para navegar: Minerar e Avançado.")

from pathlib import Path
import streamlit as st
from lib.tasks import load_tasks

# --- Configuração global da página ---
st.set_page_config(
    page_title="Miner Ecom – Arbitragem eBay → Amazon",
    page_icon="🧲",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Carregar CSS global ---
CSS_PATH = Path(__file__).parent / "assets" / "style.css"
if CSS_PATH.exists():
    st.markdown(f"<style>{CSS_PATH.read_text()}</style>", unsafe_allow_html=True)

# --- Sidebar: logo + descrição ---
logo_path = Path(__file__).parent / "assets" / "logo_miner_ecom.png"
with st.sidebar:
    if logo_path.exists():
        st.image(str(logo_path), use_column_width=True)
    st.markdown("### Miner Ecom")
    st.markdown(
        "<span class='small-muted'>Ferramenta de mineração para arbitragem entre eBay e Amazon (SP-API).</span>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

st.title("Miner Ecom - eBay & Amazon")

# Estado default (zerado) - condicao NEW permanece como padrao
if "filters" not in st.session_state:
    st.session_state.filters = {
        "category_id": None,             # passa a ser escolhido pelo nome (dropdown)
        "category_name": None,
        "source_price_min": None,        # vazio
        "min_qty": None,                 # vazio
        "condition": "NEW",              # mantem NEW
        "max_enrich": 100,               # limite para enriquecer
    }

tasks_df = load_tasks()
st.success("Use o menu lateral para navegar: Minerar e Avancado.")

"""
App principal (Home) do Miner Ecom.

- Define configuração global do Streamlit (título, layout, sidebar).
- Aplica o CSS global da pasta assets/.
- Renderiza a tela inicial com logo e mensagem de boas-vindas.
- Inicializa um estado base de filtros em st.session_state (legado, útil para fluxos futuros).
"""

from pathlib import Path

import streamlit as st

# ---------------------------------------------------------------------------
# Configuração global da página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Miner Ecom - Exploração de produtos Amazon/eBay",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS global
# ---------------------------------------------------------------------------
CSS_PATH = Path(__file__).parent / "assets" / "style.css"
if CSS_PATH.exists():
    st.markdown(f"<style>{CSS_PATH.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar: logo + separador
# ---------------------------------------------------------------------------
logo_path = Path(__file__).parent / "assets" / "logo_miner_ecom.png"
with st.sidebar:
    if logo_path.exists():
        st.image(str(logo_path), use_column_width=True)
    st.markdown("---")

# ---------------------------------------------------------------------------
# Conteúdo da home
# ---------------------------------------------------------------------------
st.markdown(
    """
    <div style="min-height:80vh; display:flex; flex-direction:column; align-items:center; justify-content:center; text-align:center; gap:12px;">
      <h1 style="margin:0;">Miner Ecom - eBay &amp; Amazon</h1>
      <div style="color:#4B5563;">
        Ferramenta de mineração para arbitragem entre eBay e Amazon (SP-API).
      </div>
      <div style="margin:0 auto; width:80%; background:#D1FAE5; color:#065F46; padding:0.8rem 1rem; border:1px solid #34D399; border-radius:10px;">
        Use o menu lateral para navegar: <strong>Minerar</strong> e <strong>Avançado</strong>.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Estado default (legado) – mantém estrutura para filtros globais
# ---------------------------------------------------------------------------
if "filters" not in st.session_state:
    st.session_state.filters = {
        "category_id": None,
        "category_name": None,
        "source_price_min": None,
        "min_qty": None,
        "condition": "NEW",
        "max_enrich": 100,
    }

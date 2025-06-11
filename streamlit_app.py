"""
Streamlit UI for AIVA – Autonomous Finance Agent
Revamped for professional, business‑ready presentation at the 2025 Snowflake Hackathon.
Author: Varun Iyer (Intern, Nihilent Ltd.) 11 June 2025
"""
import streamlit as st
from snowflake.snowpark import Session
 
# --- Local project imports ----------------------------------------------------
from config.snowflake_config import get_connection
 
# -----------------------------------------------------------------------------
# 📐  Page‑level config & global styles
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="AIVA • Autonomous Finance Agent",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)
 
# ✨ Clean, minimal corporate palette
PRIMARY_COLOR = "#2E3C7E"      # Deep Indigo (for buttons, accents)
ACCENT_COLOR = "#FCA311"       # Warm Gold (for highlights, charts)
BACKGROUND_COLOR = "#F5F7FA"   # Light Greyish Blue (safe background)
SECONDARY_TEXT_COLOR = "#5A5A5A"  # Medium Grey (for subtext)
PRIMARY_TEXT_COLOR = "#000000"   # Black (for headings and main content)

# Inject base CSS for a clean, spacious look
st.markdown(
    f"""
    <style>
      html, body, [class*="st-"] {{ background-color: {BACKGROUND_COLOR}; }}
      .block-container {{ padding-top: 2rem; }}
      footer {{ visibility: hidden; }}
      #MainMenu {{ visibility: hidden; }}
      .step-header {{ font-size: 1.25rem; font-weight: 600; margin: 0.25rem 0; }}
      .metric-label {{ color: {PRIMARY_COLOR}; }}
    </style>
    """,
    unsafe_allow_html=True,
)

 
# -----------------------------------------------------------------------------
# 🗄️  Connections & caching helpers
# -----------------------------------------------------------------------------
 
@st.cache_resource(show_spinner=False)
def _get_session() -> Session:
    """Return a cached Snowpark session (reuse across reruns)."""
    return get_connection()  # Should return a dict
    # return Session.builder.configs(connection_params).create()
 
# -----------------------------------------------------------------------------
# 🧠  Session‑state initialisation
# -----------------------------------------------------------------------------
DEFAULTS = {
    "uploaded_df": None,
    "actual_columns": [],
    "expected_columns": [],
    "missing_columns": [],
    "column_mapping": {},
    "table_name": "invoices",
    "vendor_id": "vendor_001",
}
for k, v in DEFAULTS.items():
    st.session_state.setdefault(k, v)
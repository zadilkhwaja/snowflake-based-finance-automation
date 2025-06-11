from typing import Dict
import streamlit as st
from config.snowflake_config import get_connection
from utils.schema_utils import fetch_column_mapping

st.header("🛠️  Step 3 — Apply header mapping & clean data")

if st.session_state.uploaded_df is None:
    st.warning("Please upload a file first in *Upload & Map*.")
    st.stop()

if st.button("⚡ Auto‑rename columns using saved mapping"):
    conn = get_connection()
    mapping: Dict[str, str] = fetch_column_mapping(
        conn, st.session_state.vendor_id, st.session_state.table_name
    )
    if not mapping:
        st.error("No mapping found — go back and create one.")
    else:
        st.session_state.uploaded_df.rename(columns=mapping, inplace=True)
        st.success("Headers cleaned ✔️  — preview below")
        st.dataframe(st.session_state.uploaded_df.head(), use_container_width=True)

if st.button("Load into Snowflake"):
    st.switch_page("pages/4 Load into Snowflake.py")
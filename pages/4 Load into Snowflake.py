import streamlit as st
from streamlit_app import _get_session
from utils.data_loader import load_dataframe_to_table

st.header("⬆️  Step 4 — Load into Snowflake")

if st.button("Upload cleaned data to Snowflake"):
    with st.spinner("Streaming data up north ☁️ …"):
        try:
            load_dataframe_to_table(
                _get_session(),
                st.session_state.uploaded_df,
                st.session_state.table_name,
            )
            st.success("Data successfully loaded into Snowflake table 🚀")
        except Exception as e:
            st.error(f"Load failed: {e}")

if st.button("Reconcile and Detect Anomalies"):
    st.switch_page("5 Reconcile and Detect Anomalies.py")
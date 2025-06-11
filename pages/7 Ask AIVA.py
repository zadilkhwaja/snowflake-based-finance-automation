import streamlit as st

from streamlit_app import _get_session
from utils.query_assist import generate_sql_with_cortex

st.header("💬 Chat‑based query assistant (Cortex)")

user_question = st.text_input(
    "Ask a finance question in plain English",
    placeholder="e.g. Show unpaid invoices over 10,000",
)

target_table = st.selectbox(
    "Run against table",
    ["invoices", "payments", "purchase_orders"],
    index=0,
)

if st.button("🔎  Ask AIVA") and user_question:
    with st.spinner("Generating SQL via Cortex …"):
        try:
            generated_sql = generate_sql_with_cortex(
                _get_session(), user_question, target_table
            )
            st.code(generated_sql, language="sql")
            st.markdown("**Query results**")
            result_df = _get_session().sql(generated_sql).to_pandas()
            st.dataframe(result_df, use_container_width=True)
        except Exception as e:
            st.error(f"Cortex error: {e}")
import streamlit as st
from utils.generate_Sql_with_cortex import generate_answer_with_cortex

def render():
    from config.snowflake_config import connection_parameters
    st.header("💬 Chat‑based query assistant (Cortex)")

    database = connection_parameters["database"]
    schema = connection_parameters["schema"]

    invoice_table=f"{database}.{schema}.INVOICE_STREAM"
    purchase_order_table=f"{database}.{schema}.PO_STREAM"

    table_options = {
        "INVOICE_TABLE": invoice_table,
        "PURCHASE_ORDER_TABLE": purchase_order_table
    }

    user_question = st.text_input(
        "Ask a finance question in plain English",
        placeholder="e.g. Show unpaid invoices over 10,000",
    )

    selected_label = st.selectbox(
        "Run against table",
        list(table_options.keys()),
        index=0,
    )

    # Get the actual table name for backend processing
    target_table = table_options[selected_label]

    if st.button("🔎  Ask AIVA") and user_question:
        with st.spinner("Generating SQL via Cortex …"):
            try:
                from snowflake.snowpark import Session
                from config.snowflake_config import connection_parameters
                session = Session.builder.configs(connection_parameters).create()
                response = generate_answer_with_cortex(session, user_question, target_table)

                st.markdown("### ✅ Answer in Simple English:")
                st.markdown(f"> {response['answer']}")


            except Exception as e:
                st.error(f"Cortex error: {e}")
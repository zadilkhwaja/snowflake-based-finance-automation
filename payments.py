import streamlit as st
import requests, json
import pandas as pd
from utils.file_parser import parse_file
from utils.snowflake_cortex_utils import (
    get_snowflake_connection,
    classify_doc_type_with_cortex
)
from utils.header_mapper import map_dataframe_to_schema
from utils.snowflake_uploader import (
    create_database_and_tables,
    upload_dataframe_to_snowflake
)
from utils.train_model import train_invoice_model
from config.snowflake_config import connection_parameters

def call_mismatch_api(conn):
    url = "http://localhost:8000/check-mismatch"
    payload = {"po_stream": "PO_STREAM", "invoice_stream": "INVOICE_STREAM"}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error calling API: {e}")
        return None

def to_dataframe(data):
    try:
        return pd.DataFrame(data) if data else pd.DataFrame()
    except:
        return pd.DataFrame()

# Convert terms like "3/15 Net 45" to human-readable
def interpret_discount_terms(term):
    try:
        if "Net" in term:
            part, net_days = term.split(" Net ")
            discount_percent, discount_days = map(int, part.split("/"))
            return f"{discount_percent}% discount if paid within {discount_days} days; full payment due in {net_days} days"
        return term
    except:
        return term

st.set_page_config(page_title="Auto Map to Snowflake", layout="wide")
st.title("📥 Upload Invoice & PO — Map + Upload to Snowflake")

uploaded_files = st.file_uploader(
    "Upload two files (CSV, Excel, or PDF)",
    type=["csv", "xlsx", "xls", "pdf"],
    accept_multiple_files=True
)

if uploaded_files and len(uploaded_files) != 2:
    st.warning("⚠️ Please upload exactly two files: one invoice and one purchase order.")
    st.stop()

if uploaded_files and len(uploaded_files) == 2:
    conn = get_snowflake_connection(connection_parameters)
    create_database_and_tables(conn)

    file_map = {}

    for file in uploaded_files:
        try:
            df = parse_file(file)
            headers_or_text = list(df.columns) if 'raw_text' not in df.columns else df['raw_text'][0]
            doc_type = classify_doc_type_with_cortex(conn, headers_or_text)

            if doc_type not in ["invoice", "purchase_order"]:
                continue

            mapped_df = map_dataframe_to_schema(df, doc_type)
            file_map[doc_type] = mapped_df

        except Exception as e:
            st.error(f"❌ Error processing '{file.name}': {e}")

    for doc_type in ["invoice", "purchase_order"]:
        if doc_type in file_map:
            st.subheader(f"📦 Mapped {doc_type.replace('_', ' ').title()}")
            st.dataframe(file_map[doc_type])

            if upload_dataframe_to_snowflake(file_map[doc_type], doc_type, conn):
                st.success(f"✅ {doc_type.replace('_', ' ').title()} uploaded to Snowflake successfully.")
            else:
                st.error(f"❌ Failed to upload {doc_type}.")

    st.write("Calling mismatch detection API...")
    result = call_mismatch_api(conn)

    if result:
        st.success("API call successful!")

        # View toggle
        view_option = st.radio("Select view mode:", ["Visual View", "Text View"], horizontal=True)

        if view_option == "Text View":
            st.subheader("🧾 Text Results")
            st.write("**Mismatches**")
            st.json(result.get("mismatches", []))

            st.write("**Frauds**")
            st.json(result.get("frauds", []))

            st.write("**Early Payment Opportunities**")
            st.json(result.get("early_payment_opportunities", []))

        else:
            st.subheader("📊 Visual Results")

            # Mismatches
            with st.expander("🔍 View Mismatches", expanded=False):
                mismatches = result.get("mismatches", [])
                df_mismatch = to_dataframe(mismatches)
                if not df_mismatch.empty:
                    st.dataframe(df_mismatch.style.highlight_null(color='red'), use_container_width=True)
                else:
                    st.info("No mismatches found.")

            # Frauds
            # Frauds
            with st.expander("⚠️ View Fraudulent Invoices", expanded=False):
                frauds = result.get("frauds", [])
                df_fraud = to_dataframe(frauds)

                if not df_fraud.empty:
                    # Display and filter fraud types
                    fraud_types = df_fraud['fraud_type'].unique().tolist()
                    selected_types = st.multiselect("Filter by fraud type", fraud_types, default=fraud_types)
                    filtered_frauds = df_fraud[df_fraud['fraud_type'].isin(selected_types)]

                    st.dataframe(filtered_frauds, use_container_width=True)

                    # Chart: Fraud Type Count
                    st.subheader("📊 Fraud Type Distribution")
                    fraud_counts = filtered_frauds['fraud_type'].value_counts()
                    st.bar_chart(fraud_counts)

                    # 📈 Anomaly Score Distribution
                    if 'fraud_score' in filtered_frauds.columns:
                        st.subheader("📉 Anomaly Scores of Detected Invoices")

                        anomaly_scores = filtered_frauds[['invoice_id', 'fraud_score']].dropna()
                        anomaly_scores = anomaly_scores.sort_values(by='fraud_score')

                        # Line chart with threshold marker
                        st.line_chart(anomaly_scores.set_index('invoice_id'))

                        threshold = 0.3
                        st.info(f"ℹ️ Scores below {threshold} are considered anomalies by the Isolation Forest model.")
                else:
                    st.success("No frauds detected.")

            # with st.expander("⚠️ View Fraudulent Invoices", expanded=False):
            #     frauds = result.get("frauds", [])
            #     df_fraud = to_dataframe(frauds)
            #     if not df_fraud.empty:
            #         fraud_types = df_fraud['fraud_type'].unique().tolist()
            #         selected_types = st.multiselect("Filter by fraud type", fraud_types, default=fraud_types)
            #         filtered_frauds = df_fraud[df_fraud['fraud_type'].isin(selected_types)]
            #         st.dataframe(filtered_frauds, use_container_width=True)
            #
            #         # Chart: Fraud counts
            #         fraud_counts = filtered_frauds['fraud_type'].value_counts()
            #         st.bar_chart(fraud_counts)
            #     else:
            #         st.success("No frauds detected.")

            # Early Payments
            with st.expander("💰 View Early Payment Opportunities", expanded=False):
                early_opps = result.get("early_payment_opportunities", [])
                df_early = to_dataframe(early_opps)

                if not df_early.empty:
                    # Human-readable discount interpretation
                    df_early['readable_terms'] = df_early['discount_terms'].apply(interpret_discount_terms)

                    terms = df_early['discount_terms'].unique().tolist()
                    selected_terms = st.multiselect("Filter by discount terms", terms, default=terms)
                    filtered_early = df_early[df_early['discount_terms'].isin(selected_terms)]

                    st.dataframe(
                        filtered_early[['invoice_id', 'potential_savings', 'payment_deadline', 'readable_terms']]
                        .style.format({"potential_savings": "₹{:.2f}"}),
                        use_container_width=True
                    )

                    # Chart
                    savings_chart = filtered_early[['invoice_id', 'potential_savings']].set_index('invoice_id')
                    st.bar_chart(savings_chart)
                else:
                    st.info("No early payment opportunities found.")

        st.markdown(f"🕒 **Timestamp**: `{result.get('timestamp', 'N/A')}`")
        st.markdown("---")

        st.subheader("🧠 Ask AIVA — Chat-based Query Assistant")
        from features import ask_aiva
        ask_aiva.render()

    conn.close()

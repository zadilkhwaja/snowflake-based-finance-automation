# streamlit_app.py

import streamlit as st
import pandas as pd
import os
from snowflake.snowpark import Session
from config.snowflake_config import get_connection
from utils.file_utils import read_uploaded_file
from utils.schema_utils import compare_schema, fetch_column_mapping
from utils.save_mapping_to_snowflake import save_column_mappings
from utils.data_loader import load_dataframe_to_table
from utils.anomaly_detector import detect_payment_anomalies
from utils.dashboard_utils import load_anomalies
from utils.query_assist import generate_sql_with_cortex

# Streamlit page setup
st.set_page_config(page_title="AIVA - Autonomous Finance Agent", layout="wide")
st.title("🤖 AIVA - Autonomous Finance Agent")

# Session state init
if "uploaded_df" not in st.session_state:
    st.session_state.uploaded_df = None
if "column_mapping" not in st.session_state:
    st.session_state.column_mapping = {}

# Step 1: File Upload
st.header("📤 Step 1: Upload Vendor File")
uploaded_file = st.file_uploader("Upload a CSV or Excel file", type=["csv", "xlsx"])

if uploaded_file:
    try:
        df = read_uploaded_file(uploaded_file)
        st.session_state.uploaded_df = df
        st.session_state.actual_columns = list(df.columns)
        st.success("File uploaded successfully!")
        st.dataframe(df.head())
    except Exception as e:
        st.error(f"Upload error: {e}")
        st.stop()
else:
    st.info("Awaiting file upload...")
    st.stop()

# Step 2: Header Detection
st.header("🧠 Step 2: Schema Comparison")
table_name = st.selectbox("Select table to compare:", ["invoices", "payments", "purchase_orders"])
st.session_state.table_name = table_name

conn = get_connection()
cursor = conn.cursor()

cursor.execute(f"""
    SELECT column_name 
    FROM expected_schema
    WHERE table_name = '{table_name}'
""")
expected_columns = [row[0].lower() for row in cursor.fetchall()]
st.session_state.expected_columns = expected_columns

schema_result = compare_schema(st.session_state.actual_columns, expected_columns)
st.session_state.missing_columns = schema_result["missing"]
st.write("**Matching columns:**", schema_result["matching"])
st.write("**Missing columns:**", schema_result["missing"])
st.write("**Extra columns:**", schema_result["extra"])

# Step 3: Manual Column Mapping
if schema_result["missing"]:
    st.header("🔁 Step 3: Column Mapping")
    for col in schema_result["missing"]:
        selection = st.selectbox(f"Map expected column '{col}' to:", st.session_state.actual_columns, key=f"map_{col}")
        st.session_state.column_mapping[col] = selection

    vendor_id = st.text_input("Enter Vendor ID", value="vendor_001")
    st.session_state.vendor_id = vendor_id

    if st.button("💾 Save Column Mapping"):
        save_column_mappings(conn, vendor_id, table_name, st.session_state.column_mapping)
        st.success("Mapping saved.")

# Step 4 & 5: Apply Mapping Automatically
st.header("⚙️ Step 4/5: Apply Mapping")
if st.button("🔁 Auto-Rename Columns"):
    mapping = fetch_column_mapping(conn, st.session_state.vendor_id, table_name)
    if not mapping:
        st.warning("No saved mapping found.")
    else:
        df = st.session_state.uploaded_df
        df.rename(columns=mapping, inplace=True)
        st.session_state.uploaded_df = df
        st.success("Columns renamed.")
        st.dataframe(df.head())

# Step 6: Load into Snowflake
st.header("📥 Step 6: Load Cleaned Data")
if st.button("⬆️ Load to Snowflake"):
    try:
        session = Session.builder.configs(conn._connection_params).create()
        load_dataframe_to_table(session, st.session_state.uploaded_df, table_name)
        st.success("Data loaded into Snowflake.")
    except Exception as e:
        st.error(f"Load failed: {e}")

# Step 7: Run Anomaly Detection
st.header("🚨 Step 7: Reconciliation & Anomaly Detection")
if st.button("🧠 Detect Anomalies"):
    try:
        session = Session.builder.configs(conn._connection_params).create()
        detect_payment_anomalies(session)
        st.success("Anomalies detected.")
    except Exception as e:
        st.error(f"Detection failed: {e}")

# Step 8: Dashboard & Insights
st.header("📊 Step 8: Anomaly Dashboard")
session = Session.builder.configs(conn._connection_params).create()
anomalies_df = load_anomalies(session)

if not anomalies_df.empty:
    st.metric("Total Anomalies", len(anomalies_df))
    st.metric("Vendors Affected", anomalies_df["vendor_id"].nunique())

    selected_vendor = st.selectbox("Filter by Vendor", options=["All"] + anomalies_df["vendor_id"].dropna().unique().tolist())
    selected_type = st.selectbox("Filter by Type", options=["All"] + anomalies_df["anomaly_type"].dropna().unique().tolist())

    filtered_df = anomalies_df
    if selected_vendor != "All":
        filtered_df = filtered_df[filtered_df.vendor_id == selected_vendor]
    if selected_type != "All":
        filtered_df = filtered_df[filtered_df.anomaly_type == selected_type]

    st.dataframe(filtered_df)
    st.bar_chart(filtered_df["anomaly_type"].value_counts())
else:
    st.info("No anomalies detected yet.")

# Bonus: Cortex Query Assist
st.header("💬 Query Assistant (Cortex)")
user_query = st.text_input("Ask a question:", placeholder="e.g. Show unpaid invoices over 10000")
selected_table = st.selectbox("Target Table", ["invoices", "payments", "purchase_orders"])

if st.button("Ask AIVA with Cortex"):
    try:
        generated_sql = generate_sql_with_cortex(session, user_query, selected_table)
        st.code(generated_sql, language="sql")
        result_df = session.sql(generated_sql).to_pandas()
        st.dataframe(result_df)
    except Exception as e:
        st.error(f"Cortex query failed: {e}")
  

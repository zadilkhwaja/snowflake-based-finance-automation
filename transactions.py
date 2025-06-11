import streamlit as st
import pandas as pd
import snowflake.connector
import requests
import json
from datetime import datetime
from config.snowflake_config import connection_parameters

# Initialize Snowflake connection
def get_snowflake_session():
    try:
        conn = snowflake.connector.connect(**connection_parameters)
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

# Validate and insert data into Snowflake
def insert_data_to_snowflake(conn, table_name, df):
    try:
        cursor = conn.cursor()
        if table_name == "PURCHASE_ORDERS":
            expected_columns = ["po_id", "vendor_id", "item_id", "quantity", "unit_price", "total_amount", "order_date"]
            insert_query = """
                INSERT INTO SOUVIKX.PUBLIC.PURCHASE_ORDERS 
                (po_id, vendor_id, item_id, quantity, unit_price, total_amount, order_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
        elif table_name == "INVOICES":
            expected_columns = ["invoice_id", "po_id", "vendor_id", "item_id", "quantity", "unit_price", "total_amount",
                               "invoice_date", "due_date", "discount_terms"]
            insert_query = """
                INSERT INTO SOUVIKX.PUBLIC.INVOICES 
                (invoice_id, po_id, vendor_id, item_id, quantity, unit_price, total_amount, invoice_date, due_date, discount_terms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        else:
            raise ValueError(f"Unknown table: {table_name}")

        # Validate columns
        if not all(col in df.columns for col in expected_columns):
            missing = [col for col in expected_columns if col not in df.columns]
            raise ValueError(f"Missing columns: {missing}")

        # Convert date columns to string format
        for col in ["order_date", "invoice_date", "due_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')

        # Insert data
        for _, row in df.iterrows():
            values = [row[col] for col in expected_columns]
            cursor.execute(insert_query, values)
        conn.commit()
        st.success(f"Inserted {len(df)} rows into {table_name}")
    except Exception as e:
        st.error(f"Error inserting data into {table_name}: {e}")
    finally:
        cursor.close()

# Call FastAPI endpoint
def call_mismatch_api():
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

# Streamlit UI
st.title("Purchase Order and Invoice Mismatch Detection")
st.write("Upload CSV or Excel files for Purchase Orders and Invoices to detect mismatches, frauds, and early payment opportunities.")

# File uploaders
po_file = st.file_uploader("Upload Purchase Orders (CSV/Excel)", type=["csv", "xlsx"])
invoice_file = st.file_uploader("Upload Invoices (CSV/Excel)", type=["csv", "xlsx"])

if st.button("Process Files"):
    if po_file is None and invoice_file is None:
        st.warning("Please upload at least one file.")
    else:
        conn = get_snowflake_session()
        if conn:
            # Process Purchase Orders
            if po_file:
                try:
                    if po_file.name.endswith(".csv"):
                        po_df = pd.read_csv(po_file)
                        po_df.columns = [col.lower() for col in po_df.columns]
                    else:
                        po_df = pd.read_excel(po_file)
                    po_df["order_date"] = pd.to_datetime(po_df["order_date"]).dt.strftime("%Y-%m-%d")
                    insert_data_to_snowflake(conn, "PURCHASE_ORDERS", po_df)
                except Exception as e:
                    st.error(f"Error processing Purchase Orders file: {e}")

            # Process Invoices
            if invoice_file:
                try:
                    if invoice_file.name.endswith(".csv"):
                        invoice_df = pd.read_csv(invoice_file)
                        invoice_df.columns = [col.lower() for col in invoice_df.columns]
                    else:
                        invoice_df = pd.read_excel(invoice_file)
                    invoice_df["invoice_date"] = pd.to_datetime(invoice_df["invoice_date"]).dt.strftime("%Y-%m-%d")
                    invoice_df["due_date"] = pd.to_datetime(invoice_df["due_date"]).dt.strftime("%Y-%m-%d")
                    insert_data_to_snowflake(conn, "INVOICES", invoice_df)
                except Exception as e:
                    st.error(f"Error processing Invoices file: {e}")

            # Call API
            st.write("Calling mismatch detection API...")
            result = call_mismatch_api()
            if result:
                st.success("API call successful!")
                st.subheader("Results")
                st.write("**Mismatches**")
                st.json(result.get("mismatches", []))
                st.write("**Frauds**")
                st.json(result.get("frauds", []))
                st.write("**Early Payment Opportunities**")
                st.json(result.get("early_payment_opportunities", []))
                st.write(f"**Timestamp**: {result.get('timestamp', 'N/A')}")
            conn.close()
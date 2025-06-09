import snowflake.connector
import pandas as pd
import uuid
from datetime import datetime
from config.snowflake_config import connection_parameters


def detect_mismatches(conn, po_stream, invoice_stream):
    """
    Detect mismatches between POs and Invoices using Snowflake Cortex ANOMALY_DETECTION.
    Args:
        conn: Snowflake connector connection
        po_stream: Name of the PO stream (e.g., 'PO_STREAM')
        invoice_stream: Name of the Invoice stream (e.g., 'INVOICE_STREAM')
    Returns:
        List of dictionaries with mismatch details
    """
    try:
        cursor = conn.cursor()

        # Query Streams for changed data
        po_query = f"""
            SELECT po_id, quantity, total_amount
            FROM {po_stream}
            WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
        """
        invoice_query = f"""
            SELECT po_id, quantity, total_amount
            FROM {invoice_stream}
            WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
        """

        # Fetch data into DataFrames
        cursor.execute(po_query)
        po_data = pd.DataFrame(cursor.fetchall(), columns=["po_id", "quantity", "total_amount"])
        cursor.execute(invoice_query)
        invoice_data = pd.DataFrame(cursor.fetchall(), columns=["po_id", "quantity", "total_amount"])

        # If no changes, return empty list
        if po_data.empty and invoice_data.empty:
            return []

        # Merge PO and Invoice data
        merged = po_data.merge(invoice_data, on="po_id", suffixes=("_po", "_inv"), how="inner")

        # Calculate differences
        merged["quantity_diff"] = merged["quantity_po"] - merged["quantity_inv"]
        merged["amount_diff"] = merged["total_amount_po"] - merged["total_amount_inv"]
        merged["mismatch_timestamp"] = datetime.utcnow()

        # Create temporary table for Cortex
        temp_table = f"{connection_parameters['database']}.{connection_parameters['schema']}.TEMP_MISMATCH_{uuid.uuid4().hex}"
        cursor.execute(f"""
            CREATE TEMPORARY TABLE {temp_table} (
                po_id STRING,
                quantity_diff INTEGER,
                amount_diff FLOAT,
                mismatch_timestamp TIMESTAMP
            )
        """)

        # Insert merged data
        insert_query = f"""
            INSERT INTO {temp_table} (po_id, quantity_diff, amount_diff, mismatch_timestamp)
            VALUES (%s, %s, %s, %s)
        """
        for _, row in merged.iterrows():
            cursor.execute(insert_query, (
                row["po_id"],
                int(row["quantity_diff"]),
                float(row["amount_diff"]),
                row["mismatch_timestamp"]
            ))

        # Run Cortex ANOMALY_DETECTION for mismatches
        cortex_query = f"""
            SELECT
                po_id,
                quantity_diff,
                amount_diff,
                mismatch_timestamp,
                SNOWFLAKE.CORTEX.ANOMALY_DETECTION(
                    ARRAY_CONSTRUCT(quantity_diff, amount_diff),
                    'score_threshold=0.5'
                ) AS anomaly_score
            FROM {temp_table}
            WHERE anomaly_score < 0.5
        """
        cursor.execute(cortex_query)
        mismatches = pd.DataFrame(cursor.fetchall(), columns=["po_id", "quantity_diff", "amount_diff", "mismatch_timestamp", "anomaly_score"])

        # Drop temporary table
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")

        return mismatches[["po_id", "quantity_diff", "amount_diff", "mismatch_timestamp"]].to_dict(orient="records")

    except Exception as e:
        raise Exception(f"Error in detect_mismatches: {e}")
    finally:
        cursor.close()
import snowflake.connector
import pandas as pd
import logging
from datetime import datetime
from sklearn.ensemble import IsolationForest

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def detect_mismatches(conn, po_stream, invoice_stream):
    cursor = None
    try:
        cursor = conn.cursor()

        # Query Streams for changed data
        logger.info(f"Querying streams: {po_stream}, {invoice_stream}")
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

        cursor.execute(po_query)
        po_data = pd.DataFrame(cursor.fetchall(), columns=["po_id", "quantity", "total_amount"])
        cursor.execute(invoice_query)
        invoice_data = pd.DataFrame(cursor.fetchall(), columns=["po_id", "quantity", "total_amount"])

        # If no changes, return empty list
        if po_data.empty or invoice_data.empty:
            logger.info("No changes in streams or one stream is empty")
            return []

        # Merge PO and Invoice data
        logger.info("Merging PO and Invoice data")
        merged = po_data.merge(invoice_data, on="po_id", suffixes=("_po", "_inv"), how="inner")

        # Calculate differences
        merged["quantity_diff"] = merged["quantity_po"] - merged["quantity_inv"]
        merged["amount_diff"] = merged["total_amount_po"] - merged["total_amount_inv"]
        merged["mismatch_timestamp"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        # Apply IsolationForest for anomaly detection
        logger.info("Running IsolationForest anomaly detection")
        X = merged[["quantity_diff", "amount_diff"]].values
        iso_forest = IsolationForest(contamination=0.1, random_state=42)
        anomaly_labels = iso_forest.fit_predict(X)

        # Filter anomalies (label == -1 indicates anomaly)
        mismatches = merged[anomaly_labels == -1][["po_id", "quantity_diff", "amount_diff", "mismatch_timestamp"]]

        return mismatches.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error in detect_mismatches: {e}", exc_info=True)
        raise Exception(f"Error in detect_mismatches: {e}")
    finally:
        if cursor is not None:
            cursor.close()
            logger.info("Cursor closed")
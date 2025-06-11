import snowflake.connector
import pandas as pd
import logging
from datetime import datetime
from sklearn.ensemble import IsolationForest
from config.snowflake_config import connection_parameters

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def detect_invoice_fraud(conn, invoice_stream):
    """
    Detect invoice fraud (duplicates, inflated prices) using IsolationForest.
    Args:
        conn: Snowflake connector connection
        invoice_stream: Name of the Invoice stream (e.g., 'INVOICE_STREAM')
    Returns:
        List of dictionaries with fraud details
    """
    cursor = None
    try:
        cursor = conn.cursor()

        logger.info(f"Querying {invoice_stream} for fraud detection")
        invoice_query = f"""
            SELECT
                invoice_id,
                po_id,
                vendor_id,
                total_amount,
                invoice_date
            FROM {invoice_stream}
            WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
            UNION
            SELECT
                invoice_id,
                po_id,
                vendor_id,
                total_amount,
                invoice_date
            FROM {connection_parameters['database']}.{connection_parameters['schema']}.INVOICES
        """
        cursor.execute(invoice_query)
        invoice_data = pd.DataFrame(cursor.fetchall(), columns=["invoice_id", "po_id", "vendor_id", "total_amount", "invoice_date"])

        if invoice_data.empty:
            logger.info("No invoice data found")
            return []

        # Detect duplicates (same invoice_id, different vendor_id)
        logger.info("Checking for duplicate invoices")
        duplicate_check = invoice_data.groupby('invoice_id').agg(
            vendor_count=('vendor_id', 'nunique'),
            vendor_ids=('vendor_id', lambda x: list(set(x)))
        ).reset_index()
        duplicates = duplicate_check[duplicate_check['vendor_count'] > 1]

        # Detect inflated prices using IsolationForest
        logger.info("Running IsolationForest for inflated price detection")
        X = invoice_data[["total_amount"]].values
        iso_forest = IsolationForest(contamination=0.1, random_state=42)
        anomaly_labels = iso_forest.fit_predict(X)
        invoice_data['anomaly_label'] = anomaly_labels

        # Prepare fraud results
        fraud_results = []

        # Add duplicates to fraud results
        if not duplicates.empty:
            for _, row in duplicates.iterrows():
                fraud_results.append({
                    "invoice_id": row["invoice_id"],
                    "fraud_type": "DUPLICATE",
                    "details": f"Duplicate invoice with vendors: {row['vendor_ids']}",
                    "fraud_score": 1.0,
                    "fraud_timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                })

        # Add inflated prices to fraud results
        inflated = invoice_data[invoice_data['anomaly_label'] == -1]
        if not inflated.empty:
            for _, row in inflated.iterrows():
                fraud_results.append({
                    "invoice_id": row["invoice_id"],
                    "fraud_type": "INFLATED_PRICE",
                    "details": f"Unusually high total_amount: {row['total_amount']}",
                    "fraud_score": 0.9,  # Arbitrary score for IsolationForest anomalies
                    "fraud_timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                })

        return fraud_results

    except Exception as e:
        logger.error(f"Error in detect_invoice_fraud: {e}", exc_info=True)
        raise Exception(f"Error in detect_invoice_fraud: {e}")
    finally:
        if cursor is not None:
            cursor.close()
            logger.info("Cursor closed")
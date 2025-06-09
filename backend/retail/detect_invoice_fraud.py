import snowflake.connector
import pandas as pd
from config.snowflake_config import connection_parameters

def detect_invoice_fraud(conn, invoice_stream):
    """
    Detect invoice fraud (duplicates, inflated prices) using Snowflake Cortex ANOMALY_DETECTION.
    Args:
        conn: Snowflake connector connection
        invoice_stream: Name of the Invoice stream (e.g., 'INVOICE_STREAM')
    Returns:
        List of dictionaries with fraud details
    """
    try:
        cursor = conn.cursor()

        # Query INVOICE_STREAM and INVOICES table for potential fraud
        fraud_query = f"""
            WITH invoice_data AS (
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
            ),
            duplicate_check AS (
                SELECT
                    invoice_id,
                    COUNT(DISTINCT vendor_id) AS vendor_count,
                    ARRAY_AGG(vendor_id) AS vendor_ids
                FROM invoice_data
                GROUP BY invoice_id
                HAVING COUNT(DISTINCT vendor_id) > 1
            ),
            price_check AS (
                SELECT
                    invoice_id,
                    po_id,
                    vendor_id,
                    total_amount,
                    SNOWFLAKE.CORTEX.ANOMALY_DETECTION(
                        ARRAY_CONSTRUCT(total_amount),
                        'score_threshold=0.5'
                    ) AS anomaly_score
                FROM invoice_data
                WHERE anomaly_score < 0.5
            )
            SELECT
                d.invoice_id,
                'DUPLICATE' AS fraud_type,
                'Duplicate invoice with vendors: ' || TO_VARCHAR(d.vendor_ids) AS details,
                1.0 AS fraud_score,
                CURRENT_TIMESTAMP() AS fraud_timestamp
            FROM duplicate_check d
            UNION ALL
            SELECT
                p.invoice_id,
                'INFLATED_PRICE' AS fraud_type,
                'Unusually high total_amount: ' || TO_VARCHAR(p.total_amount) AS details,
                p.anomaly_score AS fraud_score,
                CURRENT_TIMESTAMP() AS fraud_timestamp
            FROM price_check p
        """
        cursor.execute(fraud_query)
        fraud_results = pd.DataFrame(cursor.fetchall(), columns=["invoice_id", "fraud_type", "details", "fraud_score", "fraud_timestamp"])

        return fraud_results.to_dict(orient="records")

    except Exception as e:
        raise Exception(f"Error in detect_invoice_fraud: {e}")
    finally:
        cursor.close()
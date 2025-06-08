import pandas as pd
from datetime import datetime
from get_snowflake_session import connection_parameters
import uuid

def compare_po_invoice(conn, po_stream, invoice_stream):
    try:
        cursor=conn.cursor()

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
        po_data=pd.DataFrame(cursor.fetchall(), columns=["po_id", "quantity", "total_amount"])

        cursor.execute(invoice_query)
        invoice_data=pd.DataFrame(cursor.fetchall(), columns=["po_id", "quantity", "total_amount"])

        if po_data.empty and invoice_data.empty:
            return []

        merged = po_data.merge(invoice_data, on="po_id", suffixes=("_po", "_inv"), how="inner")

        # Calculate differences
        merged["quantity_diff"] = merged["quantity_po"] - merged["quantity_inv"]
        merged["amount_diff"] = merged["total_amount_po"] - merged["total_amount_inv"]
        merged["mismatch_timestamp"] = datetime.utcnow()

        temp_table = f"{connection_parameters['database']}.{connection_parameters['schema']}.TEMP_MISMATCH_{uuid.uuid4().hex}"
        create_table_query = f"""
            CREATE TEMPORARY TABLE {temp_table} (
                po_id STRING,
                quantity_diff INTEGER,
                amount_diff FLOAT,
                mismatch_timestamp TIMESTAMP
            )
        """
        cursor.execute(create_table_query)

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

        mismatches = pd.DataFrame(cursor.fetchall(),columns=["po_id", "quantity_diff", "amount_diff", "mismatch_timestamp","anomaly_score"])

        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cursor.close()

        return mismatches[["po_id", "quantity_diff", "amount_diff", "mismatch_timestamp"]].to_dict(orient="records")

    except Exception as e:
        raise Exception(f"Error in compare po_invoice: {e}")

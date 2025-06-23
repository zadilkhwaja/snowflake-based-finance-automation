

from snowflake.snowpark import Session
from snowflake.ml.registry import Registry
from config.snowflake_config import connection_parameters  # assumes you store config here
from snowflake.snowpark.functions import col, when, count
from datetime import datetime, timedelta
import pandas as pd

def detect_invoice_fraud(conn, invoice_stream):
    cursor=None
    try:
        cursor = conn.cursor()

        # Query to fetch invoices with duplicate invoice_id
        invoice_query = f"""
            SELECT 
                invoice_id,
                po_id,
                amount,
                invoice_date
            FROM {invoice_stream}
            WHERE invoice_id IN (
                SELECT invoice_id
                FROM {invoice_stream}
                GROUP BY invoice_id
                HAVING COUNT(*) > 1
            )
        """

        # Execute the query
        cursor.execute(invoice_query)

        # Fetch results into a DataFrame
        invoice_data = pd.DataFrame(cursor.fetchall(), columns=["invoice_id", "po_id", "amount", "invoice_date"])

        # Initialize fraud results list if not already done
        fraud_results = []

        # Process duplicates
        seen_invoice_ids = set()

        if not invoice_data.empty:
            for _, row in invoice_data.iterrows():
                invoice_id = row["invoice_id"]
                if invoice_id not in seen_invoice_ids:
                    fraud_results.append({
                        "invoice_id": invoice_id,
                        "fraud_type": "DUPLICATE",
                        "details": "Duplicate invoice",
                        "fraud_score": 1.0,
                        "fraud_timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    seen_invoice_ids.add(invoice_id)

        anomaly_query = f"""
                    SELECT 
                        invoice_id,
                        amount,
                        quantity,
                        unit_price
                    FROM {invoice_stream}
                """

        # Convert to Snowpark DataFrame for model prediction
        session = Session.builder.configs(connection_parameters).create()
        df_stream = session.sql(anomaly_query)

        # Initialize model registry and load model
        registry = Registry(session=session)
        loaded_model = registry.get_model("invoice_anomaly_model").version("v1")

        # Perform predictions
        result_df = loaded_model.run(df_stream.select("INVOICE_ID","AMOUNT", "QUANTITY", "UNIT_PRICE"), function_name="predict")

        for row in result_df.collect():
            anomaly_score = row['ANOMALY_SCORE']  # Use correct column name
            if anomaly_score < 0.3:  # Threshold for flagging anomalies
                fraud_results.append({
                    "invoice_id": row["INVOICE_ID"],
                    "fraud_type": "ANOMALY",
                    "details": f"Anomalous transaction detected (score: {anomaly_score})",
                    "fraud_score": anomaly_score,
                    "fraud_timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                })

        return fraud_results
    except Exception as e:
        print(f"❌ Prediction error: {e}")
        return





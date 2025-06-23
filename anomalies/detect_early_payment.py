import snowflake.connector
import pandas as pd
import logging
from datetime import datetime, timedelta
from config.snowflake_config import connection_parameters

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def detect_early_payment(conn, invoice_stream):

    cursor = None
    try:
        cursor = conn.cursor()
        current_date = '2025-06-11'

        logger.info(f"Querying {invoice_stream} for early payment opportunities")
        payment_query = f"""
            SELECT
                invoice_id,
                amount,
                invoice_date,
                due_date,
                COALESCE(discount_terms, '2/10 Net 30') AS discount_terms
            FROM {invoice_stream}
            WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
            AND invoice_date IS NOT NULL
            AND due_date IS NOT NULL
            AND due_date >= '{current_date}'
        """
        cursor.execute(payment_query)
        invoices = pd.DataFrame(cursor.fetchall(), columns=["invoice_id", "amount", "invoice_date", "due_date", "discount_terms"])

        if invoices.empty:
            logger.info("No eligible invoices found")
            return []

        logger.info("Processing early payment opportunities")
        opportunities = []
        for _, row in invoices.iterrows():
            invoice_date = row["invoice_date"]
            due_date = row["due_date"]
            total_amount = float(row["amount"])
            discount_terms = row["discount_terms"]

            try:
                discount_percent, discount_days = map(float, discount_terms.split('/')[0:2])
                discount_days = int(discount_days)
            except:
                discount_percent, discount_days = 2.0, 10

            payment_deadline = invoice_date + timedelta(days=discount_days)
            if payment_deadline >= datetime.strptime(current_date, '%Y-%m-%d').date():
                potential_savings = total_amount * (discount_percent / 100)
                opportunities.append({
                    "invoice_id": row["invoice_id"],
                    "potential_savings": potential_savings,
                    "payment_deadline": payment_deadline.strftime('%Y-%m-%d'),
                    "discount_terms": discount_terms,
                    "opportunity_timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                })

        return opportunities

    except Exception as e:
        logger.error(f"Error in detect_early_payment: {e}", exc_info=True)
        raise Exception(f"Error in detect_early_payment: {e}")
    finally:
        if cursor is not None:
            cursor.close()
            logger.info("Cursor closed")
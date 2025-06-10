import pandas as pd
from datetime import datetime, timedelta

def detect_early_payment(conn, invoice_stream):
    try:
        cursor = conn.cursor()

        # Current date (hardcoded to match system date: 2025-06-10)
        current_date = '2025-06-10'

        # Query INVOICE_STREAM for invoices with discount potential
        payment_query = f"""
            SELECT
                invoice_id,
                total_amount,
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
        invoices = pd.DataFrame(cursor.fetchall(), columns=["invoice_id", "total_amount", "invoice_date", "due_date", "discount_terms"])

        # If no eligible invoices, return empty list
        if invoices.empty:
            return []

        # Process discount opportunities
        opportunities = []
        for _, row in invoices.iterrows():
            invoice_date = row["invoice_date"]
            due_date = row["due_date"]
            total_amount = float(row["total_amount"])
            discount_terms = row["discount_terms"]

            # Parse discount terms (e.g., '2/10 Net 30' -> 2% discount if paid within 10 days)
            try:
                discount_percent, discount_days = map(float, discount_terms.split('/')[0:2])
                discount_days = int(discount_days)
            except:
                discount_percent, discount_days = 2.0, 10  # Default: 2% within 10 days

            # Calculate payment deadline and savings
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
        raise Exception(f"Error in detect_early_payment: {e}")
    finally:
        cursor.close()
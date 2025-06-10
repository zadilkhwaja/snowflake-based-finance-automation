from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
from get_snowflake_session import get_snowflake_session

from detect_invoice_fraud import detect_invoice_fraud
from detect_mismatches import detect_mismatches
from detect_early_payment import detect_early_payment

app=FastAPI()

class streamInput(BaseModel):
    po_stream: str
    invoice_stream: str

@app.post("/check-mismatch")
async def check_mismatch(input: streamInput):
    try:
        conn=get_snowflake_session()

        mismatches=detect_mismatches(conn, input.po_stream, input.invoice_stream)
        frauds=detect_invoice_fraud(conn,input.invoice_stream)
        opportunities=detect_early_payment(conn, input.invoice_stream)

        if mismatches:
            cursor = conn.cursor()

            insert_mismatch_query = """
                INSERT INTO MISMATCH_REPORTS (po_id, quantity_diff, amount_diff, mismatch_timestamp)
                VALUES (%s, %s, %s, %s)
                        """
            for mismatch in mismatches:
                cursor.execute(insert_mismatch_query, (
                    mismatch["po_id"],
                    int(mismatch["quantity_diff"]),
                    float(mismatch["amount_diff"]),
                    mismatch["mismatch_timestamp"]
                ))
            conn.commit()
            cursor.close()

        if frauds:
            cursor = conn.cursor()
            insert_fraud_query = """
                INSERT INTO FRAUD_REPORTS (invoice_id, fraud_type, details, fraud_score, fraud_timestamp)
                VALUES (%s, %s, %s, %s, %s)
                """
            for fraud in frauds:
                cursor.execute(insert_fraud_query, (
                    fraud["invoice_id"],
                    fraud["fraud_type"],
                    fraud["details"],
                    float(fraud["fraud_score"]),
                    fraud["fraud_timestamp"]
                ))
            conn.commit()
            cursor.close()

        if opportunities:
            cursor = conn.cursor()
            insert_opportunity_query = """
                            INSERT INTO EARLY_PAYMENT_OPPORTUNITIES (invoice_id, potential_savings, payment_deadline, discount_terms, opportunity_timestamp)
                            VALUES (%s, %s, %s, %s, %s)
                        """
            for opportunity in opportunities:
                cursor.execute(insert_opportunity_query, (
                    opportunity["invoice_id"],
                    float(opportunity["potential_savings"]),
                    opportunity["payment_deadline"],
                    opportunity["discount_terms"],
                    opportunity["opportunity_timestamp"]
                ))
            conn.commit()
            cursor.close()


        conn.close()
        return {
            "status": "success",
            "mismatches": mismatches,
            "frauds": frauds,
            "early_payment_opportunities": opportunities,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        return HTTPException(status_code=500, detail=f"Error processing the request : {str(e)}")


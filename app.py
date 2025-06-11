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
    print("called api..")
    try:
        conn = get_snowflake_session()

        mismatches = detect_mismatches(conn, input.po_stream, input.invoice_stream)
        print("mismatches===>", mismatches)
        frauds = detect_invoice_fraud(conn, input.invoice_stream)
        print("frauds===>", frauds)
        opportunities = detect_early_payment(conn, input.invoice_stream)
        print("early_discount==>", opportunities)

        conn.commit()

        # if frauds:
        #     with conn.cursor() as cursor:
        #         insert_fraud_query = """
        #             INSERT INTO FRAUD_REPORTS (invoice_id, fraud_type, details, fraud_score, fraud_timestamp)
        #             VALUES (?, ?, ?, ?, ?)
        #         """
        #         for fraud in frauds:
        #             cursor.execute(insert_fraud_query, (
        #                 fraud["invoice_id"],
        #                 fraud["fraud_type"],
        #                 fraud["details"],
        #                 float(fraud["fraud_score"]),
        #                 fraud["fraud_timestamp"]
        #             ))
        #     conn.commit()
        #
        # if opportunities:
        #     with conn.cursor() as cursor:
        #         insert_opportunity_query = """
        #             INSERT INTO EARLY_PAYMENT_OPPORTUNITIES (invoice_id, potential_savings, payment_deadline, discount_terms, opportunity_timestamp)
        #             VALUES (?, ?, ?, ?, ?)
        #         """
        #         for opportunity in opportunities:
        #             cursor.execute(insert_opportunity_query, (
        #                 opportunity["invoice_id"],
        #                 float(opportunity["potential_savings"]),
        #                 opportunity["payment_deadline"],
        #                 opportunity["discount_terms"],
        #                 opportunity["opportunity_timestamp"]
        #             ))
        #     conn.commit()

        conn.close()
        return {
            "status": "success",
            "mismatches": mismatches,
            "frauds": frauds,
            "early_payment_opportunities": opportunities,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        print("exception-souvik==>", e)
        raise HTTPException(status_code=500, detail=f"Error processing the request: {str(e)}")


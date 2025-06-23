from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
from utils.get_snowflake_session import get_snowflake_session
from anomalies.detect_invoice_fraud import detect_invoice_fraud
from anomalies.detect_mismatches import detect_mismatches
from anomalies.detect_early_payment import detect_early_payment

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


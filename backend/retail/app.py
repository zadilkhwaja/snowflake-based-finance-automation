import snowflake.connector
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from snowflake.snowpark import Session
from config.snowflake_config import connection_parameters
from datetime import datetime
from get_snowflake_session import get_snowflake_session
from compare_po_invoice import compare_po_invoice

app=FastAPI()

class streamInput(BaseModel):
    po_stream: str
    invoice_stream: str

@app.post("/check-mismatch")
async def check_mismatch(input: streamInput):
    try:
        conn=get_snowflake_session()
        mismatches=compare_po_invoice(conn, input.po_stream, input.invoice_stream)

        if mismatches:
            cursor = conn.cursor()

            insert_query = """
                INSERT INTO MISMATCH_REPORTS (po_id, quantity_diff, amount_diff, mismatch_timestamp)
                VALUES (%s, %s, %s, %s)
            """

            for mismatch in mismatches:
                cursor.execute(insert_query, (
                    mismatch["po_id"],
                    int(mismatch["quantity_diff"]),
                    float(mismatch["amount_diff"]),
                    mismatch["mismatch_timestamp"]
                ))

            conn.commit()
            cursor.close()

        conn.close()
        return {
            "status": "success",
            "mismatches": mismatches,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        return HTTPException(status_code=500, detail=f"Error processing the request : {str(e)}")


def detect_mismatches(conn, po_stream_name: str, invoice_stream_name: str):
    import pandas as pd
    import numpy as np
    import logging

    logger = logging.getLogger(__name__)
    cursor = None

    try:
        cursor = conn.cursor()

        logger.info(f"Detecting mismatches from streams: {po_stream_name}, {invoice_stream_name}")

        mismatch_query = f"""
            SELECT
                COALESCE(i.po_id, p.po_id) AS po_id,
                i.quantity - p.quantity AS quantity_diff,
                i.amount - p.amount AS amount_diff,
                i.unit_price - p.unit_price AS unit_diff,
                CASE
                    WHEN i.po_id IS NULL THEN 'Invoice missing for PO'
                    WHEN p.po_id IS NULL THEN 'PO missing for Invoice'
                    WHEN i.quantity IS NULL OR i.amount IS NULL OR i.unit_price IS NULL 
                         OR i.invoice_date IS NULL OR i.due_date IS NULL OR i.discount_terms IS NULL
                         THEN 'Missing fields in invoice'
                    WHEN i.quantity != p.quantity OR i.amount != p.amount OR i.unit_price != p.unit_price
                         THEN 'Value mismatch in quantity, amount, or unit price'
                    ELSE NULL
                END AS reason,
                CURRENT_TIMESTAMP() AS mismatch_timestamp
            FROM {invoice_stream_name} i
            FULL OUTER JOIN {po_stream_name} p
            ON i.po_id = p.po_id AND i.item_id = p.item_id
            WHERE
                (i.METADATA$ACTION IN ('INSERT', 'UPDATE') OR p.METADATA$ACTION IN ('INSERT', 'UPDATE'))
                AND (
                    i.po_id IS NULL OR
                    p.po_id IS NULL OR
                    i.quantity IS NULL OR i.amount IS NULL OR i.unit_price IS NULL OR
                    i.invoice_date IS NULL OR i.due_date IS NULL OR i.discount_terms IS NULL OR
                    i.quantity != p.quantity OR i.amount != p.amount OR i.unit_price != p.unit_price
                );
        """

        cursor.execute(mismatch_query)
        results = cursor.fetchall()
        columns = [col[0].lower() for col in cursor.description]

        df = pd.DataFrame(results, columns=columns)
        if 'mismatch_timestamp' in df.columns:
            df['mismatch_timestamp'] = df['mismatch_timestamp'].astype(str)

        # Ensure numeric nulls are converted for JSON
        for col in ['quantity_diff', 'amount_diff', 'unit_diff']:
            if col in df.columns:
                df[col] = df[col].replace({np.nan: None})

        logger.info(f"{len(df)} mismatches detected")
        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error during mismatch detection: {e}", exc_info=True)
        raise

    finally:
        if cursor:
            cursor.close()

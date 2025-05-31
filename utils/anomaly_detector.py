def detect_payment_anomalies(session):
    query = """
    INSERT INTO anomalies
    SELECT
        UUID_STRING(),
        i.invoice_id,
        i.po_id,
        p.payment_id,
        'Payment Mismatch',
        CASE 
            WHEN p.payment_id IS NULL THEN 'Missing payment'
            WHEN i.amount != p.amount THEN 'Amount mismatch'
        END,
        CURRENT_TIMESTAMP()
    FROM invoices i
    LEFT JOIN payments p ON i.invoice_id = p.invoice_id
    WHERE p.payment_id IS NULL OR i.amount != p.amount;
    """
    session.sql(query).collect()
  

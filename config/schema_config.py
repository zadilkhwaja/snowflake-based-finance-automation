TARGET_SCHEMAS = {
    "invoice": ["invoice_id", "po_id", "item_id", "quantity", "unit_price", "amount", "invoice_date", "due_date", "discount_terms"],
    "purchase_order": ["po_id", "item_id", "quantity", "unit_price", "amount", "order_date"]
}

HEADER_SYNONYMS = {
    "invoice_id": ["invoice_no", "inv_id", "invoice_number"],
    "po_id": ["purchase_order_id", "po_number", "poid", "purchase_id"],
    "item_id": ["item_code", "product_id", "item"],
    "quantity": ["qty", "quant", "amount_purchased"],
    "unit_price": ["price_per_unit", "unitcost", "unit_rate"],
    "amount": ["total", "total_amount", "line_total", "po_amount"],
    "invoice_date": ["inv_date", "invoice_dt", "date_of_invoice"],
    "due_date": ["payment_due", "due"],
    "discount_terms": ["discount", "disc_terms", "terms"],
    "order_date": ["po_date", "purchase_date", "order_dt"]
}

import random
from datetime import datetime, timedelta
from snowflake.snowpark import Session
from snowflake.ml.modeling.ensemble.isolation_forest import IsolationForest
from snowflake.ml.registry import Registry

def train_invoice_model(conn, connection_parameters):
    cursor = conn.cursor()

    create_table_query = """
        CREATE TABLE IF NOT EXISTS INVOICE_FEATURES (
                INVOICE_ID STRING,
                po_id STRING,
                item_id STRING,
                quantity INTEGER,
                unit_price FLOAT,
                amount FLOAT,
                invoice_date DATE,
                due_date STRING,
                discount_terms STRING
            )
        """
    try:
        cursor.execute(create_table_query)
        print("✅ INVOICE_FEATURES table created/replaced.")
    except Exception as e:
        print(f"❌ Table creation error: {e}")
        return

    # Step 1: Insert 100 synthetic invoice rows (excluding vendor_id)
    insert_query = """
        INSERT INTO INVOICE_FEATURES (INVOICE_ID, po_id, item_id, quantity, unit_price, amount, invoice_date, due_date, discount_terms)
        VALUES
        {}
        """
    today = datetime.today()
    rows = []

    for i in range(1000):
        invoice_id = f"INV{i + 1:03}"
        po_id = f"PO{i + 1:03}"
        item_id = f"ITEM{i + 1:03}"
        quantity = random.randint(1, 500)
        unit_price = round(random.uniform(1, 77), 2)
        amount = round(quantity * unit_price, 2)
        invoice_date = today - timedelta(days=random.randint(0, 10))
        due_date = invoice_date + timedelta(days=30)
        discount_terms = f"{random.randint(0, 15)}%"

        row = f"('{invoice_id}', '{po_id}', '{item_id}', {quantity}, {unit_price}, {amount}, " \
              f"'{invoice_date.strftime('%Y-%m-%d')}', '{due_date.strftime('%Y-%m-%d')}', '{discount_terms}')"
        rows.append(row)

    try:
        cursor.execute(insert_query.format(",\n".join(rows)))
        print("✅ 1000 invoice_feature rows inserted.")
    except Exception as e:
        print(f"❌ Insert error: {e}")
        return


    # Step 3: Train IsolationForest using Snowpark + snowflake.ml
    try:
        session = Session.builder.configs(connection_parameters).create()
        df = session.table("INVOICE_FEATURES").select("AMOUNT", "QUANTITY", "UNIT_PRICE")

        model = IsolationForest(input_cols=["AMOUNT", "QUANTITY", "UNIT_PRICE"], output_cols="ANOMALY_SCORE")

        trained_model = model.fit(df)

        registry = Registry(session=session)
        model_ref = registry.log_model(
            model=trained_model,
            model_name="invoice_anomaly_model",
            version_name="v1",
            comment="Isolation Forest model for invoice fraud detection"
        )

        print("✅ Model trained and saved successfully.")
    except Exception as e:
        print(f"❌ ML model training error: {e}")

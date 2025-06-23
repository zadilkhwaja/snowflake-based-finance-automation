import pandas as pd
from config.snowflake_config import connection_parameters

def create_invoice_features_table(conn):
    query = """
    CREATE OR REPLACE TABLE INVOICE_FEATURES AS
    SELECT 
        invoice_id,
        invoice_date::DATE AS TS,
        amount AS invoice_amount,
        quantity,
        unit_price,
        DATEDIFF(DAY, invoice_date::DATE, due_date::DATE) AS due_in_days,
        TRY_TO_NUMBER(REGEXP_SUBSTR(discount_terms, '\\d+')) AS discount_days
    FROM INVOICE_STREAM
    WHERE METADATA$ACTION = 'INSERT'
      AND amount IS NOT NULL 
      AND invoice_date IS NOT NULL;
    """
    conn.cursor().execute(query)


def create_recent_invoice_view(conn):
    query = """
    CREATE OR REPLACE TEMPORARY VIEW recent_invoice_view AS
    SELECT 
        TS,
        invoice_amount,
        quantity,
        unit_price,
        due_in_days,
        discount_days
    FROM INVOICE_FEATURES
    WHERE TS >= DATEADD(DAY, -14, CURRENT_DATE)
    """
    conn.cursor().execute(query)

# def train_invoice_anomaly_model(conn):
#     print("*****************@@@@@@@@@@@")
#     print("went inside snowflex")
#     print("*****************@@@@@@@@@@@")
#
#
#
#     train_query = """
#     CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION invoice_anomaly_detector (
#         INPUT_DATA => SYSTEM$REFERENCE('TABLE', INVOICE_FEATURES),
#         TIMESTAMP_COLNAME => 'TS',
#         TARGET_COLNAME => 'invoice_amount'
#     );
#     """
#     conn.cursor().execute(train_query)

def train_invoice_anomaly_model(conn):
    print("########!!!!!@@#####")
    print("🚀 Training Anomaly Detection Model...")
    print("########!!!!!@@#####")

    # Extract DB and Schema from .env
    db = connection_parameters.get("database")
    schema = connection_parameters.get("schema")

    cortex_model_name = f"{db}.{schema}.invoice_anomaly_detector"
    input_table_name = f"{db}.{schema}.INVOICE_FEATURES"

    cortex_query = f"""
    CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION model_name (
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'db.schema.table'),
    TIMESTAMP_COLNAME => 'ts',
    TARGET_COLNAME => 'amount'
    );
    """

    try:
        conn.cursor().execute(cortex_query)
        print("✅ Cortex Anomaly Detection model created successfully.")
    except Exception as e:
        print(f"❌ Error training anomaly model: {e}")
        raise



def detect_invoice_anomalies(conn):
    print("%%%%%%%%%!!!!!!")
    print("Freeza")
    print("%%%%%%%%%!!!!!!")
    detect_query = """
    CREATE OR REPLACE TABLE INVOICE_ANOMALIES AS
    SELECT *
    FROM TABLE(
        invoice_anomaly_detector!DETECT_ANOMALIES(
            INPUT_DATA => SYSTEM$REFERENCE('TABLE', recent_invoice_view),
            TIMESTAMP_COLNAME => 'TS',
            TARGET_COLNAME => 'invoice_amount',
            CONFIG_OBJECT => OBJECT_CONSTRUCT('prediction_interval', 0.99)
        )
    );
    """
    conn.cursor().execute(detect_query)



def fetch_anomaly_results(conn):
    query = """
    SELECT 
        a.*, 
        f.invoice_id,
        f.quantity,
        f.unit_price,
        f.due_in_days,
        f.discount_days
    FROM INVOICE_ANOMALIES a
    JOIN INVOICE_FEATURES f
    ON a.TS = f.TS AND a.invoice_amount = f.invoice_amount
    ORDER BY a.TS DESC
    """
    return pd.read_sql(query, conn)

def run_invoice_anomaly_pipeline(conn):
    create_invoice_features_table(conn)      # ✅ from INVOICE_STREAM
    train_invoice_anomaly_model(conn)
    create_recent_invoice_view(conn)
    detect_invoice_anomalies(conn)
    df = fetch_anomaly_results(conn)

    return df

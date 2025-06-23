from snowflake.connector.pandas_tools import write_pandas

def create_database_and_tables(conn, db_name="FINANCE_DB", schema="PUBLIC"):
    TARGET_TABLES = {
        "invoice": """
            CREATE TABLE IF NOT EXISTS invoice (
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
        """,
        "purchase_order": """
            CREATE TABLE IF NOT EXISTS purchase_order (
                po_id STRING,
                item_id STRING,
                quantity INTEGER,
                unit_price FLOAT,
                amount FLOAT,
                order_date DATE
            )
        """
    }

    STREAMS = {
        "PO_STREAM": "CREATE OR REPLACE STREAM PO_STREAM ON TABLE PURCHASE_ORDER",
        "INVOICE_STREAM": "CREATE OR REPLACE STREAM INVOICE_STREAM ON TABLE INVOICE"
    }

    cs = conn.cursor()
    try:
        cs.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        cs.execute(f"USE DATABASE {db_name}")
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cs.execute(f"USE SCHEMA {schema}")

        # Create tables
        for ddl in TARGET_TABLES.values():
            cs.execute(ddl)

        # Create streams
        for stream in STREAMS.values():
            cs.execute(stream)

    finally:
        cs.close()



def upload_dataframe_to_snowflake(df, table_name, conn):
    """
    Uploads a Pandas DataFrame to a Snowflake table.
    Ensures column names are uppercased to match Snowflake identifiers.
    """
    df.columns = [col.upper() for col in df.columns]
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())
    return success

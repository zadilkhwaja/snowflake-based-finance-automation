from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, StringType

def save_column_mappings(session: Session, vendor_id: str, table_name: str, mapping_dict: dict):
    # 1. Create table if it doesn't exist (Snowpark handles this fine with raw SQL)
    create_stmt = """
        CREATE TABLE IF NOT EXISTS column_mappings (
            vendor_id VARCHAR,
            table_name VARCHAR,
            expected_column VARCHAR,
            actual_column VARCHAR
        )
    """
    session.sql(create_stmt).collect()

    # 2. Prepare data as a list of tuples
    rows_to_insert = [
        (vendor_id, table_name, expected, actual)
        for expected, actual in mapping_dict.items()
    ]

    # 3. Define schema for the DataFrame
    schema = StructType([
        StructField("vendor_id", StringType()),
        StructField("table_name", StringType()),
        StructField("expected_column", StringType()),
        StructField("actual_column", StringType()),
    ])

    # 4. Create and write the DataFrame
    df = session.create_dataframe(rows_to_insert, schema=schema)

    df.write.mode("append").save_as_table("column_mappings")

from snowflake.snowpark.functions import col

def normalize_columns(cols):
    return [col.strip().lower() for col in cols]


def compare_schema(actual_cols, expected_cols):
    actual = set(normalize_columns(actual_cols))
    expected = set(normalize_columns(expected_cols))

    return {
        "matching": list(actual & expected),
        "missing": list(expected - actual),
        "extra": list(actual - expected),
    }


def apply_column_mapping(df, column_mapping):
    return df.rename(columns=column_mapping)


def fetch_column_mapping(session, vendor_id, table_name):
    df = (
        session.table("column_mappings")
        .filter(
            (col("vendor_id") == vendor_id) & (col("table_name") == table_name)
        )
        .select("expected_column", "actual_column")
    )

    rows = df.collect()
    print(rows)
    return {row["ACTUAL_COLUMN"]: row["EXPECTED_COLUMN"] for row in rows}

def normalize_columns(cols):
    return [col.strip().lower() for col in cols]


def compare_schema(actual_cols, expected_cols):
    actual = set(normalize_columns(actual_cols))
    expected = set(normalize_columns(expected_cols))

    return {
        "matching": list(actual & expected),
        "missing": list(expected - actual),
        "extra": list(actual - expected)
    }


def apply_column_mapping(df, column_mapping):
    return df.rename(columns=column_mapping)


def fetch_column_mapping(session, vendor_id, table_name):
    sql = f"""
        SELECT expected_column, actual_column
        FROM column_mappings
        WHERE vendor_id = %s AND table_name = %s
    """
    cur = session.cursor()
    cur.execute(sql, (vendor_id, table_name))
    return {row[1]: row[0] for row in cur.fetchall()}  # Reverse mapping for .rename()
    

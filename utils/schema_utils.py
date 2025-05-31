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

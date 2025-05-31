def save_column_mappings(session, vendor_id, table_name, mapping_dict):
    rows_to_insert = [
        (vendor_id, table_name, expected, actual)
        for expected, actual in mapping_dict.items()
    ]

    insert_stmt = """
        INSERT INTO column_mappings (vendor_id, table_name, expected_column, actual_column)
        VALUES (%s, %s, %s, %s)
    """

    for row in rows_to_insert:
        session.cursor().execute(insert_stmt, row)
      

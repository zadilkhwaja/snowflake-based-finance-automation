def generate_sql_with_cortex(session, natural_query: str, table_name: str) -> str:
    # Use Cortex QUERY_ASSIST
    query = f"""
        SELECT SNOWFLAKE.CORTEX.QUERY_ASSIST(
            OBJECT_CONSTRUCT(
                'question', '{natural_query}',
                'table_name', '{table_name}'
            )
        ) AS sql_query;
    """
    result = session.sql(query).collect()
    return result[0]["SQL_QUERY"]
  

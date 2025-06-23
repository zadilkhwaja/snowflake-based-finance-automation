import snowflake.connector



def get_snowflake_connection(config):
    return snowflake.connector.connect(**config)


def classify_doc_type_with_cortex(conn, headers_or_text):
    input_text = ', '.join(headers_or_text) if isinstance(headers_or_text, list) else headers_or_text
    prompt = f"answer in one word only: What type of document is this: invoice or purchase order? Columns/Text: {input_text}"

    query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        '{prompt}'
    ) AS result;
    """
    cursor = conn.cursor()
    cursor.execute(query)

    text=cursor.fetchone()[0].strip().lower()
    if text=="purchase order":
        text="purchase_order"
    print("----------")
    print(text)
    print("--------")
    return text


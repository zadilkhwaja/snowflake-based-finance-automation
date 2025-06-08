import snowflake
from config.snowflake_config import connection_parameters

def get_snowflake_session():
    try:
        conn=snowflake.connector.connect(**connection_parameters)
        return conn
    except Exception as e:
        raise Exception(f"Error creating Snowflake connection: {e}")
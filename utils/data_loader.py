from snowflake.snowpark import Session
import pandas as pd

def load_dataframe_to_table(session: Session, df: pd.DataFrame, table_name: str):
    session.write_pandas(df, table_name=table_name, auto_create_table=False)
  

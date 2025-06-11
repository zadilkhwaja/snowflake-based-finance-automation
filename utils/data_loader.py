from snowflake.snowpark import Session
import pandas as pd

def load_dataframe_to_table(session: Session, df: pd.DataFrame, table_name: str):
    snowpark_df = session.createDataFrame(df)
    snowpark_df.write.mode("overwrite").save_as_table(table_name)
  

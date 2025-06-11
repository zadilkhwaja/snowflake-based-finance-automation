import pandas as pd
from snowflake.snowpark import Session

def load_anomalies(session: Session) -> pd.DataFrame:
    df = session.table("anomalies").to_pandas()
    print(df)
    df["DETECTED_AT"] = pd.to_datetime(df["DETECTED_AT"])
    return df.sort_values("DETECTED_AT", ascending=False)
  

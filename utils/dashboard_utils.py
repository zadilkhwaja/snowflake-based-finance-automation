import pandas as pd
from snowflake.snowpark import Session

def load_anomalies(session: Session) -> pd.DataFrame:
    df = session.table("anomalies").to_pandas()
    df["detected_at"] = pd.to_datetime(df["detected_at"])
    return df.sort_values("detected_at", ascending=False)
  

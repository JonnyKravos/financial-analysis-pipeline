import os
from dotenv import load_dotenv
import numpy as np
import pandas as pd
from python.utils.snowflake_connection import get_snowflake_connection
from python.utils.write_to_snowflake import write_dataframe_to_snowflake


def main():

    load_dotenv()

    database = os.getenv("SNOWFLAKE_DATABASE")
    schema=os.getenv("SNOWFLAKE_SCHEMA")
    table_name = "TRANSACTION_ANOMALIES"
    conn = get_snowflake_connection()

    try:
        query = """
        SELECT
            TRANSACTION_ID,
            CUSTOMER_ID,
            TRANSACTION_DATE,
            AMOUNT_DOLLARS,
            CARD_BRAND,
            CARD_TYPE
        FROM FINANCE_DB.ANALYTICS.FCT_TRANSACTIONS_ENRICHED
        """
        df = pd.read_sql(query, conn)

        amount_mean = df["AMOUNT_DOLLARS"].mean()
        amount_std = df["AMOUNT_DOLLARS"].std()

        if amount_std == 0 or pd.isna(amount_std):
            df["z_score"] = 0
        else:
            df["z_score"] = (df["AMOUNT_DOLLARS"] - amount_mean) / amount_std

        df["anomaly_flag"] = np.where(df["z_score"].abs() > 3, 1, 0)

        anomalies = df[df["anomaly_flag"] == 1].copy()

        write_dataframe_to_snowflake(conn, anomalies, table_name, database, schema)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
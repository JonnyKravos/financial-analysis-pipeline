import os
from dotenv import load_dotenv
import pandas as pd
from python.utils.snowflake_connection import get_snowflake_connection
from python.utils.write_to_snowflake import write_dataframe_to_snowflake


def main():

    load_dotenv()

    database = os.getenv("SNOWFLAKE_DATABASE")
    schema=os.getenv("SNOWFLAKE_SCHEMA")
    table_name = "MONTHLY_SPEND_TRENDS"
    conn = get_snowflake_connection()

    try:
        query = """
        SELECT
            TRANSACTION_ID,
            TRANSACTION_DATE,
            AMOUNT_DOLLARS
        FROM FINANCE_DB.ANALYTICS.FCT_TRANSACTIONS_ENRICHED
        """
        df = pd.read_sql(query, conn)

        df["TRANSACTION_DATE"] = pd.to_datetime(df["TRANSACTION_DATE"])
        df["TRANSACTION_MONTH"] = df["TRANSACTION_DATE"].dt.to_period("M").astype(str)

        monthly_spend = (
            df.groupby("TRANSACTION_MONTH", as_index=False)
              .agg(
                  total_spend=("AMOUNT_DOLLARS", "sum"),
                  transaction_count=("TRANSACTION_ID", "count"),
                  avg_transaction_value=("AMOUNT_DOLLARS", "mean"),
              )
              .sort_values("TRANSACTION_MONTH")
        )

        write_dataframe_to_snowflake(conn, monthly_spend, table_name, database, schema)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
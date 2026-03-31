import os
from dotenv import load_dotenv
import pandas as pd
from python.utils.snowflake_connection import get_snowflake_connection
from python.utils.write_to_snowflake import write_dataframe_to_snowflake


def main():

    load_dotenv()

    database = os.getenv("SNOWFLAKE_DATABASE")
    schema=os.getenv("SNOWFLAKE_SCHEMA")
    table_name = "CUSTOMER_VALUE_SUMMARY"
    conn = get_snowflake_connection()

    try:
        query = """
        SELECT
            TRANSACTION_ID,
            CUSTOMER_ID,
            TRANSACTION_DATE,
            AMOUNT_DOLLARS
        FROM FINANCE_DB.ANALYTICS.FCT_TRANSACTIONS_ENRICHED
        """
        df = pd.read_sql(query, conn)

        df["TRANSACTION_DATE"] = pd.to_datetime(df["TRANSACTION_DATE"])

        customer_value_summary = (
            df.groupby("CUSTOMER_ID", as_index=False)
              .agg(
                  total_spent=("AMOUNT_DOLLARS", "sum"),
                  transaction_count=("TRANSACTION_ID", "count"),
                  avg_transaction_value=("AMOUNT_DOLLARS", "mean"),
                  first_transaction_date=("TRANSACTION_DATE", "min"),
                  last_transaction_date=("TRANSACTION_DATE", "max"),
              )
        )

        customer_value_summary["customer_active_days"] = (
            customer_value_summary["last_transaction_date"] -
            customer_value_summary["first_transaction_date"]
        ).dt.days

        customer_value_summary["value_segment"] = pd.qcut(
            customer_value_summary["total_spent"],
            q=4,
            labels=["Low", "Medium", "High", "Very High"],
            duplicates="drop",
        )

        write_dataframe_to_snowflake(conn, customer_value_summary, table_name, database, schema)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
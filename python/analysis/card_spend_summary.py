import os
from dotenv import load_dotenv
import pandas as pd
from python.utils.snowflake_connection import get_snowflake_connection
from python.utils.write_to_snowflake import write_dataframe_to_snowflake


def main():

    load_dotenv()

    database = os.getenv("SNOWFLAKE_DATABASE")
    schema=os.getenv("SNOWFLAKE_SCHEMA")
    table_name = "CARD_SPEND_SUMMARY"
    conn = get_snowflake_connection()

    try:
        query = """
        SELECT
            TRANSACTION_ID,
            AMOUNT_DOLLARS,
            CARD_BRAND,
            CARD_TYPE
        FROM FINANCE_DB.ANALYTICS.FCT_TRANSACTIONS_ENRICHED
        """
        df = pd.read_sql(query, conn)

        card_spend_summary = (
            df.groupby(["CARD_BRAND", "CARD_TYPE"], as_index=False)
              .agg(
                  total_spend=("AMOUNT_DOLLARS", "sum"),
                  transaction_count=("TRANSACTION_ID", "count"),
                  avg_transaction_value=("AMOUNT_DOLLARS", "mean"),
              )
              .sort_values(["CARD_BRAND", "CARD_TYPE"])
        )

        write_dataframe_to_snowflake(conn, card_spend_summary, table_name, database, schema)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
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
    table_name = "CREDIT_LIMIT_ANALYSIS"
    conn = get_snowflake_connection()

    try:
        query = """
        SELECT
            CARD_ID,
            CARD_BRAND,
            CARD_TYPE,
            CREDIT_LIMIT,
            AMOUNT_DOLLARS
        FROM FINANCE_DB.ANALYTICS.FCT_TRANSACTIONS_ENRICHED
        WHERE CREDIT_LIMIT IS NOT NULL
        """
        df = pd.read_sql(query, conn)

        card_credit_summary = (
            df.groupby(["CARD_ID", "CARD_BRAND", "CARD_TYPE", "CREDIT_LIMIT"], as_index=False)
              .agg(
                  total_spend=("AMOUNT_DOLLARS", "sum"),
                  avg_transaction_value=("AMOUNT_DOLLARS", "mean"),
                  transaction_count=("AMOUNT_DOLLARS", "count"),
              )
        )

        card_credit_summary["spend_to_credit_limit_ratio"] = np.where(
            card_credit_summary["CREDIT_LIMIT"] != 0,
            card_credit_summary["total_spend"] / card_credit_summary["CREDIT_LIMIT"],
            None,
        )

        write_dataframe_to_snowflake(conn, card_credit_summary, table_name, database, schema)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
import os
from dotenv import load_dotenv
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from python.utils.snowflake_connection import get_snowflake_connection
from python.utils.write_to_snowflake import write_dataframe_to_snowflake

load_dotenv()

database = os.getenv("SNOWFLAKE_DATABASE")
schema=os.getenv("SNOWFLAKE_SCHEMA")
table_name = "CUSTOMER_SEGMENTS"
conn = get_snowflake_connection()


query = "SELECT * FROM FCT_TRANSACTIONS_ENRICHED"
df = pd.read_sql(query, conn)

print("Data loaded:")
#print(df.head())

# The .groupby() method works like GroupBy in SQL
# The .agg() mehtod allows you to create aggregated columns
# by specifying the column and the aggregation type
customer_summary = (
    df.groupby("CUSTOMER_ID", as_index=False)
      .agg(
          total_spent=("AMOUNT_DOLLARS", "sum"),
          transaction_count=("TRANSACTION_ID", "count"),
          avg_transaction_value=("AMOUNT_DOLLARS", "mean")
      )
)

# Creates a new column in customer_summary called spend_segment
# This uses a quartile cut. A normal cut method would split the values
# into 3 equal chunks (0-33%, 33-66%, 66-100%). A qcut splits it based
# on the distribution of data rather than an equal cut
customer_summary["spend_segment"] = pd.qcut(
    customer_summary["total_spent"],
    q=3,
    labels=["Low", "Medium", "High"]
)

write_dataframe_to_snowflake(conn, customer_summary, table_name, database, schema)


conn.close()
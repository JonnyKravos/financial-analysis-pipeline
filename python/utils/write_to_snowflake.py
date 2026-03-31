import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "NUMBER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP_NTZ"
    else:
        return "VARCHAR"

def create_snowflake_table(conn, df, table_name, database, schema):
    cols = []
    for col in df.columns:
        #In the original code chatGPT was allocated the data types
        #After querying it its better to set the data types to a 
        #default (VARCHAR) and set to the correct datatypes using 
        #bdt to create the views in the analytical schema

        dtype = map_dtype(df[col].dtype)
        cols.append(f'"{col}" {dtype}')
    sql = f"""
    CREATE OR REPLACE TABLE {database}.{schema}.{table_name} (
        {", ".join(cols)}
    )
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def write_dataframe_to_snowflake(conn, df, table_name, database, schema):
    create_snowflake_table(conn, df, table_name, database, schema)

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        database=database,
        schema=schema,
        auto_create_table=True,
        overwrite=True,
    )

    if not success:
        raise RuntimeError(f"Failed to load data into {database}.{schema}.{table_name}")

    print(f"Loaded {nrows} rows into {database}.{schema}.{table_name}")

def write_dataframe_to_snowflake_with_success(conn, df, table_name, database, schema):
    create_snowflake_table(conn, df, table_name, database, schema)

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        database=database,
        schema=schema,
        auto_create_table=True,
        overwrite=True,
    )

    if not success:
        raise RuntimeError(f"Failed to load data into {database}.{schema}.{table_name}")

    print(f"Loaded {nrows} rows into {database}.{schema}.{table_name}")
    return success
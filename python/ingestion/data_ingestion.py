import os
from dotenv import load_dotenv
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from python.utils.snowflake_connection import get_snowflake_connection
from python.utils.write_to_snowflake import write_dataframe_to_snowflake_with_success
import re
import shutil

# -----------------------------
# CONFIG (only set once)
# -----------------------------

load_dotenv()

INCOMING_FOLDER = os.getenv("INCOMING_FOLDER_PATH")
ARCHIVE_FOLDER = os.getenv("ARCHIVE_FOLDER_PATH")

WAREHOUSE=os.getenv("SNOWFLAKE_WAREHOUSE")
DATABASE=os.getenv("SNOWFLAKE_DATABASE")
SCHEMA=os.getenv("SNOWFLAKE_SCHEMA")

conn = get_snowflake_connection()

# -----------------------------
# HELPERS
# -----------------------------
def clean_name(name):
    # strip() removes leading and trailing whitespace
    # upper() capitalises the string
    name = name.strip().upper()
    # sub takes a specified pattern, a replacement and a string
    # it goes over the provided string and replaces any instance
    # of the provided pattern. In this case the provided pattern
    # is a regular expression and the raplcement is an underscore. 
    # ^ means negates. A-Z means A through Z capitalised and 0-9 
    # is numbers 0 through to 9. Overall this is looking for any
    # characters that are not in the range A-Z or 0-9 and replacing
    # them with an underscore
    name = re.sub(r"[^A-Z0-9]+", "_", name)
    # it then strips out underscores at the beginning or end of string
    return name.strip("_")


def derive_table_name(file_path):
    # basename extracts the last part of the path. So here will extract the file
    # e.g example.csv
    filename = os.path.basename(file_path)
    # splitext splits the file to name and ext. By specifying [0] we are getting 
    # the filename without filetype
    table = os.path.splitext(filename)[0]
    return clean_name(table)


# -----------------------------
# PROCESS FILE
# -----------------------------
def process_file(file_path):
    print(f"Processing: {file_path}")

    # read the file path provided and make a data frame of the csv
    df = pd.read_csv(file_path, dtype=str)

    # Skip empty files
    if df.empty:
        print("Skipping empty file")
        return

    # Gooes through the different column names and cleans them
    df.columns = [clean_name(c) for c in df.columns]
    #print(df.dtypes)
    #print(df[['expires', 'acct_open_year']].head())

    table_name = derive_table_name(file_path)

    success = write_dataframe_to_snowflake_with_success(conn, df, table_name, DATABASE, SCHEMA)
    print(success)

    if success:
        # Move file to archive
        shutil.move(
            file_path,
            os.path.join(ARCHIVE_FOLDER, os.path.basename(file_path))
        )
    else:
        print(f"Failed to load {table_name}")


# -----------------------------
# MAIN LOOP
# -----------------------------
def main():
    # Goes through files in the incoming_folder.
    # os.listdir lists all files and directories
    # in the specified directory
    for file in os.listdir(INCOMING_FOLDER):
        # Checks for files ending with .csv
        if file.endswith(".csv"):
            # os.path.join will take 'Strings' (Specifically Path Strings)
            # and append them togetehr to create a valid path
            full_path = os.path.join(INCOMING_FOLDER, file)
            process_file(full_path)


if __name__ == "__main__":
    main()
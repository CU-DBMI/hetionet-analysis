# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # Gather Connectivity Search `PathCount` Table
#
# Negar mentioned needing data from a PostgreSQL database archive,
# `connectivity-search-pg_dump.sql.gz`, which was created as part of
# https://github.com/greenelab/connectivity-search-backend/blob/main/README.md .
# The archive is available under https://zenodo.org/records/3978766 .
# Only the `PathCount` Table is needed in order to extract single metapaths
# at a time (needed for other work).

# +
import gzip
import pathlib

import duckdb
import requests
from pyarrow import parquet

from hetionet_utils.sql import extract_and_write_sql_block

# create the data dir
pathlib.Path("data").mkdir(exist_ok=True)

# url for source data
url = (
    "https://zenodo.org/records/3978766/files/"
    "connectivity-search-pg_dump.sql.gz?download=1"
)

# local archive file location
sql_file = "data/connectivity-search-pg_dump.sql.gz"

# expected number of tables within dump
expected_table_count = 15

# table which is targeted within the sql archive above
target_table_name = "public.dj_hetmech_app_pathcount"

# duckdb filename
duckdb_filename = "data/connectivity-search.duckdb"

# +
# gather postgresql database archive

# if the file doesn't exist, download it
if not pathlib.Path(sql_file).exists():
    # Download the file in streaming mode
    response = requests.get(url, stream=True)

    # Check if the request was successful
    response.raise_for_status()

    # Write the response content to a file in chunks
    with open(sql_file, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)

pathlib.Path(sql_file).exists()
# -

# show the tables
count = 0
with gzip.open(sql_file, "rt") as f:
    for line in f:
        # seek table creation lines
        if "CREATE TABLE" in line:
            print(line)
            count += 1
            # there are roughly 15 tables
            # so we break here to avoid further processing
            if count == expected_table_count:
                break

# gather the create table statement
extract_and_write_sql_block(
    sql_file=sql_file,
    sql_start=f"CREATE TABLE {target_table_name}",
    sql_end=";",
    output_file=(create_table_file := f"create_table.{target_table_name}.sql"),
)

# +
# show the create table statement
with open(create_table_file, "r") as create_file:
    create_sql = "".join(create_file.readlines())

print(create_sql)
# -

# gather the data for the table
extract_and_write_sql_block(
    sql_file=sql_file,
    sql_start=f"COPY {target_table_name}",
    sql_end="\\.",
    output_file=(copy_data_file := f"copy_data.{target_table_name}.sql"),
)

# +
# replace the first and last lines of the copy file
# as these are the header and data termination lines
# which have no actual values.
input_file = pathlib.Path(copy_data_file)
# Temporary file with .tmp extension
temp_file = input_file.with_suffix(".tmp")

with input_file.open("r") as infile, temp_file.open("w") as outfile:
    # Skip the first line
    first_line = next(infile, None)

    # Only proceed if the file is not empty
    if first_line is not None:
        # Start with the second line
        prev_line = next(infile, None)
        for line in infile:
            # Write the previous line
            outfile.write(prev_line)
            # Update the previous line buffer
            prev_line = line

        # Note: the last line is in `prev_line` and is not written

# Replace the original file with the temporary file
temp_file.replace(input_file)
# -

# create the table
with duckdb.connect(duckdb_filename) as ddb:
    ddb.execute(create_sql.replace("public.", ""))

# # copy the data from the file to duckdb database
# as tab-delimited file.
with duckdb.connect(duckdb_filename) as ddb:
    ddb.execute(
        f"""
        COPY dj_hetmech_app_pathcount
        FROM '{copy_data_file}'
        (DELIMITER '\t', HEADER false);
        """
    )

# +
# read and export data to parquet for simpler use
with duckdb.connect(duckdb_filename) as ddb:
    tbl_pathcount = ddb.execute(
        """
        SELECT *
        FROM dj_hetmech_app_pathcount
        """
    ).arrow()

parquet.write_table(
    table=tbl_pathcount,
    where="data/dj_hetmech_app_pathcount.parquet",
    # compress with zstd for higher compression than snappy
    compression="zstd",
)

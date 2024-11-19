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

from hetionet_utils.sql import (
    extract_and_write_sql_block,
    remove_first_and_last_line_of_file,
)

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
target_pathcount_table_name = "public.dj_hetmech_app_pathcount"
target_identifier_table_name = "public.public.dj_hetmech_app_node"

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
    sql_start=f"CREATE TABLE {target_pathcount_table_name}",
    sql_end=";",
    output_file=(
        create_table_file := f"create_table.{target_pathcount_table_name}.sql"
    ),
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
    sql_start=f"COPY {target_pathcount_table_name}",
    sql_end="\\.",
    output_file=(copy_data_file := f"copy_data.{target_pathcount_table_name}.sql"),
)

# replace the first and last lines of the copy file
# as these are the header and data termination lines
# which have no actual values.
copy_data_file = remove_first_and_last_line_of_file(target_file=copy_data_file)
copy_data_file

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

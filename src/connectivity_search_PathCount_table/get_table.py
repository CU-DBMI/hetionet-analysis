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
# Negar mentioned needing data from a PostgreSQL database archive, `connectivity-search-pg_dump.sql.gz`, which was created as part of https://github.com/greenelab/connectivity-search-backend/blob/main/README.md .
# The archive is available under https://zenodo.org/records/3978766 . Only the `PathCount` Table is needed in order to extract single metapaths at a time (needed for other work).
#                                                                                                                   This PR was mentioned as a resource in case it's needed greenelab/connectivity-search-backend#79 .
#                                                                                                                                                                                                            

# +
import gzip
import pathlib
from typing import List, Optional

import duckdb
import requests
from pyarrow import parquet

from hetionet_utils.sql import extract_and_write_sql_block

# create the data dir
pathlib.Path("data").mkdir(exist_ok=True)

# url for source data
url = "https://zenodo.org/records/3978766/files/connectivity-search-pg_dump.sql.gz?download=1"
# local archive file location
sql_file = "data/connectivity-search-pg_dump.sql.gz"
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
    response.raise_for_status()  # Check if the request was successful

    # Write the response content to a file in chunks
    with open(output_file, "wb") as file:
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
            if count == 15:
                break

# gather the create table statement
extract_and_write_sql_block(
    sql_file=sql_file,
    sql_start=f"CREATE TABLE {target_table_name}",
    sql_end=";",
    output_file=(create_table_file := f"create_table.{target_table_name}.sql"),
)

# show the create table statement
with open(create_table_file, "r") as create_file:
    print("".join(create_file.readlines()))

# gather the data for the table
# note: we avoid printing the file below as it is
# relatively large at ~14.3 GB.
extract_and_write_sql_block(
    sql_file=sql_file,
    sql_start=f"COPY {target_table_name}",
    sql_end="\\.",
    output_file=f"copy_data.{target_table_name}.sql",
)

# create the table
with duckdb.connect(duckdb_filename) as ddb:
    ddb.execute(create_sql.replace("public.", ""))

# remove the first and last line of the data to avoid conflicts with a load
# note: we use sed from a macos terminal, which may vary from system to system.
# sed was used here to help avoid unnecessary data duplication and complexity
# in processing through python.
# !sed -i '' '1d;$d' copy_data.public.dj_hetmech_app_pathcount.sql

# # copy the data from the file to duckdb database
# as tab-delimited file.
with duckdb.connect(duckdb_filename) as ddb:
    ddb.execute(
        """
        COPY dj_hetmech_app_pathcount
        FROM 'copy_data.public.dj_hetmech_app_pathcount.sql'
        (DELIMITER '\t', HEADER false);
        """
    )

# +
# read and export data to parquet for ease of use
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
# -



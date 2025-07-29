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

# # Bioprocess data extraction
#
# Extract specific bioprocess data from metapath dataset.

# +
import duckdb

metapath_ids = "../bioprocess_metapath_to_gene_pval_and_dwpc/data/sources/metapaths.csv"
metapath_data = "./data/connectivity-search-precalculated-metapath-data.parquet"

# +
with duckdb.connect() as ddb:
    # gather the metapath ids as a SQL formatted list
    ids = (
        "'"
        + "','".join(
            ddb.execute(
                f"""
                SELECT *
                FROM read_csv('{metapath_ids}')
                """
            )
            .df()["metapath"]
            .to_list()
        )
        + "'"
    )

    # use the list to search the full dataset, returning the results
    # which match the metapath ids
    metapath_results = ddb.execute(
        f"""
        SELECT *
        FROM read_parquet('{metapath_data}')
        WHERE metapath_id in ({ids})
        """
    ).df()

metapath_results
# -

# export the results to parquet
metapath_results.to_parquet("data/bp_subset_metapaths.parquet")

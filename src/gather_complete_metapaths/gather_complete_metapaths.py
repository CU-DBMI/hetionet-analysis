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

# # Gather complete metapaths
#
# Demonstrating how to gather complete metapath data given a source and target.

# +
import pandas as pd

from hetionet_utils.udf import async_get_complete_metapaths

results = await async_get_complete_metapaths(21400, 32981)

pd.DataFrame(results)

"""
Tests for udf.py
"""

import asyncio

import httpx
import pandas as pd
import pytest

from hetionet_utils.udf import (
    fetch_metapaths,
    fetch_path_count,
    get_complete_metapaths,
    get_paths_json,
)


@pytest.mark.parametrize(
    "source, target, metapath",
    [
        # Case 1: Successful extraction
        (42494, 39906, "BPpGdCrC"),
    ],
)
def test_get_paths_json(source: int, target: int, metapath: str):
    """
    Fetch real data from the Het.io API and verify structure.
    """
    result = get_paths_json(source, target, metapath)
    # The live API should return at least one path
    assert isinstance(result, str)


@pytest.mark.parametrize(
    "source, target, metapath",
    [
        # Case 1: Successful extraction
        (42494, 39906, "BPpGdCrC"),
    ],
)
def test_get_path_count_info(source: int, target: int, metapath: str):
    """
    Fetch real data from the Het.io API and verify structure.
    """
    result = asyncio.run(
        fetch_path_count(httpx.AsyncClient(), source, target, metapath)
    )
    # The live API should return at least one path
    assert isinstance(result, dict)


@pytest.mark.parametrize(
    "source, target",
    [
        # Case 1: Successful extraction
        (
            21400,
            32981,
        ),
    ],
)
def test_get_metapaths_per_source_and_target(source: int, target: int):
    """
    Fetch real data from the Het.io API and verify structure.
    """
    result = asyncio.run(fetch_metapaths(httpx.AsyncClient(), source, target))
    assert isinstance(result, list)
    assert {type(val) for val in result} == {str}


@pytest.mark.parametrize(
    "source, target",
    [
        # Case 1: Successful extraction
        (
            21400,
            32981,
        ),
    ],
)
def test_get_complete_metapaths(source: int, target: int):
    """
    Fetch real data from the Het.io API and verify structure.
    """
    result = get_complete_metapaths(source, target)
    print(pd.DataFrame(result))
    assert isinstance(result, list)

"""
Creates UDFs for various Het.io API calls.
"""

import asyncio
import json

import httpx
import requests


def get_paths_json(source: int, target: int, metapath: str) -> str:
    """
    Fetch the full Het.io “paths” JSON blob for the given triple
    and return it as a raw JSON string.
    """
    url = (
        f"https://search-api.het.io/v1/paths/"
        f"source/{source}/target/{target}/metapath/{metapath}/"
        "?format=json"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    raw_paths = resp.json()["paths"]

    EXPECTED_KEYS = [
        "metapath",
        "node_ids",
        "rel_ids",
        "percent_of_DWPC",
        "PC",
        "DWPC",
        "score",
        "PDP",
    ]

    return json.dumps([{k: p.get(k) for k in EXPECTED_KEYS} for p in raw_paths])


async def fetch_metapaths(
    client: httpx.AsyncClient,
    source: int,
    target: int,
) -> list:
    url = (
        f"https://search-api.het.io/v1/metapaths/"
        f"source/{source}/target/{target}/"
        "?format=json&complete=true"
    )
    resp = await client.get(url, timeout=30.0)
    resp.raise_for_status()
    data = resp.json()["path_counts"]
    return [item["metapath_id"] for item in data]


async def fetch_path_count(
    client: httpx.AsyncClient,
    source: int,
    target: int,
    metapath: str,
) -> dict:
    url = (
        f"https://search-api.het.io/v1/paths/"
        f"source/{source}/target/{target}/metapath/{metapath}/"
        "?format=json"
    )
    resp = await client.get(url, timeout=30.0)
    resp.raise_for_status()
    raw = resp.json()["path_count_info"]

    EXPECTED_KEYS = [
        "source",
        "target",
        "metapath_abbreviation",
        "path_count",
        "adjusted_p_value",
        "p_value",
        "dwpc",
        "dgp_source_degree",
        "dgp_target_degree",
        "dgp_n_dwpcs",
        "dgp_n_nonzero_dwpcs",
        "dgp_nonzero_mean",
        "dgp_nonzero_sd",
    ]

    # reorder to EXPECTED_KEYS, filling missing with None
    return {k: raw.get(k) for k in EXPECTED_KEYS}


async def async_get_complete_metapaths(
    source: int,
    target: int,
    concurrency: int = 20,
) -> list:
    """
    Fetch all complete metapaths concurrently via asyncio+httpx.
    """
    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        # 1) fetch the list of metapaths
        metapaths = await fetch_metapaths(client, source, target)

        # 2) define a semaphore guarded worker
        async def worker(metapath_id: str) -> dict:
            async with semaphore:
                return await fetch_path_count(client, source, target, metapath_id)

        # 3) schedule & gather
        tasks = [asyncio.create_task(worker(m)) for m in metapaths]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # 4) split successes & errors (optional)
    output, errors = [], []
    for mid, res in zip(metapaths, results):
        if isinstance(res, Exception):
            errors.append((mid, res))
        else:
            output.append(res)

    if errors:
        for mid, exc in errors:
            print(f"[ERROR] metapath={mid!r} → {exc!r}")

    return output


def get_complete_metapaths(source: str, target: str) -> list:
    return asyncio.run(async_get_complete_metapaths(source, target))

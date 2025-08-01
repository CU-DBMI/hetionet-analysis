# Hetionet Analysis

[![Software DOI badge](https://zenodo.org/badge/DOI/10.5281/zenodo.15597460.svg)](https://doi.org/10.5281/zenodo.15597460)

Various data analysis performed using [Hetionet](https://het.io/), a [hetnet](https://en.wikipedia.org/wiki/Heterogeneous_network) of biomedical knowledge.

## Development

1. [Install `uv`](https://docs.astral.sh/uv/getting-started/installation/).
1. Install package locally (e.g. `uv pip install -e ".[dev]"`).
1. Run tests (e.g. `uv run poe test`, through [poethepoet](https://poethepoet.natn.io/index.html) task).
1. Run various tasks (e.g. `uv run poe run_bioproc_gene_metapath_test`)

## Tasks

Poe the poet tasks may be run to help generate results without needing to run individual files or perform additional discovery within this project.
You can show all available tasks with `uv run poe`.

- Create Connectivity Search PathCount table: `uv run poe run_pathcount_extract`

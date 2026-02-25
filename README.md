# NYC Yellow Taxi — P90 Trip Distance Filter

Filters NYC Yellow Taxi trips above the **90th percentile** in `trip_distance` for a given parquet file. One file is processed per execution and the result is saved as a parquet file.

## Project structure

```
.
├── input/                  # Place your input parquet files here
├── output/                 # Filtered results are written here
├── src/
│   └── taxi_filter/
│       ├── processor.py    # TaxiProcessor class (load → filter → save)
│       └── cli.py          # CLI entry point
├── tests/
│   └── test_processor.py
└── pyproject.toml
```

## Requirements

- [uv](https://docs.astral.sh/uv/getting-started/installation/) — Python package manager

## Setup

```bash
uv sync
```

This installs all dependencies and the `taxi-filter` command inside a local virtual environment. No manual activation needed.

## Usage

```bash
# Local file
uv run taxi-filter input/yellow_tripdata_2025-01.parquet

# Remote URL
uv run taxi-filter https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

# Custom output path
uv run taxi-filter input/yellow_tripdata_2025-01.parquet -o output/result.parquet
```

The result is saved to `output/output.parquet` by default.

## Run tests

```bash
uv run pytest tests/ -v
```

## Approach

1. **Input**: accepts a local `.parquet` file path or a URL (downloaded in-memory with `requests`).
2. **Filter**: computes the P90 threshold on `trip_distance` using `pandas.quantile(0.9)` and keeps only rows strictly above it. The threshold is computed per file.
3. **Output**: writes the filtered DataFrame to a parquet file via `pyarrow`.

### Why these libraries?

| Library | Reason |
|---|---|
| `pandas` | Standard for tabular data manipulation in Python |
| `pyarrow` | Efficient parquet read/write backend |
| `requests` | Simple HTTP downloads; no extra setup vs `urllib` |
| `pytest` | Standard test framework, integrates with `uv` |

## Data source

NYC Taxi & Limousine Commission trip record data:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

# NYC Yellow Taxi — P90 Trip Distance Filter

Filters NYC Yellow Taxi trips above the **90th percentile** in `trip_distance` for a given parquet file. One file is processed per execution and the result is saved as a parquet file.

## Project structure

```
.
├── input/                  # Drop your parquet files here before running (gitignored)
├── output/                 # Filtered results land here automatically (gitignored)
├── src/
│   └── taxi_filter/
│       ├── processor.py    # TaxiProcessor class (load → filter → save)
│       └── cli.py          # CLI entry point built with Typer
├── tests/
│   └── test_processor.py   # Unit tests for TaxiProcessor
└── pyproject.toml          # Dependencies, uv config, and taxi-filter command definition
```

> **Note on data files**: parquet files are not committed to the repository. They are large binary files (100MB+) sourced from a public dataset, so tracking them in git would bloat the repo without adding value. The `input/` and `output/` directories are present via `.gitkeep` files to make the expected structure explicit without committing any data.

## Requirements

- [uv](https://docs.astral.sh/uv/getting-started/installation/) — Python package manager

## Setup

1. Clone the repository:

```bash
git clone https://github.com/esterfb3397/package-python-tinybird-interview.git
```

2. Navigate into the project directory:

```bash
cd package-python-tinybird-interview
```

3. Install dependencies:

```bash
uv sync
```

This installs all dependencies and the `taxi-filter` command inside a local virtual environment. No manual activation needed.

## Data source

NYC Taxi & Limousine Commission trip record data:
[https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

URL pattern for direct parquet download:

```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
```

Download a file into `input/` to use it locally, or pass the URL directly to the command (see Usage below).

## Usage

```bash
# Local file
uv run taxi-filter input/yellow_tripdata_2025-01.parquet

# Remote URL (downloaded in-memory, no file saved to disk)
uv run taxi-filter https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

# Custom output path
uv run taxi-filter input/yellow_tripdata_2025-01.parquet -o output/result.parquet
```

The output filename is derived from the input by default:

```
input/yellow_tripdata_2025-01.parquet → output/yellow_tripdata_2025-01_p90.parquet
```

Example output:

```
Loading: input/yellow_tripdata_2025-01.parquet   # source being processed
Total rows      : 3,475,226                      # total trips in the file
P90 threshold   : 7.7000 miles                   # computed cut-off for this file
Rows above P90  : 346,521                        # trips written to the output (~10%)
Output saved to : output/yellow_tripdata_2025-01_p90.parquet
```

## Run tests and linting

```bash
uv run pytest tests/ -v      # run tests
uv run ruff check src/ tests/ # lint
uv run ruff format src/ tests/ # format
```

The test suite covers 5 cases, all using synthetic data (no real parquet files required):

| Test | What it verifies |
|------|-----------------|
| `test_load_local_file` | Reads a local parquet file and loads it into the processor |
| `test_lf_not_loaded_raises` | Accessing data before `load()` raises a clear error |
| `test_filter_above_percentile` | P90 filter keeps exactly the top 10% of trips |
| `test_load_from_url` | URL path works correctly — `requests.get` is mocked, no network needed |
| `test_save_parquet` | Output file is a valid parquet with the expected rows and columns |

## Approach

1. **Input**: accepts a local `.parquet` file path or a URL. When a URL is provided, the file is downloaded in-memory (`io.BytesIO`) and never written to disk. This keeps the tool stateless — it produces exactly one output and no side effects. For repeated runs on the same file, downloading it once to `input/` and using the local path is the recommended workflow.
2. **Filter**: computes the P90 threshold on `trip_distance` using Polars `quantile(0.9)` and keeps only rows strictly above it. Both the total row count and the threshold are computed in a single scan before filtering. The threshold is printed so the result is transparent and auditable.
3. **Output**: writes the filtered data to a parquet file using Polars `sink_parquet`, which streams the result to disk without loading the full dataset into memory. The output directory is created automatically if it does not exist.

### Why these libraries?

| Library   | Reason |
|-----------|--------|
| `polars`  | Lazy evaluation via `scan_parquet` + `sink_parquet` — processes files in streaming chunks, safe for files larger than available RAM |
| `requests`| Simple HTTP downloads with custom headers; no extra setup vs `urllib` |
| `typer`   | Declarative CLI built on type hints — auto-generates `--help`, argument validation, and shell completion with no boilerplate |
| `pytest`  | Standard test framework, integrates with `uv` |
| `ruff`    | Fast Python linter and formatter — enforces code style and catches issues in one tool |

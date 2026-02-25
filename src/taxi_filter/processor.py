import io
from pathlib import Path

import polars as pl
import requests


class TaxiProcessor:
    """Loads a Yellow Taxi parquet file, filters trips above a given percentile
    in trip_distance, and saves the result as a parquet file.

    Uses Polars lazy evaluation (scan_parquet + sink_parquet) so the full
    dataset is never loaded into memory — it is processed in streaming chunks.
    This makes the tool safe to run against files larger than available RAM.
    """

    def __init__(self, source: str) -> None:
        self.source = source
        self._lf: pl.LazyFrame | None = None
        self.threshold: float | None = None
        self.total_rows: int | None = None
        self.filtered_rows: int | None = None

    @property
    def lf(self) -> pl.LazyFrame:
        if self._lf is None:
            raise RuntimeError("Data not loaded. Call load() first.")
        return self._lf

    def load(self) -> "TaxiProcessor":
        """Load parquet data from a local path or a URL."""
        if self.source.startswith(("http://", "https://")):
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
            }
            response = requests.get(self.source, headers=headers)
            response.raise_for_status()
            self._lf = pl.read_parquet(io.BytesIO(response.content)).lazy()
        else:
            self._lf = pl.scan_parquet(self.source)
        return self

    def filter_above_percentile(
        self,
        column: str = "trip_distance",
        percentile: float = 0.9,
    ) -> "TaxiProcessor":
        """Keep only rows where column value is strictly above the given percentile.

        Total row count and threshold are computed in a single scan before filtering.
        """
        stats = self.lf.select(
            pl.len().alias("total"),
            pl.col(column).quantile(percentile).alias("threshold"),
        ).collect()

        self.total_rows = stats["total"].item()
        self.threshold = stats["threshold"].item()

        self._lf = self.lf.filter(pl.col(column) > self.threshold)
        self.filtered_rows = self.lf.select(pl.len()).collect().item()
        return self

    def save(self, output_path: str) -> str:
        """Write the filtered LazyFrame to a parquet file using streaming."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        self.lf.sink_parquet(output_path)
        return output_path

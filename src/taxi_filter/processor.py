import io

import pandas as pd
import requests


class TaxiProcessor:
    """Loads a Yellow Taxi parquet file, filters trips above a given percentile
    in trip_distance, and saves the result as a parquet file."""

    def __init__(self, source: str) -> None:
        self.source = source
        self._df: pd.DataFrame | None = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            raise RuntimeError("Data not loaded. Call load() first.")
        return self._df

    def load(self) -> "TaxiProcessor":
        """Load parquet data from a local path or a URL."""
        if self.source.startswith(("http://", "https://")):
            response = requests.get(self.source)
            response.raise_for_status()
            self._df = pd.read_parquet(io.BytesIO(response.content))
        else:
            self._df = pd.read_parquet(self.source)
        return self

    def filter_above_percentile(
        self,
        column: str = "trip_distance",
        percentile: float = 0.9,
    ) -> "TaxiProcessor":
        """Keep only rows where column value is strictly above the given percentile."""
        threshold = self.df[column].quantile(percentile)
        self._df = self.df[self.df[column] > threshold].reset_index(drop=True)
        return self

    def save(self, output_path: str) -> str:
        """Write the filtered DataFrame to a parquet file."""
        self.df.to_parquet(output_path, index=False)
        return output_path

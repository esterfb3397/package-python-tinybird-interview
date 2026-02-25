import io
from unittest.mock import Mock

import polars as pl
import pytest
import requests

from taxi_filter.processor import TaxiProcessor


@pytest.fixture
def sample_df():
    """100 rows with trip_distance 1..100."""
    return pl.DataFrame({"trip_distance": list(range(1, 101)), "passenger_count": [1] * 100})


@pytest.fixture
def sample_parquet(sample_df, tmp_path):
    """Write sample_df to a temporary parquet file."""
    path = tmp_path / "test.parquet"
    sample_df.write_parquet(path)
    return str(path)


def test_load_local_file(sample_parquet):
    processor = TaxiProcessor(sample_parquet)
    processor.load()
    assert processor.lf is not None


def test_lf_not_loaded_raises(sample_parquet):
    processor = TaxiProcessor(sample_parquet)
    with pytest.raises(RuntimeError, match="Call load\\(\\) first"):
        _ = processor.lf


def test_filter_above_percentile(sample_parquet):
    # P90 of [1..100] with nearest interpolation = 90
    # rows with trip_distance > 90 → 91..100 → 10 rows
    processor = TaxiProcessor(sample_parquet)
    processor.load().filter_above_percentile()

    assert processor.total_rows == 100
    assert processor.filtered_rows == 10
    assert processor.threshold == 90.0


def test_load_from_url(sample_df, monkeypatch):
    buf = io.BytesIO()
    sample_df.write_parquet(buf)

    mock_response = Mock()
    mock_response.content = buf.getvalue()
    monkeypatch.setattr(requests, "get", lambda url, **kwargs: mock_response)

    processor = TaxiProcessor("https://example.com/trips.parquet")
    processor.load()
    assert processor.lf is not None


def test_save_parquet(sample_parquet, tmp_path):
    output = str(tmp_path / "output.parquet")
    TaxiProcessor(sample_parquet).load().filter_above_percentile().save(output)

    result = pl.read_parquet(output)
    assert len(result) == 10
    assert "trip_distance" in result.columns
    assert result["trip_distance"].min() == 91

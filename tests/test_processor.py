import pandas as pd
import pytest

from taxi_filter.processor import TaxiProcessor


@pytest.fixture
def sample_parquet(tmp_path):
    """Write 100 rows (trip_distance 1..100) to a temporary parquet file."""
    df = pd.DataFrame({"trip_distance": list(range(1, 101)), "passenger_count": [1] * 100})
    path = tmp_path / "test.parquet"
    df.to_parquet(path, index=False)
    return str(path)


def test_load_local_file(sample_parquet):
    processor = TaxiProcessor(sample_parquet)
    processor.load()
    assert len(processor.df) == 100


def test_df_not_loaded_raises(sample_parquet):
    processor = TaxiProcessor(sample_parquet)
    with pytest.raises(RuntimeError, match="Call load\\(\\) first"):
        _ = processor.df


def test_filter_above_percentile(sample_parquet):
    # P90 of [1..100] ≈ 90.1  →  rows with trip_distance > 90.1  →  91..100  →  10 rows
    processor = TaxiProcessor(sample_parquet)
    processor.load().filter_above_percentile()

    assert len(processor.df) == 10
    assert processor.df["trip_distance"].min() == 91
    assert (processor.df["trip_distance"] > 90).all()


def test_save_parquet(sample_parquet, tmp_path):
    output = str(tmp_path / "output.parquet")
    TaxiProcessor(sample_parquet).load().filter_above_percentile().save(output)

    result = pd.read_parquet(output)
    assert len(result) == 10
    assert "trip_distance" in result.columns

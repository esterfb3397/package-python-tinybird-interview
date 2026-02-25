import argparse

from taxi_filter.processor import TaxiProcessor


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Filter NYC Yellow Taxi trips above the 90th percentile in trip distance.",
    )
    parser.add_argument(
        "source",
        help="Path to a local .parquet file or a URL pointing to one.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="output/output.parquet",
        help="Output parquet file path (default: output/output.parquet).",
    )
    args = parser.parse_args()

    print(f"Loading: {args.source}")
    processor = TaxiProcessor(args.source)
    processor.load()

    total = len(processor.df)
    processor.filter_above_percentile()
    filtered = len(processor.df)

    processor.save(args.output)

    print(f"Total rows      : {total:,}")
    print(f"Rows above P90  : {filtered:,}")
    print(f"Output saved to : {args.output}")


if __name__ == "__main__":
    main()

from pathlib import Path
from typing import Annotated
from urllib.parse import urlparse

import typer

from taxi_filter.processor import TaxiProcessor

app = typer.Typer()


def _default_output(source: str) -> str:
    """Derive output filename from the source path or URL."""
    if source.startswith(("http://", "https://")):
        stem = Path(urlparse(source).path).stem
    else:
        stem = Path(source).stem
    return f"output/{stem}_p90.parquet"


@app.command()
def main(
    source: Annotated[
        str,
        typer.Argument(help="Path to a local .parquet file or a URL pointing to one."),
    ],
    output: Annotated[
        str | None,
        typer.Option("--output", "-o", help="Output parquet file path (default: output/<filename>_p90.parquet)."),
    ] = None,
) -> None:
    """Filter NYC Yellow Taxi trips above the 90th percentile in trip distance."""
    resolved_output = output or _default_output(source)

    typer.echo(f"Loading: {source}")
    processor = TaxiProcessor(source)
    processor.load().filter_above_percentile().save(resolved_output)

    typer.echo(f"Total rows      : {processor.total_rows:,}")
    typer.echo(f"P90 threshold   : {processor.threshold:.4f} miles")
    typer.echo(f"Rows above P90  : {processor.filtered_rows:,}")
    typer.echo(f"Output saved to : {resolved_output}")


if __name__ == "__main__":
    app()

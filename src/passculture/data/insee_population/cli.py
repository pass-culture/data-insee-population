"""INSEE population data import CLI.

Creates multi-level population tables at department, EPCI, and IRIS levels
with monthly granularity.
"""

from pathlib import Path

import typer
from rich.console import Console

app = typer.Typer(
    name="insee-population",
    help="INSEE population data import - creates multi-level geographic tables",
)
console = Console()


@app.command()
def population(
    year: int = typer.Option(2022, help="Census year (INDCVI reference)"),
    min_age: int = typer.Option(0, help="Minimum age"),
    max_age: int = typer.Option(120, help="Maximum age"),
    start_year: int = typer.Option(
        ...,
        "--start-year",
        help="First projection year",
    ),
    end_year: int = typer.Option(
        ...,
        "--end-year",
        help="Last projection year",
    ),
    output_dir: str = typer.Option(
        "data/output",
        "--output",
        "-o",
        help="Output directory for parquet files",
    ),
    include_dom: bool = typer.Option(
        True,
        "--include-dom/--no-dom",
        help="Include DOM departments (971-974)",
    ),
    include_com: bool = typer.Option(
        True,
        "--include-com/--no-com",
        help="Include COM territories (975, 977, 978)",
    ),
    include_mayotte: bool = typer.Option(
        True,
        "--include-mayotte/--no-mayotte",
        help="Include Mayotte (976) synthesized from estimates",
    ),
    correct_student_mobility: bool = typer.Option(
        True,
        "--correct-student-mobility/--no-student-mobility",
        help="Adjust EPCI geo ratios for student commuting (MOBSCO)",
    ),
    cache_dir: str = typer.Option(
        "data/cache",
        help="Cache directory for downloads",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview only, no file output",
    ),
) -> None:
    """Import INSEE population data at multiple geographic levels.

    Creates three parquet files with monthly granularity:
    - population_department.parquet (100% coverage)
    - population_epci.parquet (100% coverage)
    - population_iris.parquet (~60% coverage, exact only)

    Examples:

        # Preview (dry run)
        uv run insee-population population \\
            --start-year 2020 --end-year 2024 --dry-run

        # Ages 15-24, 2015-2030
        uv run insee-population population --min-age 15 --max-age 24 \\
            --start-year 2015 --end-year 2030
    """
    from passculture.data.insee_population.constants import MAX_CAGR_EXTENSION
    from passculture.data.insee_population.duckdb_processor import (
        PopulationProcessor,
    )

    # Validate forecast horizon before starting any work
    max_pipeline_year = year + min_age
    max_allowed = max_pipeline_year + MAX_CAGR_EXTENSION
    if end_year > max_allowed:
        console.print(
            f"[red bold]Error: end_year {end_year} exceeds maximum "
            f"reliable forecast year ({max_allowed}).[/red bold]"
        )
        console.print(
            f"  With census year {year} and min_age {min_age}, "
            f"ratio-based projection is valid to {max_pipeline_year}."
        )
        console.print(f"  CAGR can extend up to {MAX_CAGR_EXTENSION} more years.")
        console.print("  Reduce --end-year or increase --min-age to extend the range.")
        raise typer.Exit(code=1)

    console.print("[bold blue]INSEE Population Import[/bold blue]")
    console.print(
        f"Census year: {year} | Ages: {min_age}-{max_age} | "
        f"Projection: {start_year}-{end_year} (monthly)"
    )

    if include_mayotte:
        console.print("[dim]+ Including Mayotte (976)[/dim]")

    processor = PopulationProcessor(
        year=year,
        min_age=min_age,
        max_age=max_age,
        start_year=start_year,
        end_year=end_year,
        include_dom=include_dom,
        include_com=include_com,
        include_mayotte=include_mayotte,
        correct_student_mobility=correct_student_mobility,
        cache_dir=cache_dir,
    )

    processor.download_and_process()
    processor.create_multi_level_tables()

    _print_summary(processor)

    if dry_run:
        _print_preview(processor)
    else:
        output_path = Path(output_dir)
        paths = processor.save_multi_level(output_path)

        console.print(f"\n[green]Saved to {output_path}/[/green]")
        for _level, path in paths.items():
            size_mb = path.stat().st_size / 1024 / 1024
            console.print(f"  {path.name} ({size_mb:.1f} MB)")


def _print_preview(processor) -> None:
    """Print preview of first 10 rows for each level."""
    console.print("\n[bold]Preview - Department level (first 10 rows):[/bold]")
    preview = processor.conn.execute(
        "SELECT * FROM population_department LIMIT 10"
    ).df()
    console.print(preview.to_string(index=False))

    console.print("\n[bold]Preview - EPCI level (first 10 rows):[/bold]")
    preview = processor.conn.execute("SELECT * FROM population_epci LIMIT 10").df()
    console.print(preview.to_string(index=False))

    console.print(
        "\n[yellow]Dry run - no files written. Remove --dry-run to save.[/yellow]"
    )


def _print_summary(processor) -> None:
    """Print summary statistics for the processed data."""
    console.print("\n[bold]Summary by geographic level:[/bold]")

    for level in ["department", "epci", "iris"]:
        table = f"population_{level}"
        try:
            _print_level_summary(processor, level, table)
        except Exception:
            console.print(f"  {level}: [dim]not available[/dim]")


def _print_level_summary(processor, level: str, table: str) -> None:
    """Print summary for a single geographic level."""
    conn = processor.conn

    if level == "epci":
        geo_col = "epci_code"
        geo_label = "EPCIs"
    elif level == "iris":
        geo_col = "iris_code"
        geo_label = "IRIS"
    else:
        geo_col = "department_code"
        geo_label = "depts"

    stats = conn.execute(
        f"SELECT COUNT(*), SUM(population), COUNT(DISTINCT {geo_col}) FROM {table}"
    ).fetchone()
    count, total_pop, n_geo = stats

    n_years = conn.execute(f"SELECT COUNT(DISTINCT year) FROM {table}").fetchone()[0]
    n_ages = conn.execute(f"SELECT COUNT(DISTINCT age) FROM {table}").fetchone()[0]
    avg_pop = total_pop / (n_years * 12) if n_years else 0
    avg_cohort = avg_pop / n_ages if n_ages else 0
    console.print(
        f"  {level}: {n_geo} {geo_label}, {n_years} years, "
        f"avg {avg_pop:,.0f} pop/month, "
        f"avg {avg_cohort:,.0f}/age "
        f"({count:,} rows)"
    )


@app.command()
def info() -> None:
    """Show available census years and output schema."""
    from passculture.data.insee_population.constants import INDCVI_URLS

    console.print("[bold blue]Available Census Years[/bold blue]")
    for year in sorted(INDCVI_URLS.keys()):
        console.print(f"  {year}")

    console.print("\n[bold blue]Output Tables[/bold blue]")

    console.print("\n  [bold]population_department.parquet[/bold] (100% coverage)")
    console.print("    Aggregated by department - includes all DROM/TOM/Mayotte")

    console.print("\n  [bold]population_epci.parquet[/bold] (100% coverage)")
    console.print("    Aggregated by EPCI")

    console.print("\n  [bold]population_iris.parquet[/bold] (~60% coverage)")
    console.print("    Fine-grained IRIS level - only precise data")

    console.print("\n[bold blue]Columns[/bold blue]")
    console.print("  All levels: year, month, snapshot_month, born_date, decimal_age,")
    console.print(
        "    department_code, region_code, age, sex, geo_precision, population"
    )
    console.print("  epci: + epci_code")
    console.print("  iris: + epci_code, commune_code, iris_code")


if __name__ == "__main__":
    app()

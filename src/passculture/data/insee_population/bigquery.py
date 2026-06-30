"""BigQuery export helper for INSEE population data."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from google.cloud import bigquery
from loguru import logger

from passculture.data.insee_population.constants import POPULATION_SCHEMAS

if TYPE_CHECKING:
    from collections.abc import Iterable

    from passculture.data.insee_population.duckdb_processor import PopulationProcessor

ALL_LEVELS = ["department", "epci", "canton", "iris"]


def export_to_bigquery(
    processor: PopulationProcessor,
    level: str,
    project_id: str,
    dataset: str,
    table: str,
    write_disposition: str = "WRITE_TRUNCATE",
    *,
    yearly: bool = False,
) -> None:
    """Export a population level table to BigQuery.

    The level is streamed to a temporary Parquet file via DuckDB's COPY and
    loaded with ``load_table_from_file`` — the full birth-month-expanded
    result (up to ~300M rows for IRIS) is never materialised in a pandas
    DataFrame, keeping peak memory bounded.

    Args:
        processor: A PopulationProcessor with tables already created.
        level: One of "department", "epci", "canton", "iris".
        project_id: GCP project ID.
        dataset: BigQuery dataset name.
        table: BigQuery table name.
        write_disposition: BigQuery write disposition (default WRITE_TRUNCATE).
        yearly: Export only the Jan 1st snapshot (``month = 1``), downsampling
            a monthly-built table to yearly resolution (12x fewer rows).
    """
    if level not in POPULATION_SCHEMAS:
        raise ValueError(
            f"Unknown level {level!r}. Expected one of {sorted(POPULATION_SCHEMAS)}"
        )

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.{table}"

    schema = [
        bigquery.SchemaField(
            col["name"],
            col["type"],
            description=col.get("description", ""),
        )
        for col in POPULATION_SCHEMAS[level]
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = Path(tmpdir) / f"{table}.parquet"
        processor.copy_level_to_parquet(level, parquet_path, yearly=yearly)
        with parquet_path.open("rb") as fh:
            job = client.load_table_from_file(fh, table_ref, job_config=job_config)
        job.result()

    grain = "yearly" if yearly else "monthly"
    logger.info("Loaded {} rows ({}) to {}", job.output_rows, grain, table_ref)


def export_all_to_bigquery(
    processor: PopulationProcessor,
    project_id: str,
    dataset: str,
    table_prefix: str = "population",
    write_disposition: str = "WRITE_TRUNCATE",
    *,
    levels: Iterable[str] | None = None,
    yearly_levels: Iterable[str] | None = None,
) -> None:
    """Export geographic levels to BigQuery as separate tables.

    Creates tables named ``{table_prefix}_{level}`` for each selected level.

    Args:
        processor: A PopulationProcessor with tables already created.
        project_id: GCP project ID.
        dataset: BigQuery dataset name.
        table_prefix: Prefix for table names (default "population").
        write_disposition: BigQuery write disposition (default WRITE_TRUNCATE).
        levels: Levels to export (default: all four). Useful to skip the
            expensive high-resolution levels.
        yearly_levels: Levels to export at yearly (Jan 1st) resolution instead
            of monthly. Lets you keep department monthly while emitting the
            lower-resolution levels yearly to bound memory and storage.
    """
    selected = list(levels) if levels is not None else list(ALL_LEVELS)
    unknown = [lv for lv in selected if lv not in POPULATION_SCHEMAS]
    if unknown:
        raise ValueError(
            f"Unknown level(s) {unknown}. Expected from {sorted(POPULATION_SCHEMAS)}"
        )

    yearly_set = set(yearly_levels or ())

    for level in selected:
        table = f"{table_prefix}_{level}"
        export_to_bigquery(
            processor,
            level,
            project_id,
            dataset,
            table,
            write_disposition,
            yearly=level in yearly_set,
        )

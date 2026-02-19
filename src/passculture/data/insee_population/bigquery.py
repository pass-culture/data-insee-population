"""BigQuery export helper for INSEE population data."""

from __future__ import annotations

from typing import TYPE_CHECKING

from google.cloud import bigquery

from passculture.data.insee_population.constants import POPULATION_SCHEMAS

if TYPE_CHECKING:
    from passculture.data.insee_population.duckdb_processor import PopulationProcessor


def export_to_bigquery(
    processor: PopulationProcessor,
    level: str,
    project_id: str,
    dataset: str,
    table: str,
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    """Export a population level table to BigQuery.

    Args:
        processor: A PopulationProcessor with tables already created.
        level: One of "department", "epci", "canton", "iris".
        project_id: GCP project ID.
        dataset: BigQuery dataset name.
        table: BigQuery table name.
        write_disposition: BigQuery write disposition (default WRITE_TRUNCATE).
    """
    if level not in POPULATION_SCHEMAS:
        raise ValueError(
            f"Unknown level {level!r}. Expected one of {sorted(POPULATION_SCHEMAS)}"
        )

    df = processor.to_pandas(level)

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
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    print(f"Loaded {job.output_rows} rows to {table_ref}")


def export_all_to_bigquery(
    processor: PopulationProcessor,
    project_id: str,
    dataset: str,
    table_prefix: str = "population",
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    """Export all 4 geographic levels to BigQuery as separate tables.

    Creates tables named ``{table_prefix}_department``,
    ``{table_prefix}_epci``, ``{table_prefix}_canton``, and
    ``{table_prefix}_iris``.

    Args:
        processor: A PopulationProcessor with tables already created.
        project_id: GCP project ID.
        dataset: BigQuery dataset name.
        table_prefix: Prefix for table names (default "population").
        write_disposition: BigQuery write disposition (default WRITE_TRUNCATE).
    """
    for level in ["department", "epci", "canton", "iris"]:
        table = f"{table_prefix}_{level}"
        export_to_bigquery(
            processor, level, project_id, dataset, table, write_disposition
        )

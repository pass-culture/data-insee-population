"""INSEE population data for pass Culture data platform."""

from passculture.data.insee_population.bigquery import (
    export_all_to_bigquery,
    export_to_bigquery,
)
from passculture.data.insee_population.duckdb_processor import PopulationProcessor

__all__ = ["PopulationProcessor", "export_all_to_bigquery", "export_to_bigquery"]

"""Tests for BigQuery export helpers and population schemas."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from passculture.data.insee_population.constants import (
    POPULATION_SCHEMA,
    POPULATION_SCHEMA_CANTON,
    POPULATION_SCHEMA_DEPARTMENT,
    POPULATION_SCHEMA_EPCI,
    POPULATION_SCHEMA_IRIS,
    POPULATION_SCHEMAS,
)
from passculture.data.insee_population.duckdb_processor import PopulationProcessor

# -------------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------------


@pytest.fixture
def projection_processor() -> PopulationProcessor:
    """Create a processor with projected tables for all 4 levels."""
    from passculture.data.insee_population import sql
    from passculture.data.insee_population.projections import (
        compute_age_ratios,
        compute_geo_ratios,
        project_multi_year,
    )

    processor = PopulationProcessor(
        year=2022,
        min_age=15,
        max_age=20,
        start_year=2022,
        end_year=2023,
        cache_dir=None,
    )

    # Create base population table
    processor.conn.execute("""
        CREATE OR REPLACE TABLE population AS
        SELECT * FROM (VALUES
            (2022, '75', '11', '7599', '75101', '751010101', 15, 'male', 500.0),
            (2022, '75', '11', '7599', '75101', '751010101', 16, 'male', 520.0),
            (2022, '75', '11', '7599', '75101', '751010101', 17, 'male', 510.0),
            (2022, '75', '11', '7599', '75101', '751010101', 18, 'male', 530.0),
            (2022, '75', '11', '7599', '75101', '751010101', 19, 'male', 490.0),
            (2022, '75', '11', '7599', '75101', '751010101', 15, 'female', 480.0),
            (2022, '75', '11', '7599', '75101', '751010101', 16, 'female', 500.0),
            (2022, '75', '11', '7599', '75101', '751010101', 17, 'female', 490.0),
            (2022, '75', '11', '7599', '75101', '751010101', 18, 'female', 510.0),
            (2022, '75', '11', '7599', '75101', '751010101', 19, 'female', 470.0),
            (2022, '13', '93', '1301', '13001', '130010101', 15, 'male', 400.0),
            (2022, '13', '93', '1301', '13001', '130010101', 16, 'male', 410.0),
            (2022, '13', '93', '1301', '13001', '130010101', 17, 'male', 390.0),
            (2022, '13', '93', '1301', '13001', '130010101', 18, 'male', 420.0),
            (2022, '13', '93', '1301', '13001', '130010101', 19, 'male', 380.0),
            (2022, '13', '93', '1301', '13001', '130010101', 15, 'female', 390.0),
            (2022, '13', '93', '1301', '13001', '130010101', 16, 'female', 400.0),
            (2022, '13', '93', '1301', '13001', '130010101', 17, 'female', 380.0),
            (2022, '13', '93', '1301', '13001', '130010101', 18, 'female', 410.0),
            (2022, '13', '93', '1301', '13001', '130010101', 19, 'female', 370.0)
        ) AS t(year, department_code, region_code,
               canton_code, commune_code, iris_code,
               age, sex, population)
    """)
    processor._base_table_created = True

    # Geo mappings
    commune_epci = pd.DataFrame(
        {
            "commune_code": ["75101", "13001"],
            "epci_code": ["200054781", "200054807"],
            "commune_name": ["Paris 1er", "Marseille"],
            "department_code": ["75", "13"],
            "commune_population": [10000, 50000],
        }
    )
    canton_weights = pd.DataFrame(
        {
            "canton_code": ["7599", "1301"],
            "epci_code": ["200054781", "200054807"],
            "weight": [1.0, 1.0],
        }
    )
    processor._register_dataframe("commune_epci_df", commune_epci)
    processor._execute(
        "CREATE OR REPLACE TABLE commune_epci AS SELECT * FROM commune_epci_df"
    )
    processor._register_dataframe("canton_weights_df", canton_weights)
    processor._execute(
        "CREATE OR REPLACE TABLE canton_weights AS SELECT * FROM canton_weights_df"
    )
    processor._geo_mappings_loaded = True

    # Quinquennal estimates
    quinquennal_df = pd.DataFrame(
        [
            {
                "year": y,
                "department_code": d,
                "sex": s,
                "age_band": "15_19",
                "population": p,
            }
            for y in [2022, 2023]
            for d, p_base in [("75", 5000.0), ("13", 4000.0)]
            for s, factor in [("male", 1.0), ("female", 0.95)]
            for p in [p_base * factor * (1.01 if y == 2023 else 1.0)]
        ]
    )
    processor._register_dataframe("quinquennal_df", quinquennal_df)
    processor._execute(sql.REGISTER_QUINQUENNAL)

    compute_age_ratios(processor.conn, census_year=2022)

    monthly_df = pd.DataFrame(
        [
            {"department_code": d, "month": m, "month_ratio": 1.0 / 12}
            for d in ["75", "13"]
            for m in range(1, 13)
        ]
    )
    processor._register_dataframe("monthly_births_df", monthly_df)
    processor._execute(sql.REGISTER_MONTHLY_BIRTHS)

    compute_geo_ratios(processor.conn, "epci")
    compute_geo_ratios(processor.conn, "canton")
    compute_geo_ratios(processor.conn, "iris")
    project_multi_year(processor.conn, 15, 20)

    return processor


# -------------------------------------------------------------------------
# Test: Schema column names match actual SQL output
# -------------------------------------------------------------------------


class TestSchemaMatchesSQLOutput:
    """Verify that POPULATION_SCHEMAS column lists match actual table output."""

    def test_department_schema_columns(self, projection_processor):
        """Department schema column names match actual table columns."""
        df = projection_processor.to_pandas("department")
        schema_cols = [c["name"] for c in POPULATION_SCHEMA_DEPARTMENT]
        assert list(df.columns) == schema_cols

    def test_epci_schema_columns(self, projection_processor):
        """EPCI schema column names match actual table columns."""
        df = projection_processor.to_pandas("epci")
        schema_cols = [c["name"] for c in POPULATION_SCHEMA_EPCI]
        assert list(df.columns) == schema_cols

    def test_canton_schema_columns(self, projection_processor):
        """Canton schema column names match actual table columns."""
        df = projection_processor.to_pandas("canton")
        schema_cols = [c["name"] for c in POPULATION_SCHEMA_CANTON]
        assert list(df.columns) == schema_cols

    def test_iris_schema_columns(self, projection_processor):
        """IRIS schema column names match actual table columns."""
        df = projection_processor.to_pandas("iris")
        schema_cols = [c["name"] for c in POPULATION_SCHEMA_IRIS]
        assert list(df.columns) == schema_cols

    def test_backward_compat_alias(self):
        """POPULATION_SCHEMA is an alias for POPULATION_SCHEMA_IRIS."""
        assert POPULATION_SCHEMA is POPULATION_SCHEMA_IRIS


class TestSchemaStructure:
    """Verify schema dict structure and completeness."""

    def test_schemas_dict_has_four_levels(self):
        """POPULATION_SCHEMAS has exactly department, epci, canton, iris."""
        assert set(POPULATION_SCHEMAS) == {"department", "epci", "canton", "iris"}

    def test_all_schemas_have_common_columns(self):
        """All schemas contain the common columns."""
        common_names = {
            "year",
            "month",
            "birth_month",
            "snapshot_month",
            "born_date",
            "decimal_age",
            "department_code",
            "region_code",
            "age",
            "sex",
            "geo_precision",
            "population",
            "confidence_pct",
            "population_low",
            "population_high",
        }
        for level, schema in POPULATION_SCHEMAS.items():
            col_names = {c["name"] for c in schema}
            missing = common_names - col_names
            assert not missing, f"{level} schema missing common columns: {missing}"

    def test_epci_has_epci_code(self):
        """EPCI schema includes epci_code."""
        col_names = {c["name"] for c in POPULATION_SCHEMA_EPCI}
        assert "epci_code" in col_names

    def test_canton_has_canton_code(self):
        """Canton schema includes canton_code."""
        col_names = {c["name"] for c in POPULATION_SCHEMA_CANTON}
        assert "canton_code" in col_names

    def test_iris_has_geo_codes(self):
        """IRIS schema includes epci_code, commune_code, iris_code."""
        col_names = {c["name"] for c in POPULATION_SCHEMA_IRIS}
        assert "epci_code" in col_names
        assert "commune_code" in col_names
        assert "iris_code" in col_names

    def test_department_has_no_extra_geo(self):
        """Department schema has no epci/canton/iris/commune columns."""
        col_names = {c["name"] for c in POPULATION_SCHEMA_DEPARTMENT}
        for extra in ("epci_code", "canton_code", "commune_code", "iris_code"):
            assert extra not in col_names, f"Unexpected {extra} in department schema"


# -------------------------------------------------------------------------
# Test: export_to_bigquery selects correct schema per level
# -------------------------------------------------------------------------


class TestExportToBigQuery:
    """Tests for export_to_bigquery function."""

    @patch("passculture.data.insee_population.bigquery.bigquery")
    def test_uses_correct_schema_per_level(self, mock_bq, projection_processor):
        """export_to_bigquery builds a SchemaField list matching the level."""
        mock_client = MagicMock()
        mock_bq.Client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.output_rows = 42
        mock_client.load_table_from_dataframe.return_value = mock_job

        from passculture.data.insee_population.bigquery import export_to_bigquery

        for level in ["department", "epci", "canton", "iris"]:
            mock_bq.SchemaField.reset_mock()

            export_to_bigquery(
                projection_processor, level, "proj", "ds", f"pop_{level}"
            )

            # Extract the first positional arg (field name) from each SchemaField call
            schema_names = [call[0][0] for call in mock_bq.SchemaField.call_args_list]

            expected_names = [c["name"] for c in POPULATION_SCHEMAS[level]]
            assert schema_names == expected_names, (
                f"Schema mismatch for {level}: {schema_names} != {expected_names}"
            )

    @patch("passculture.data.insee_population.bigquery.bigquery")
    def test_rejects_unknown_level(self, mock_bq, projection_processor):
        """export_to_bigquery raises ValueError for unknown level."""
        from passculture.data.insee_population.bigquery import export_to_bigquery

        with pytest.raises(ValueError, match="Unknown level"):
            export_to_bigquery(projection_processor, "commune", "p", "d", "t")


class TestExportAllToBigQuery:
    """Tests for export_all_to_bigquery function."""

    @patch("passculture.data.insee_population.bigquery.bigquery")
    def test_exports_all_four_levels(self, mock_bq, projection_processor):
        """export_all_to_bigquery creates 4 tables with correct names."""
        mock_client = MagicMock()
        mock_bq.Client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.output_rows = 10
        mock_client.load_table_from_dataframe.return_value = mock_job

        from passculture.data.insee_population.bigquery import export_all_to_bigquery

        export_all_to_bigquery(projection_processor, "proj", "ds", "pop")

        # Should have been called 4 times
        assert mock_client.load_table_from_dataframe.call_count == 4

        # Check table refs
        table_refs = [
            call[0][1] for call in mock_client.load_table_from_dataframe.call_args_list
        ]
        assert "proj.ds.pop_department" in table_refs
        assert "proj.ds.pop_epci" in table_refs
        assert "proj.ds.pop_canton" in table_refs
        assert "proj.ds.pop_iris" in table_refs

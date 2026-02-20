"""Tests for DuckDB population processor."""

from __future__ import annotations

import pandas as pd
import pytest

from passculture.data.insee_population.duckdb_processor import PopulationProcessor

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture
def processor() -> PopulationProcessor:
    """Create a processor instance for testing."""
    return PopulationProcessor(year=2022, cache_dir=None)


@pytest.fixture
def processor_with_data(processor: PopulationProcessor) -> PopulationProcessor:
    """Create a processor with sample population data."""
    processor.conn.execute("""
        CREATE OR REPLACE TABLE population AS
        SELECT * FROM (VALUES
            (2022, '75', '11', '7599', '75101', '751010101', 18, 'male', 1000.0),
            (2022, '75', '11', '7599', '75101', '751010101', 18, 'female', 1200.0),
            (2022, '75', '11', '7599', '75101', '751010101', 19, 'male', 900.0),
            (2022, '75', '11', '7599', '75101', '751010101', 19, 'female', 1100.0),
            (2022, '13', '93', '1301', '13001', '130010101', 18, 'male', 800.0),
            (2022, '13', '93', '1301', '13001', '130010101', 18, 'female', 850.0)
        ) AS t(year, department_code, region_code,
               canton_code, commune_code, iris_code,
               age, sex, population)
    """)
    processor._base_table_created = True
    return processor


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a sample parquet file mimicking INDCVI format."""
    import duckdb

    data = {
        "IRIS": ["751010101", "751010102", "75101XXXX", "ZZZZZZZZZ", "971010101"],
        "DEPT": ["75", "75", "75", "75", "971"],
        "REGION": ["11", "11", "11", "11", "01"],
        "CANTVILLE": ["7599", "7599", "7599", "7599", "9711"],
        "AGEREV": ["18", "18", "19", "20", "18"],
        "SEXE": ["1", "2", "1", "2", "1"],
        "IPONDI": ["100.5", "150.3", "200.0", "50.0", "75.5"],
    }
    df = pd.DataFrame(data)  # noqa: F841 — referenced by duckdb.sql

    parquet_path = tmp_path / "test_indcvi.parquet"
    duckdb.sql("SELECT * FROM df").write_parquet(str(parquet_path))

    return parquet_path


def _setup_geo_mappings(processor: PopulationProcessor) -> None:
    """Helper to set up mock geo mappings on a processor."""
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


# -----------------------------------------------------------------------------
# Test: Initialization
# -----------------------------------------------------------------------------


class TestProcessorInit:
    """Tests for processor initialization."""

    def test_default_values(self):
        """Test default initialization values."""
        processor = PopulationProcessor()
        assert processor.year == 2022
        assert processor.min_age == 0
        assert processor.max_age == 120
        assert processor.include_dom is True
        assert processor.include_com is True
        assert processor.include_mayotte is True
        assert processor.start_year == 2015
        assert processor.end_year == 2030

    def test_custom_values(self):
        """Test custom initialization values."""
        processor = PopulationProcessor(
            year=2021,
            min_age=15,
            max_age=20,
            start_year=2020,
            end_year=2024,
            include_dom=False,
            include_mayotte=True,
        )
        assert processor.year == 2021
        assert processor.min_age == 15
        assert processor.max_age == 20
        assert processor.start_year == 2020
        assert processor.end_year == 2024
        assert processor.include_dom is False
        assert processor.include_mayotte is True

    def test_connection_created(self, processor: PopulationProcessor):
        """Test DuckDB connection is created."""
        assert processor.conn is not None

    def test_base_table_not_created_initially(self, processor: PopulationProcessor):
        """Test base table flag starts as False."""
        assert processor._base_table_created is False

    def test_rejects_end_year_beyond_forecast_horizon(self):
        """Test that end_year beyond max reliable forecast raises ValueError."""
        # census 2022 + min_age 15 + MAX_CAGR_EXTENSION 10 = 2047
        with pytest.raises(ValueError, match="exceeds maximum reliable forecast"):
            PopulationProcessor(
                year=2022,
                min_age=15,
                max_age=24,
                start_year=2015,
                end_year=2048,
                cache_dir=None,
            )

    def test_accepts_end_year_at_forecast_limit(self):
        """Test that end_year exactly at the limit is accepted."""
        # census 2022 + min_age 15 + MAX_CAGR_EXTENSION 10 = 2047
        processor = PopulationProcessor(
            year=2022,
            min_age=15,
            max_age=24,
            start_year=2015,
            end_year=2047,
            cache_dir=None,
        )
        assert processor.end_year == 2047


# -----------------------------------------------------------------------------
# Test: Data Processing
# -----------------------------------------------------------------------------


class TestDataProcessing:
    """Tests for data processing from parquet files."""

    def test_process_local_parquet(
        self, processor: PopulationProcessor, sample_parquet
    ):
        """Test processing a local parquet file."""
        from passculture.data.insee_population import sql
        from passculture.data.insee_population.constants import IRIS_SENTINEL_NO_GEO

        processor._execute(
            sql.CREATE_BASE_TABLE.format(
                parquet_path=sample_parquet,
                where_clause="WHERE CAST(AGEREV AS INT) BETWEEN 0 AND 120",
                year=2022,
                iris_sentinel_no_geo=IRIS_SENTINEL_NO_GEO,
            )
        )
        processor._base_table_created = True

        # Check basic results
        result = processor.conn.execute("SELECT * FROM population").df()
        assert len(result) > 0
        assert "population" in result.columns
        assert "department_code" in result.columns
        assert "canton_code" in result.columns

    def test_iris_masked_handling(self, processor: PopulationProcessor, sample_parquet):
        """Test that masked IRIS codes (XXXX suffix) fall back to commune."""
        processor.conn.execute(f"""
            CREATE OR REPLACE TABLE population AS
            SELECT DISTINCT
                CASE
                    WHEN RIGHT(TRIM(IRIS), 4) = 'XXXX' THEN LEFT(TRIM(IRIS), 5)
                    ELSE TRIM(IRIS)
                END AS iris_code
            FROM read_parquet('{sample_parquet}')
        """)

        result = processor.conn.execute("SELECT * FROM population").df()
        iris_codes = result["iris_code"].tolist()

        assert "75101" in iris_codes  # From 75101XXXX
        assert "75101XXXX" not in iris_codes

    def test_requires_base_table(self, processor: PopulationProcessor):
        """Test that multi-level tables require base table to be created first."""
        with pytest.raises(RuntimeError, match="download_and_process"):
            processor.create_multi_level_tables()


# -----------------------------------------------------------------------------
# Test: Validation
# -----------------------------------------------------------------------------


class TestValidation:
    """Tests for data validation."""

    def test_validate_returns_valid(self, processor_with_data: PopulationProcessor):
        """Test validation returns valid for good data."""
        result = processor_with_data.validate()

        assert result["is_valid"] is True
        assert len(result["errors"]) == 0

    def test_validate_stats(self, processor_with_data: PopulationProcessor):
        """Test validation returns correct statistics."""
        result = processor_with_data.validate()

        assert result["stats"]["total_rows"] == 6
        assert result["stats"]["total_population"] == pytest.approx(5850.0)
        assert result["stats"]["departments"] == 2

    def test_validate_detects_negative_population(self, processor: PopulationProcessor):
        """Test validation detects negative populations."""
        processor.conn.execute("""
            CREATE OR REPLACE TABLE population AS
            SELECT 2022 AS year, '75' AS department_code,
                   '11' AS region_code, '7599' AS canton_code,
                   '75101' AS commune_code,
                   '751010101' AS iris_code,
                   18 AS age, 'male' AS sex, -100.0 AS population
        """)
        processor._base_table_created = True

        result = processor.validate()

        assert result["is_valid"] is False
        assert len(result["errors"]) > 0
        assert any("negative" in err.lower() for err in result["errors"])

    def test_validate_detects_null_population(self, processor: PopulationProcessor):
        """Test validation detects null populations."""
        processor.conn.execute("""
            CREATE OR REPLACE TABLE population AS
            SELECT 2022 AS year, '75' AS department_code,
                   '11' AS region_code, '7599' AS canton_code,
                   '75101' AS commune_code,
                   '751010101' AS iris_code,
                   18 AS age, 'male' AS sex, NULL AS population
        """)
        processor._base_table_created = True

        result = processor.validate()

        assert result["is_valid"] is False


# -----------------------------------------------------------------------------
# Test: Department Coverage
# -----------------------------------------------------------------------------


class TestDepartmentCoverage:
    """Tests for department filtering options."""

    def test_dom_filtering_flag(self):
        """Test DOM filtering option is stored correctly."""
        processor = PopulationProcessor(include_dom=False, cache_dir=None)
        assert processor.include_dom is False

    def test_com_filtering_flag(self):
        """Test COM filtering option is stored correctly."""
        processor = PopulationProcessor(include_com=False, cache_dir=None)
        assert processor.include_com is False

    def test_mayotte_option_flag(self):
        """Test Mayotte option is stored correctly."""
        processor = PopulationProcessor(include_mayotte=True, cache_dir=None)
        assert processor.include_mayotte is True


# -----------------------------------------------------------------------------
# Test: Multi-Year Projection Mode
# -----------------------------------------------------------------------------


class TestMultiYearProjection:
    """Tests for multi-year monthly projection mode."""

    @pytest.fixture
    def projection_processor(self) -> PopulationProcessor:
        """Create a processor configured for multi-year projection with mock data."""
        processor = PopulationProcessor(
            year=2022,
            min_age=15,
            max_age=20,
            start_year=2022,
            end_year=2023,
            cache_dir=None,
        )

        # Create base population table (from INDCVI)
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

        # Set up geo mappings
        _setup_geo_mappings(processor)

        return processor

    def _setup_projection_tables(self, processor: PopulationProcessor) -> None:
        """Set up all required tables for projection."""
        from passculture.data.insee_population import sql
        from passculture.data.insee_population.projections import (
            compute_age_ratios,
            compute_geo_ratios,
        )

        # Register quinquennal estimates (needed by cohort-shifted age ratios)
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

        # Compute cohort-shifted age ratios from base table
        compute_age_ratios(processor.conn, census_year=2022)

        # Register monthly birth distribution (uniform for simplicity)
        months = list(range(1, 13))
        monthly_df = pd.DataFrame(
            [
                {"department_code": d, "month": m, "month_ratio": 1.0 / 12}
                for d in ["75", "13"]
                for m in months
            ]
        )
        processor._register_dataframe("monthly_births_df", monthly_df)
        processor._execute(sql.REGISTER_MONTHLY_BIRTHS)

        # Compute geo ratios
        compute_geo_ratios(processor.conn, "epci")
        compute_geo_ratios(processor.conn, "canton")
        compute_geo_ratios(processor.conn, "iris")

    def test_projected_department_has_required_columns(self, projection_processor):
        """Test projected department table has all required columns."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20)

        result = projection_processor.to_pandas("department")
        required_cols = {
            "year",
            "month",
            "birth_month",
            "snapshot_month",
            "born_date",
            "decimal_age",
            "department_code",
            "age",
            "sex",
            "geo_precision",
            "population",
            "confidence_pct",
            "population_low",
            "population_high",
        }
        assert required_cols.issubset(set(result.columns))

    def test_projected_department_yearly_default(self, projection_processor):
        """Test yearly mode: month is always 1, birth_month has values 1-12."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20)

        result = projection_processor.to_pandas("department")
        # Yearly mode: only month=1 (January snapshot)
        assert sorted(result["month"].unique()) == [1]
        # Birth month should have all 12 values
        assert sorted(result["birth_month"].unique()) == list(range(1, 13))

    def test_projected_department_has_multiple_years(self, projection_processor):
        """Test projected data spans start_year to end_year."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20)

        result = projection_processor.to_pandas("department")
        years = sorted(result["year"].unique())
        assert 2022 in years
        assert 2023 in years

    def test_projected_decimal_age(self, projection_processor):
        """Test decimal_age varies with birth_month in yearly mode."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20)

        result = projection_processor.to_pandas("department")
        # For age=18, birth_month=1, month=1: born 2004-01-01, snapshot 2022-01-01
        # decimal_age = 216/12 = 18.0
        row_bm1 = result[
            (result["age"] == 18)
            & (result["month"] == 1)
            & (result["birth_month"] == 1)
        ].iloc[0]
        assert abs(row_bm1["decimal_age"] - 18.0) < 0.01

        # For age=18, birth_month=7, month=1: born 2004-07-01, snapshot 2022-01-01
        # decimal_age = 210/12 = 17.5
        row_bm7 = result[
            (result["age"] == 18)
            & (result["month"] == 1)
            & (result["birth_month"] == 7)
        ].iloc[0]
        assert abs(row_bm7["decimal_age"] - 17.5) < 0.01

    def test_projected_born_date(self, projection_processor):
        """Test born_date uses birth_month."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20)

        result = projection_processor.to_pandas("department")
        import datetime

        # For year=2022, age=18, birth_month=1: born_date = 2004-01-01
        row_bm1 = result[
            (result["year"] == 2022)
            & (result["age"] == 18)
            & (result["birth_month"] == 1)
        ].iloc[0]
        born1 = row_bm1["born_date"]
        assert born1 == datetime.date(2004, 1, 1) or str(born1).startswith("2004-01-01")

        # For year=2022, age=18, birth_month=7: born_date = 2004-07-01
        row_bm7 = result[
            (result["year"] == 2022)
            & (result["age"] == 18)
            & (result["birth_month"] == 7)
        ].iloc[0]
        born7 = row_bm7["born_date"]
        assert born7 == datetime.date(2004, 7, 1) or str(born7).startswith("2004-07-01")

    def test_projected_population_positive(self, projection_processor):
        """Test all projected populations are positive."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20)

        for level in ["department", "epci", "iris"]:
            result = projection_processor.to_pandas(level)
            assert (result["population"] > 0).all(), f"Negative population in {level}"

    def test_projected_epci_has_epci_code(self, projection_processor):
        """Test EPCI projection includes epci_code."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20)

        result = projection_processor.to_pandas("epci")
        assert "epci_code" in result.columns
        assert result["epci_code"].notna().all()

    def test_projected_iris_has_iris_code(self, projection_processor):
        """Test IRIS projection includes iris_code."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20)

        result = projection_processor.to_pandas("iris")
        assert "iris_code" in result.columns
        assert result["iris_code"].notna().all()

    def test_population_sums_consistent(self, projection_processor):
        """Test total population at EPCI/IRIS ≤ department (ratios sum to ≤1)."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20)

        dept_pop = projection_processor.conn.execute(
            "SELECT SUM(population) FROM population_department"
        ).fetchone()[0]
        iris_pop = projection_processor.conn.execute(
            "SELECT SUM(population) FROM population_iris"
        ).fetchone()[0]

        # IRIS should be <= department (not all geo can be mapped)
        assert iris_pop <= dept_pop * 1.01  # allow tiny floating point rounding

    def test_monthly_flag_produces_12_snapshots(self, projection_processor):
        """With monthly=True, month column should have values 1-12."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20, monthly=True)

        result = projection_processor.to_pandas("department")
        months = sorted(result["month"].unique())
        assert months == list(range(1, 13))
        # birth_month should also be present
        birth_months = sorted(result["birth_month"].unique())
        assert birth_months == list(range(1, 13))

    def test_birth_month_population_sum_preserved(self, projection_processor):
        """Sum across birth_months should equal the annual quinquennal total."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20)

        # In yearly mode, sum of population across all birth_months for a
        # given year/dept/age/sex should equal quinquennal * age_ratio
        # (since month_ratios sum to 1)
        total = projection_processor.conn.execute("""
            SELECT SUM(population)
            FROM population_department
            WHERE year = 2022 AND department_code = '75' AND sex = 'male'
        """).fetchone()[0]

        quinq_total = projection_processor.conn.execute("""
            SELECT SUM(population)
            FROM quinquennal
            WHERE year = 2022 AND department_code = '75' AND sex = 'male'
        """).fetchone()[0]

        assert abs(total - quinq_total) < 1.0, (
            f"Birth-month sum ({total:.1f}) should equal "
            f"quinquennal ({quinq_total:.1f})"
        )

    def test_birth_month_column_always_present(self, projection_processor):
        """birth_month column exists in all geographic levels."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20)

        for level in ["department", "epci", "canton", "iris"]:
            result = projection_processor.to_pandas(level)
            assert "birth_month" in result.columns, f"birth_month missing from {level}"

    def test_yearly_exported_population_equals_quinquennal(self, projection_processor):
        """In yearly mode, SUM(exported pop) per band must equal quinquennal input.

        The birth-month expansion (x12 rows) uses month_ratio which sums to 1,
        so total population is preserved.
        """
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20)

        exported = projection_processor.to_pandas("department")
        # Sum across all birth_months for year=2022, dept=75, male
        total = exported[
            (exported["year"] == 2022)
            & (exported["department_code"] == "75")
            & (exported["sex"] == "male")
        ]["population"].sum()

        quinq_total = projection_processor.conn.execute("""
            SELECT SUM(population) FROM quinquennal
            WHERE year = 2022 AND department_code = '75' AND sex = 'male'
        """).fetchone()[0]

        assert abs(total - quinq_total) < 1.0, (
            f"Exported population ({total:.1f}) != quinquennal ({quinq_total:.1f})"
        )

    def test_monthly_snapshot_population_is_full_stock(self, projection_processor):
        """In monthly mode, each snapshot month's total must equal the annual stock.

        Population is a stock variable — the number of 18-year-olds in January
        is the same as in July. month_ratio only splits birth-month sub-cohorts,
        it must NOT reduce the snapshot total.

        Regression: before fix, month_ratio was applied twice in monthly mode
        (once during projection, once at export), giving 1/12 of expected values.
        """
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20, monthly=True)

        exported = projection_processor.to_pandas("department")

        quinq_total = projection_processor.conn.execute("""
            SELECT SUM(population) FROM quinquennal
            WHERE year = 2022 AND department_code = '75' AND sex = 'male'
        """).fetchone()[0]

        # Check EACH snapshot month individually
        for month in range(1, 13):
            month_total = exported[
                (exported["year"] == 2022)
                & (exported["month"] == month)
                & (exported["department_code"] == "75")
                & (exported["sex"] == "male")
            ]["population"].sum()

            assert abs(month_total - quinq_total) < 1.0, (
                f"Month {month}: exported pop ({month_total:.1f}) != "
                f"quinquennal stock ({quinq_total:.1f}). "
                f"Ratio: {month_total / quinq_total:.4f}"
            )

    def test_monthly_and_yearly_same_per_snapshot_total(self, projection_processor):
        """Monthly and yearly modes must produce the same total per snapshot month.

        Yearly mode has 1 snapshot (Jan) with 12 birth_month rows.
        Monthly mode has 12 snapshots, each with 12 birth_month rows.
        Per-snapshot totals should be identical.
        """
        from passculture.data.insee_population.projections import project_multi_year

        # Yearly mode
        self._setup_projection_tables(projection_processor)
        project_multi_year(projection_processor.conn, 15, 20, monthly=False)
        yearly_exported = projection_processor.to_pandas("department")
        yearly_jan = yearly_exported[
            (yearly_exported["year"] == 2022)
            & (yearly_exported["month"] == 1)
            & (yearly_exported["department_code"] == "75")
            & (yearly_exported["sex"] == "male")
        ]["population"].sum()

        # Monthly mode (re-run projection)
        project_multi_year(projection_processor.conn, 15, 20, monthly=True)
        monthly_exported = projection_processor.to_pandas("department")
        monthly_jan = monthly_exported[
            (monthly_exported["year"] == 2022)
            & (monthly_exported["month"] == 1)
            & (monthly_exported["department_code"] == "75")
            & (monthly_exported["sex"] == "male")
        ]["population"].sum()

        assert abs(yearly_jan - monthly_jan) < 1.0, (
            f"Yearly Jan ({yearly_jan:.1f}) != Monthly Jan ({monthly_jan:.1f})"
        )


# -----------------------------------------------------------------------------
# Test: Age Ratios
# -----------------------------------------------------------------------------


class TestAgeRatios:
    """Tests for age ratio computation."""

    @staticmethod
    def _register_quinquennal(processor, year=2022):
        """Register a minimal quinquennal table for the given year."""
        processor.conn.execute(f"""
            CREATE OR REPLACE TABLE quinquennal AS
            SELECT * FROM (VALUES
                ({year}, '75', 'male', '15_19', 5000.0)
            ) AS t(year, department_code, sex, age_band, population)
        """)

    def test_age_ratios_sum_to_one(self):
        """Test that age ratios within each band sum to approximately 1."""
        from passculture.data.insee_population.projections import compute_age_ratios

        processor = PopulationProcessor(cache_dir=None)
        # Create a population with known distribution
        processor.conn.execute("""
            CREATE OR REPLACE TABLE population AS
            SELECT * FROM (VALUES
                (2022, '75', '11', '7599', '75101', '751010101', 15, 'male', 100.0),
                (2022, '75', '11', '7599', '75101', '751010101', 16, 'male', 120.0),
                (2022, '75', '11', '7599', '75101', '751010101', 17, 'male', 110.0),
                (2022, '75', '11', '7599', '75101', '751010101', 18, 'male', 130.0),
                (2022, '75', '11', '7599', '75101', '751010101', 19, 'male', 140.0)
            ) AS t(year, department_code, region_code,
                   canton_code, commune_code, iris_code,
                   age, sex, population)
        """)

        self._register_quinquennal(processor, 2022)
        compute_age_ratios(processor.conn, census_year=2022)

        # Check ratios sum to 1 for band 15_19
        ratio_sum = processor.conn.execute("""
            SELECT SUM(age_ratio) FROM age_ratios
            WHERE department_code = '75' AND sex = 'male' AND age_band = '15_19'
        """).fetchone()[0]
        assert abs(ratio_sum - 1.0) < 0.001

    def test_age_ratio_proportional(self):
        """Test that age ratios are proportional to population."""
        from passculture.data.insee_population.projections import compute_age_ratios

        processor = PopulationProcessor(cache_dir=None)
        processor.conn.execute("""
            CREATE OR REPLACE TABLE population AS
            SELECT * FROM (VALUES
                (2022, '75', '11', '7599', '75101', '751010101', 15, 'male', 200.0),
                (2022, '75', '11', '7599', '75101', '751010101', 16, 'male', 100.0),
                (2022, '75', '11', '7599', '75101', '751010101', 17, 'male', 100.0),
                (2022, '75', '11', '7599', '75101', '751010101', 18, 'male', 100.0),
                (2022, '75', '11', '7599', '75101', '751010101', 19, 'male', 100.0)
            ) AS t(year, department_code, region_code,
                   canton_code, commune_code, iris_code,
                   age, sex, population)
        """)

        self._register_quinquennal(processor, 2022)
        compute_age_ratios(processor.conn, census_year=2022)

        # Age 15 has 200 out of 600 total = 1/3
        ratio_15 = processor.conn.execute("""
            SELECT age_ratio FROM age_ratios
            WHERE department_code = '75' AND sex = 'male' AND age = 15
        """).fetchone()[0]
        assert abs(ratio_15 - 200 / 600) < 0.001

    def test_age_ratio_boundary_not_inflated(self):
        """Test age ratios at band boundaries are correct with full band data.

        Regression test: when max_age=25 and the base table includes the full
        25_29 band, age 25's ratio should be ~0.20 (1/5), not 1.0.
        If the base table were filtered to ages 1-25, age 25 would be the
        only member of its band, giving it ratio=1.0 and inflating its
        projected population by ~5x.
        """
        from passculture.data.insee_population import sql
        from passculture.data.insee_population.projections import (
            compute_age_ratios,
            compute_geo_ratios,
            project_multi_year,
        )

        processor = PopulationProcessor(
            year=2022,
            min_age=1,
            max_age=25,
            start_year=2022,
            end_year=2022,
            cache_dir=None,
        )

        # Build population with full bands (including ages 25-29)
        # This simulates what skip_age_filter=True produces:
        # the base table has ALL ages, not just 1-25.
        rows = []
        for age in range(0, 30):
            rows.append(
                f"(2022, '75', '11', '7599', '75101',"
                f" '751010101', {age}, 'male', 100.0)"
            )
            rows.append(
                f"(2022, '75', '11', '7599', '75101', '751010101',"
                f" {age}, 'female', 100.0)"
            )
        values = ",\n                ".join(rows)
        processor.conn.execute(f"""
            CREATE OR REPLACE TABLE population AS
            SELECT * FROM (VALUES
                {values}
            ) AS t(year, department_code, region_code,
                   canton_code, commune_code, iris_code,
                   age, sex, population)
        """)
        processor._base_table_created = True
        _setup_geo_mappings(processor)

        # Register quinquennal first (needed by cohort-shifted age ratios)
        quinquennal_df = pd.DataFrame(
            [
                {
                    "year": 2022,
                    "department_code": "75",
                    "sex": s,
                    "age_band": ab,
                    "population": 5000.0,
                }
                for s in ["male", "female"]
                for ab in ["0_4", "5_9", "10_14", "15_19", "20_24", "25_29"]
            ]
        )
        processor._register_dataframe("quinquennal_df", quinquennal_df)
        processor._execute(sql.REGISTER_QUINQUENNAL)

        # Compute age ratios — with full band data, age 25 ratio ≈ 0.20
        compute_age_ratios(processor.conn, census_year=2022)

        ratio_25 = processor.conn.execute("""
            SELECT age_ratio FROM age_ratios
            WHERE department_code = '75' AND sex = 'male' AND age = 25
        """).fetchone()[0]
        assert abs(ratio_25 - 0.2) < 0.01, (
            f"Age 25 ratio should be ~0.2 (1/5 of band), got {ratio_25}"
        )

        monthly_df = pd.DataFrame(
            [
                {"department_code": "75", "month": m, "month_ratio": 1.0 / 12}
                for m in range(1, 13)
            ]
        )
        processor._register_dataframe("monthly_births_df", monthly_df)
        processor._execute(sql.REGISTER_MONTHLY_BIRTHS)

        compute_geo_ratios(processor.conn, "epci")
        compute_geo_ratios(processor.conn, "canton")
        compute_geo_ratios(processor.conn, "iris")
        project_multi_year(processor.conn, 1, 25)

        dept_df = processor.to_pandas("department")
        # Compare population of age 24 and age 25 (same month/year/birth_month)
        mask_24 = (
            (dept_df["age"] == 24)
            & (dept_df["month"] == 1)
            & (dept_df["birth_month"] == 1)
            & (dept_df["sex"] == "male")
        )
        pop_24 = dept_df[mask_24]["population"].iloc[0]
        mask_25 = (
            (dept_df["age"] == 25)
            & (dept_df["month"] == 1)
            & (dept_df["birth_month"] == 1)
            & (dept_df["sex"] == "male")
        )
        pop_25 = dept_df[mask_25]["population"].iloc[0]

        # With uniform census data, they should be very close (same ratio ~0.2)
        assert pop_25 < pop_24 * 2, (
            f"Age 25 pop ({pop_25}) should not be inflated vs age 24 ({pop_24})"
        )


# -----------------------------------------------------------------------------
# Test: Cohort-Shifted Age Ratios
# -----------------------------------------------------------------------------


class TestCohortAgeRatios:
    """Tests for cohort-shifted age ratio computation."""

    def _setup_census_and_quinquennal(self, processor, census_ages, quinquennal_years):
        """Create population and quinquennal tables for testing."""
        # Census has ages with known populations (dept='75', sex='male')
        rows = ",\n            ".join(
            f"(2022, '75', '11', '7599', '75101', '751010101', {age}, 'male', {pop})"
            for age, pop in census_ages.items()
        )
        processor.conn.execute(f"""
            CREATE OR REPLACE TABLE population AS
            SELECT * FROM (VALUES
                {rows}
            ) AS t(year, department_code, region_code,
                   canton_code, commune_code, iris_code,
                   age, sex, population)
        """)

        # Quinquennal with given years
        q_rows = ",\n            ".join(
            f"({y}, '75', 'male', '15_19', 5000.0)" for y in quinquennal_years
        )
        processor.conn.execute(f"""
            CREATE OR REPLACE TABLE quinquennal AS
            SELECT * FROM (VALUES
                {q_rows}
            ) AS t(year, department_code, sex, age_band, population)
        """)

    def test_cohort_shift_2025(self):
        """Verify cohort shift: year=2025 looks at census ages shifted by -3.

        Census year=2022, target year=2025, target ages 15-19.
        census_age = target_age + (2022 - 2025) = target_age - 3
        So ages 15-19 in 2025 map to census ages 12-16.
        """
        from passculture.data.insee_population.projections import compute_age_ratios

        processor = PopulationProcessor(cache_dir=None)

        # Census has ages 0-25 with known populations
        census_ages = dict.fromkeys(range(0, 26), 1000.0)
        # Make ages 12-16 distinctive
        census_ages[12] = 800.0
        census_ages[13] = 750.0
        census_ages[14] = 820.0
        census_ages[15] = 790.0
        census_ages[16] = 810.0

        self._setup_census_and_quinquennal(processor, census_ages, [2025])
        compute_age_ratios(processor.conn, census_year=2022)

        # For year=2025, age=15 should use census_age=12 (pop=800)
        # Band total = 800+750+820+790+810 = 3970
        expected_ratio = 800 / 3970
        ratio_15 = processor.conn.execute("""
            SELECT age_ratio FROM age_ratios
            WHERE year = 2025 AND department_code = '75'
              AND sex = 'male' AND age = 15
        """).fetchone()[0]
        assert abs(ratio_15 - expected_ratio) < 0.001, (
            f"Expected {expected_ratio:.4f}, got {ratio_15:.4f}"
        )

    def test_no_shift_same_year(self):
        """When projection year == census year, no shift occurs.

        census_age = target_age + (2022 - 2022) = target_age.
        """
        from passculture.data.insee_population.projections import compute_age_ratios

        processor = PopulationProcessor(cache_dir=None)

        census_ages = {
            15: 200.0,
            16: 100.0,
            17: 100.0,
            18: 100.0,
            19: 100.0,
        }
        self._setup_census_and_quinquennal(processor, census_ages, [2022])
        compute_age_ratios(processor.conn, census_year=2022)

        # For year=2022, age=15 should be 200/600
        ratio_15 = processor.conn.execute("""
            SELECT age_ratio FROM age_ratios
            WHERE year = 2022 AND department_code = '75'
              AND sex = 'male' AND age = 15
        """).fetchone()[0]
        assert abs(ratio_15 - 200 / 600) < 0.001

    def test_ratios_differ_across_years(self):
        """Ratios for different projection years should differ (different cohorts)."""
        from passculture.data.insee_population.projections import compute_age_ratios

        processor = PopulationProcessor(cache_dir=None)

        # Census ages 10-24 with varying populations
        census_ages = {}
        for age in range(10, 25):
            census_ages[age] = 500.0 + age * 10.0  # increasing with age

        self._setup_census_and_quinquennal(processor, census_ages, [2022, 2025])
        compute_age_ratios(processor.conn, census_year=2022)

        # Ratio for age=15 in 2022 uses census_age=15
        ratio_2022 = processor.conn.execute("""
            SELECT age_ratio FROM age_ratios
            WHERE year = 2022 AND department_code = '75'
              AND sex = 'male' AND age = 15
        """).fetchone()[0]

        # Ratio for age=15 in 2025 uses census_age=12
        ratio_2025 = processor.conn.execute("""
            SELECT age_ratio FROM age_ratios
            WHERE year = 2025 AND department_code = '75'
              AND sex = 'male' AND age = 15
        """).fetchone()[0]

        assert ratio_2022 != ratio_2025, (
            f"Ratios should differ: 2022={ratio_2022}, 2025={ratio_2025}"
        )


# -----------------------------------------------------------------------------
# Test: Downloaders (unit tests for parsing helpers)
# -----------------------------------------------------------------------------


class TestDownloaderHelpers:
    """Tests for downloader parsing functions."""

    def test_parse_quinquennal_sheet(self):
        """Test quinquennal sheet parsing with mock data."""
        from passculture.data.insee_population.downloaders import (
            _FEMALE_OFFSET,
            _MALE_OFFSET,
            _parse_quinquennal_sheet,
        )

        # Verify offsets: 0=dept, 1=name, 2..22=ensemble(20+total),
        # 23..43=male(20+total), 44..64=female(20+total)
        assert _MALE_OFFSET == 23
        assert _FEMALE_OFFSET == 44

        # 65 cols: 2 + 3*(20 bands + 1 total)
        ncols = 65
        row_data = [None] * ncols
        row_data[0] = "75"
        row_data[1] = "Paris"
        # Male age band 15_19 (index 3 -> col 26)
        row_data[_MALE_OFFSET + 3] = 5000.0
        # Female age band 15_19
        row_data[_FEMALE_OFFSET + 3] = 4800.0

        pd.DataFrame([row_data])

        assert callable(_parse_quinquennal_sheet)

    def test_month_name_mapping(self):
        """Test French month name mapping."""
        from passculture.data.insee_population.downloaders import _MONTH_NAMES

        assert _MONTH_NAMES["janvier"] == 1
        assert _MONTH_NAMES["décembre"] == 12
        assert len(_MONTH_NAMES) == 12


# -----------------------------------------------------------------------------
# Test: Quinquennal Cache Re-extrapolation
# -----------------------------------------------------------------------------


class TestQuinquennalCacheReextrapolation:
    """Tests for quinquennal cache re-extrapolation when end_year exceeds cache."""

    def test_reextrapolates_when_end_year_exceeds_cache(self, tmp_path):
        """Cached parquet with years 2022-2025 should re-extrapolate to 2028."""
        from passculture.data.insee_population.downloaders import (
            download_quinquennal_estimates,
        )

        # Create a cached parquet with years 2022-2025
        rows = []
        for year in range(2022, 2026):
            for dept in ["75", "13"]:
                for sex in ["male", "female"]:
                    for band in ["15_19", "20_24"]:
                        rows.append(
                            {
                                "year": year,
                                "department_code": dept,
                                "sex": sex,
                                "age_band": band,
                                "population": 5000.0 + (year - 2022) * 50,
                            }
                        )
        cache_df = pd.DataFrame(rows)
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        cache_df.to_parquet(cache_dir / "quinquennal_estimates.parquet", index=False)

        # Request years 2022-2028 (beyond cached max of 2025)
        result = download_quinquennal_estimates(2022, 2028, cache_dir)

        years = sorted(result["year"].unique())
        assert min(years) == 2022
        assert max(years) == 2028
        assert len(years) == 7  # 2022, 2023, 2024, 2025, 2026, 2027, 2028

    def test_no_reextrapolation_when_cache_sufficient(self, tmp_path):
        """Cached parquet covering requested range should not re-extrapolate."""
        from passculture.data.insee_population.downloaders import (
            download_quinquennal_estimates,
        )

        rows = []
        for year in range(2020, 2031):
            rows.append(
                {
                    "year": year,
                    "department_code": "75",
                    "sex": "male",
                    "age_band": "15_19",
                    "population": 5000.0,
                }
            )
        cache_df = pd.DataFrame(rows)
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        cache_df.to_parquet(cache_dir / "quinquennal_estimates.parquet", index=False)

        result = download_quinquennal_estimates(2022, 2028, cache_dir)

        years = sorted(result["year"].unique())
        assert years == list(range(2022, 2029))


# -----------------------------------------------------------------------------
# Test: Mayotte Age Distribution
# -----------------------------------------------------------------------------


class TestMayotteAgeDistribution:
    """Tests for Mayotte age distribution using quinquennal estimates."""

    def test_uses_mayotte_own_data(self):
        """When 976 data is present, it is used directly."""
        from unittest.mock import patch

        from passculture.data.insee_population.constants import AGE_BUCKETS
        from passculture.data.insee_population.downloaders import (
            _get_dom_age_distribution,
        )

        rows = []
        # 976 with distinctive distribution: heavy on 0_4
        for sex in ["male", "female"]:
            for band_name in AGE_BUCKETS:
                pop = 5000.0 if band_name == "0_4" else 100.0
                rows.append(
                    {
                        "year": 2022,
                        "department_code": "976",
                        "sex": sex,
                        "age_band": band_name,
                        "population": pop,
                    }
                )
        mock_df = pd.DataFrame(rows)

        with patch(
            "passculture.data.insee_population.downloaders.download_quinquennal_estimates",
            return_value=mock_df,
        ):
            dist = _get_dom_age_distribution(2022)

        # Ages 0-4 should dominate (976's own skewed distribution)
        young_pct = sum(dist[a] for a in range(0, 5))
        assert young_pct > 0.4, (
            f"Expected 976's skewed distribution, got 0-4 share={young_pct:.2f}"
        )

    def test_returns_empty_when_no_976(self):
        """Returns empty dict when 976 data is absent (no DOM fallback)."""
        from unittest.mock import patch

        from passculture.data.insee_population.constants import AGE_BUCKETS
        from passculture.data.insee_population.downloaders import (
            _get_dom_age_distribution,
        )

        # Build mock quinquennal data for DOM departments only (no 976)
        rows = []
        for dept in ["971", "972", "973", "974"]:
            for sex in ["male", "female"]:
                for band_name in AGE_BUCKETS:
                    rows.append(
                        {
                            "year": 2022,
                            "department_code": dept,
                            "sex": sex,
                            "age_band": band_name,
                            "population": 1000.0,
                        }
                    )
        mock_df = pd.DataFrame(rows)

        with patch(
            "passculture.data.insee_population.downloaders.download_quinquennal_estimates",
            return_value=mock_df,
        ):
            dist = _get_dom_age_distribution(2022)

        assert dist == {}

    def test_returns_empty_when_no_976_data(self):
        """Returns empty dict when 976 is not in quinquennal data."""
        from unittest.mock import patch

        from passculture.data.insee_population.downloaders import (
            _get_dom_age_distribution,
        )

        # Data with non-DOM department only
        mock_df = pd.DataFrame(
            [
                {
                    "year": 2022,
                    "department_code": "75",
                    "sex": "male",
                    "age_band": "15_19",
                    "population": 5000.0,
                }
            ]
        )

        with patch(
            "passculture.data.insee_population.downloaders.download_quinquennal_estimates",
            return_value=mock_df,
        ):
            dist = _get_dom_age_distribution(2022)

        assert dist == {}

    def test_mayotte_projection_all_ages(self):
        """In projection mode, Mayotte synthesized data covers ages 0-120."""
        from unittest.mock import patch

        from passculture.data.insee_population.constants import AGE_BUCKETS
        from passculture.data.insee_population.downloaders import (
            synthesize_mayotte_population,
        )

        # Mock quinquennal data for 976
        q_rows = []
        for sex in ["male", "female"]:
            for band_name in AGE_BUCKETS:
                q_rows.append(
                    {
                        "year": 2022,
                        "department_code": "976",
                        "sex": sex,
                        "age_band": band_name,
                        "population": 1000.0,
                    }
                )
        mock_quinquennal = pd.DataFrame(q_rows)

        # Mock estimates for 976
        mock_estimates = pd.DataFrame(
            [
                {
                    "department_code": "976",
                    "year": 2022,
                    "sex": "male",
                    "population": 150000,
                },
                {
                    "department_code": "976",
                    "year": 2022,
                    "sex": "female",
                    "population": 155000,
                },
            ]
        )

        with (
            patch(
                "passculture.data.insee_population.downloaders.download_quinquennal_estimates",
                return_value=mock_quinquennal,
            ),
            patch(
                "passculture.data.insee_population.downloaders.download_estimates",
                return_value=mock_estimates,
            ),
        ):
            # Projection mode: all ages 0-120
            df_all = synthesize_mayotte_population(2022, 0, 120)
            # Census mode: restricted ages 15-25
            df_restricted = synthesize_mayotte_population(2022, 15, 25)

        # All-ages should cover 0 to 120
        assert df_all["age"].min() == 0
        assert df_all["age"].max() == 120

        # Restricted should only have 15-25
        assert df_restricted["age"].min() == 15
        assert df_restricted["age"].max() == 25

        # All-ages should have more rows
        assert len(df_all) > len(df_restricted)


# -----------------------------------------------------------------------------
# Test: Student Mobility Correction
# -----------------------------------------------------------------------------


class TestStudentMobilityCorrection:
    """Tests for MOBSCO student mobility correction on EPCI geo ratios."""

    def test_correct_student_mobility_flag_stored(self):
        """Test that correct_student_mobility flag is stored on processor."""
        proc = PopulationProcessor(correct_student_mobility=True, cache_dir=None)
        assert proc.correct_student_mobility is True

        proc2 = PopulationProcessor(correct_student_mobility=False, cache_dir=None)
        assert proc2.correct_student_mobility is False

    @pytest.fixture
    def mobility_processor(self, tmp_path):
        """Create a processor with population, geo mappings, geo_ratios_epci,
        and a mock MOBSCO parquet file for testing student mobility correction.

        Setup:
        - Dept 75 has 2 EPCIs: 200054781 (commune 75101) and 200054782 (commune 75102)
        - Dept 13 has 1 EPCI: 200054807 (commune 13001)
        - MOBSCO: students from dept 75 study in commune 13001
          (dept 13) and commune 75102 (dept 75)
        """
        import duckdb as _duckdb

        processor = PopulationProcessor(
            year=2022, min_age=15, max_age=24, cache_dir=None
        )

        # Population table with ages 15-24 in dept 75 and dept 13
        rows = []
        for dept, commune, iris, region, canton in [
            ("75", "75101", "751010101", "11", "7599"),
            ("75", "75102", "751020101", "11", "7599"),
            ("13", "13001", "130010101", "93", "1301"),
        ]:
            for age in range(15, 25):
                for sex in ["male", "female"]:
                    pop = 100.0
                    rows.append(
                        f"(2022, '{dept}', '{region}', '{canton}', "
                        f"'{commune}', '{iris}', {age}, '{sex}', {pop})"
                    )
        values = ",\n            ".join(rows)
        processor.conn.execute(f"""
            CREATE OR REPLACE TABLE population AS
            SELECT * FROM (VALUES
                {values}
            ) AS t(year, department_code, region_code,
                   canton_code, commune_code, iris_code,
                   age, sex, population)
        """)
        processor._base_table_created = True

        # Commune-EPCI mapping
        commune_epci = pd.DataFrame(
            {
                "commune_code": ["75101", "75102", "13001"],
                "epci_code": ["200054781", "200054782", "200054807"],
                "commune_name": ["Paris 1er", "Paris 2e", "Marseille"],
                "department_code": ["75", "75", "13"],
                "commune_population": [10000, 8000, 50000],
            }
        )
        processor._register_dataframe("commune_epci_df", commune_epci)
        processor._execute(
            "CREATE OR REPLACE TABLE commune_epci AS SELECT * FROM commune_epci_df"
        )
        processor._geo_mappings_loaded = True

        # Compute geo_ratios_epci from population
        from passculture.data.insee_population.projections import compute_geo_ratios

        compute_geo_ratios(processor.conn, "epci")

        # Create mock MOBSCO parquet
        # Students from dept 75 study in commune 13001 (cross-dept)
        # and commune 75102 (within-dept shift)
        mobsco_data = {
            "COMMUNE": ["75101"] * 4 + ["13001"] * 2,
            "DCETUF": ["13001", "13001", "75102", "75102", "13001", "13001"],
            "AGEREV10": ["18"] * 6,
            "SEXE": ["1", "2", "1", "2", "1", "2"],
            "IPONDI": ["50.0", "50.0", "30.0", "30.0", "80.0", "80.0"],
        }
        # Add some non-student rows (should be filtered out)
        mobsco_data["COMMUNE"].extend(["75101", "75101"])
        mobsco_data["DCETUF"].extend(["13001", "13001"])
        mobsco_data["AGEREV10"].extend(["25", "30"])
        mobsco_data["SEXE"].extend(["1", "2"])
        mobsco_data["IPONDI"].extend(["100.0", "100.0"])

        mobsco_df = pd.DataFrame(mobsco_data)  # noqa: F841
        mobsco_path = tmp_path / "mobsco_test.parquet"
        _duckdb.sql("SELECT * FROM mobsco_df").write_parquet(str(mobsco_path))

        # Compute per-department mobility weights (needed before corrections)
        from passculture.data.insee_population.projections import (
            compute_department_mobility_rates,
        )

        compute_department_mobility_rates(processor.conn, mobsco_path)

        return processor, mobsco_path

    def test_student_flows_computed(self, mobility_processor):
        """Test that student_flows_epci table is created with expected rows."""
        processor, mobsco_path = mobility_processor
        from passculture.data.insee_population import sql

        # Rename geo_ratios_epci to _base
        processor.conn.execute(sql.RENAME_GEO_RATIOS_EPCI_TO_BASE)

        # Create student flows
        processor.conn.execute(
            sql.CREATE_STUDENT_FLOWS_EPCI.format(mobsco_path=mobsco_path)
        )

        flows = processor.conn.execute(
            "SELECT * FROM student_flows_epci ORDER BY department_code, epci_code, sex"
        ).df()

        # Dept 75 students study in: 200054807 (13001) and 200054782 (75102)
        dept75_flows = flows[flows["department_code"] == "75"]
        assert len(dept75_flows) > 0

        # Dept 13 students study in: 200054807 (13001)
        dept13_flows = flows[flows["department_code"] == "13"]
        assert len(dept13_flows) > 0

        # Ratios per dept/sex should sum to 1
        for (dept, sex), group in flows.groupby(["department_code", "sex"]):
            ratio_sum = group["study_geo_ratio"].sum()
            assert abs(ratio_sum - 1.0) < 0.001, (
                f"Flows for {dept}/{sex} sum to {ratio_sum}"
            )

        # Cleanup for next test
        processor.conn.execute("DROP TABLE geo_ratios_epci_base")
        processor.conn.execute("DROP TABLE student_flows_epci")

    def test_correction_shifts_weight(self, mobility_processor):
        """Test that correction shifts geo_ratio weight for student bands."""
        processor, mobsco_path = mobility_processor
        from passculture.data.insee_population.projections import (
            apply_student_mobility_correction,
        )

        # Record baseline ratios for dept 75 before correction
        base_ratios_75 = processor.conn.execute("""
            SELECT epci_code, age_band, sex, geo_ratio
            FROM geo_ratios_epci
            WHERE department_code = '75' AND age_band IN ('15_19', '20_24')
            ORDER BY epci_code, age_band, sex
        """).df()

        # Apply correction
        apply_student_mobility_correction(processor.conn, mobsco_path)

        # Get corrected ratios
        corrected_ratios_75 = processor.conn.execute("""
            SELECT epci_code, age_band, sex, geo_ratio
            FROM geo_ratios_epci
            WHERE department_code = '75' AND age_band IN ('15_19', '20_24')
            ORDER BY epci_code, age_band, sex
        """).df()

        # EPCI 200054782 (75102) should have INCREASED ratio for student bands
        # because MOBSCO says students from 75101 study in 75102
        for band in ["15_19", "20_24"]:
            base_782 = base_ratios_75[
                (base_ratios_75["epci_code"] == "200054782")
                & (base_ratios_75["age_band"] == band)
                & (base_ratios_75["sex"] == "male")
            ]["geo_ratio"].values[0]

            corrected_782 = corrected_ratios_75[
                (corrected_ratios_75["epci_code"] == "200054782")
                & (corrected_ratios_75["age_band"] == band)
                & (corrected_ratios_75["sex"] == "male")
            ]["geo_ratio"].values[0]

            assert corrected_782 > base_782, (
                f"EPCI 200054782 geo_ratio for {band}/male should increase: "
                f"base={base_782:.4f}, corrected={corrected_782:.4f}"
            )

    def test_ratios_still_sum_to_one(self, mobility_processor):
        """After correction, geo_ratios per (dept, band, sex) still sum to ~1.0."""
        processor, mobsco_path = mobility_processor
        from passculture.data.insee_population.projections import (
            apply_student_mobility_correction,
        )

        apply_student_mobility_correction(processor.conn, mobsco_path)

        ratio_sums = processor.conn.execute("""
            SELECT department_code, age_band, sex, SUM(geo_ratio) AS ratio_sum
            FROM geo_ratios_epci
            GROUP BY department_code, age_band, sex
        """).df()

        violations = ratio_sums[
            (ratio_sums["ratio_sum"] < 0.999) | (ratio_sums["ratio_sum"] > 1.001)
        ]
        assert len(violations) == 0, (
            f"Geo ratios don't sum to ~1.0 after correction:\n{violations}"
        )

    def test_non_student_bands_unchanged(self, mobility_processor):
        """Bands other than 15_19/20_24 should be unchanged after correction."""
        processor, mobsco_path = mobility_processor
        from passculture.data.insee_population.projections import (
            apply_student_mobility_correction,
        )

        # Get baseline for non-student bands
        base_other = processor.conn.execute("""
            SELECT department_code, epci_code, age_band, sex, geo_ratio
            FROM geo_ratios_epci
            WHERE age_band NOT IN ('15_19', '20_24')
            ORDER BY department_code, epci_code, age_band, sex
        """).df()

        apply_student_mobility_correction(processor.conn, mobsco_path)

        corrected_other = processor.conn.execute("""
            SELECT department_code, epci_code, age_band, sex, geo_ratio
            FROM geo_ratios_epci
            WHERE age_band NOT IN ('15_19', '20_24')
            ORDER BY department_code, epci_code, age_band, sex
        """).df()

        # Should be identical
        assert len(base_other) == len(corrected_other), (
            f"Row count changed: {len(base_other)} -> {len(corrected_other)}"
        )
        pd.testing.assert_frame_equal(
            base_other.reset_index(drop=True),
            corrected_other.reset_index(drop=True),
        )

    def test_blend_weight_varies_by_department(self, mobility_processor):
        """Mobility weights should vary by department based on MOBSCO data."""
        processor, _mobsco_path = mobility_processor

        weights = processor.conn.execute("""
            SELECT department_code, blend_weight, mobility_rate
            FROM mobility_weights
            ORDER BY department_code
        """).df()

        # Dept 75: students from 75101 study in 13001 (cross-dept) and 75102
        # → has inter-departmental mobility (50+50 out of 50+50+30+30 = 100/160)
        dept_75 = weights[weights["department_code"] == "75"]
        assert len(dept_75) == 1
        assert dept_75["mobility_rate"].values[0] > 0  # has cross-dept flows

        # Dept 13: all students study within dept 13 (commune 13001)
        # → 0% inter-departmental mobility
        dept_13 = weights[weights["department_code"] == "13"]
        assert len(dept_13) == 1
        assert dept_13["mobility_rate"].values[0] == 0.0

        # Blend weights should differ between departments
        assert dept_75["blend_weight"].values[0] != dept_13["blend_weight"].values[0], (
            "Blend weights should differ between departments with different mobility"
        )


# -----------------------------------------------------------------------------
# Test: Clamped Age Ratios (far-future projection fix)
# -----------------------------------------------------------------------------


class TestClampedAgeRatios:
    """Tests for clamped census_age in age ratio computation."""

    def test_clamped_age_ratios_sum_to_one(self):
        """For far-future year (2039, census 2022), all 5 ages in band 15_19
        should have age_ratios that sum to 1.0, even though census_age
        would be negative without clamping.
        """
        from passculture.data.insee_population.projections import compute_age_ratios

        processor = PopulationProcessor(cache_dir=None)
        # Census has ages 0-25
        rows = ",\n            ".join(
            f"(2022, '75', '11', '7599', '75101', '751010101',"
            f" {age}, 'male', {100.0 + age})"
            for age in range(0, 26)
        )
        processor.conn.execute(f"""
            CREATE OR REPLACE TABLE population AS
            SELECT * FROM (VALUES
                {rows}
            ) AS t(year, department_code, region_code,
                   canton_code, commune_code, iris_code,
                   age, sex, population)
        """)

        # Quinquennal for year 2039 (shift = 2022-2039 = -17)
        # Target ages 15-19 would map to census ages -2 to 2 without clamping
        processor.conn.execute("""
            CREATE OR REPLACE TABLE quinquennal AS
            SELECT * FROM (VALUES
                (2039, '75', 'male', '15_19', 5000.0)
            ) AS t(year, department_code, sex, age_band, population)
        """)

        compute_age_ratios(processor.conn, census_year=2022)

        # All 5 ages should be present
        ages = (
            processor.conn.execute("""
            SELECT age FROM age_ratios
            WHERE year = 2039 AND department_code = '75'
              AND sex = 'male' AND age_band = '15_19'
            ORDER BY age
        """)
            .df()["age"]
            .tolist()
        )
        assert ages == [15, 16, 17, 18, 19], f"Expected all 5 ages, got {ages}"

        # Ratios should sum to 1.0
        ratio_sum = processor.conn.execute("""
            SELECT SUM(age_ratio) FROM age_ratios
            WHERE year = 2039 AND department_code = '75'
              AND sex = 'male' AND age_band = '15_19'
        """).fetchone()[0]
        assert abs(ratio_sum - 1.0) < 0.001, (
            f"Ratios should sum to ~1.0, got {ratio_sum}"
        )

    def test_far_future_age_not_inflated(self):
        """Regression: in year 2039 (census 2022), age 19 should not be
        inflated ~1.67x due to other ages being filtered out.
        With uniform census data and clamping, all ages in the band should
        get roughly equal ratios (~0.2).
        """
        from passculture.data.insee_population.projections import compute_age_ratios

        processor = PopulationProcessor(cache_dir=None)
        # Uniform census: all ages have population 100
        rows = ",\n            ".join(
            f"(2022, '75', '11', '7599', '75101', '751010101', {age}, 'male', 100.0)"
            for age in range(0, 30)
        )
        processor.conn.execute(f"""
            CREATE OR REPLACE TABLE population AS
            SELECT * FROM (VALUES
                {rows}
            ) AS t(year, department_code, region_code,
                   canton_code, commune_code, iris_code,
                   age, sex, population)
        """)

        processor.conn.execute("""
            CREATE OR REPLACE TABLE quinquennal AS
            SELECT * FROM (VALUES
                (2039, '75', 'male', '15_19', 5000.0)
            ) AS t(year, department_code, sex, age_band, population)
        """)

        compute_age_ratios(processor.conn, census_year=2022)

        # Each of 5 ages should have ratio ~0.2
        ratios = processor.conn.execute("""
            SELECT age, age_ratio FROM age_ratios
            WHERE year = 2039 AND department_code = '75'
              AND sex = 'male' AND age_band = '15_19'
            ORDER BY age
        """).df()

        for _, row in ratios.iterrows():
            assert abs(row["age_ratio"] - 0.2) < 0.05, (
                f"Age {row['age']}: ratio {row['age_ratio']:.4f} should be ~0.2"
            )


# -----------------------------------------------------------------------------
# Test: Quinquennal Pass-Through
# -----------------------------------------------------------------------------


class TestQuinquennalPassThrough:
    """Tests that quinquennal values pass through unchanged to projection."""

    def test_quinquennal_values_preserved(self):
        """Quinquennal band totals should be used as-is (no census replacement).

        The age_ratios sum to 1 within each band, so department-level
        yearly totals should equal quinquennal values exactly.
        """
        from passculture.data.insee_population import sql
        from passculture.data.insee_population.projections import (
            compute_age_ratios,
            project_multi_year,
        )

        processor = PopulationProcessor(
            year=2022,
            min_age=15,
            max_age=19,
            start_year=2022,
            end_year=2022,
            cache_dir=None,
        )

        # Census with known age distribution
        rows = ",\n            ".join(
            f"(2022, '75', '11', '7599', '75101', '751010101',"
            f" {age}, 'male', {100.0 + age * 2})"
            for age in range(0, 25)
        )
        processor.conn.execute(f"""
            CREATE OR REPLACE TABLE population AS
            SELECT * FROM (VALUES
                {rows}
            ) AS t(year, department_code, region_code,
                   canton_code, commune_code, iris_code,
                   age, sex, population)
        """)
        processor._base_table_created = True

        # Quinquennal with a specific value for 15_19
        quinquennal_pop = 5000.0
        processor.conn.execute(f"""
            CREATE OR REPLACE TABLE quinquennal AS
            SELECT * FROM (VALUES
                (2022, '75', 'male', '15_19', {quinquennal_pop})
            ) AS t(year, department_code, sex, age_band, population)
        """)

        compute_age_ratios(processor.conn, census_year=2022)

        # Monthly births (uniform)
        monthly_df = pd.DataFrame(
            [
                {"department_code": "75", "month": m, "month_ratio": 1.0 / 12}
                for m in range(1, 13)
            ]
        )
        processor._register_dataframe("monthly_births_df", monthly_df)
        processor._execute(sql.REGISTER_MONTHLY_BIRTHS)

        # Need geo ratios for project_multi_year but only checking department
        _setup_geo_mappings(processor)
        from passculture.data.insee_population.projections import compute_geo_ratios

        compute_geo_ratios(processor.conn, "epci")
        compute_geo_ratios(processor.conn, "canton")
        compute_geo_ratios(processor.conn, "iris")

        project_multi_year(processor.conn, 15, 19)

        # Sum population across months for year=2022, dept=75, male, ages 15-19
        total = processor.conn.execute("""
            SELECT SUM(population)
            FROM population_department
            WHERE year = 2022 AND department_code = '75' AND sex = 'male'
        """).fetchone()[0]

        # Should equal quinquennal value (age_ratios sum to 1, month_ratios sum to 1)
        assert abs(total - quinquennal_pop) < 1.0, (
            f"Department total ({total:.1f}) should equal "
            f"quinquennal ({quinquennal_pop:.1f})"
        )


# -----------------------------------------------------------------------------
# Test: IRIS Student Mobility Correction
# -----------------------------------------------------------------------------


class TestIRISStudentMobilityCorrection:
    """Tests for MOBSCO student mobility correction on IRIS geo ratios."""

    @pytest.fixture
    def iris_mobility_processor(self, tmp_path):
        """Create a processor with population, geo mappings, geo_ratios_iris,
        and a mock MOBSCO parquet file for testing IRIS student mobility correction.

        Setup:
        - Dept 75 has 2 IRIS: 751010101 (commune 75101) and 751020101 (commune 75102)
        - Dept 13 has 1 IRIS: 130010101 (commune 13001)
        - MOBSCO: students from dept 75 study in commune 13001 and 75102
        """
        import duckdb as _duckdb

        processor = PopulationProcessor(
            year=2022, min_age=15, max_age=24, cache_dir=None
        )

        # Population table
        rows = []
        for dept, commune, iris, region, canton in [
            ("75", "75101", "751010101", "11", "7599"),
            ("75", "75102", "751020101", "11", "7599"),
            ("13", "13001", "130010101", "93", "1301"),
        ]:
            for age in range(15, 25):
                for sex in ["male", "female"]:
                    pop = 100.0
                    rows.append(
                        f"(2022, '{dept}', '{region}', '{canton}', "
                        f"'{commune}', '{iris}', {age}, '{sex}', {pop})"
                    )
        values = ",\n            ".join(rows)
        processor.conn.execute(f"""
            CREATE OR REPLACE TABLE population AS
            SELECT * FROM (VALUES
                {values}
            ) AS t(year, department_code, region_code,
                   canton_code, commune_code, iris_code,
                   age, sex, population)
        """)
        processor._base_table_created = True

        # Commune-EPCI mapping
        commune_epci = pd.DataFrame(
            {
                "commune_code": ["75101", "75102", "13001"],
                "epci_code": ["200054781", "200054782", "200054807"],
                "commune_name": ["Paris 1er", "Paris 2e", "Marseille"],
                "department_code": ["75", "75", "13"],
                "commune_population": [10000, 8000, 50000],
            }
        )
        processor._register_dataframe("commune_epci_df", commune_epci)
        processor._execute(
            "CREATE OR REPLACE TABLE commune_epci AS SELECT * FROM commune_epci_df"
        )
        processor._geo_mappings_loaded = True

        # Compute geo_ratios_iris from population
        from passculture.data.insee_population.projections import compute_geo_ratios

        compute_geo_ratios(processor.conn, "iris")

        # Create mock MOBSCO parquet
        mobsco_data = {
            "COMMUNE": ["75101"] * 4 + ["13001"] * 2,
            "DCETUF": ["13001", "13001", "75102", "75102", "13001", "13001"],
            "AGEREV10": ["18"] * 6,
            "SEXE": ["1", "2", "1", "2", "1", "2"],
            "IPONDI": ["50.0", "50.0", "30.0", "30.0", "80.0", "80.0"],
        }
        # Non-student rows (filtered out)
        mobsco_data["COMMUNE"].extend(["75101", "75101"])
        mobsco_data["DCETUF"].extend(["13001", "13001"])
        mobsco_data["AGEREV10"].extend(["25", "30"])
        mobsco_data["SEXE"].extend(["1", "2"])
        mobsco_data["IPONDI"].extend(["100.0", "100.0"])

        mobsco_df = pd.DataFrame(mobsco_data)  # noqa: F841
        mobsco_path = tmp_path / "mobsco_test.parquet"
        _duckdb.sql("SELECT * FROM mobsco_df").write_parquet(str(mobsco_path))

        # Compute per-department mobility weights (needed before corrections)
        from passculture.data.insee_population.projections import (
            compute_department_mobility_rates,
        )

        compute_department_mobility_rates(processor.conn, mobsco_path)

        return processor, mobsco_path

    def test_iris_student_flows_computed(self, iris_mobility_processor):
        """Test that student_flows_iris table has expected IRIS codes."""
        processor, mobsco_path = iris_mobility_processor
        from passculture.data.insee_population import sql

        processor.conn.execute(sql.RENAME_GEO_RATIOS_IRIS_TO_BASE)
        processor.conn.execute(
            sql.CREATE_STUDENT_FLOWS_IRIS.format(mobsco_path=mobsco_path)
        )

        flows = processor.conn.execute(
            "SELECT * FROM student_flows_iris ORDER BY department_code, iris_code, sex"
        ).df()

        # Should have IRIS codes from the study communes
        iris_codes = flows["iris_code"].unique().tolist()
        assert "751020101" in iris_codes, (
            f"Expected 751020101 in flows, got {iris_codes}"
        )
        assert "130010101" in iris_codes, (
            f"Expected 130010101 in flows, got {iris_codes}"
        )

        # Ratios per dept/sex should sum to ~1
        for (dept, sex), group in flows.groupby(["department_code", "sex"]):
            ratio_sum = group["study_geo_ratio"].sum()
            assert abs(ratio_sum - 1.0) < 0.001, (
                f"IRIS flows for {dept}/{sex} sum to {ratio_sum}"
            )

        # Cleanup
        processor.conn.execute("DROP TABLE geo_ratios_iris_base")
        processor.conn.execute("DROP TABLE student_flows_iris")

    def test_iris_correction_shifts_weight(self, iris_mobility_processor):
        """Test that correction shifts geo_ratio weight for student bands."""
        processor, mobsco_path = iris_mobility_processor
        from passculture.data.insee_population.projections import (
            apply_student_mobility_correction_iris,
        )

        # Record baseline for student bands
        base_ratios = processor.conn.execute("""
            SELECT iris_code, age_band, sex, geo_ratio
            FROM geo_ratios_iris
            WHERE department_code = '75' AND age_band IN ('15_19', '20_24')
            ORDER BY iris_code, age_band, sex
        """).df()

        apply_student_mobility_correction_iris(processor.conn, mobsco_path)

        corrected_ratios = processor.conn.execute("""
            SELECT iris_code, age_band, sex, geo_ratio
            FROM geo_ratios_iris
            WHERE department_code = '75' AND age_band IN ('15_19', '20_24')
            ORDER BY iris_code, age_band, sex
        """).df()

        # IRIS 751020101 (commune 75102) should increase for student bands
        # because MOBSCO says students study in commune 75102
        for band in ["15_19", "20_24"]:
            base_val = base_ratios[
                (base_ratios["iris_code"] == "751020101")
                & (base_ratios["age_band"] == band)
                & (base_ratios["sex"] == "male")
            ]["geo_ratio"].values[0]

            corrected_val = corrected_ratios[
                (corrected_ratios["iris_code"] == "751020101")
                & (corrected_ratios["age_band"] == band)
                & (corrected_ratios["sex"] == "male")
            ]["geo_ratio"].values[0]

            assert corrected_val > base_val, (
                f"IRIS 751020101 geo_ratio for {band}/male should increase: "
                f"base={base_val:.4f}, corrected={corrected_val:.4f}"
            )

    def test_iris_ratios_still_sum_to_one(self, iris_mobility_processor):
        """After correction, geo_ratios_iris per (dept, band, sex) sum to ~1.0."""
        processor, mobsco_path = iris_mobility_processor
        from passculture.data.insee_population.projections import (
            apply_student_mobility_correction_iris,
        )

        apply_student_mobility_correction_iris(processor.conn, mobsco_path)

        ratio_sums = processor.conn.execute("""
            SELECT department_code, age_band, sex, SUM(geo_ratio) AS ratio_sum
            FROM geo_ratios_iris
            GROUP BY department_code, age_band, sex
        """).df()

        violations = ratio_sums[
            (ratio_sums["ratio_sum"] < 0.999) | (ratio_sums["ratio_sum"] > 1.001)
        ]
        assert len(violations) == 0, (
            f"IRIS geo ratios don't sum to ~1.0 after correction:\n{violations}"
        )

    def test_iris_non_student_bands_unchanged(self, iris_mobility_processor):
        """Non-student bands should be unchanged after IRIS correction."""
        processor, mobsco_path = iris_mobility_processor
        from passculture.data.insee_population.projections import (
            apply_student_mobility_correction_iris,
        )

        base_other = processor.conn.execute("""
            SELECT department_code, iris_code, age_band, sex, geo_ratio
            FROM geo_ratios_iris
            WHERE age_band NOT IN ('15_19', '20_24')
            ORDER BY department_code, iris_code, age_band, sex
        """).df()

        apply_student_mobility_correction_iris(processor.conn, mobsco_path)

        corrected_other = processor.conn.execute("""
            SELECT department_code, iris_code, age_band, sex, geo_ratio
            FROM geo_ratios_iris
            WHERE age_band NOT IN ('15_19', '20_24')
            ORDER BY department_code, iris_code, age_band, sex
        """).df()

        assert len(base_other) == len(corrected_other), (
            f"Row count changed: {len(base_other)} -> {len(corrected_other)}"
        )
        pd.testing.assert_frame_equal(
            base_other.reset_index(drop=True),
            corrected_other.reset_index(drop=True),
        )

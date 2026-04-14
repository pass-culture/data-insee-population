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
    return PopulationProcessor(
        year=2022,
        start_year=2022,
        end_year=2022,
        cache_dir=None,
    )


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
        processor = PopulationProcessor(start_year=2022, end_year=2022)
        assert processor.year == 2022
        assert processor.min_age == 0
        assert processor.max_age == 120
        assert processor.include_dom is True
        assert processor.include_com is True
        assert processor.include_mayotte is True
        assert processor.start_year == 2022
        assert processor.end_year == 2022

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
        """Test that end_year beyond max valid projection raises ValueError."""
        # census 2022 + min_age 15 = 2037
        with pytest.raises(ValueError, match="exceeds maximum valid projection"):
            PopulationProcessor(
                year=2022,
                min_age=15,
                max_age=24,
                start_year=2015,
                end_year=2038,
                cache_dir=None,
            )

    def test_accepts_end_year_at_forecast_limit(self):
        """Test that end_year exactly at the limit is accepted."""
        # census 2022 + min_age 15 = 2037
        processor = PopulationProcessor(
            year=2022,
            min_age=15,
            max_age=24,
            start_year=2015,
            end_year=2037,
            cache_dir=None,
        )
        assert processor.end_year == 2037


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
        processor = PopulationProcessor(
            include_dom=False, start_year=2022, end_year=2022, cache_dir=None
        )
        assert processor.include_dom is False

    def test_com_filtering_flag(self):
        """Test COM filtering option is stored correctly."""
        processor = PopulationProcessor(
            include_com=False, start_year=2022, end_year=2022, cache_dir=None
        )
        assert processor.include_com is False

    def test_mayotte_option_flag(self):
        """Test Mayotte option is stored correctly."""
        processor = PopulationProcessor(
            include_mayotte=True, start_year=2022, end_year=2022, cache_dir=None
        )
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
        """Set up all required tables for projection (simple aging mode)."""
        from passculture.data.insee_population import sql
        from passculture.data.insee_population.projections import compute_geo_ratios

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
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
        )

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
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
        )

        result = projection_processor.to_pandas("department")
        # Yearly mode: only month=1 (January snapshot)
        assert sorted(result["month"].unique()) == [1]
        # Birth month should have all 12 values
        assert sorted(result["birth_month"].unique()) == list(range(1, 13))

    def test_projected_department_has_multiple_years(self, projection_processor):
        """Test projected data spans start_year to end_year."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
        )

        result = projection_processor.to_pandas("department")
        years = sorted(result["year"].unique())
        assert 2022 in years
        assert 2023 in years

    def test_projected_decimal_age(self, projection_processor):
        """Test decimal_age varies with birth_month in yearly mode."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
        )

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
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
        )

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
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
        )

        for level in ["department", "epci", "iris"]:
            result = projection_processor.to_pandas(level)
            assert (result["population"] > 0).all(), f"Negative population in {level}"

    def test_projected_epci_has_epci_code(self, projection_processor):
        """Test EPCI projection includes epci_code."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
        )

        result = projection_processor.to_pandas("epci")
        assert "epci_code" in result.columns
        assert result["epci_code"].notna().all()

    def test_projected_iris_has_iris_code(self, projection_processor):
        """Test IRIS projection includes iris_code."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
        )

        result = projection_processor.to_pandas("iris")
        assert "iris_code" in result.columns
        assert result["iris_code"].notna().all()

    def test_population_sums_consistent(self, projection_processor):
        """Test total population at EPCI/IRIS ≤ department (ratios sum to ≤1)."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
        )

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
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
            monthly=True,
        )

        result = projection_processor.to_pandas("department")
        months = sorted(result["month"].unique())
        assert months == list(range(1, 13))
        # birth_month should also be present
        birth_months = sorted(result["birth_month"].unique())
        assert birth_months == list(range(1, 13))

    def test_birth_month_population_sum_preserved(self, projection_processor):
        """Sum of department population should equal census total for same year."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
        )

        # Department total for census year should equal census population
        dept_total = projection_processor.conn.execute("""
            SELECT SUM(population)
            FROM population_department
            WHERE year = 2022 AND department_code = '75' AND sex = 'male'
        """).fetchone()[0]

        census_total = float(
            projection_processor.conn.execute("""
            SELECT SUM(population)
            FROM population
            WHERE department_code = '75' AND sex = 'male'
              AND age BETWEEN 15 AND 20
        """).fetchone()[0]
        )

        assert abs(dept_total - census_total) < 1.0, (
            f"Department total ({dept_total:.1f}) should equal "
            f"census ({census_total:.1f})"
        )

    def test_birth_month_column_always_present(self, projection_processor):
        """birth_month column exists in all geographic levels."""
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
        )

        for level in ["department", "epci", "canton", "iris"]:
            result = projection_processor.to_pandas(level)
            assert "birth_month" in result.columns, f"birth_month missing from {level}"

    def test_yearly_exported_population_equals_census(self, projection_processor):
        """In yearly mode, SUM(exported pop) must equal census input.

        The birth-month expansion (x12 rows) uses month_ratio which sums to 1,
        so total population is preserved.
        """
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
        )

        exported = projection_processor.to_pandas("department")
        # Sum across all birth_months for year=2022, dept=75, male
        total = exported[
            (exported["year"] == 2022)
            & (exported["department_code"] == "75")
            & (exported["sex"] == "male")
        ]["population"].sum()

        census_total = float(
            projection_processor.conn.execute("""
            SELECT SUM(population) FROM population
            WHERE department_code = '75' AND sex = 'male'
              AND age BETWEEN 15 AND 20
        """).fetchone()[0]
        )

        assert abs(total - census_total) < 1.0, (
            f"Exported population ({total:.1f}) != census ({census_total:.1f})"
        )

    def test_monthly_snapshot_population_is_full_stock(self, projection_processor):
        """In monthly mode, each snapshot month's total must equal the annual stock.

        Population is a stock variable — the number of 18-year-olds in January
        is the same as in July. month_ratio only splits birth-month sub-cohorts,
        it must NOT reduce the snapshot total.
        """
        from passculture.data.insee_population.projections import project_multi_year

        self._setup_projection_tables(projection_processor)
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
            monthly=True,
        )

        exported = projection_processor.to_pandas("department")

        census_total = float(
            projection_processor.conn.execute("""
            SELECT SUM(population) FROM population
            WHERE department_code = '75' AND sex = 'male'
              AND age BETWEEN 15 AND 20
        """).fetchone()[0]
        )

        # Check EACH snapshot month individually
        for month in range(1, 13):
            month_total = exported[
                (exported["year"] == 2022)
                & (exported["month"] == month)
                & (exported["department_code"] == "75")
                & (exported["sex"] == "male")
            ]["population"].sum()

            assert abs(month_total - census_total) < 1.0, (
                f"Month {month}: exported pop ({month_total:.1f}) != "
                f"census stock ({census_total:.1f}). "
                f"Ratio: {month_total / census_total:.4f}"
            )

    def test_monthly_and_yearly_same_per_snapshot_total(self, projection_processor):
        """Monthly and yearly modes must produce the same total per snapshot month."""
        from passculture.data.insee_population.projections import project_multi_year

        # Yearly mode
        self._setup_projection_tables(projection_processor)
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
            monthly=False,
        )
        yearly_exported = projection_processor.to_pandas("department")
        yearly_jan = yearly_exported[
            (yearly_exported["year"] == 2022)
            & (yearly_exported["month"] == 1)
            & (yearly_exported["department_code"] == "75")
            & (yearly_exported["sex"] == "male")
        ]["population"].sum()

        # Monthly mode (re-run projection)
        project_multi_year(
            projection_processor.conn,
            15,
            20,
            start_year=2022,
            end_year=2023,
            monthly=True,
        )
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
# Test: Simple Aging Projection
# -----------------------------------------------------------------------------


class TestSimpleAging:
    """Tests for simple census aging projection."""

    def test_census_year_population_preserved(self):
        """For census year, projected population must equal census population."""
        from passculture.data.insee_population import sql
        from passculture.data.insee_population.projections import (
            compute_geo_ratios,
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
        processor._base_table_created = True
        _setup_geo_mappings(processor)

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
        project_multi_year(processor.conn, 15, 19, start_year=2022, end_year=2022)

        # Department total should equal census total
        total = processor.conn.execute("""
            SELECT SUM(population) FROM population_department
            WHERE year = 2022 AND department_code = '75' AND sex = 'male'
        """).fetchone()[0]
        assert abs(total - 600.0) < 1.0, (
            f"Department total ({total:.1f}) should equal census (600.0)"
        )

    def test_cohort_aging_forward(self):
        """Census age 15 in 2022 should become age 18 in 2025."""
        from passculture.data.insee_population import sql
        from passculture.data.insee_population.projections import (
            compute_geo_ratios,
            project_multi_year,
        )

        processor = PopulationProcessor(
            year=2022,
            min_age=15,
            max_age=20,
            start_year=2022,
            end_year=2025,
            cache_dir=None,
        )
        # Census: age 15 has 500, age 16 has 300
        processor.conn.execute("""
            CREATE OR REPLACE TABLE population AS
            SELECT * FROM (VALUES
                (2022, '75', '11', '7599', '75101', '751010101', 12, 'male', 500.0),
                (2022, '75', '11', '7599', '75101', '751010101', 13, 'male', 300.0),
                (2022, '75', '11', '7599', '75101', '751010101', 15, 'male', 200.0),
                (2022, '75', '11', '7599', '75101', '751010101', 16, 'male', 250.0),
                (2022, '75', '11', '7599', '75101', '751010101', 17, 'male', 180.0)
            ) AS t(year, department_code, region_code,
                   canton_code, commune_code, iris_code,
                   age, sex, population)
        """)
        processor._base_table_created = True
        _setup_geo_mappings(processor)

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
        project_multi_year(processor.conn, 15, 20, start_year=2022, end_year=2025)

        # In 2025, age 15 = census age 12 (=500), age 16 = census age 13 (=300)
        pop_15_2025 = processor.conn.execute("""
            SELECT population FROM population_department
            WHERE year = 2025 AND age = 15 AND department_code = '75' AND sex = 'male'
        """).fetchone()[0]
        assert abs(pop_15_2025 - 500.0) < 0.1, (
            f"2025 age 15 ({pop_15_2025}) should equal census age 12 (500.0)"
        )

        pop_16_2025 = processor.conn.execute("""
            SELECT population FROM population_department
            WHERE year = 2025 AND age = 16 AND department_code = '75' AND sex = 'male'
        """).fetchone()[0]
        assert abs(pop_16_2025 - 300.0) < 0.1, (
            f"2025 age 16 ({pop_16_2025}) should equal census age 13 (300.0)"
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

        assert _MALE_OFFSET == 23
        assert _FEMALE_OFFSET == 44
        assert callable(_parse_quinquennal_sheet)

    def test_month_name_mapping(self):
        """Test French month name mapping."""
        from passculture.data.insee_population.downloaders import _MONTH_NAMES

        assert _MONTH_NAMES["janvier"] == 1
        assert _MONTH_NAMES["décembre"] == 12
        assert len(_MONTH_NAMES) == 12

    def test_parse_n4d_birth_csv(self):
        """Test N4D CSV parser extracts department codes and monthly ratios."""
        from passculture.data.insee_population.downloaders import _parse_n4d_birth_csv

        csv = "\n".join(
            [
                "REGDEP_DOMI_MERE;MNAIS;NBNAIS",
                "1175;01;1200",
                "1175;02;900",
                "1175;AN;2100",
                "2418;01;100",
                "2418;02;200",
                "2418;AN;300",
                "971;01;50",
                "971;02;50",
                "971;AN;100",
                "11XX;01;9999",
                "97XX;01;9999",
            ]
        )
        result = _parse_n4d_birth_csv(csv)

        depts = set(result["department_code"])
        assert "75" in depts
        assert "18" in depts
        assert "971" in depts

        for dept, grp in result.groupby("department_code"):
            assert abs(grp["month_ratio"].sum() - 1.0) < 1e-9, (
                f"{dept} ratios don't sum to 1"
            )

        paris = result[result["department_code"] == "75"].set_index("month")
        assert abs(paris.loc[1, "month_ratio"] - 1200 / 2100) < 1e-9


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

        result = download_quinquennal_estimates(2022, 2028, cache_dir)

        years = sorted(result["year"].unique())
        assert min(years) == 2022
        assert max(years) == 2028
        assert len(years) == 7

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

        young_pct = sum(dist[a] for a in range(0, 5))
        assert young_pct > 0.4

    def test_returns_empty_when_no_976(self):
        """Returns empty dict when 976 data is absent."""
        from unittest.mock import patch

        from passculture.data.insee_population.constants import AGE_BUCKETS
        from passculture.data.insee_population.downloaders import (
            _get_dom_age_distribution,
        )

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


# -----------------------------------------------------------------------------
# Test: Student Mobility Correction
# -----------------------------------------------------------------------------


class TestStudentMobilityCorrection:
    """Tests for MOBSCO student mobility correction on EPCI geo ratios."""

    def test_correct_student_mobility_flag_stored(self):
        """Test that correct_student_mobility flag is stored on processor."""
        proc = PopulationProcessor(
            correct_student_mobility=True,
            min_age=15,
            max_age=24,
            start_year=2022,
            end_year=2037,
            cache_dir=None,
        )
        assert proc.correct_student_mobility is True

    @pytest.fixture
    def mobility_processor(self, tmp_path):
        """Create a processor with population, geo mappings, geo_ratios_epci,
        and a mock MOBSCO parquet file for testing student mobility correction.
        """
        import duckdb as _duckdb

        processor = PopulationProcessor(
            year=2022,
            min_age=15,
            max_age=24,
            start_year=2022,
            end_year=2037,
            cache_dir=None,
        )

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

        from passculture.data.insee_population.projections import compute_geo_ratios

        compute_geo_ratios(processor.conn, "epci")

        mobsco_data = {
            "COMMUNE": ["75101"] * 4 + ["13001"] * 2,
            "DCETUF": ["13001", "13001", "75102", "75102", "13001", "13001"],
            "AGEREV10": ["18"] * 6,
            "SEXE": ["1", "2", "1", "2", "1", "2"],
            "IPONDI": ["50.0", "50.0", "30.0", "30.0", "80.0", "80.0"],
        }
        mobsco_data["COMMUNE"].extend(["75101", "75101", "13001", "13001"])
        mobsco_data["DCETUF"].extend(["75102", "75102", "13001", "13001"])
        mobsco_data["AGEREV10"].extend(["15", "15", "15", "15"])
        mobsco_data["SEXE"].extend(["1", "2", "1", "2"])
        mobsco_data["IPONDI"].extend(["40.0", "40.0", "60.0", "60.0"])
        mobsco_data["COMMUNE"].extend(["75101", "75101"])
        mobsco_data["DCETUF"].extend(["13001", "13001"])
        mobsco_data["AGEREV10"].extend(["25", "30"])
        mobsco_data["SEXE"].extend(["1", "2"])
        mobsco_data["IPONDI"].extend(["100.0", "100.0"])

        mobsco_df = pd.DataFrame(mobsco_data)  # noqa: F841
        mobsco_path = tmp_path / "mobsco_test.parquet"
        _duckdb.sql("SELECT * FROM mobsco_df").write_parquet(str(mobsco_path))

        from passculture.data.insee_population.projections import (
            compute_department_mobility_rates,
        )

        compute_department_mobility_rates(processor.conn, mobsco_path)

        return processor, mobsco_path

    def test_student_flows_computed(self, mobility_processor):
        """Test that student_flows_epci table is created with expected rows."""
        processor, mobsco_path = mobility_processor
        from passculture.data.insee_population import sql
        from passculture.data.insee_population.projections import _build_band_config_sql

        processor.conn.execute(sql.RENAME_GEO_RATIOS_EPCI_TO_BASE)
        processor.conn.execute(
            sql.CREATE_STUDENT_FLOWS_EPCI.format(
                mobsco_path=mobsco_path,
                band_config_sql=_build_band_config_sql(),
            )
        )

        flows = processor.conn.execute(
            "SELECT * FROM student_flows_epci"
            " ORDER BY age_band, department_code, epci_code, sex"
        ).df()

        assert "age_band" in flows.columns
        assert len(flows[flows["department_code"] == "75"]) > 0
        assert len(flows[flows["department_code"] == "13"]) > 0

        for (_dept, _band, _sex), group in flows.groupby(
            ["department_code", "age_band", "sex"]
        ):
            ratio_sum = group["study_geo_ratio"].sum()
            assert abs(ratio_sum - 1.0) < 0.001

        processor.conn.execute("DROP TABLE geo_ratios_epci_base")
        processor.conn.execute("DROP TABLE student_flows_epci")

    def test_correction_shifts_weight(self, mobility_processor):
        """Test that correction shifts geo_ratio for student bands."""
        processor, mobsco_path = mobility_processor
        from passculture.data.insee_population.projections import (
            apply_student_mobility_correction,
        )

        base_ratios_75 = processor.conn.execute("""
            SELECT epci_code, age_band, sex, geo_ratio
            FROM geo_ratios_epci
            WHERE department_code = '75' AND age_band IN ('15_19', '20_24')
            ORDER BY epci_code, age_band, sex
        """).df()

        apply_student_mobility_correction(processor.conn, mobsco_path)

        corrected_ratios_75 = processor.conn.execute("""
            SELECT epci_code, age_band, sex, geo_ratio
            FROM geo_ratios_epci
            WHERE department_code = '75' AND age_band IN ('15_19', '20_24')
            ORDER BY epci_code, age_band, sex
        """).df()

        def get_ratio(df, epci, band, sex):
            return df[
                (df["epci_code"] == epci)
                & (df["age_band"] == band)
                & (df["sex"] == sex)
            ]["geo_ratio"].values[0]

        assert get_ratio(corrected_ratios_75, "200054782", "20_24", "male") > get_ratio(
            base_ratios_75, "200054782", "20_24", "male"
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
        assert len(violations) == 0

    def test_non_student_bands_unchanged(self, mobility_processor):
        """Bands other than 15_19/20_24 should be unchanged after correction."""
        processor, mobsco_path = mobility_processor
        from passculture.data.insee_population.projections import (
            apply_student_mobility_correction,
        )

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

        assert len(base_other) == len(corrected_other)
        pd.testing.assert_frame_equal(
            base_other.reset_index(drop=True),
            corrected_other.reset_index(drop=True),
        )


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
            year=2022,
            min_age=15,
            max_age=24,
            start_year=2022,
            end_year=2037,
            cache_dir=None,
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
        from passculture.data.insee_population.projections import _build_band_config_sql

        processor.conn.execute(sql.RENAME_GEO_RATIOS_IRIS_TO_BASE)
        processor.conn.execute(
            sql.CREATE_STUDENT_FLOWS_IRIS.format(
                mobsco_path=mobsco_path,
                band_config_sql=_build_band_config_sql(),
            )
        )

        flows = processor.conn.execute(
            "SELECT * FROM student_flows_iris"
            " ORDER BY age_band, department_code, iris_code, sex"
        ).df()

        # Flows must have age_band column
        assert "age_band" in flows.columns

        # Should have IRIS codes from the study communes
        iris_codes = flows["iris_code"].unique().tolist()
        assert "751020101" in iris_codes, (
            f"Expected 751020101 in flows, got {iris_codes}"
        )
        assert "130010101" in iris_codes, (
            f"Expected 130010101 in flows, got {iris_codes}"
        )

        # iris_dept column must be present (used by blended_raw to filter intra-dept
        # flows)
        assert "iris_dept" in flows.columns, (
            "student_flows_iris must have iris_dept column"
        )

        # Ratios per (dept, age_band, sex) should sum to ~1
        for (dept, band, sex), group in flows.groupby(
            ["department_code", "age_band", "sex"]
        ):
            ratio_sum = group["study_geo_ratio"].sum()
            assert abs(ratio_sum - 1.0) < 0.001, (
                f"IRIS flows for {dept}/{band}/{sex} sum to {ratio_sum}"
            )

        # Cleanup
        processor.conn.execute("DROP TABLE geo_ratios_iris_base")
        processor.conn.execute("DROP TABLE student_flows_iris")

    def test_iris_correction_shifts_weight(self, iris_mobility_processor):
        """Test that correction shifts geo_ratio weight for student bands.

        Fixture: dept 75 students (AGEREV10='18', 20_24 band) go to:
        - Marseille (dept 13, commune 13001): 62.5% of total flows (cross-dept)
        - Local (commune 75102, IRIS 751020101): 37.5% of total flows (intra-dept)

        Intra-dept fraction p = 0.375, blend_weight w = 0.6, effective w*p = 0.225.
        Expected ratios (both sum to 1.0):
        - 751010101: (1-0.225)*0.5 + 0 = 0.3875  (no intra-dept study destination)
        - 751020101: (1-0.225)*0.5 + 0.6*0.375 = 0.6125  (intra-dept study destination)
        """
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

        def get_ratio(df: object, iris: str, band: str, sex: str) -> float:
            return df[
                (df["iris_code"] == iris)
                & (df["age_band"] == band)
                & (df["sex"] == sex)
            ]["geo_ratio"].values[0]

        # 20_24: IRIS 751020101 is an intra-dept study destination → gains ratio
        assert get_ratio(corrected_ratios, "751020101", "20_24", "male") > get_ratio(
            base_ratios, "751020101", "20_24", "male"
        ), "20_24: IRIS 751020101 should gain ratio (intra-dept study destination)"

        # 751010101 has no intra-dept study flows → loses ratio
        assert get_ratio(corrected_ratios, "751010101", "20_24", "male") < get_ratio(
            base_ratios, "751010101", "20_24", "male"
        ), "20_24: IRIS 751010101 should lose ratio (no intra-dept study destination)"

        # 15_19: fixture has no AGEREV10='15' rows → primary is empty.
        # Secondary (AGEREV10='18') flows renorm to full distribution.
        # blend_weight = default 0.10 (no primary data); intra_frac = 0.375.
        # Effective w*p = 0.0375 → small shift same direction as 20_24.
        assert get_ratio(corrected_ratios, "751020101", "15_19", "male") > get_ratio(
            base_ratios, "751020101", "15_19", "male"
        ), "15_19: 751020101 should gain (intra-dept study destination via secondary)"
        assert get_ratio(corrected_ratios, "751010101", "15_19", "male") < get_ratio(
            base_ratios, "751010101", "15_19", "male"
        ), "15_19: 751010101 should lose (cross-dept renorm effect via secondary)"

    def test_iris_intra_dept_effective_weight(self, iris_mobility_processor):
        """Effective blend weight scales by intra-dept fraction to prevent IRIS
        inflation.

        When most students leave for another department (cross-dept), the effective
        correction weight w*p is smaller than the raw blend_weight w. This prevents
        local IRIS ratios from being inflated by the renormalization step.

        In the fixture: p=0.375, w=0.6 → effective w*p=0.225.
        - 751010101: (1-0.225)*0.5 = 0.3875  (approx)
        - 751020101: (1-0.225)*0.5 + 0.6*0.375 = 0.6125  (approx)
        - Sum = 1.0 (preserved, no inflation)
        """
        processor, mobsco_path = iris_mobility_processor
        from passculture.data.insee_population.projections import (
            apply_student_mobility_correction_iris,
        )

        apply_student_mobility_correction_iris(processor.conn, mobsco_path)

        corrected = processor.conn.execute("""
            SELECT iris_code, geo_ratio
            FROM geo_ratios_iris
            WHERE department_code = '75' AND age_band = '20_24' AND sex = 'male'
            ORDER BY iris_code
        """).df()

        def ratio(iris_code: str) -> float:
            return corrected[corrected["iris_code"] == iris_code]["geo_ratio"].values[0]

        # p=0.375, w=0.6, w*p=0.225; base ratio = 0.5 for both IRIS
        assert ratio("751010101") == pytest.approx(0.3875, abs=0.001), (
            "IRIS 751010101: expected (1-0.225)*0.5 = 0.3875"
        )
        assert ratio("751020101") == pytest.approx(0.6125, abs=0.001), (
            "IRIS 751020101: expected (1-0.225)*0.5 + 0.6*0.375 = 0.6125"
        )
        # Sum must be 1.0 (no inflation from cross-dept flows)
        total = corrected["geo_ratio"].sum()
        assert abs(total - 1.0) < 0.001, (
            f"Dept 75 20_24 male IRIS ratios must sum to 1.0, got {total}"
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

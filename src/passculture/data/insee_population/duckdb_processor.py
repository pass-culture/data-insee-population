"""DuckDB-based processor for INSEE population data.

Creates multi-level population tables at department, EPCI, canton, and IRIS
levels with geo_precision indicators for data reliability.

Two dept-level methods are supported (selected via ``method``):

* ``cohort-stable`` (default): national cohort size times age-specific
  census dept share. Age-specific dept distribution is frozen at the
  census pattern and applied afresh each year.
* ``cohort-aging`` (legacy): census cohort aged in place — the 2022
  population at age A in dept D becomes the year-Y population at age
  A+(Y-2022) in dept D.

Both methods preserve national cohort totals (no mortality, no net
migration). Geographic sub-department ratios and monthly birth
distribution are applied on top identically for either method.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pandas as pd
from loguru import logger

from passculture.data.insee_population import sql
from passculture.data.insee_population.constants import (
    DEPARTMENTS_DOM,
    DEPARTMENTS_METRO,
    DEPARTMENTS_TOM,
    IRIS_SENTINEL_NO_GEO,
    MAX_AGE,
)
from passculture.data.insee_population.downloaders import (
    download_indcvi,
    download_mnai_birth_distribution,
    download_mobsco,
    synthesize_mayotte_population,
    synthesize_tom_population,
)
from passculture.data.insee_population.geo_mappings import get_geo_mappings
from passculture.data.insee_population.projections import (
    ProjectionMethod,
    apply_student_mobility_correction,
    apply_student_mobility_correction_iris,
    compute_department_mobility_rates,
    compute_geo_ratios,
    project_multi_year,
)

if TYPE_CHECKING:
    from typing import Any

# Geo columns per level — used by SELECT_WITH_BIRTH_MONTH to expand
# compact tables with birth_month on-the-fly at read/export time.
_GEO_COLUMNS = {
    "department": "pd.department_code, pd.region_code,",
    "epci": "pd.department_code, pd.region_code, pd.epci_code,",
    "canton": "pd.department_code, pd.region_code, pd.canton_code,",
    "iris": (
        "pd.department_code, pd.region_code, pd.epci_code,"
        " pd.commune_code, pd.iris_code,"
    ),
}


class PopulationProcessor:
    """DuckDB-based INSEE population processor.

    Creates three output tables at different geographic levels with monthly
    granularity:
    - population_department: 100% coverage, aggregated by department
    - population_epci: 100% coverage
    - population_iris: 100% pop coverage; ~60% has sub-commune spatial resolution

    Example:
        processor = PopulationProcessor(year=2022, min_age=15, max_age=24,
                                        start_year=2015, end_year=2030)
        processor.download_and_process()
        processor.create_multi_level_tables()
        processor.save_multi_level("output/")
    """

    def __init__(
        self,
        year: int = 2022,
        min_age: int = 0,
        max_age: int = MAX_AGE,
        start_year: int = 2022,
        end_year: int = 2023,
        include_dom: bool = True,
        include_com: bool = True,
        include_mayotte: bool = True,
        include_tom: bool = True,
        correct_student_mobility: bool = True,
        monthly: bool = False,
        method: ProjectionMethod = "cohort-stable",
        cache_dir: str | Path | None = "data/cache",
    ) -> None:
        """Initialize processor with filtering options."""
        self.year = year
        self.min_age = min_age
        self.max_age = max_age
        self.start_year = start_year
        self.end_year = end_year
        self.include_dom = include_dom
        self.include_com = include_com
        self.include_mayotte = include_mayotte
        self.include_tom = include_tom
        self.correct_student_mobility = correct_student_mobility
        self.monthly = monthly
        self.method: ProjectionMethod = method
        self.cache_dir = Path(cache_dir) if cache_dir else None

        # For each (projection_year, age) we need the cohort born in
        # (projection_year - age) to exist at census: age_at_census =
        # age - (projection_year - year) must be >= 0. The SQL JOIN on
        # cohort_totals drops rows that don't meet this; warn if any
        # requested (year, age) pair falls outside the census range so
        # users understand why their output has fewer rows than expected.
        if end_year > year + min_age:
            lost_years = end_year - (year + min_age)
            logger.warning(
                "end_year={} exceeds year+min_age={}: the {} youngest "
                "cohort(s) weren't born at census time and will be absent "
                "from the output for projection years {}-{}.",
                end_year,
                year + min_age,
                lost_years,
                year + min_age + 1,
                end_year,
            )

        self.conn = duckdb.connect()
        self.conn.execute("SET preserve_insertion_order=false")
        # Allow DuckDB to spill to disk when in-memory tables exceed RAM
        if self.cache_dir:
            temp_dir = Path(self.cache_dir) / "duckdb_temp"
            temp_dir.mkdir(parents=True, exist_ok=True)
            self.conn.execute(f"SET temp_directory='{temp_dir}'")
        self._base_table_created = False
        self._geo_mappings_loaded = False

    def download_and_process(self) -> PopulationProcessor:
        """Download INDCVI census data and create base population table."""
        parquet_path = download_indcvi(self.year, self.cache_dir)

        logger.info(
            "Processing INDCVI {} (ages {}-{})...",
            self.year,
            self.min_age,
            self.max_age,
        )
        self._execute(
            sql.CREATE_BASE_TABLE.format(
                parquet_path=parquet_path,
                where_clause=self._build_where_clause(),
                year=self.year,
                iris_sentinel_no_geo=IRIS_SENTINEL_NO_GEO,
            )
        )

        logger.info("Base table: {:,} rows", self._row_count())
        self._base_table_created = True

        if self.include_mayotte:
            self._add_mayotte()

        if self.include_tom:
            self._add_tom()

        return self

    def create_multi_level_tables(self) -> PopulationProcessor:
        """Create population tables at department, EPCI, canton, and IRIS levels.

        Uses simple census aging with monthly birth distribution and
        geographic ratios to produce multi-year population projections.

        Each table includes:
        - All geographic columns (department_code, region_code, etc.)
        - geo_precision column indicating data reliability
        - month, snapshot_month, born_date, decimal_age
        """
        self._ensure_base_table()
        self._load_geo_mappings()
        return self._create_projected_tables()

    def _create_projected_tables(self) -> PopulationProcessor:
        """Create multi-year projected tables with monthly granularity.

        Uses simple census aging: population at age A in year Y equals
        the census population at age A-(Y-census_year).
        """
        sy, ey = self.start_year, self.end_year
        logger.info(
            "Creating projected tables ({}-{}, method={})...", sy, ey, self.method
        )

        # 1. Load monthly birth distribution from INDREG MNAI.
        # INSEE disclosure rules mean small departments are published only at
        # REGION level and Mayotte has no dedicated MNAI row; the helper
        # handles both by falling back within INDREG itself to the regional
        # and metropolitan aggregates.
        logger.info("Step 1: Loading monthly birth distribution (MNAI)...")
        monthly_births_df = download_mnai_birth_distribution(self.year, self.cache_dir)
        if monthly_births_df.empty:
            raise RuntimeError(
                "INDREG MNAI is unavailable — cannot compute monthly "
                "birth distribution. Check data/cache or network access."
            )
        self._register_dataframe("monthly_births_df", monthly_births_df)
        self._execute(sql.REGISTER_MONTHLY_BIRTHS)

        # 2. Compute geographic ratios
        logger.info("Step 2: Computing geographic ratios...")
        compute_geo_ratios(self.conn, "epci")
        compute_geo_ratios(self.conn, "canton")
        compute_geo_ratios(self.conn, "iris")

        # 2b. Apply student mobility correction to EPCI and IRIS geo ratios
        if self.correct_student_mobility:
            logger.info("Step 2b: Computing department mobility rates...")
            mobsco_path = download_mobsco(self.cache_dir)
            compute_department_mobility_rates(self.conn, mobsco_path)

            logger.info("Step 2c: Correcting EPCI geo ratios for student mobility...")
            apply_student_mobility_correction(self.conn, mobsco_path)

            logger.info("Step 2d: Correcting IRIS geo ratios for student mobility...")
            apply_student_mobility_correction_iris(self.conn, mobsco_path)

        # 3. Project multi-year at all levels
        logger.info("Step 3: Projecting population (method={})...", self.method)
        project_multi_year(
            self.conn,
            self.min_age,
            self.max_age,
            start_year=self.start_year,
            end_year=self.end_year,
            census_year=self.year,
            monthly=self.monthly,
            method=self.method,
        )

        return self

    def save_multi_level(self, output_dir: str | Path) -> dict[str, Path]:
        """Save all multi-level tables to parquet files.

        Birth-month expansion (12 sub-rows per cohort) is applied on-the-fly
        via streaming COPY — the full expanded result is never materialised
        in memory.

        Args:
            output_dir: Directory to save the files

        Returns:
            Dict mapping level name to file path
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        paths = {}
        for level in ["department", "epci", "canton", "iris"]:
            path = output_dir / f"population_{level}.parquet"
            select = sql.SELECT_WITH_BIRTH_MONTH.format(
                level=level,
                geo_columns=_GEO_COLUMNS[level],
            )
            self._execute(f"COPY ({select}) TO '{path}' (FORMAT PARQUET)")
            paths[level] = path

        return paths

    def to_pandas(self, level: str = "department") -> pd.DataFrame:
        """Export a specific level to pandas DataFrame.

        Birth-month expansion is applied on-the-fly so in-memory tables
        stay compact.

        Args:
            level: One of 'department', 'epci', 'canton', 'iris'
        """
        select = sql.SELECT_WITH_BIRTH_MONTH.format(
            level=level,
            geo_columns=_GEO_COLUMNS[level],
        )
        return self._execute(select).df()

    def validate(self) -> dict[str, Any]:
        """Validate population data against expected structure."""
        results: dict[str, Any] = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "stats": {},
        }

        # Check for invalid populations
        nulls = self._fetchone(sql.COUNT_INVALID_POPULATION)
        if nulls > 0:
            results["errors"].append(
                f"Found {nulls} rows with null/negative population"
            )
            results["is_valid"] = False

        # Gather statistics
        results["stats"] = (
            self._execute(sql.GET_VALIDATION_STATS).df().iloc[0].to_dict()
        )

        # Check department coverage
        present = set(
            self._execute(sql.GET_DISTINCT_DEPARTMENTS).df()["department_code"].tolist()
        )
        self._check_department_coverage(present, results)

        return results

    # -------------------------------------------------------------------------
    # Private helpers
    # -------------------------------------------------------------------------

    def _execute(self, query: str) -> duckdb.DuckDBPyRelation:
        """Execute SQL query."""
        return self.conn.execute(query)

    def _fetchone(self, query: str) -> Any:
        """Execute query and return first value."""
        return self.conn.execute(query).fetchone()[0]

    def _register_dataframe(self, name: str, df: pd.DataFrame) -> None:
        """Register DataFrame as DuckDB table."""
        self.conn.register(name, df)

    def _row_count(self) -> int:
        """Get current population table row count."""
        return self._fetchone(sql.GET_ROW_COUNT)

    def _ensure_base_table(self) -> None:
        """Ensure base table has been created."""
        if not self._base_table_created:
            raise RuntimeError("Call download_and_process() first")

    def _build_where_clause(self) -> str:
        """Build WHERE clause for filtering census data (dept-level only).

        Age filtering is deliberately NOT applied here: cohort-stable needs
        every cohort referenced by any projection year to exist in census,
        including ages below ``min_age``.
        """
        filters = []
        if not self.include_dom:
            filters.append("DEPT NOT IN ('971', '972', '973', '974')")
        if not self.include_com:
            filters.append("DEPT NOT IN ('975', '977', '978')")
        return "WHERE " + " AND ".join(filters) if filters else ""

    def _add_mayotte(self) -> None:
        """Add Mayotte data from POP1B census (raw, aged forward)."""
        mayotte_df = synthesize_mayotte_population(self.year, cache_dir=self.cache_dir)
        if mayotte_df.empty:
            raise RuntimeError(
                "Mayotte POP1B is unavailable — use --no-mayotte to skip it."
            )
        self._register_dataframe("mayotte_df", mayotte_df)
        self._execute(sql.INSERT_MAYOTTE)

    def _add_tom(self) -> None:
        """Add TOM Pacifique data from territory censuses (aged forward)."""
        tom_df = synthesize_tom_population(self.year, cache_dir=self.cache_dir)
        if tom_df.empty:
            raise RuntimeError(
                "All TOM Pacifique censuses unavailable — use --no-tom to skip."
            )
        self._register_dataframe("tom_df", tom_df)
        self._execute(sql.INSERT_TOM)

    def _load_geo_mappings(self) -> None:
        """Load commune→EPCI and canton→EPCI weight mappings."""
        if self._geo_mappings_loaded:
            return

        commune_epci, canton_weights = get_geo_mappings(self.cache_dir)

        self._register_dataframe("commune_epci_df", commune_epci)
        self._execute(sql.REGISTER_COMMUNE_EPCI)

        self._register_dataframe("canton_weights_df", canton_weights)
        self._execute(sql.REGISTER_CANTON_WEIGHTS)

        self._geo_mappings_loaded = True

    def _check_department_coverage(self, present: set[str], results: dict) -> None:
        """Check department coverage and add warnings."""
        missing_metro = set(DEPARTMENTS_METRO) - present
        if missing_metro:
            results["warnings"].append(
                f"Missing metro departments: {sorted(missing_metro)}"
            )

        expected_dom = set(DEPARTMENTS_DOM)
        present_dom = expected_dom & present
        missing_dom = expected_dom - present
        results["stats"]["dom_present"] = list(present_dom)
        if missing_dom:
            results["warnings"].append(
                f"Missing DOM departments: {sorted(missing_dom)}"
            )

        results["stats"]["mayotte_present"] = "976" in present

        present_tom = set(DEPARTMENTS_TOM) & present
        missing_tom = set(DEPARTMENTS_TOM) - present
        results["stats"]["tom_present"] = sorted(present_tom)
        if missing_tom:
            results["warnings"].append(
                f"Missing TOM departments: {sorted(missing_tom)}"
            )

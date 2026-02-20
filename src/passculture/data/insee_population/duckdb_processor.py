"""DuckDB-based processor for INSEE population data.

Creates multi-level population tables at department, EPCI, and IRIS levels
with geo_precision indicators for data reliability.

Supports two modes:
1. Single-year census mode (original): uses INDCVI data directly
2. Multi-year projection mode: combines quinquennal estimates x age ratios x
   monthly birth distribution x geographic ratios to produce monthly population
   with decimal_age and born_date columns.
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
    IRIS_SENTINEL_NO_GEO,
    MAX_AGE,
    MAX_CAGR_EXTENSION,
)
from passculture.data.insee_population.downloaders import (
    download_indcvi,
    download_monthly_birth_distribution,
    download_quinquennal_estimates,
    synthesize_mayotte_population,
)
from passculture.data.insee_population.geo_mappings import get_geo_mappings
from passculture.data.insee_population.projections import (
    compute_age_ratios,
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
    - population_iris: ~60% coverage, only precise IRIS codes

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
        start_year: int = 2015,
        end_year: int = 2030,
        include_dom: bool = True,
        include_com: bool = True,
        include_mayotte: bool = True,
        correct_student_mobility: bool = True,
        monthly: bool = False,
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
        self.correct_student_mobility = correct_student_mobility
        self.monthly = monthly
        self.cache_dir = Path(cache_dir) if cache_dir else None

        # Validate forecast horizon
        max_pipeline_year = year + min_age
        max_allowed = max_pipeline_year + MAX_CAGR_EXTENSION
        if end_year > max_allowed:
            raise ValueError(
                f"end_year={end_year} exceeds maximum reliable forecast year "
                f"({max_allowed}). With census year {year} and min_age "
                f"{min_age}, ratio-based projection is valid to "
                f"{max_pipeline_year} and CAGR can extend up to "
                f"{MAX_CAGR_EXTENSION} more years."
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
                where_clause=self._build_where_clause(skip_age_filter=True),
                year=self.year,
                iris_sentinel_no_geo=IRIS_SENTINEL_NO_GEO,
            )
        )

        logger.info("Base table: {:,} rows", self._row_count())
        self._base_table_created = True

        if self.include_mayotte:
            self._add_mayotte()

        return self

    def create_multi_level_tables(self) -> PopulationProcessor:
        """Create population tables at department, EPCI, and IRIS levels.

        Produces multi-year monthly projections using quinquennal estimates x
        age ratios x monthly birth distribution x geographic ratios.

        Each table includes:
        - All geographic columns (department_code, region_code, etc.)
        - geo_precision column indicating data reliability
        - month, snapshot_month, born_date, decimal_age
        """
        self._ensure_base_table()
        self._load_geo_mappings()
        return self._create_projected_tables()

    def _create_projected_tables(self) -> PopulationProcessor:
        """Create multi-year projected tables with monthly granularity."""
        sy, ey = self.start_year, self.end_year
        # Limit pipeline to years where cohort-shifted age ratios are valid:
        # census_age = target_age + (census_year - year) >= 0
        # → year <= census_year + min_age
        max_pipeline_year = min(ey, self.year + self.min_age)
        logger.info("Creating multi-year projected tables ({}-{})...", sy, ey)
        if max_pipeline_year < ey:
            logger.info(
                "  Pipeline valid to {}, CAGR will extend to {}",
                max_pipeline_year,
                ey,
            )

        # 1. Download and register quinquennal estimates (needed by age ratios)
        logger.info("Step 1: Loading quinquennal estimates...")
        quinquennal_df = download_quinquennal_estimates(
            self.start_year, max_pipeline_year, self.cache_dir
        )
        self._register_dataframe("quinquennal_df", quinquennal_df)
        self._execute(sql.REGISTER_QUINQUENNAL)

        if self.include_mayotte:
            mayotte_years = quinquennal_df[quinquennal_df["department_code"] == "976"][
                "year"
            ]
            if mayotte_years.empty:
                logger.warning(
                    "  No quinquennal data for Mayotte (976) -- population will be 0"
                )
            elif self.start_year < mayotte_years.min():
                logger.warning(
                    "  Mayotte (976) quinquennal data starts at"
                    " {}, population will be 0 for"
                    " {}-{}",
                    mayotte_years.min(),
                    self.start_year,
                    int(mayotte_years.min()) - 1,
                )

        # 2. Compute cohort-shifted age ratios from INDCVI base table
        logger.info("Step 2: Computing cohort-shifted age ratios from census...")
        compute_age_ratios(self.conn, census_year=self.year)

        # 3. Download and register monthly birth distribution
        logger.info("Step 3: Loading monthly birth distribution...")
        monthly_births_df = download_monthly_birth_distribution(self.cache_dir)
        if monthly_births_df.empty:
            logger.warning("  Birth data unavailable, using uniform 1/12 distribution")
            monthly_births_df = self._build_uniform_monthly_distribution()

        # Pad departments present in population but missing from birth data
        depts_in_pop = set(
            self._execute("SELECT DISTINCT department_code FROM population").df()[
                "department_code"
            ]
        )
        depts_in_births = set(monthly_births_df["department_code"])
        missing = depts_in_pop - depts_in_births
        if missing:
            logger.debug(
                "  Padding {} depts missing from birth data: {}",
                len(missing),
                sorted(missing),
            )
            pad_rows = [
                {"department_code": d, "month": m, "month_ratio": 1.0 / 12}
                for d in missing
                for m in range(1, 13)
            ]
            monthly_births_df = pd.concat(
                [monthly_births_df, pd.DataFrame(pad_rows)], ignore_index=True
            )

        self._register_dataframe("monthly_births_df", monthly_births_df)
        self._execute(sql.REGISTER_MONTHLY_BIRTHS)

        # 4. Compute geographic ratios
        logger.info("Step 4: Computing geographic ratios...")
        compute_geo_ratios(self.conn, "epci")
        compute_geo_ratios(self.conn, "canton")
        compute_geo_ratios(self.conn, "iris")

        # 4b. Apply student mobility correction to EPCI and IRIS geo ratios
        if self.correct_student_mobility:
            from passculture.data.insee_population.downloaders import download_mobsco
            from passculture.data.insee_population.projections import (
                apply_student_mobility_correction,
                apply_student_mobility_correction_iris,
                compute_department_mobility_rates,
            )

            logger.info("Step 4b: Computing department mobility rates...")
            mobsco_path = download_mobsco(self.cache_dir)
            compute_department_mobility_rates(self.conn, mobsco_path)

            logger.info("Step 4c: Correcting EPCI geo ratios for student mobility...")
            apply_student_mobility_correction(self.conn, mobsco_path)

            logger.info("Step 4d: Correcting IRIS geo ratios for student mobility...")
            apply_student_mobility_correction_iris(self.conn, mobsco_path)

        # 5. Project multi-year at all levels
        logger.info("Step 5: Projecting population...")
        project_multi_year(
            self.conn,
            self.min_age,
            self.max_age,
            end_year=self.end_year,
            census_year=self.year,
            monthly=self.monthly,
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

    def get_level_summary(self) -> pd.DataFrame:
        """Get summary of all geographic levels."""
        summaries = []
        for _level, query in [
            ("department", sql.GET_DEPARTMENT_SUMMARY),
            ("epci", sql.GET_EPCI_SUMMARY),
            ("canton", sql.GET_CANTON_SUMMARY),
            ("iris", sql.GET_IRIS_SUMMARY),
        ]:
            try:
                df = self._execute(query).df()
                summaries.append(df)
            except Exception:
                pass

        if summaries:
            return pd.concat(summaries, ignore_index=True)
        return pd.DataFrame()

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

    def _build_where_clause(self, *, skip_age_filter: bool = False) -> str:
        """Build WHERE clause for filtering census data."""
        filters = []
        if not skip_age_filter:
            filters.append(
                f"CAST(AGEREV AS INT) BETWEEN {self.min_age} AND {self.max_age}"
            )
        if not self.include_dom:
            filters.append("DEPT NOT IN ('971', '972', '973', '974')")
        if not self.include_com:
            filters.append("DEPT NOT IN ('975', '977', '978')")
        return "WHERE " + " AND ".join(filters) if filters else ""

    def _add_mayotte(self) -> None:
        """Add Mayotte data from estimates."""
        try:
            # Use all ages — age filtering happens in projection SQL
            mayotte_df = synthesize_mayotte_population(
                self.year, 0, MAX_AGE, cache_dir=self.cache_dir
            )
            if not mayotte_df.empty:
                self._register_dataframe("mayotte_df", mayotte_df)
                self._execute(sql.INSERT_MAYOTTE)
        except Exception as e:
            logger.warning("  Could not add Mayotte: {}", e)

    def _build_uniform_monthly_distribution(self) -> pd.DataFrame:
        """Build uniform 1/12 monthly distribution from departments in base table."""
        depts = (
            self._execute("SELECT DISTINCT department_code FROM population")
            .df()["department_code"]
            .tolist()
        )
        rows = [
            {"department_code": dept, "month": m, "month_ratio": 1.0 / 12}
            for dept in depts
            for m in range(1, 13)
        ]
        return pd.DataFrame(rows)

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

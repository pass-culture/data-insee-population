"""Multi-year population projections using simple census aging.

Projects population by shifting census cohorts forward in time:
    pop(Y, A, sex, dept) = census_pop(census_year, A-(Y-census_year), sex, dept)

Geographic ratios, monthly birth distribution, and student mobility
corrections are applied on top of the department-level projection.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from loguru import logger

from passculture.data.insee_population import sql
from passculture.data.insee_population.constants import (
    AGE_BUCKETS,
    CI_BASE_MID,
    CI_BASE_NEAR,
    CI_EXTRA_CANTON,
    CI_EXTRA_EPCI,
    CI_EXTRA_IRIS,
    CI_PER_YEAR,
    IRIS_SENTINEL_MASKED_SUFFIX,
    IRIS_SENTINEL_NO_GEO,
    MAX_AGE,
    STUDENT_BAND_AGEREV10,
    STUDENT_BAND_AGEREV10_SECONDARY,
    STUDENT_BAND_SECONDARY_WEIGHT,
    STUDENT_MOBILITY_BLEND_CAP_BY_BAND,
    STUDENT_MOBILITY_BLEND_DEFAULT_BY_BAND,
)

if TYPE_CHECKING:
    import duckdb

ProjectionMethod = Literal["cohort-stable", "cohort-aging"]


def _build_age_band_cases() -> str:
    """Build SQL CASE expression mapping age -> age_band from AGE_BUCKETS."""
    cases = []
    for band_name, age_range in AGE_BUCKETS.items():
        min_age = min(age_range)
        max_age = max(age_range)
        cases.append(f"WHEN age BETWEEN {min_age} AND {max_age} THEN '{band_name}'")
    return "\n            ".join(cases)


_GEO_RATIO_CONFIG = {
    "epci": (
        sql.CREATE_GEO_RATIOS_EPCI,
        "geo_ratios_epci",
        "epci_code",
        "EPCIs",
    ),
    "canton": (
        sql.CREATE_GEO_RATIOS_CANTON,
        "geo_ratios_canton",
        "canton_code",
        "cantons",
    ),
    "iris": (
        sql.CREATE_GEO_RATIOS_IRIS,
        "geo_ratios_iris",
        "iris_code",
        "IRIS codes",
    ),
}


def compute_geo_ratios(conn: duckdb.DuckDBPyConnection, level: str) -> None:
    """Compute geographic share within each dept/age_band/sex from INDCVI.

    Uses age-band-level aggregation to avoid coverage gaps caused by
    individual ages missing from IRIS-coded communes in rural departments.

    Args:
        conn: DuckDB connection with `population` and `commune_epci` tables
        level: 'epci', 'canton', or 'iris'
    """
    if level not in _GEO_RATIO_CONFIG:
        raise ValueError(f"Unknown level: {level}")

    sql_template, table, distinct_col, label = _GEO_RATIO_CONFIG[level]
    age_band_cases = _build_age_band_cases()
    conn.execute(
        sql_template.format(
            age_band_cases=age_band_cases,
            max_age=MAX_AGE,
            iris_sentinel_no_geo=IRIS_SENTINEL_NO_GEO,
            iris_sentinel_masked_suffix=IRIS_SENTINEL_MASKED_SUFFIX,
        )
    )
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    n_geo = conn.execute(
        f"SELECT COUNT(DISTINCT {distinct_col}) FROM {table}"
    ).fetchone()[0]
    logger.debug(
        "  {} geo ratios: {:,} rows across {} {}",
        level.upper(),
        count,
        n_geo,
        label,
    )


def project_multi_year(
    conn: duckdb.DuckDBPyConnection,
    min_age: int,
    max_age: int,
    start_year: int = 2022,
    end_year: int | None = None,
    census_year: int = 2022,
    monthly: bool = False,
    method: ProjectionMethod = "cohort-stable",
) -> None:
    """Project multi-year population at all geographic levels.

    Two dept-level methods (see ``sql/projections.py`` docstring for the
    algebra):

    * ``cohort-stable`` (default, from the INSEE spec doc): national
      cohort totals times age-specific dept shares frozen at census.
      Renews the age-specific distribution each year, implicitly
      capturing post-bac migration.
    * ``cohort-aging`` (legacy): ages each census cohort in place.

    Default mode (yearly): one snapshot at January 1st per year, exploded
    into 12 birth-month sub-rows per cohort.
    Monthly mode: 12 snapshot months x 12 birth months = 144 rows per
    year/dept/age/sex.

    Requires these tables to already exist in the connection:
    - `population`: from download_and_process (INDCVI census)
    - `monthly_births`: from download_mnai_birth_distribution
    - `geo_ratios_epci`: from compute_geo_ratios('epci')
    - `geo_ratios_canton`: from compute_geo_ratios('canton')
    - `geo_ratios_iris`: from compute_geo_ratios('iris')

    Creates: `population_department`, `population_epci`, `population_canton`,
             `population_iris`
    """
    if end_year is None:
        end_year = census_year

    mode_label = "monthly" if monthly else "yearly"
    logger.info(
        "Projecting multi-year population ({}, method={})...",
        mode_label,
        method,
    )

    age_band_cases = _build_age_band_cases()
    ci_params = {
        "census_year": census_year,
        "ci_base_near": CI_BASE_NEAR,
        "ci_base_mid": CI_BASE_MID,
        "ci_per_year": CI_PER_YEAR,
    }

    # Monthly vs yearly mode controls how snapshot months are generated.
    # Join alias depends on method: cohort-aging uses `c` (census_dept),
    # cohort-stable uses `ds` (dept_age_sex_shares).
    dept_alias = "c" if method == "cohort-aging" else "ds"
    if monthly:
        month_params = {
            "month_select": "mb.month",
            "month_factor": "",
            "month_join": (
                "JOIN monthly_births mb\n"
                f"        ON {dept_alias}.department_code = mb.department_code"
            ),
        }
    else:
        month_params = {
            "month_select": "1",
            "month_factor": "",
            "month_join": "",
        }

    def _timed(label, fn):
        """Execute fn() and log elapsed time."""
        t0 = time.time()
        logger.debug("{}...", label)
        result = fn()
        elapsed = time.time() - t0
        logger.debug("{} done ({:.1f}s)", label, elapsed)
        return result

    if method == "cohort-stable":
        dept_template = sql.CREATE_PROJECTED_DEPARTMENT_COHORT_STABLE
    elif method == "cohort-aging":
        dept_template = sql.CREATE_PROJECTED_DEPARTMENT
    else:
        raise ValueError(
            f"Unknown projection method: {method!r}. "
            "Expected 'cohort-stable' or 'cohort-aging'."
        )

    _timed(
        f"Department projection ({method})",
        lambda: conn.execute(
            dept_template.format(
                min_age=min_age,
                max_age=max_age,
                start_year=start_year,
                end_year=end_year,
                **ci_params,
                **month_params,
            )
        ),
    )

    dept_rows = conn.execute("SELECT COUNT(*) FROM population_department").fetchone()[0]
    logger.debug("  Department (compact): {:,} rows", dept_rows)

    # Geo levels — join from compact department table (before birth-month
    # explosion) so the joins operate on 12x fewer rows.
    _timed(
        "EPCI geo join",
        lambda: conn.execute(
            sql.CREATE_PROJECTED_EPCI.format(
                age_band_cases=age_band_cases,
                ci_extra_epci=CI_EXTRA_EPCI,
                max_age=MAX_AGE,
            )
        ),
    )
    _timed(
        "Canton geo join",
        lambda: conn.execute(
            sql.CREATE_PROJECTED_CANTON.format(
                age_band_cases=age_band_cases,
                ci_extra_canton=CI_EXTRA_CANTON,
                max_age=MAX_AGE,
            )
        ),
    )
    _timed(
        "IRIS geo join",
        lambda: conn.execute(
            sql.CREATE_PROJECTED_IRIS.format(
                age_band_cases=age_band_cases,
                ci_extra_iris=CI_EXTRA_IRIS,
                max_age=MAX_AGE,
            )
        ),
    )

    # Tables are kept compact (without birth_month).  The 12x birth-month
    # expansion is applied lazily at read/export time via
    # SELECT_WITH_BIRTH_MONTH to avoid materialising hundreds of millions
    # of rows in memory.

    # Print statistics (compact table sizes)
    dept_stats = conn.execute("""
        SELECT COUNT(*), SUM(population),
               COUNT(DISTINCT year), COUNT(DISTINCT department_code)
        FROM population_department
    """).fetchone()
    dept_count, dept_pop, n_years, n_depts = dept_stats
    n_months = conn.execute(
        "SELECT COUNT(DISTINCT month) FROM population_department"
    ).fetchone()[0]
    avg_pop = float(dept_pop) / (n_years * n_months) if n_years and n_months else 0
    logger.debug(
        "  Department: {:,} rows (x12 at export), "
        "{} years, {} depts, "
        "avg {:,.0f} pop/month",
        dept_count,
        n_years,
        n_depts,
        avg_pop,
    )

    epci_stats = conn.execute("""
        SELECT COUNT(*), SUM(population), COUNT(DISTINCT epci_code)
        FROM population_epci
    """).fetchone()
    epci_count, _epci_pop, n_epcis = epci_stats
    logger.debug("  EPCI: {:,} rows, {} EPCIs", epci_count, n_epcis)

    canton_stats = conn.execute("""
        SELECT COUNT(*), SUM(population), COUNT(DISTINCT canton_code)
        FROM population_canton
    """).fetchone()
    canton_count, canton_pop, n_cantons = canton_stats
    canton_pct = round(100 * float(canton_pop) / float(dept_pop), 1) if dept_pop else 0
    logger.debug(
        "  Canton: {:,} rows, {} cantons ({}% coverage)",
        canton_count,
        n_cantons,
        canton_pct,
    )

    iris_stats = conn.execute("""
        SELECT COUNT(*), SUM(population), COUNT(DISTINCT iris_code)
        FROM population_iris
    """).fetchone()
    iris_count, iris_pop, n_irises = iris_stats
    iris_pct = round(100 * float(iris_pop) / float(dept_pop), 1) if dept_pop else 0
    logger.debug(
        "  IRIS: {:,} rows, {} IRIS ({}% coverage)",
        iris_count,
        n_irises,
        iris_pct,
    )


def _build_band_config_sql() -> str:
    """Build the SQL VALUES block mapping age_band -> AGEREV10, cap, default.

    Includes secondary_agerev10 and secondary_weight for bands with a mixed
    population (e.g. 15_19 uses 75% lycee + 25% higher-ed flows).

    Returns a UNION ALL of SELECT rows for use in a band_config CTE.
    """
    rows = []
    for band, agerev10 in STUDENT_BAND_AGEREV10.items():
        cap = STUDENT_MOBILITY_BLEND_CAP_BY_BAND[band]
        default = STUDENT_MOBILITY_BLEND_DEFAULT_BY_BAND[band]
        secondary = STUDENT_BAND_AGEREV10_SECONDARY.get(band)
        sec_weight = STUDENT_BAND_SECONDARY_WEIGHT.get(band, 0.0)
        secondary_sql = f"'{secondary}'" if secondary else "NULL"
        rows.append(
            f"SELECT '{band}' AS age_band, '{agerev10}' AS agerev10, "
            f"{cap} AS blend_cap, {default} AS blend_default, "
            f"{secondary_sql} AS secondary_agerev10, {sec_weight} AS secondary_weight"
        )
    return "\n    UNION ALL\n    ".join(rows)


def compute_department_mobility_rates(
    conn: duckdb.DuckDBPyConnection,
    mobsco_path: Path,
) -> None:
    """Compute per-department, per-age-band student inter-departmental mobility rates.

    For each (department, age_band) pair, computes the fraction of students in
    that band who study in a different department, using the MOBSCO AGEREV10
    group appropriate for that band (AGEREV10='15' for lycee-age 15_19,
    AGEREV10='18' for higher-ed 20_24). Creates a `mobility_weights` table with
    `blend_weight = min(mobility_rate, per-band cap)`, falling back to a
    per-band default for departments not in MOBSCO.

    Requires `commune_epci` table to exist (for dept lookup from commune).

    Args:
        conn: DuckDB connection
        mobsco_path: Path to MOBSCO parquet file
    """
    conn.execute(
        sql.CREATE_MOBILITY_WEIGHTS.format(
            mobsco_path=mobsco_path,
            band_config_sql=_build_band_config_sql(),
        )
    )
    count = conn.execute("SELECT COUNT(*) FROM mobility_weights").fetchone()[0]
    logger.debug("  Mobility weights: {} (dept, band) pairs", count)


def apply_student_mobility_correction(
    conn: duckdb.DuckDBPyConnection,
    mobsco_path: Path,
) -> None:
    """Blend MOBSCO study-destination ratios into EPCI geo_ratios for student bands.

    For age bands 15_19 and 20_24, blends census-based geo_ratios with
    study-destination ratios from the MOBSCO file, using per-department
    blend weights from the `mobility_weights` table:
        corrected = (1 - w) * census_ratio + w * study_ratio
    Then renormalizes so ratios sum to 1 per (dept, band, sex).

    Other age bands are unchanged.

    Requires `geo_ratios_epci`, `commune_epci`, and `mobility_weights` tables.

    Args:
        conn: DuckDB connection with geo_ratios_epci table
        mobsco_path: Path to MOBSCO parquet file
    """
    # 1. Rename existing geo_ratios_epci to _base
    conn.execute(sql.RENAME_GEO_RATIOS_EPCI_TO_BASE)

    # 2. Compute student flows from MOBSCO (per age band)
    conn.execute(
        sql.CREATE_STUDENT_FLOWS_EPCI.format(
            mobsco_path=mobsco_path,
            band_config_sql=_build_band_config_sql(),
        )
    )
    flow_count = conn.execute("SELECT COUNT(*) FROM student_flows_epci").fetchone()[0]
    flow_depts = conn.execute(
        "SELECT COUNT(DISTINCT department_code) FROM student_flows_epci"
    ).fetchone()[0]
    logger.debug(
        "  Student flows: {:,} rows across {} departments",
        flow_count,
        flow_depts,
    )

    # 3. Create corrected geo_ratios_epci (blend + renormalize)
    conn.execute(sql.CREATE_CORRECTED_GEO_RATIOS_EPCI)
    new_count = conn.execute("SELECT COUNT(*) FROM geo_ratios_epci").fetchone()[0]
    logger.debug(
        "  Corrected EPCI geo ratios: {:,} rows (per-dept blend weights)",
        new_count,
    )

    # 4. Clean up temporary tables
    conn.execute("DROP TABLE IF EXISTS geo_ratios_epci_base")
    conn.execute("DROP TABLE IF EXISTS student_flows_epci")


def apply_student_mobility_correction_iris(
    conn: duckdb.DuckDBPyConnection,
    mobsco_path: Path,
) -> None:
    """Blend MOBSCO study-destination ratios into IRIS geo_ratios for student bands.

    Same logic as the EPCI version but at IRIS level. MOBSCO commune-level
    flows are distributed to IRIS using census population proportions.

    Requires `geo_ratios_iris`, `population`, `commune_epci`, and
    `mobility_weights` tables.

    Args:
        conn: DuckDB connection with geo_ratios_iris table
        mobsco_path: Path to MOBSCO parquet file
    """
    # 1. Rename existing geo_ratios_iris to _base
    conn.execute(sql.RENAME_GEO_RATIOS_IRIS_TO_BASE)

    # 2. Compute student flows from MOBSCO at IRIS level (per age band)
    conn.execute(
        sql.CREATE_STUDENT_FLOWS_IRIS.format(
            mobsco_path=mobsco_path,
            band_config_sql=_build_band_config_sql(),
        )
    )
    flow_count = conn.execute("SELECT COUNT(*) FROM student_flows_iris").fetchone()[0]
    flow_depts = conn.execute(
        "SELECT COUNT(DISTINCT department_code) FROM student_flows_iris"
    ).fetchone()[0]
    logger.debug(
        "  IRIS student flows: {:,} rows across {} departments",
        flow_count,
        flow_depts,
    )

    # 3. Create corrected geo_ratios_iris (blend + renormalize)
    conn.execute(sql.CREATE_CORRECTED_GEO_RATIOS_IRIS)
    new_count = conn.execute("SELECT COUNT(*) FROM geo_ratios_iris").fetchone()[0]
    logger.debug(
        "  Corrected IRIS geo ratios: {:,} rows (per-dept blend weights)",
        new_count,
    )

    # 4. Clean up temporary tables
    conn.execute("DROP TABLE IF EXISTS geo_ratios_iris_base")
    conn.execute("DROP TABLE IF EXISTS student_flows_iris")

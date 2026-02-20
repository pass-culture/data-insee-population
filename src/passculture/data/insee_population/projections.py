"""Multi-year monthly population projections.

Computes ratio tables from INDCVI census data and applies them to quinquennal
estimates to produce monthly population at department, EPCI, and IRIS levels.

Algorithm:
    pop(year, month, age, sex, geo) =
        quinquennal(year, age_band, sex, dept)
      * age_ratio(age | age_band, sex, dept)
      * month_ratio(month | dept)
      * geo_ratio(geo | dept, age, sex)
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from passculture.data.insee_population import sql
from passculture.data.insee_population.constants import (
    AGE_BUCKETS,
    CAGR_RATE_CLAMP,
    CI_BASE_MID,
    CI_BASE_NEAR,
    CI_EXTRA_CANTON,
    CI_EXTRA_EPCI,
    CI_EXTRA_IRIS,
    CI_PER_YEAR,
    IRIS_SENTINEL_MASKED_SUFFIX,
    IRIS_SENTINEL_NO_GEO,
    MAX_AGE,
    STUDENT_MOBILITY_BLEND_CAP,
    STUDENT_MOBILITY_BLEND_DEFAULT,
)

if TYPE_CHECKING:
    import duckdb


def _build_age_band_cases() -> str:
    """Build SQL CASE expression mapping age -> age_band from AGE_BUCKETS."""
    cases = []
    for band_name, age_range in AGE_BUCKETS.items():
        min_age = min(age_range)
        max_age = max(age_range)
        cases.append(f"WHEN age BETWEEN {min_age} AND {max_age} THEN '{band_name}'")
    return "\n            ".join(cases)


def compute_age_ratios(conn: duckdb.DuckDBPyConnection, census_year: int) -> None:
    """Compute cohort-shifted age share within each 5-year band per year/dept/sex.

    For each projection year Y and target age A, looks up census population at
    census_age = A + (census_year - Y) to use the actual birth cohort.

    Requires `population` (INDCVI census) and `quinquennal` (for projection years)
    tables to exist in the connection.

    Creates DuckDB tables: `age_ratios` and `age_ratios_fallback`.
    """
    age_band_cases = _build_age_band_cases()
    conn.execute(
        sql.CREATE_AGE_RATIOS.format(
            age_band_cases=age_band_cases,
            census_year=census_year,
            max_age=MAX_AGE,
        )
    )
    conn.execute(sql.CREATE_AGE_RATIOS_FALLBACK)

    count = conn.execute("SELECT COUNT(*) FROM age_ratios").fetchone()[0]
    depts = conn.execute(
        "SELECT COUNT(DISTINCT department_code) FROM age_ratios"
    ).fetchone()[0]
    logger.debug("  Age ratios: {:,} rows across {} departments", count, depts)


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


TREND_YEARS = 5
"""Number of years used to compute CAGR for post-pipeline extension."""


def project_multi_year(
    conn: duckdb.DuckDBPyConnection,
    min_age: int,
    max_age: int,
    end_year: int | None = None,
    census_year: int = 2022,
    monthly: bool = False,
) -> None:
    """Project multi-year population at all geographic levels.

    Default mode (yearly): one snapshot at January 1st per year, exploded
    into 12 birth-month sub-rows per cohort.
    Monthly mode: 12 snapshot months x 12 birth months = 144 rows per
    year/dept/age/sex.

    Requires these tables to already exist in the connection:
    - `quinquennal`: from download_quinquennal_estimates
    - `monthly_births`: from download_monthly_birth_distribution
    - `age_ratios`, `age_ratios_fallback`: from compute_age_ratios
    - `geo_ratios_epci`: from compute_geo_ratios('epci')
    - `geo_ratios_canton`: from compute_geo_ratios('canton')
    - `geo_ratios_iris`: from compute_geo_ratios('iris')

    When end_year exceeds the quinquennal data range, the department table
    is first built from real data then extended using CAGR computed on
    the final projected output (not raw quinquennal input).

    Creates: `population_department`, `population_epci`, `population_canton`,
             `population_iris`
    """
    mode_label = "monthly" if monthly else "yearly"
    logger.info("Projecting multi-year population ({})...", mode_label)

    age_band_cases = _build_age_band_cases()
    ci_params = {
        "census_year": census_year,
        "ci_base_near": CI_BASE_NEAR,
        "ci_base_mid": CI_BASE_MID,
        "ci_per_year": CI_PER_YEAR,
    }

    # Monthly vs yearly mode controls how snapshot months are generated.
    # Population is a stock variable (not a flow), so the full annual
    # population is replicated across all 12 snapshot months — month_ratio
    # is only applied once at export time for birth-month sub-cohorts.
    if monthly:
        month_params = {
            "month_select": "mb.month",
            "month_factor": "",
            "month_join": (
                "JOIN monthly_births mb\n"
                "        ON q.department_code = mb.department_code"
            ),
            "month_cross_join": (
                "CROSS JOIN (SELECT DISTINCT month FROM monthly_births) mb"
            ),
        }
    else:
        month_params = {
            "month_select": "1",
            "month_factor": "",
            "month_join": "",
            "month_cross_join": "",
        }

    def _timed(label, fn):
        """Execute fn() and log elapsed time."""
        t0 = time.time()
        logger.debug("{}...", label)
        result = fn()
        elapsed = time.time() - t0
        logger.debug("{} done ({:.1f}s)", label, elapsed)
        return result

    # Department level (from real pipeline data)
    _timed(
        "Department projection",
        lambda: conn.execute(
            sql.CREATE_PROJECTED_DEPARTMENT.format(
                min_age=min_age, max_age=max_age, **ci_params, **month_params
            )
        ),
    )

    # Extend with CAGR if requested end_year exceeds projected data
    max_data_year = conn.execute(
        "SELECT MAX(year) FROM population_department"
    ).fetchone()[0]

    if end_year and end_year > max_data_year:
        first_trend_year = max(
            max_data_year - TREND_YEARS,
            conn.execute("SELECT MIN(year) FROM population_department").fetchone()[0],
        )
        _timed(
            f"CAGR extension {max_data_year}→{end_year}",
            lambda: conn.execute(
                sql.EXTEND_DEPARTMENT_WITH_CAGR.format(
                    max_data_year=max_data_year,
                    first_trend_year=first_trend_year,
                    end_year=end_year,
                    cagr_rate_clamp=CAGR_RATE_CLAMP,
                    **ci_params,
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
    avg_pop = dept_pop / (n_years * n_months) if n_years and n_months else 0
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
    canton_pct = round(100 * canton_pop / dept_pop, 1) if dept_pop else 0
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
    iris_pct = round(100 * iris_pop / dept_pop, 1) if dept_pop else 0
    logger.debug(
        "  IRIS: {:,} rows, {} IRIS ({}% coverage)",
        iris_count,
        n_irises,
        iris_pct,
    )


def compute_department_mobility_rates(
    conn: duckdb.DuckDBPyConnection,
    mobsco_path: Path,
) -> None:
    """Compute per-department student inter-departmental mobility rates.

    For each department, computes the fraction of students (AGEREV10='18',
    ages 18-24) who study in a different department. Creates a
    `mobility_weights` table with `blend_weight = min(mobility_rate, CAP)`,
    falling back to BLEND_DEFAULT for departments not in MOBSCO.

    Requires `commune_epci` table to exist (for dept lookup from commune).

    Args:
        conn: DuckDB connection
        mobsco_path: Path to MOBSCO parquet file
    """
    conn.execute(
        sql.CREATE_MOBILITY_WEIGHTS.format(
            mobsco_path=mobsco_path,
            blend_default=STUDENT_MOBILITY_BLEND_DEFAULT,
            blend_cap=STUDENT_MOBILITY_BLEND_CAP,
        )
    )
    count = conn.execute("SELECT COUNT(*) FROM mobility_weights").fetchone()[0]
    logger.debug("  Mobility weights: {} departments", count)


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

    # 2. Compute student flows from MOBSCO
    conn.execute(sql.CREATE_STUDENT_FLOWS_EPCI.format(mobsco_path=mobsco_path))
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

    # 2. Compute student flows from MOBSCO at IRIS level
    conn.execute(sql.CREATE_STUDENT_FLOWS_IRIS.format(mobsco_path=mobsco_path))
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

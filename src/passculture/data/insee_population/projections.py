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

from pathlib import Path
from typing import TYPE_CHECKING

from passculture.data.insee_population import sql
from passculture.data.insee_population.constants import AGE_BUCKETS

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
            age_band_cases=age_band_cases, census_year=census_year
        )
    )
    conn.execute(sql.CREATE_AGE_RATIOS_FALLBACK)

    count = conn.execute("SELECT COUNT(*) FROM age_ratios").fetchone()[0]
    depts = conn.execute(
        "SELECT COUNT(DISTINCT department_code) FROM age_ratios"
    ).fetchone()[0]
    print(f"  Age ratios: {count:,} rows across {depts} departments")


def compute_geo_ratios(conn: duckdb.DuckDBPyConnection, level: str) -> None:
    """Compute geographic share within each dept/age_band/sex from INDCVI.

    Uses age-band-level aggregation to avoid coverage gaps caused by
    individual ages missing from IRIS-coded communes in rural departments.

    Args:
        conn: DuckDB connection with `population` and `commune_epci` tables
        level: 'epci' or 'iris'
    """
    age_band_cases = _build_age_band_cases()
    if level == "epci":
        conn.execute(sql.CREATE_GEO_RATIOS_EPCI.format(age_band_cases=age_band_cases))
        count = conn.execute("SELECT COUNT(*) FROM geo_ratios_epci").fetchone()[0]
        epcis = conn.execute(
            "SELECT COUNT(DISTINCT epci_code) FROM geo_ratios_epci"
        ).fetchone()[0]
        print(f"  EPCI geo ratios: {count:,} rows across {epcis} EPCIs")
    elif level == "iris":
        conn.execute(sql.CREATE_GEO_RATIOS_IRIS.format(age_band_cases=age_band_cases))
        count = conn.execute("SELECT COUNT(*) FROM geo_ratios_iris").fetchone()[0]
        irises = conn.execute(
            "SELECT COUNT(DISTINCT iris_code) FROM geo_ratios_iris"
        ).fetchone()[0]
        print(f"  IRIS geo ratios: {count:,} rows across {irises} IRIS codes")
    else:
        raise ValueError(f"Unknown level: {level}")


TREND_YEARS = 5
"""Number of years used to compute CAGR for post-pipeline extension."""


def project_multi_year(
    conn: duckdb.DuckDBPyConnection,
    min_age: int,
    max_age: int,
    end_year: int | None = None,
) -> None:
    """Project multi-year monthly population at all geographic levels.

    Requires these tables to already exist in the connection:
    - `quinquennal`: from download_quinquennal_estimates
    - `monthly_births`: from download_monthly_birth_distribution
    - `age_ratios`, `age_ratios_fallback`: from compute_age_ratios
    - `geo_ratios_epci`: from compute_geo_ratios('epci')
    - `geo_ratios_iris`: from compute_geo_ratios('iris')

    When end_year exceeds the quinquennal data range, the department table
    is first built from real data then extended using CAGR computed on
    the final projected output (not raw quinquennal input).

    Creates: `population_department`, `population_epci`, `population_iris`
    """
    print("Projecting multi-year population...")

    age_band_cases = _build_age_band_cases()

    # Department level (from real pipeline data)
    conn.execute(
        sql.CREATE_PROJECTED_DEPARTMENT.format(min_age=min_age, max_age=max_age)
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
        print(
            f"  Extending department from {max_data_year} to {end_year} "
            f"(CAGR from {first_trend_year}-{max_data_year})..."
        )
        conn.execute(
            sql.EXTEND_DEPARTMENT_WITH_CAGR.format(
                max_data_year=max_data_year,
                first_trend_year=first_trend_year,
                end_year=end_year,
            )
        )

    dept_stats = conn.execute("""
        SELECT COUNT(*), SUM(population),
               COUNT(DISTINCT year), COUNT(DISTINCT department_code)
        FROM population_department
    """).fetchone()
    dept_count, dept_pop, n_years, n_depts = dept_stats
    avg_month_pop = dept_pop / (n_years * 12) if n_years else 0
    print(
        f"  Department: {dept_count:,} rows, "
        f"{n_years} years, {n_depts} depts, "
        f"avg {avg_month_pop:,.0f} pop/month"
    )

    # EPCI level (from extended department table)
    conn.execute(sql.CREATE_PROJECTED_EPCI.format(age_band_cases=age_band_cases))
    epci_stats = conn.execute("""
        SELECT COUNT(*), SUM(population), COUNT(DISTINCT epci_code)
        FROM population_epci
    """).fetchone()
    epci_count, _epci_pop, n_epcis = epci_stats
    print(f"  EPCI: {epci_count:,} rows, {n_epcis} EPCIs")

    # IRIS level (from extended department table)
    conn.execute(sql.CREATE_PROJECTED_IRIS.format(age_band_cases=age_band_cases))
    iris_stats = conn.execute("""
        SELECT COUNT(*), SUM(population), COUNT(DISTINCT iris_code)
        FROM population_iris
    """).fetchone()
    iris_count, iris_pop, n_irises = iris_stats
    iris_pct = round(100 * iris_pop / dept_pop, 1) if dept_pop else 0
    print(f"  IRIS: {iris_count:,} rows, {n_irises} IRIS ({iris_pct}% coverage)")


def apply_student_mobility_correction(
    conn: duckdb.DuckDBPyConnection,
    mobsco_path: Path,
    blend_weight: float = 0.3,
) -> None:
    """Blend MOBSCO study-destination ratios into EPCI geo_ratios for student bands.

    For age bands 15_19 and 20_24, blends census-based geo_ratios with
    study-destination ratios from the MOBSCO file:
        corrected = (1 - w) * census_ratio + w * study_ratio
    Then renormalizes so ratios sum to 1 per (dept, band, sex).

    Other age bands are unchanged.

    Requires `geo_ratios_epci` and `commune_epci` tables to exist.

    Args:
        conn: DuckDB connection with geo_ratios_epci table
        mobsco_path: Path to MOBSCO parquet file
        blend_weight: Weight for study-based ratios (0-1)
    """
    # 1. Rename existing geo_ratios_epci to _base
    conn.execute(sql.RENAME_GEO_RATIOS_EPCI_TO_BASE)

    # 2. Compute student flows from MOBSCO
    conn.execute(sql.CREATE_STUDENT_FLOWS_EPCI.format(mobsco_path=mobsco_path))
    flow_count = conn.execute("SELECT COUNT(*) FROM student_flows_epci").fetchone()[0]
    flow_depts = conn.execute(
        "SELECT COUNT(DISTINCT department_code) FROM student_flows_epci"
    ).fetchone()[0]
    print(f"  Student flows: {flow_count:,} rows across {flow_depts} departments")

    # 3. Create corrected geo_ratios_epci (blend + renormalize)
    conn.execute(sql.CREATE_CORRECTED_GEO_RATIOS_EPCI.format(blend_weight=blend_weight))
    new_count = conn.execute("SELECT COUNT(*) FROM geo_ratios_epci").fetchone()[0]
    print(
        f"  Corrected EPCI geo ratios: {new_count:,} rows (blend_weight={blend_weight})"
    )

    # 4. Clean up temporary tables
    conn.execute("DROP TABLE IF EXISTS geo_ratios_epci_base")
    conn.execute("DROP TABLE IF EXISTS student_flows_epci")


def apply_student_mobility_correction_iris(
    conn: duckdb.DuckDBPyConnection,
    mobsco_path: Path,
    blend_weight: float = 0.3,
) -> None:
    """Blend MOBSCO study-destination ratios into IRIS geo_ratios for student bands.

    Same logic as the EPCI version but at IRIS level. MOBSCO commune-level
    flows are distributed to IRIS using census population proportions.

    Requires `geo_ratios_iris`, `population`, and `commune_epci` tables.

    Args:
        conn: DuckDB connection with geo_ratios_iris table
        mobsco_path: Path to MOBSCO parquet file
        blend_weight: Weight for study-based ratios (0-1)
    """
    # 1. Rename existing geo_ratios_iris to _base
    conn.execute(sql.RENAME_GEO_RATIOS_IRIS_TO_BASE)

    # 2. Compute student flows from MOBSCO at IRIS level
    conn.execute(sql.CREATE_STUDENT_FLOWS_IRIS.format(mobsco_path=mobsco_path))
    flow_count = conn.execute("SELECT COUNT(*) FROM student_flows_iris").fetchone()[0]
    flow_depts = conn.execute(
        "SELECT COUNT(DISTINCT department_code) FROM student_flows_iris"
    ).fetchone()[0]
    print(f"  IRIS student flows: {flow_count:,} rows across {flow_depts} departments")

    # 3. Create corrected geo_ratios_iris (blend + renormalize)
    conn.execute(sql.CREATE_CORRECTED_GEO_RATIOS_IRIS.format(blend_weight=blend_weight))
    new_count = conn.execute("SELECT COUNT(*) FROM geo_ratios_iris").fetchone()[0]
    print(
        f"  Corrected IRIS geo ratios: {new_count:,} rows (blend_weight={blend_weight})"
    )

    # 4. Clean up temporary tables
    conn.execute("DROP TABLE IF EXISTS geo_ratios_iris_base")
    conn.execute("DROP TABLE IF EXISTS student_flows_iris")

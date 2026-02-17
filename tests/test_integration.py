"""Integration tests that download real INSEE data and cross-validate aggregations.

These tests require network access (or a populated data/cache directory) and
are skipped by default. Run with:

    uv run python -m pytest tests/ -v --run-integration -m integration
"""

from __future__ import annotations

from pathlib import Path

import pytest

from passculture.data.insee_population.constants import (
    AGE_BUCKETS,
    DEPARTMENTS_DOM,
    DEPARTMENTS_MAYOTTE,
    DEPARTMENTS_METRO,
)
from passculture.data.insee_population.downloaders import (
    download_quinquennal_estimates,
)
from passculture.data.insee_population.duckdb_processor import PopulationProcessor

pytestmark = pytest.mark.integration

CACHE_DIR = Path("data/cache")

REQUIRED_CACHE_FILES = [
    "indcvi_2022.parquet",
    "quinquennal_estimates.parquet",
    "monthly_birth_distribution.parquet",
    "population_estimates.parquet",
    "commune_epci.parquet",
    "canton_epci_weights.parquet",
]


# ---------------------------------------------------------------------------
# Module-scoped fixtures (run full pipeline once)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def integration_processor():
    """Run the full pipeline once and return the processor."""
    for fname in REQUIRED_CACHE_FILES:
        if not (CACHE_DIR / fname).exists():
            pytest.skip(f"Missing cache file: {fname}")

    proc = PopulationProcessor(
        year=2022,
        min_age=0,
        max_age=25,
        start_year=2022,
        end_year=2024,
        correct_student_mobility=False,
        cache_dir=CACHE_DIR,
    )
    proc.download_and_process()
    proc.create_multi_level_tables()
    return proc


@pytest.fixture(scope="module")
def quinquennal_df():
    """Load quinquennal estimates for cross-checking."""
    for fname in REQUIRED_CACHE_FILES:
        if not (CACHE_DIR / fname).exists():
            pytest.skip(f"Missing cache file: {fname}")

    return download_quinquennal_estimates(2022, 2024, CACHE_DIR)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BAND_FOR_AGE: dict[int, str] = {}
for _band_name, _ages in AGE_BUCKETS.items():
    for _age in _ages:
        _BAND_FOR_AGE[_age] = _band_name

# Age bands fully covered by the narrowed fixture (max_age=25 → 0_4 through 20_24)
_COVERED_BANDS = {
    band for band, ages in AGE_BUCKETS.items() if max(ages) <= 25 and min(ages) >= 0
}

# Per-band tolerances for census vs quinquennal comparison (Test 1).
# Young bands have no student mobility effect but small rural departments
# show ~9-10% sampling variance; 15_19 is transitional (lycee mobility);
# 20_24 has strong student mobility causing larger discrepancies.
_BAND_TOLERANCES = {
    "0_4": 0.10,
    "5_9": 0.10,
    "10_14": 0.10,
    "15_19": 0.12,
    "20_24": 0.20,
}


# ---------------------------------------------------------------------------
# Test 1: Census department totals match quinquennal for census year
# ---------------------------------------------------------------------------


def test_census_vs_quinquennal_census_year(integration_processor, quinquennal_df):
    """Census dept/sex/band totals should respect per-band tolerances."""
    census_pop = integration_processor.conn.execute("""
        SELECT
            department_code,
            sex,
            age,
            SUM(population) AS pop
        FROM population
        GROUP BY department_code, sex, age
    """).df()

    census_pop["age_band"] = census_pop["age"].map(_BAND_FOR_AGE)
    census_by_band = (
        census_pop.groupby(["department_code", "sex", "age_band"])["pop"]
        .sum()
        .reset_index()
    )

    quint_2022 = quinquennal_df[quinquennal_df["year"] == 2022].copy()

    # Only compare age bands fully covered by the fixture's age range
    census_by_band = census_by_band[census_by_band["age_band"].isin(_COVERED_BANDS)]
    quint_2022 = quint_2022[quint_2022["age_band"].isin(_COVERED_BANDS)]

    merged = census_by_band.merge(
        quint_2022,
        on=["department_code", "sex", "age_band"],
        how="inner",
        suffixes=("_census", "_quint"),
    )

    assert len(merged) > 0, "No matching rows between census and quinquennal"

    merged["rel_diff"] = (
        abs(merged["pop"] - merged["population"]) / merged["population"]
    )

    # Apply per-band tolerances instead of a blanket 20% threshold.
    all_failures = []
    for band, tolerance in _BAND_TOLERANCES.items():
        band_rows = merged[merged["age_band"] == band]
        if len(band_rows) == 0:
            continue
        failures = band_rows[band_rows["rel_diff"] > tolerance]
        if len(failures) > 0:
            cols = ["department_code", "sex", "age_band", "rel_diff"]
            all_failures.append(
                f"  {band} ({tolerance:.0%}): "
                f"{len(failures)} failures\n"
                f"{failures[cols].head(5)}"
            )
    assert len(all_failures) == 0, (
        "Census vs quinquennal per-band tolerance exceeded:\n" + "\n".join(all_failures)
    )


# ---------------------------------------------------------------------------
# Test 2: IRIS aggregation <= department
# ---------------------------------------------------------------------------


def test_iris_sum_leq_department(integration_processor):
    """IRIS population summed by dept should not exceed department population."""
    comparison = integration_processor.conn.execute("""
        SELECT
            d.year, d.month, d.department_code, d.age, d.sex,
            d.population AS dept_pop,
            COALESCE(i.iris_pop, 0) AS iris_pop
        FROM population_department d
        LEFT JOIN (
            SELECT
                year, month, LEFT(iris_code, 2) AS department_code,
                age, sex, SUM(population) AS iris_pop
            FROM population_iris
            GROUP BY year, month, LEFT(iris_code, 2), age, sex
        ) i
          ON d.year = i.year
          AND d.month = i.month
          AND d.department_code = i.department_code
          AND d.age = i.age
          AND d.sex = i.sex
        WHERE i.iris_pop IS NOT NULL
    """).df()

    violations = comparison[comparison["iris_pop"] > comparison["dept_pop"] * 1.01]
    assert len(violations) == 0, (
        f"{len(violations)} rows where IRIS sum exceeds department by >1%:\n"
        f"{violations.head(10)}"
    )


# ---------------------------------------------------------------------------
# Test 3: EPCI aggregation ~ department
# ---------------------------------------------------------------------------


def test_epci_sum_approx_department(integration_processor):
    """EPCI population summed by dept should be within 5% of department population."""
    comparison = integration_processor.conn.execute("""
        WITH epci_by_dept AS (
            SELECT
                year, month, department_code, age, sex,
                SUM(population) AS epci_pop
            FROM population_epci
            GROUP BY year, month, department_code, age, sex
        )
        SELECT
            d.year, d.month, d.department_code, d.age, d.sex,
            d.population AS dept_pop,
            e.epci_pop
        FROM population_department d
        JOIN epci_by_dept e
          ON d.year = e.year
          AND d.month = e.month
          AND d.department_code = e.department_code
          AND d.age = e.age
          AND d.sex = e.sex
    """).df()

    assert len(comparison) > 0, "No matching rows between EPCI and department"

    comparison["ratio"] = comparison["epci_pop"] / comparison["dept_pop"]
    violations = comparison[(comparison["ratio"] < 0.95) | (comparison["ratio"] > 1.05)]
    assert len(violations) == 0, (
        f"{len(violations)} rows where EPCI/dept ratio outside [0.95, 1.05]:\n"
        f"{violations.head(10)}"
    )


# ---------------------------------------------------------------------------
# Test 4: Projected department totals vs quinquennal
# ---------------------------------------------------------------------------


def test_projected_dept_sums_to_quinquennal(integration_processor):
    """Dept yearly population by band should equal quinquennal (month_ratios sum to 1).

    Compares against the quinquennal table as it exists in DuckDB after the
    pipeline (which may include census-derived replacements), not the raw
    downloaded estimates.
    """
    dept_by_band = integration_processor.conn.execute("""
        SELECT
            year,
            department_code,
            sex,
            age,
            SUM(population) AS total_pop
        FROM population_department
        GROUP BY year, department_code, sex, age
    """).df()

    dept_by_band["age_band"] = dept_by_band["age"].map(_BAND_FOR_AGE)
    # Only compare age bands fully covered by the fixture's age range
    dept_by_band = dept_by_band[dept_by_band["age_band"].isin(_COVERED_BANDS)]
    dept_agg = (
        dept_by_band.groupby(["year", "department_code", "sex", "age_band"])[
            "total_pop"
        ]
        .sum()
        .reset_index()
    )

    # Use the quinquennal table from the processor's DuckDB connection,
    # which includes census-derived replacements applied by the pipeline.
    quinq_db = integration_processor.conn.execute("SELECT * FROM quinquennal").df()
    quint_covered = quinq_db[quinq_db["age_band"].isin(_COVERED_BANDS)]
    merged = dept_agg.merge(
        quint_covered,
        on=["year", "department_code", "sex", "age_band"],
        how="inner",
    )

    assert len(merged) > 0, "No matching rows for projected vs quinquennal"

    # Expected: total_pop == population (month_ratios sum to 1, age_ratios sum to 1)
    merged["rel_diff"] = (
        abs(merged["total_pop"] - merged["population"]) / merged["population"]
    )
    failures = merged[merged["rel_diff"] > 0.001]
    assert len(failures) == 0, (
        f"{len(failures)} year/dept/sex/band combos differ "
        f"from quinquennal by >0.1%:\n"
        f"{failures[['year', 'department_code', 'sex', 'rel_diff']].head(10)}"
    )


# ---------------------------------------------------------------------------
# Test 5: Age ratios sum to 1
# ---------------------------------------------------------------------------


def test_age_ratios_sum_to_one(integration_processor):
    """Age ratios within each (year, dept, sex, band) should sum to ~1.0."""
    ratio_sums = integration_processor.conn.execute("""
        SELECT
            year,
            department_code,
            sex,
            age_band,
            SUM(age_ratio) AS ratio_sum
        FROM age_ratios
        GROUP BY year, department_code, sex, age_band
    """).df()

    violations = ratio_sums[
        (ratio_sums["ratio_sum"] < 0.999) | (ratio_sums["ratio_sum"] > 1.001)
    ]
    assert len(violations) == 0, (
        f"{len(violations)} groups where age ratios don't sum to ~1.0:\n"
        f"{violations.head(10)}"
    )


# ---------------------------------------------------------------------------
# Test 6: All months present
# ---------------------------------------------------------------------------


def test_all_months_present(integration_processor):
    """Every year/department/age/sex should have exactly 12 months."""
    month_counts = integration_processor.conn.execute("""
        SELECT
            year,
            department_code,
            age,
            sex,
            COUNT(DISTINCT month) AS n_months
        FROM population_department
        GROUP BY year, department_code, age, sex
        HAVING COUNT(DISTINCT month) != 12
    """).df()

    assert len(month_counts) == 0, (
        f"{len(month_counts)} groups don't have 12 months:\n{month_counts.head(10)}"
    )


# ---------------------------------------------------------------------------
# Test 7: Department coverage
# ---------------------------------------------------------------------------


def test_department_coverage(integration_processor):
    """All expected departments should be present in output."""
    depts = set(
        integration_processor.conn.execute(
            "SELECT DISTINCT department_code FROM population_department"
        )
        .df()["department_code"]
        .tolist()
    )

    missing_metro = set(DEPARTMENTS_METRO) - depts
    assert len(missing_metro) == 0, f"Missing metro departments: {missing_metro}"

    missing_dom = set(DEPARTMENTS_DOM) - depts
    assert len(missing_dom) == 0, f"Missing DOM departments: {missing_dom}"

    missing_mayotte = set(DEPARTMENTS_MAYOTTE) - depts
    assert len(missing_mayotte) == 0, f"Missing Mayotte: {missing_mayotte}"


# ---------------------------------------------------------------------------
# Test 8: No negative or zero populations
# ---------------------------------------------------------------------------


def test_positive_populations(integration_processor):
    """All output rows should have strictly positive population."""
    for table in ["population_department", "population_epci", "population_iris"]:
        result = integration_processor.conn.execute(
            f"SELECT MIN(population) AS min_pop FROM {table}"
        ).fetchone()
        min_pop = result[0]
        assert min_pop is not None, f"No rows in {table}"
        assert min_pop > 0, f"Non-positive population in {table}: min={min_pop}"


# ---------------------------------------------------------------------------
# Test: Projected department population bounds
# ---------------------------------------------------------------------------


def test_projected_department_population_bounds(integration_processor):
    """Individual population cells should stay within plausible bounds."""
    stats = integration_processor.conn.execute("""
        SELECT
            MIN(population) AS min_pop,
            MAX(population) AS max_pop,
            AVG(population) AS avg_pop
        FROM population_department
    """).fetchone()

    min_pop, max_pop, avg_pop = stats
    assert min_pop > 0, f"min population should be > 0, got {min_pop}"
    assert max_pop < 5000, f"max population should be < 5000, got {max_pop}"
    assert 50 <= avg_pop <= 1000, (
        f"avg population should be in [50, 1000], got {avg_pop:.1f}"
    )


# ---------------------------------------------------------------------------
# Test: Year-over-year population change
# ---------------------------------------------------------------------------


def test_year_over_year_population_change(integration_processor):
    """National population per year should not change by more than 3%."""
    yearly = integration_processor.conn.execute("""
        SELECT year, SUM(population) AS total_pop
        FROM population_department
        GROUP BY year
        ORDER BY year
    """).df()

    assert len(yearly) >= 2, "Need at least 2 years for year-over-year comparison"

    for i in range(1, len(yearly)):
        prev = yearly.iloc[i - 1]
        curr = yearly.iloc[i]
        change = abs(curr["total_pop"] - prev["total_pop"]) / prev["total_pop"]
        assert change < 0.03, (
            f"Year {int(curr['year'])} vs {int(prev['year'])}: "
            f"{change:.2%} change exceeds 3% threshold"
        )


# ---------------------------------------------------------------------------
# Test: No NULL critical columns
# ---------------------------------------------------------------------------


def test_no_null_critical_columns(integration_processor):
    """Critical columns should have zero NULLs in output tables."""
    checks = {
        "population_department": [
            "year",
            "month",
            "department_code",
            "age",
            "sex",
            "population",
        ],
        "population_epci": [
            "year",
            "month",
            "department_code",
            "epci_code",
            "age",
            "sex",
            "population",
        ],
    }

    for table, columns in checks.items():
        null_filters = ", ".join(
            f"COUNT(*) FILTER (WHERE {col} IS NULL) AS {col}_nulls" for col in columns
        )
        result = integration_processor.conn.execute(
            f"SELECT {null_filters} FROM {table}"
        ).fetchone()

        for col, null_count in zip(columns, result, strict=True):
            assert null_count == 0, f"{table}.{col} has {null_count} NULL values"


# ---------------------------------------------------------------------------
# Census-mode fixture (no projection)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def census_processor():
    """Run census-only pipeline (no start_year/end_year) and return the processor."""
    for fname in REQUIRED_CACHE_FILES:
        if not (CACHE_DIR / fname).exists():
            pytest.skip(f"Missing cache file: {fname}")

    proc = PopulationProcessor(year=2022, min_age=0, max_age=25, cache_dir=CACHE_DIR)
    proc.download_and_process()
    proc.create_multi_level_tables()
    return proc


# ---------------------------------------------------------------------------
# Test 9: Precision degrades at finer geographic levels
# ---------------------------------------------------------------------------


def test_precision_degrades_at_finer_levels(census_processor):
    """IRIS and EPCI coverage ratios degrade relative to department totals."""
    coverage = census_processor.conn.execute("""
        WITH dept_totals AS (
            SELECT department_code, SUM(population) AS dept_pop
            FROM population_department
            GROUP BY department_code
        ),
        epci_by_dept AS (
            SELECT department_code, SUM(population) AS epci_pop
            FROM population_epci
            GROUP BY department_code
        ),
        iris_by_dept AS (
            SELECT department_code, SUM(population) AS iris_pop
            FROM population_iris
            GROUP BY department_code
        )
        SELECT
            d.department_code,
            d.dept_pop,
            COALESCE(e.epci_pop, 0) AS epci_pop,
            COALESCE(i.iris_pop, 0) AS iris_pop,
            COALESCE(e.epci_pop, 0) / d.dept_pop AS epci_ratio,
            COALESCE(i.iris_pop, 0) / d.dept_pop AS iris_ratio
        FROM dept_totals d
        LEFT JOIN epci_by_dept e ON d.department_code = e.department_code
        LEFT JOIN iris_by_dept i ON d.department_code = i.department_code
    """).df()

    avg_epci = coverage["epci_ratio"].mean()
    avg_iris = coverage["iris_ratio"].mean()

    assert avg_epci >= 0.95, f"EPCI national avg coverage {avg_epci:.3f} < 0.95"
    assert 0.40 <= avg_iris <= 0.80, (
        f"IRIS national avg coverage {avg_iris:.3f} outside [0.40, 0.80]"
    )

    # IRIS coverage should generally not exceed EPCI coverage.
    # A few departments may have slightly higher IRIS than EPCI coverage because
    # the IRIS table uses direct commune->EPCI joins while the EPCI table also
    # distributes ZZZZZZZZZ rows via canton weights (which may drop some pop).
    # Allow up to 5% tolerance and at most 3 outlier departments.
    violations = coverage[coverage["iris_ratio"] > coverage["epci_ratio"] + 0.05]
    assert len(violations) <= 3, (
        f"{len(violations)} departments where IRIS > EPCI + 5%:\n"
        f"{violations[['department_code', 'epci_ratio', 'iris_ratio']].head(10)}"
    )


# ---------------------------------------------------------------------------
# Test 10: IRIS coverage urban vs rural
# ---------------------------------------------------------------------------


def test_iris_coverage_urban_vs_rural(census_processor):
    """Urban departments have higher IRIS coverage than rural ones."""
    coverage = census_processor.conn.execute("""
        WITH dept_totals AS (
            SELECT department_code, SUM(population) AS dept_pop
            FROM population_department
            GROUP BY department_code
        ),
        iris_by_dept AS (
            SELECT department_code, SUM(population) AS iris_pop
            FROM population_iris
            GROUP BY department_code
        )
        SELECT
            d.department_code,
            COALESCE(i.iris_pop, 0) / d.dept_pop AS iris_ratio
        FROM dept_totals d
        LEFT JOIN iris_by_dept i ON d.department_code = i.department_code
    """).df()

    coverage = coverage.set_index("department_code")

    urban_depts = ["75", "69", "13"]
    rural_depts = ["23", "15", "46"]

    for dept in urban_depts:
        ratio = coverage.loc[dept, "iris_ratio"]
        assert ratio > 0.70, f"Urban dept {dept} IRIS coverage {ratio:.3f} <= 0.70"

    for dept in rural_depts:
        ratio = coverage.loc[dept, "iris_ratio"]
        assert ratio < 0.50, f"Rural dept {dept} IRIS coverage {ratio:.3f} >= 0.50"


# ---------------------------------------------------------------------------
# Test 11: EPCI geo_precision distribution (census mode only)
# ---------------------------------------------------------------------------


def test_epci_geo_precision_distribution(census_processor):
    """Census-mode EPCI table has meaningful exact vs weighted precision split."""
    precision = census_processor.conn.execute("""
        SELECT
            department_code,
            SUM(CASE WHEN geo_precision = 'exact' THEN population ELSE 0 END)
                AS exact_pop,
            SUM(population) AS total_pop
        FROM population_epci
        GROUP BY department_code
    """).df()

    precision["exact_pct"] = precision["exact_pop"] / precision["total_pop"]

    # National weighted average of exact precision.
    # Most INDCVI rows have iris_code='ZZZZZZZZZ' (no fine geo) and go through
    # canton distribution, so exact_pct is typically low (~5-10%).
    national_exact_pct = precision["exact_pop"].sum() / precision["total_pop"].sum()
    assert national_exact_pct > 0, (
        f"National exact precision should be > 0, got {national_exact_pct:.3f}"
    )

    # Most INDCVI rows have iris_code='ZZZZZZZZZ' and go through canton
    # distribution, so many departments have 0% exact.  At least some
    # departments should have non-zero exact precision.
    nonzero_exact = precision[precision["exact_pct"] > 0]
    assert len(nonzero_exact) > 0, "No department has any exact-precision EPCI rows"


# ---------------------------------------------------------------------------
# Test 12: National census vs quinquennal (tighter than per-department)
# ---------------------------------------------------------------------------


def test_national_census_vs_quinquennal(integration_processor, quinquennal_df):
    """At national level, census and quinquennal should agree within 1.5%."""
    census_pop = integration_processor.conn.execute("""
        SELECT sex, age, SUM(population) AS pop
        FROM population
        GROUP BY sex, age
    """).df()

    census_pop["age_band"] = census_pop["age"].map(_BAND_FOR_AGE)
    census_national = census_pop.groupby(["sex", "age_band"])["pop"].sum().reset_index()
    census_national = census_national[census_national["age_band"].isin(_COVERED_BANDS)]

    quint_2022 = quinquennal_df[quinquennal_df["year"] == 2022].copy()
    quint_national = (
        quint_2022.groupby(["sex", "age_band"])["population"].sum().reset_index()
    )
    quint_national = quint_national[quint_national["age_band"].isin(_COVERED_BANDS)]

    merged = census_national.merge(quint_national, on=["sex", "age_band"], how="inner")
    assert len(merged) > 0, "No matching national-level rows"

    merged["rel_diff"] = (
        abs(merged["pop"] - merged["population"]) / merged["population"]
    )
    failures = merged[merged["rel_diff"] > 0.015]
    assert len(failures) == 0, (
        f"{len(failures)} national sex/band combos exceed 1.5% difference:\n"
        f"{failures[['sex', 'age_band', 'rel_diff']].to_string()}"
    )


# ---------------------------------------------------------------------------
# Test 13: Student mobility causes 20_24 band to have highest discrepancy
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Test: Student mobility correction (EPCI geo ratios)
# ---------------------------------------------------------------------------


MOBSCO_CACHE_FILE = "mobsco_2022.parquet"


def test_student_mobility_correction_valid(integration_processor):
    """Run full pipeline with student mobility correction and validate."""
    if not (CACHE_DIR / MOBSCO_CACHE_FILE).exists():
        pytest.skip(f"Missing cache file: {MOBSCO_CACHE_FILE}")

    proc = PopulationProcessor(
        year=2022,
        min_age=0,
        max_age=25,
        start_year=2022,
        end_year=2024,
        correct_student_mobility=True,
        cache_dir=CACHE_DIR,
    )
    proc.download_and_process()
    proc.create_multi_level_tables()

    # All populations should be positive
    for table in ["population_department", "population_epci", "population_iris"]:
        min_pop = proc.conn.execute(f"SELECT MIN(population) FROM {table}").fetchone()[
            0
        ]
        assert min_pop > 0, f"Non-positive population in {table}: {min_pop}"

    # EPCI population by dept should still be within 5% of department
    comparison = proc.conn.execute("""
        WITH epci_by_dept AS (
            SELECT year, month, department_code, age, sex,
                   SUM(population) AS epci_pop
            FROM population_epci
            GROUP BY year, month, department_code, age, sex
        )
        SELECT
            d.year, d.month, d.department_code,
            d.population AS dept_pop,
            e.epci_pop
        FROM population_department d
        JOIN epci_by_dept e
          ON d.year = e.year AND d.month = e.month
          AND d.department_code = e.department_code
          AND d.age = e.age AND d.sex = e.sex
    """).df()

    if len(comparison) > 0:
        comparison["ratio"] = comparison["epci_pop"] / comparison["dept_pop"]
        violations = comparison[
            (comparison["ratio"] < 0.95) | (comparison["ratio"] > 1.05)
        ]
        assert len(violations) == 0, (
            f"{len(violations)} rows where EPCI/dept ratio outside [0.95, 1.05] "
            f"with student mobility correction:\n{violations.head(10)}"
        )


# ---------------------------------------------------------------------------
# Test 13: Student mobility causes 20_24 band to have highest discrepancy
# ---------------------------------------------------------------------------


def test_student_mobility_band_discrepancy(integration_processor, quinquennal_df):
    """20_24 has highest median dept discrepancy (student mobility)."""
    import numpy as np

    census_pop = integration_processor.conn.execute("""
        SELECT department_code, sex, age, SUM(population) AS pop
        FROM population
        GROUP BY department_code, sex, age
    """).df()

    census_pop["age_band"] = census_pop["age"].map(_BAND_FOR_AGE)
    census_by_band = (
        census_pop.groupby(["department_code", "sex", "age_band"])["pop"]
        .sum()
        .reset_index()
    )
    census_by_band = census_by_band[census_by_band["age_band"].isin(_COVERED_BANDS)]

    quint_2022 = quinquennal_df[quinquennal_df["year"] == 2022].copy()
    quint_2022 = quint_2022[quint_2022["age_band"].isin(_COVERED_BANDS)]

    merged = census_by_band.merge(
        quint_2022,
        on=["department_code", "sex", "age_band"],
        how="inner",
    )
    merged["rel_diff"] = (
        abs(merged["pop"] - merged["population"]) / merged["population"]
    )

    # Median relative difference per age band across departments
    band_medians = merged.groupby("age_band")["rel_diff"].median()

    # 20_24 should have the highest median discrepancy
    worst_band = band_medians.idxmax()
    assert worst_band == "20_24", (
        f"Expected '20_24' to have highest median discrepancy, "
        f"got '{worst_band}': {band_medians.to_dict()}"
    )

    # 20_24 median should be >2x the median of other bands
    median_20_24 = band_medians["20_24"]
    other_medians = band_medians.drop("20_24")
    median_others = np.median(other_medians.values)
    assert median_20_24 > 2 * median_others, (
        f"20_24 median ({median_20_24:.4f}) should be >2x "
        f"others median ({median_others:.4f})"
    )


# ---------------------------------------------------------------------------
# Corrected processor fixture (with student mobility correction)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def corrected_processor():
    """Run pipeline with student mobility correction and return the processor."""
    for fname in REQUIRED_CACHE_FILES:
        if not (CACHE_DIR / fname).exists():
            pytest.skip(f"Missing cache file: {fname}")
    if not (CACHE_DIR / MOBSCO_CACHE_FILE).exists():
        pytest.skip(f"Missing cache file: {MOBSCO_CACHE_FILE}")

    proc = PopulationProcessor(
        year=2022,
        min_age=0,
        max_age=25,
        start_year=2022,
        end_year=2024,
        correct_student_mobility=True,
        cache_dir=CACHE_DIR,
    )
    proc.download_and_process()
    proc.create_multi_level_tables()
    return proc


# ---------------------------------------------------------------------------
# Test: Student mobility correction increases 20_24 concentration
# ---------------------------------------------------------------------------


def test_student_mobility_increases_20_24_concentration(
    integration_processor, corrected_processor
):
    """Mobility correction should concentrate 20_24 geo_ratios toward university EPCIs.

    The corrected geo_ratios for the 20_24 band should have higher per-(dept, sex)
    standard deviation than uncorrected, because the correction shifts population
    toward university-city EPCIs (increasing inequality = higher stddev).
    """
    import numpy as np

    def _mean_geo_ratio_stddev(conn):
        ratios = conn.execute("""
            SELECT department_code, sex, epci_code, geo_ratio
            FROM geo_ratios_epci
            WHERE age_band = '20_24'
        """).df()
        stddevs = ratios.groupby(["department_code", "sex"])["geo_ratio"].std().dropna()
        return np.mean(stddevs.values)

    uncorrected_stddev = _mean_geo_ratio_stddev(integration_processor.conn)
    corrected_stddev = _mean_geo_ratio_stddev(corrected_processor.conn)

    assert corrected_stddev > uncorrected_stddev, (
        f"Corrected 20_24 geo_ratio stddev ({corrected_stddev:.6f}) should be "
        f"> uncorrected ({uncorrected_stddev:.6f})"
    )

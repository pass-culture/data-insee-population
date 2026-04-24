"""Multi-year projected population tables (department, EPCI, IRIS).

Two dept-level projection methods are available:

* ``cohort-stable`` (default): for year Y and current age A, take the
  national cohort size ``effectif(birth_year=Y-A, sex)`` from the census
  and redistribute it across departments using the census
  ``(sex, age)`` pattern ``pct_dept(D | sex, age)``. The age-specific
  geographic distribution is frozen at the census pattern — e.g. the
  share of 18-year-olds in dept 50 is assumed constant across years,
  which implicitly captures post-bac migration.

* ``cohort-aging`` (legacy): age each census cohort in place. The 2022
  population of 14-year-olds in Paris becomes the 2026 population of
  18-year-olds in Paris. Useful as a reference / for comparison.

Both methods preserve national cohort size (no mortality, no migration
balance) and degenerate to the census for Y = census_year.
"""

__all__ = [
    "CREATE_PROJECTED_CANTON",
    "CREATE_PROJECTED_DEPARTMENT",
    "CREATE_PROJECTED_DEPARTMENT_COHORT_STABLE",
    "CREATE_PROJECTED_EPCI",
    "CREATE_PROJECTED_IRIS",
]

# Project department-level population by simple cohort aging.
# For projection year Y, a person of age A was age A-(Y-census_year) at census.
# We aggregate census population across geographic sub-levels to get department
# totals, then cross-join with projection years to shift ages forward.
# Includes confidence intervals based on census offset.
# Placeholders {month_select}, {month_factor}, {month_join}, {month_cross_join}
# control monthly vs yearly mode.
CREATE_PROJECTED_DEPARTMENT = """
CREATE OR REPLACE TABLE population_department AS
WITH census_dept AS (
    SELECT
        department_code,
        (SELECT DISTINCT region_code FROM population p2
         WHERE p2.department_code = p.department_code LIMIT 1) AS region_code,
        age,
        sex,
        SUM(population) AS population
    FROM population p
    GROUP BY department_code, age, sex
),
projection_years AS (
    SELECT generate_series AS year
    FROM generate_series({start_year}, {end_year})
),
projected AS (
    SELECT
        py.year,
        {month_select} AS month,
        c.department_code,
        c.region_code,
        (c.age + (py.year - {census_year})) AS age,
        c.sex,
        CAST(c.population AS DOUBLE){month_factor} AS population
    FROM census_dept c
    CROSS JOIN projection_years py
    {month_join}
    WHERE (c.age + (py.year - {census_year})) BETWEEN {min_age} AND {max_age}
)
SELECT
    year,
    month,
    MAKE_DATE(year, month, 1) AS snapshot_month,
    MAKE_DATE(year - age, 1, 1) AS born_date,
    DATEDIFF('month', MAKE_DATE(year - age, 1, 1),
            MAKE_DATE(year, month, 1)) / 12.0 AS decimal_age,
    department_code,
    region_code,
    age,
    sex,
    'exact' AS geo_precision,
    population,
    CASE
        WHEN ABS(year - {census_year}) <= 1 THEN {ci_base_near}
        WHEN ABS(year - {census_year}) <= 3 THEN {ci_base_mid}
        ELSE {ci_per_year} * ABS(year - {census_year})
    END AS confidence_pct,
    population * (1.0 - CASE
        WHEN ABS(year - {census_year}) <= 1 THEN {ci_base_near}
        WHEN ABS(year - {census_year}) <= 3 THEN {ci_base_mid}
        ELSE {ci_per_year} * ABS(year - {census_year})
    END) AS population_low,
    population * (1.0 + CASE
        WHEN ABS(year - {census_year}) <= 1 THEN {ci_base_near}
        WHEN ABS(year - {census_year}) <= 3 THEN {ci_base_mid}
        ELSE {ci_per_year} * ABS(year - {census_year})
    END) AS population_high
FROM projected
WHERE population > 0
"""

# Project department-level population by cohort-stable (docx) method.
# For year Y and current age A:
#   pop(Y, D, A, S) = cohort_totals(age_at_census = A - (Y - census_year), S)
#                   * dept_share(D | sex = S, age = A)
# where dept_share is frozen at the census pattern for age A (NOT the
# cohort age). Preserves national cohort size across years and renews
# age-specific dept distribution each year.
# Placeholders {month_select}, {month_factor}, {month_join} match the
# cohort-aging template so monthly vs yearly mode works identically.
CREATE_PROJECTED_DEPARTMENT_COHORT_STABLE = """
CREATE OR REPLACE TABLE population_department AS
WITH census_dept AS (
    SELECT
        department_code,
        (SELECT DISTINCT region_code FROM population p2
         WHERE p2.department_code = p.department_code LIMIT 1) AS region_code,
        age,
        sex,
        SUM(population) AS population
    FROM population p
    GROUP BY department_code, age, sex
),
cohort_totals AS (
    SELECT age, sex, SUM(population) AS total_effectif
    FROM census_dept
    GROUP BY age, sex
),
dept_age_sex_shares AS (
    SELECT
        cd.department_code,
        cd.region_code,
        cd.age,
        cd.sex,
        cd.population / NULLIF(ct.total_effectif, 0) AS dept_share
    FROM census_dept cd
    JOIN cohort_totals ct
        ON cd.age = ct.age AND cd.sex = ct.sex
),
projection_years AS (
    SELECT generate_series AS year
    FROM generate_series({start_year}, {end_year})
),
projected AS (
    SELECT
        py.year,
        {month_select} AS month,
        ds.department_code,
        ds.region_code,
        ds.age AS age,
        ds.sex,
        CAST(ct.total_effectif AS DOUBLE){month_factor} * ds.dept_share AS population
    FROM projection_years py
    CROSS JOIN dept_age_sex_shares ds
    JOIN cohort_totals ct
        ON ct.age = ds.age - (py.year - {census_year})
        AND ct.sex = ds.sex
    {month_join}
    WHERE ds.age BETWEEN {min_age} AND {max_age}
)
SELECT
    year,
    month,
    MAKE_DATE(year, month, 1) AS snapshot_month,
    MAKE_DATE(year - age, 1, 1) AS born_date,
    DATEDIFF('month', MAKE_DATE(year - age, 1, 1),
            MAKE_DATE(year, month, 1)) / 12.0 AS decimal_age,
    department_code,
    region_code,
    age,
    sex,
    'exact' AS geo_precision,
    population,
    CASE
        WHEN ABS(year - {census_year}) <= 1 THEN {ci_base_near}
        WHEN ABS(year - {census_year}) <= 3 THEN {ci_base_mid}
        ELSE {ci_per_year} * ABS(year - {census_year})
    END AS confidence_pct,
    population * (1.0 - CASE
        WHEN ABS(year - {census_year}) <= 1 THEN {ci_base_near}
        WHEN ABS(year - {census_year}) <= 3 THEN {ci_base_mid}
        ELSE {ci_per_year} * ABS(year - {census_year})
    END) AS population_low,
    population * (1.0 + CASE
        WHEN ABS(year - {census_year}) <= 1 THEN {ci_base_near}
        WHEN ABS(year - {census_year}) <= 3 THEN {ci_base_mid}
        ELSE {ci_per_year} * ABS(year - {census_year})
    END) AS population_high
FROM projected
WHERE population > 0
"""

# Template for geo-level population projection: department projection * geo_ratio.
# Parameterized by level name, geo columns, and CI extra placeholder.
# Uses {{ }} for runtime placeholders (age_band_cases, ci_extra_{level}).
_PROJECTED_GEO_TEMPLATE = """
CREATE OR REPLACE TABLE population_{level} AS
WITH age_band_map AS (
    SELECT
        age,
        CASE
            {{age_band_cases}}
        END AS age_band
    FROM generate_series(0, {{max_age}}) AS t(age)
)
SELECT
    pd.year,
    pd.month,
    pd.snapshot_month,
    pd.born_date,
    pd.decimal_age,
    {geo_columns}
    pd.age,
    pd.sex,
    'exact' AS geo_precision,
    pd.population * gr.geo_ratio AS population,
    pd.confidence_pct + {{ci_extra_{level}}} AS confidence_pct,
    pd.population * gr.geo_ratio * (1.0 - (pd.confidence_pct + {{ci_extra_{level}}}))
        AS population_low,
    pd.population * gr.geo_ratio * (1.0 + (pd.confidence_pct + {{ci_extra_{level}}}))
        AS population_high
FROM population_department pd
JOIN age_band_map abm ON pd.age = abm.age
JOIN geo_ratios_{level} gr
    ON pd.department_code = gr.department_code
    AND abm.age_band = gr.age_band
    AND pd.sex = gr.sex
WHERE pd.population * gr.geo_ratio > 0
"""

# Generate geo-level projection SQL from template.
# The geo_columns differ per level to match each ratio table's column set.
CREATE_PROJECTED_EPCI = _PROJECTED_GEO_TEMPLATE.format(
    level="epci",
    geo_columns="""pd.department_code,
    pd.region_code,
    gr.epci_code,""",
)

CREATE_PROJECTED_CANTON = _PROJECTED_GEO_TEMPLATE.format(
    level="canton",
    geo_columns="""pd.department_code,
    gr.region_code,
    gr.canton_code,""",
)

CREATE_PROJECTED_IRIS = _PROJECTED_GEO_TEMPLATE.format(
    level="iris",
    geo_columns="""gr.department_code,
    gr.region_code,
    gr.epci_code,
    gr.commune_code,
    gr.iris_code,""",
)

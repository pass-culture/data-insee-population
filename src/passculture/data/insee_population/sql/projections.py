"""Multi-year projected population tables (department, EPCI, IRIS)."""

__all__ = [
    "CREATE_PROJECTED_CANTON",
    "CREATE_PROJECTED_DEPARTMENT",
    "CREATE_PROJECTED_EPCI",
    "CREATE_PROJECTED_IRIS",
    "EXTEND_DEPARTMENT_WITH_CAGR",
]

# Project department-level population: quinquennal * age_ratio (* month_ratio)
# Includes confidence intervals based on census offset.
# Placeholders {month_select}, {month_factor}, {month_join}, {month_cross_join}
# control monthly vs yearly mode.
CREATE_PROJECTED_DEPARTMENT = """
CREATE OR REPLACE TABLE population_department AS
WITH base AS (
    SELECT
        q.year,
        {month_select} AS month,
        q.department_code,
        (SELECT DISTINCT region_code FROM population p
         WHERE p.department_code = q.department_code LIMIT 1) AS region_code,
        ar.age,
        q.sex,
        q.population * ar.age_ratio{month_factor} AS population
    FROM quinquennal q
    JOIN age_ratios ar
        ON q.year = ar.year
        AND q.department_code = ar.department_code
        AND q.sex = ar.sex
        AND q.age_band = ar.age_band
    {month_join}
    WHERE ar.age BETWEEN {min_age} AND {max_age}
),
base_with_fallback AS (
    SELECT * FROM base
    UNION ALL
    SELECT
        q.year,
        {month_select} AS month,
        q.department_code,
        NULL AS region_code,
        ar.age,
        q.sex,
        q.population * ar.age_ratio{month_factor} AS population
    FROM quinquennal q
    JOIN age_ratios_fallback ar
        ON q.year = ar.year
        AND q.sex = ar.sex
        AND q.age_band = ar.age_band
    {month_cross_join}
    WHERE ar.age BETWEEN {min_age} AND {max_age}
      AND q.department_code NOT IN (SELECT DISTINCT department_code FROM age_ratios)
      AND q.department_code IN (
          SELECT DISTINCT department_code FROM monthly_births
          UNION
          SELECT q2.department_code FROM quinquennal q2
      )
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
FROM base_with_fallback
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

# Extend population_department beyond max_data_year using CAGR computed
# from the last TREND_YEARS of the *projected* output.
# This ensures the growth rate reflects the full pipeline (quinquennal *
# age_ratio * month_ratio), not just the raw quinquennal input.
# Confidence interval grows with census offset for extended years.
EXTEND_DEPARTMENT_WITH_CAGR = """
CREATE OR REPLACE TABLE population_department AS
WITH kept AS (
    SELECT * FROM population_department
    WHERE year <= {max_data_year}
),
boundary AS (
    SELECT
        department_code, month, region_code, age, sex, geo_precision,
        MAX(CASE WHEN year = {max_data_year} THEN population END)
            AS last_pop,
        MAX(CASE WHEN year = {first_trend_year} THEN population END)
            AS first_pop
    FROM population_department
    WHERE year IN ({max_data_year}, {first_trend_year})
    GROUP BY department_code, month, region_code, age, sex, geo_precision
),
cagr AS (
    SELECT *,
        CASE
            WHEN first_pop > 0 AND last_pop > 0 THEN
                GREATEST(-{cagr_rate_clamp}, LEAST({cagr_rate_clamp},
                    POWER(last_pop / first_pop,
                          1.0 / ({max_data_year} - {first_trend_year})) - 1
                ))
            ELSE 0
        END AS rate
    FROM boundary
    WHERE last_pop > 0
),
future_years AS (
    SELECT generate_series AS year
    FROM generate_series({max_data_year} + 1, {end_year})
),
extended AS (
    SELECT
        fy.year,
        c.month,
        MAKE_DATE(fy.year, c.month, 1) AS snapshot_month,
        MAKE_DATE(fy.year - c.age, 1, 1) AS born_date,
        DATEDIFF('month', MAKE_DATE(fy.year - c.age, 1, 1),
                MAKE_DATE(fy.year, c.month, 1)) / 12.0
            AS decimal_age,
        c.department_code,
        c.region_code,
        c.age,
        c.sex,
        c.geo_precision,
        c.last_pop
            * POWER(1 + c.rate, fy.year - {max_data_year}) AS population,
        CASE
            WHEN ABS(fy.year - {census_year}) <= 1 THEN {ci_base_near}
            WHEN ABS(fy.year - {census_year}) <= 3 THEN {ci_base_mid}
            ELSE {ci_per_year} * ABS(fy.year - {census_year})
        END AS confidence_pct,
        c.last_pop * POWER(1 + c.rate, fy.year - {max_data_year})
            * (1.0 - CASE
                WHEN ABS(fy.year - {census_year}) <= 1 THEN {ci_base_near}
                WHEN ABS(fy.year - {census_year}) <= 3 THEN {ci_base_mid}
                ELSE {ci_per_year} * ABS(fy.year - {census_year})
            END) AS population_low,
        c.last_pop * POWER(1 + c.rate, fy.year - {max_data_year})
            * (1.0 + CASE
                WHEN ABS(fy.year - {census_year}) <= 1 THEN {ci_base_near}
                WHEN ABS(fy.year - {census_year}) <= 3 THEN {ci_base_mid}
                ELSE {ci_per_year} * ABS(fy.year - {census_year})
            END) AS population_high
    FROM cagr c
    CROSS JOIN future_years fy
)
SELECT * FROM kept
UNION ALL
SELECT * FROM extended
"""

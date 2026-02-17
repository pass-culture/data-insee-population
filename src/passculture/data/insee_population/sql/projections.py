"""Multi-year projected population tables (department, EPCI, IRIS)."""

__all__ = [
    "CREATE_PROJECTED_DEPARTMENT",
    "CREATE_PROJECTED_EPCI",
    "CREATE_PROJECTED_IRIS",
    "EXTEND_DEPARTMENT_WITH_CAGR",
]

# Project department-level population: quinquennal * age_ratio * month_ratio
CREATE_PROJECTED_DEPARTMENT = """
CREATE OR REPLACE TABLE population_department AS
WITH base AS (
    SELECT
        q.year,
        mb.month,
        q.department_code,
        (SELECT DISTINCT region_code FROM population p
         WHERE p.department_code = q.department_code LIMIT 1) AS region_code,
        ar.age,
        q.sex,
        q.population * ar.age_ratio * mb.month_ratio AS population
    FROM quinquennal q
    JOIN age_ratios ar
        ON q.year = ar.year
        AND q.department_code = ar.department_code
        AND q.sex = ar.sex
        AND q.age_band = ar.age_band
    JOIN monthly_births mb
        ON q.department_code = mb.department_code
    WHERE ar.age BETWEEN {min_age} AND {max_age}
),
base_with_fallback AS (
    SELECT * FROM base
    UNION ALL
    SELECT
        q.year,
        mb.month,
        q.department_code,
        NULL AS region_code,
        ar.age,
        q.sex,
        q.population * ar.age_ratio * mb.month_ratio AS population
    FROM quinquennal q
    JOIN age_ratios_fallback ar
        ON q.year = ar.year
        AND q.sex = ar.sex
        AND q.age_band = ar.age_band
    CROSS JOIN (SELECT DISTINCT month, month_ratio FROM monthly_births) mb
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
    MAKE_DATE(year, month, 1) AS current_date,
    MAKE_DATE(year - age, month, 1) AS born_date,
    CAST(age AS DOUBLE) + (CAST(month - 1 AS DOUBLE) / 12.0) AS decimal_age,
    department_code,
    region_code,
    age,
    sex,
    'exact' AS geo_precision,
    population
FROM base_with_fallback
WHERE population > 0
ORDER BY year, month, department_code, age, sex
"""

# Project EPCI-level population: department projection * geo_ratio
CREATE_PROJECTED_EPCI = """
CREATE OR REPLACE TABLE population_epci AS
WITH age_band_map AS (
    SELECT
        age,
        CASE
            {age_band_cases}
        END AS age_band
    FROM generate_series(0, 120) AS t(age)
)
SELECT
    pd.year,
    pd.month,
    pd.current_date,
    pd.born_date,
    pd.decimal_age,
    pd.department_code,
    pd.region_code,
    gr.epci_code,
    pd.age,
    pd.sex,
    'exact' AS geo_precision,
    pd.population * gr.geo_ratio AS population
FROM population_department pd
JOIN age_band_map abm ON pd.age = abm.age
JOIN geo_ratios_epci gr
    ON pd.department_code = gr.department_code
    AND abm.age_band = gr.age_band
    AND pd.sex = gr.sex
WHERE pd.population * gr.geo_ratio > 0
ORDER BY pd.year, pd.month, gr.epci_code, pd.age, pd.sex
"""

# Project IRIS-level population: department projection * geo_ratio
CREATE_PROJECTED_IRIS = """
CREATE OR REPLACE TABLE population_iris AS
WITH age_band_map AS (
    SELECT
        age,
        CASE
            {age_band_cases}
        END AS age_band
    FROM generate_series(0, 120) AS t(age)
)
SELECT
    pd.year,
    pd.month,
    pd.current_date,
    pd.born_date,
    pd.decimal_age,
    gr.department_code,
    gr.region_code,
    gr.epci_code,
    gr.commune_code,
    gr.iris_code,
    pd.age,
    pd.sex,
    'exact' AS geo_precision,
    pd.population * gr.geo_ratio AS population
FROM population_department pd
JOIN age_band_map abm ON pd.age = abm.age
JOIN geo_ratios_iris gr
    ON pd.department_code = gr.department_code
    AND abm.age_band = gr.age_band
    AND pd.sex = gr.sex
WHERE pd.population * gr.geo_ratio > 0
ORDER BY pd.year, pd.month, gr.iris_code, pd.age, pd.sex
"""

# Extend population_department beyond max_data_year using CAGR computed
# from the last TREND_YEARS of the *projected* output.
# This ensures the growth rate reflects the full pipeline (quinquennal *
# age_ratio * month_ratio), not just the raw quinquennal input.
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
                GREATEST(-0.05, LEAST(0.05,
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
        MAKE_DATE(fy.year, c.month, 1) AS current_date,
        MAKE_DATE(fy.year - c.age, c.month, 1) AS born_date,
        CAST(c.age AS DOUBLE)
            + (CAST(c.month - 1 AS DOUBLE) / 12.0) AS decimal_age,
        c.department_code,
        c.region_code,
        c.age,
        c.sex,
        c.geo_precision,
        c.last_pop
            * POWER(1 + c.rate, fy.year - {max_data_year}) AS population
    FROM cagr c
    CROSS JOIN future_years fy
)
SELECT * FROM kept
UNION ALL
SELECT * FROM extended
ORDER BY year, month, department_code, age, sex
"""

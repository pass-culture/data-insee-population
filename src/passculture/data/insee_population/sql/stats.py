"""Output queries, statistics, and validation templates."""

__all__ = [
    "COUNT_INVALID_POPULATION",
    "GET_CANTON_SUMMARY",
    "GET_DEPARTMENT_SUMMARY",
    "GET_DISTINCT_DEPARTMENTS",
    "GET_EPCI_SUMMARY",
    "GET_IRIS_SUMMARY",
    "GET_ROW_COUNT",
    "GET_VALIDATION_STATS",
    "SELECT_WITH_BIRTH_MONTH",
]

# Streaming birth-month expansion: reads compact table and joins with
# monthly_births on-the-fly.  Avoids materialising 12x rows in memory.
# Placeholders: {level} = table suffix, {geo_columns} = level-specific cols.
SELECT_WITH_BIRTH_MONTH = """
SELECT
    pd.year,
    pd.month,
    bb.month AS birth_month,
    pd.snapshot_month,
    MAKE_DATE(pd.year - pd.age, bb.month, 1) AS born_date,
    DATEDIFF('month', MAKE_DATE(pd.year - pd.age, bb.month, 1),
            pd.snapshot_month) / 12.0 AS decimal_age,
    {geo_columns}
    pd.age,
    pd.sex,
    pd.geo_precision,
    pd.population * bb.month_ratio AS population,
    pd.confidence_pct,
    pd.population * bb.month_ratio * (1.0 - pd.confidence_pct)
        AS population_low,
    pd.population * bb.month_ratio * (1.0 + pd.confidence_pct)
        AS population_high
FROM population_{level} pd
JOIN monthly_births bb ON pd.department_code = bb.department_code
WHERE pd.population * bb.month_ratio > 0
"""

GET_ROW_COUNT = "SELECT COUNT(*) FROM population"

GET_VALIDATION_STATS = """
SELECT
    COUNT(*) AS total_rows,
    SUM(population) AS total_population,
    COUNT(DISTINCT department_code) AS departments,
    COUNT(DISTINCT year) AS years,
    MIN(age) AS min_age,
    MAX(age) AS max_age
FROM population
"""

COUNT_INVALID_POPULATION = """
SELECT COUNT(*) FROM population WHERE population IS NULL OR population < 0
"""

GET_DISTINCT_DEPARTMENTS = "SELECT DISTINCT department_code FROM population"

GET_DEPARTMENT_SUMMARY = """
SELECT
    'department' AS geo_level,
    COUNT(*) AS rows,
    SUM(population) AS total_population,
    COUNT(DISTINCT department_code) AS geo_units,
    COUNT(DISTINCT geo_precision) AS precision_types
FROM population_department
"""

GET_IRIS_SUMMARY = """
SELECT
    'iris' AS geo_level,
    COUNT(*) AS rows,
    SUM(population) AS total_population,
    COUNT(DISTINCT iris_code) AS geo_units,
    COUNT(DISTINCT geo_precision) AS precision_types
FROM population_iris
"""

GET_EPCI_SUMMARY = """
SELECT
    'epci' AS geo_level,
    COUNT(*) AS rows,
    SUM(population) AS total_population,
    COUNT(DISTINCT epci_code) AS geo_units,
    COUNT(DISTINCT geo_precision) AS precision_types,
    SUM(CASE WHEN geo_precision = 'exact'
        THEN population ELSE 0 END) AS exact_population,
    SUM(CASE WHEN geo_precision = 'canton_weighted'
        THEN population ELSE 0 END) AS weighted_population
FROM population_epci
"""

GET_CANTON_SUMMARY = """
SELECT
    'canton' AS geo_level,
    COUNT(*) AS rows,
    SUM(population) AS total_population,
    COUNT(DISTINCT canton_code) AS geo_units,
    COUNT(DISTINCT geo_precision) AS precision_types
FROM population_canton
"""

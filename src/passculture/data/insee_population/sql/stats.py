"""Output queries, statistics, and validation templates."""

__all__ = [
    "COPY_CANTON_TO_PARQUET",
    "COPY_DEPARTMENT_TO_PARQUET",
    "COPY_EPCI_TO_PARQUET",
    "COPY_IRIS_TO_PARQUET",
    "COUNT_INVALID_POPULATION",
    "GET_CANTON_SUMMARY",
    "GET_DEPARTMENT_SUMMARY",
    "GET_DISTINCT_DEPARTMENTS",
    "GET_EPCI_SUMMARY",
    "GET_IRIS_SUMMARY",
    "GET_ROW_COUNT",
    "GET_VALIDATION_STATS",
    "SELECT_CANTON",
    "SELECT_DEPARTMENT",
    "SELECT_EPCI",
    "SELECT_IRIS",
]

SELECT_CANTON = "SELECT * FROM population_canton"
SELECT_DEPARTMENT = "SELECT * FROM population_department"
SELECT_IRIS = "SELECT * FROM population_iris"
SELECT_EPCI = "SELECT * FROM population_epci"

COPY_CANTON_TO_PARQUET = "COPY population_canton TO '{path}' (FORMAT PARQUET)"
COPY_DEPARTMENT_TO_PARQUET = "COPY population_department TO '{path}' (FORMAT PARQUET)"
COPY_IRIS_TO_PARQUET = "COPY population_iris TO '{path}' (FORMAT PARQUET)"
COPY_EPCI_TO_PARQUET = "COPY population_epci TO '{path}' (FORMAT PARQUET)"

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

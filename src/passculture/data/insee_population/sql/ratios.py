"""Geographic ratio computation templates."""

__all__ = [
    "CREATE_GEO_RATIOS_CANTON",
    "CREATE_GEO_RATIOS_EPCI",
    "CREATE_GEO_RATIOS_IRIS",
]

# Compute EPCI share within each dept/age_band/sex from INDCVI population
CREATE_GEO_RATIOS_EPCI = """
CREATE OR REPLACE TABLE geo_ratios_epci AS
WITH age_band_map AS (
    SELECT
        age,
        CASE
            {age_band_cases}
        END AS age_band
    FROM generate_series(0, {max_age}) AS t(age)
),
epci_pop AS (
    SELECT
        p.department_code,
        ce.epci_code,
        abm.age_band,
        p.sex,
        SUM(p.population) AS epci_population
    FROM population p
    INNER JOIN commune_epci ce ON p.commune_code = ce.commune_code
    JOIN age_band_map abm ON p.age = abm.age
    WHERE p.commune_code <> '' AND p.iris_code <> '{iris_sentinel_no_geo}'
      AND abm.age_band IS NOT NULL
    GROUP BY p.department_code, ce.epci_code, abm.age_band, p.sex
),
dept_totals AS (
    SELECT
        department_code,
        age_band,
        sex,
        SUM(epci_population) AS dept_population
    FROM epci_pop
    GROUP BY department_code, age_band, sex
),
-- Departments with no commune-level data (e.g. Mayotte) get a passthrough
-- EPCI entry using the department code, with geo_ratio=1.0.
fallback_depts AS (
    SELECT DISTINCT department_code
    FROM population
    WHERE department_code NOT IN (SELECT DISTINCT department_code FROM epci_pop)
      AND iris_code = '{iris_sentinel_no_geo}'
),
synthetic_epci AS (
    SELECT
        p.department_code,
        p.department_code AS epci_code,
        abm.age_band,
        p.sex,
        1.0 AS geo_ratio
    FROM population p
    JOIN age_band_map abm ON p.age = abm.age
    JOIN fallback_depts fd ON p.department_code = fd.department_code
    WHERE abm.age_band IS NOT NULL
    GROUP BY p.department_code, abm.age_band, p.sex
)
SELECT
    ep.department_code,
    ep.epci_code,
    ep.age_band,
    ep.sex,
    CASE
        WHEN dt.dept_population > 0 THEN ep.epci_population / dt.dept_population
        ELSE 0
    END AS geo_ratio
FROM epci_pop ep
JOIN dept_totals dt
    ON ep.department_code = dt.department_code
    AND ep.age_band = dt.age_band
    AND ep.sex = dt.sex
UNION ALL
SELECT * FROM synthetic_epci
"""

# Compute IRIS share within each dept/age_band/sex from INDCVI population
CREATE_GEO_RATIOS_IRIS = """
CREATE OR REPLACE TABLE geo_ratios_iris AS
WITH age_band_map AS (
    SELECT
        age,
        CASE
            {age_band_cases}
        END AS age_band
    FROM generate_series(0, {max_age}) AS t(age)
),
iris_pop AS (
    SELECT
        p.department_code,
        p.region_code,
        p.commune_code,
        p.iris_code,
        ce.epci_code,
        abm.age_band,
        p.sex,
        SUM(p.population) AS iris_population
    FROM population p
    LEFT JOIN commune_epci ce ON p.commune_code = ce.commune_code
    JOIN age_band_map abm ON p.age = abm.age
    WHERE p.iris_code <> '{iris_sentinel_no_geo}'
      AND RIGHT(p.iris_code, 4) <> '{iris_sentinel_masked_suffix}'
      AND LENGTH(p.iris_code) = 9
      AND abm.age_band IS NOT NULL
    GROUP BY p.department_code, p.region_code, p.commune_code,
             p.iris_code, ce.epci_code, abm.age_band, p.sex
),
dept_totals AS (
    SELECT
        department_code,
        age_band,
        sex,
        SUM(iris_population) AS dept_population
    FROM iris_pop
    GROUP BY department_code, age_band, sex
),
-- Departments with no IRIS data get a passthrough entry using the sentinel
-- iris_code already assigned to them, with geo_ratio=1.0.
fallback_depts AS (
    SELECT DISTINCT department_code
    FROM population
    WHERE department_code NOT IN (SELECT DISTINCT department_code FROM iris_pop)
      AND iris_code = '{iris_sentinel_no_geo}'
),
synthetic_iris AS (
    SELECT
        p.department_code,
        p.region_code,
        p.commune_code,
        p.iris_code,
        NULL AS epci_code,
        abm.age_band,
        p.sex,
        1.0 AS geo_ratio
    FROM population p
    JOIN age_band_map abm ON p.age = abm.age
    JOIN fallback_depts fd ON p.department_code = fd.department_code
    WHERE abm.age_band IS NOT NULL
    GROUP BY p.department_code, p.region_code, p.commune_code, p.iris_code,
             abm.age_band, p.sex
)
SELECT
    ip.department_code,
    ip.region_code,
    ip.commune_code,
    ip.iris_code,
    ip.epci_code,
    ip.age_band,
    ip.sex,
    CASE
        WHEN dt.dept_population > 0 THEN ip.iris_population / dt.dept_population
        ELSE 0
    END AS geo_ratio
FROM iris_pop ip
JOIN dept_totals dt
    ON ip.department_code = dt.department_code
    AND ip.age_band = dt.age_band
    AND ip.sex = dt.sex
UNION ALL
SELECT * FROM synthetic_iris
"""

# Compute canton share within each dept/age_band/sex from INDCVI population
CREATE_GEO_RATIOS_CANTON = """
CREATE OR REPLACE TABLE geo_ratios_canton AS
WITH age_band_map AS (
    SELECT
        age,
        CASE
            {age_band_cases}
        END AS age_band
    FROM generate_series(0, {max_age}) AS t(age)
),
canton_pop AS (
    SELECT
        p.department_code,
        p.region_code,
        p.canton_code,
        abm.age_band,
        p.sex,
        SUM(p.population) AS canton_population
    FROM population p
    JOIN age_band_map abm ON p.age = abm.age
    WHERE p.canton_code IS NOT NULL
      AND p.canton_code <> ''
      AND abm.age_band IS NOT NULL
    GROUP BY p.department_code, p.region_code, p.canton_code,
             abm.age_band, p.sex
),
dept_totals AS (
    SELECT
        department_code,
        age_band,
        sex,
        SUM(canton_population) AS dept_population
    FROM canton_pop
    GROUP BY department_code, age_band, sex
)
SELECT
    cp.department_code,
    cp.region_code,
    cp.canton_code,
    cp.age_band,
    cp.sex,
    CASE
        WHEN dt.dept_population > 0 THEN cp.canton_population / dt.dept_population
        ELSE 0
    END AS geo_ratio
FROM canton_pop cp
JOIN dept_totals dt
    ON cp.department_code = dt.department_code
    AND cp.age_band = dt.age_band
    AND cp.sex = dt.sex
"""

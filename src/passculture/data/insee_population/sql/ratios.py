"""Age and geographic ratio computation templates."""

__all__ = [
    "CREATE_AGE_RATIOS",
    "CREATE_AGE_RATIOS_FALLBACK",
    "CREATE_GEO_RATIOS_CANTON",
    "CREATE_GEO_RATIOS_EPCI",
    "CREATE_GEO_RATIOS_IRIS",
]

# Compute cohort-shifted age share within each 5-year band, per year/dept/sex.
# For projection year Y and target age A, looks up census population at
# census_age = A + (census_year - Y), so the actual birth cohort is used.
# Uses the INDCVI-based `population` table as reference distribution.
# Requires `quinquennal` table for projection years.
CREATE_AGE_RATIOS = """
CREATE OR REPLACE TABLE age_ratios AS
WITH age_band_map AS (
    SELECT
        age,
        CASE
            {age_band_cases}
        END AS age_band
    FROM generate_series(0, 120) AS t(age)
),
projection_years AS (
    SELECT DISTINCT year FROM quinquennal
),
target_ages AS (
    SELECT
        py.year,
        abm.age AS target_age,
        abm.age_band,
        GREATEST(0, LEAST(120, abm.age + ({census_year} - py.year))) AS census_age
    FROM projection_years py
    CROSS JOIN age_band_map abm
    WHERE abm.age_band IS NOT NULL
),
census_lookup AS (
    SELECT
        ta.year,
        ta.target_age,
        ta.age_band,
        ta.census_age,
        p.department_code,
        p.sex,
        SUM(p.population) AS census_pop
    FROM target_ages ta
    JOIN population p ON p.age = ta.census_age
    GROUP BY ta.year, ta.target_age, ta.age_band, ta.census_age,
             p.department_code, p.sex
),
band_totals AS (
    SELECT year, age_band, department_code, sex,
           SUM(census_pop) AS band_total
    FROM census_lookup
    GROUP BY year, age_band, department_code, sex
)
SELECT
    cl.year,
    cl.department_code,
    cl.sex,
    cl.age_band,
    cl.target_age AS age,
    CASE WHEN bt.band_total > 0
         THEN cl.census_pop / bt.band_total
         ELSE 1.0 / COUNT(*) OVER (
             PARTITION BY cl.year, cl.department_code, cl.sex, cl.age_band)
    END AS age_ratio
FROM census_lookup cl
JOIN band_totals bt
    ON cl.year = bt.year
    AND cl.department_code = bt.department_code
    AND cl.sex = bt.sex
    AND cl.age_band = bt.age_band
"""

# Compute fallback age ratios (national average) for departments not in INDCVI
CREATE_AGE_RATIOS_FALLBACK = """
CREATE OR REPLACE TABLE age_ratios_fallback AS
SELECT
    year,
    sex,
    age_band,
    age,
    AVG(age_ratio) AS age_ratio
FROM age_ratios
GROUP BY year, sex, age_band, age
"""

# Compute EPCI share within each dept/age_band/sex from INDCVI population
CREATE_GEO_RATIOS_EPCI = """
CREATE OR REPLACE TABLE geo_ratios_epci AS
WITH age_band_map AS (
    SELECT
        age,
        CASE
            {age_band_cases}
        END AS age_band
    FROM generate_series(0, 120) AS t(age)
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
    WHERE p.commune_code <> '' AND p.iris_code <> 'ZZZZZZZZZ'
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
    FROM generate_series(0, 120) AS t(age)
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
    WHERE p.iris_code <> 'ZZZZZZZZZ'
      AND RIGHT(p.iris_code, 4) <> 'XXXX'
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
    FROM generate_series(0, 120) AS t(age)
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

"""Student mobility correction (MOBSCO) templates for EPCI and IRIS."""

from passculture.data.insee_population.constants import (
    IRIS_SENTINEL_MASKED_SUFFIX,
    IRIS_SENTINEL_NO_GEO,
    STUDENT_AGE_BANDS,
)

__all__ = [
    "CREATE_CORRECTED_GEO_RATIOS_EPCI",
    "CREATE_CORRECTED_GEO_RATIOS_IRIS",
    "CREATE_MOBILITY_WEIGHTS",
    "CREATE_STUDENT_FLOWS_EPCI",
    "CREATE_STUDENT_FLOWS_IRIS",
    "RENAME_GEO_RATIOS_EPCI_TO_BASE",
    "RENAME_GEO_RATIOS_IRIS_TO_BASE",
]

# Build SQL UNION for student age bands from constants
_STUDENT_BANDS_SQL = "\n    UNION ALL\n    ".join(
    f"SELECT '{band}' AS age_band" for band in STUDENT_AGE_BANDS
)

RENAME_GEO_RATIOS_EPCI_TO_BASE = (
    "ALTER TABLE geo_ratios_epci RENAME TO geo_ratios_epci_base"
)

# Compute per-department inter-departmental student mobility rate.
# For each department, mobility_rate = fraction of students studying
# in a different department. blend_weight = min(mobility_rate, cap),
# with a fallback default for departments not in MOBSCO.
CREATE_MOBILITY_WEIGHTS = """
CREATE OR REPLACE TABLE mobility_weights AS
WITH mobsco_raw AS (
    SELECT
        CASE
            WHEN LEFT(TRIM(COMMUNE), 2) = '97' THEN LEFT(TRIM(COMMUNE), 3)
            ELSE LEFT(TRIM(COMMUNE), 2)
        END AS residence_dept,
        CASE
            WHEN LEFT(TRIM(DCETUF), 2) = '97' THEN LEFT(TRIM(DCETUF), 3)
            ELSE LEFT(TRIM(DCETUF), 2)
        END AS study_dept,
        CAST(IPONDI AS DOUBLE) AS weight
    FROM read_parquet('{mobsco_path}')
    WHERE TRIM(AGEREV10) = '18'
),
dept_totals AS (
    SELECT
        residence_dept AS department_code,
        SUM(weight) AS total_students,
        SUM(CASE WHEN residence_dept <> study_dept THEN weight ELSE 0 END)
            AS inter_dept_students
    FROM mobsco_raw
    GROUP BY residence_dept
),
rates AS (
    SELECT
        department_code,
        CASE
            WHEN total_students > 0
            THEN inter_dept_students / total_students
            ELSE 0
        END AS mobility_rate
    FROM dept_totals
)
SELECT
    department_code,
    mobility_rate,
    LEAST(mobility_rate, {blend_cap}) AS blend_weight
FROM rates
UNION ALL
SELECT
    d.department_code,
    {blend_default} AS mobility_rate,
    {blend_default} AS blend_weight
FROM (SELECT DISTINCT department_code FROM population) d
WHERE d.department_code NOT IN (SELECT department_code FROM rates)
"""

# Compute study-destination EPCI distribution from MOBSCO parquet.
# AGEREV10='18' covers ages 18-24. We join DCETUF (study commune) to
# commune_epci to find the study EPCI, then compute each study EPCI's share
# of the department's total student flow.
# Department is extracted from COMMUNE (residence): LEFT(COMMUNE,2) for metro,
# LEFT(COMMUNE,3) for DOM (starts with '97').
CREATE_STUDENT_FLOWS_EPCI = """
CREATE OR REPLACE TABLE student_flows_epci AS
WITH mobsco_raw AS (
    SELECT
        CASE
            WHEN LEFT(TRIM(COMMUNE), 2) = '97' THEN LEFT(TRIM(COMMUNE), 3)
            ELSE LEFT(TRIM(COMMUNE), 2)
        END AS department_code,
        TRIM(DCETUF) AS study_commune,
        CASE SEXE WHEN '1' THEN 'male' WHEN '2' THEN 'female' END AS sex,
        CAST(IPONDI AS DOUBLE) AS weight
    FROM read_parquet('{mobsco_path}')
    WHERE TRIM(AGEREV10) = '18'
),
study_epci AS (
    SELECT
        m.department_code,
        ce.epci_code AS study_epci_code,
        m.sex,
        SUM(m.weight) AS study_pop
    FROM mobsco_raw m
    INNER JOIN commune_epci ce ON m.study_commune = ce.commune_code
    WHERE ce.epci_code IS NOT NULL
    GROUP BY m.department_code, ce.epci_code, m.sex
),
dept_totals AS (
    SELECT department_code, sex, SUM(study_pop) AS dept_total
    FROM study_epci
    GROUP BY department_code, sex
)
SELECT
    se.department_code,
    se.study_epci_code AS epci_code,
    se.sex,
    CASE
        WHEN dt.dept_total > 0 THEN se.study_pop / dt.dept_total
        ELSE 0
    END AS study_geo_ratio
FROM study_epci se
JOIN dept_totals dt
    ON se.department_code = dt.department_code
    AND se.sex = dt.sex
"""

# Blend study-based geo ratios into census-based ones for student age bands.
# For bands 15_19 and 20_24: corrected = (1-w)*base + w*study, renormalized.
# Uses per-department blend weights from the mobility_weights table.
# Other bands: unchanged from base.
CREATE_CORRECTED_GEO_RATIOS_EPCI = f"""
CREATE OR REPLACE TABLE geo_ratios_epci AS
WITH student_bands AS (
    {_STUDENT_BANDS_SQL}
),
-- Base ratios for non-student bands (pass through unchanged)
non_student AS (
    SELECT
        b.department_code,
        b.epci_code,
        b.age_band,
        b.sex,
        b.geo_ratio
    FROM geo_ratios_epci_base b
    WHERE b.age_band NOT IN (SELECT age_band FROM student_bands)
),
-- Blended ratios for student bands (per-department blend weights)
blended_raw AS (
    SELECT
        COALESCE(b.department_code, sf.department_code) AS department_code,
        COALESCE(b.epci_code, sf.epci_code) AS epci_code,
        sb.age_band,
        COALESCE(b.sex, sf.sex) AS sex,
        (1.0 - mw.blend_weight) * COALESCE(b.geo_ratio, 0)
            + mw.blend_weight * COALESCE(sf.study_geo_ratio, 0) AS raw_ratio
    FROM student_bands sb
    CROSS JOIN (
        SELECT DISTINCT department_code, sex FROM geo_ratios_epci_base
    ) ds
    JOIN mobility_weights mw ON mw.department_code = ds.department_code
    LEFT JOIN geo_ratios_epci_base b
        ON b.department_code = ds.department_code
        AND b.sex = ds.sex
        AND b.age_band = sb.age_band
    LEFT JOIN student_flows_epci sf
        ON sf.department_code = ds.department_code
        AND sf.sex = ds.sex
        AND sf.epci_code = COALESCE(b.epci_code, sf.epci_code)
    WHERE COALESCE(b.geo_ratio, 0) > 0
       OR COALESCE(sf.study_geo_ratio, 0) > 0
),
-- Renormalize so ratios sum to 1 per (dept, band, sex)
blended_totals AS (
    SELECT department_code, age_band, sex, SUM(raw_ratio) AS total_ratio
    FROM blended_raw
    GROUP BY department_code, age_band, sex
),
blended AS (
    SELECT
        br.department_code,
        br.epci_code,
        br.age_band,
        br.sex,
        CASE
            WHEN bt.total_ratio > 0 THEN br.raw_ratio / bt.total_ratio
            ELSE 0
        END AS geo_ratio
    FROM blended_raw br
    JOIN blended_totals bt
        ON br.department_code = bt.department_code
        AND br.age_band = bt.age_band
        AND br.sex = bt.sex
)
SELECT * FROM non_student
UNION ALL
SELECT * FROM blended
"""

# IRIS student mobility correction (parallel to EPCI above)
RENAME_GEO_RATIOS_IRIS_TO_BASE = (
    "ALTER TABLE geo_ratios_iris RENAME TO geo_ratios_iris_base"
)

# Compute study-destination IRIS distribution from MOBSCO parquet.
# Distributes commune-level MOBSCO flows to IRIS using census population
# proportions within each study commune.
CREATE_STUDENT_FLOWS_IRIS = f"""
CREATE OR REPLACE TABLE student_flows_iris AS
WITH mobsco_raw AS (
    SELECT
        CASE
            WHEN LEFT(TRIM(COMMUNE), 2) = '97' THEN LEFT(TRIM(COMMUNE), 3)
            ELSE LEFT(TRIM(COMMUNE), 2)
        END AS department_code,
        TRIM(DCETUF) AS study_commune,
        CASE SEXE WHEN '1' THEN 'male' WHEN '2' THEN 'female' END AS sex,
        CAST(IPONDI AS DOUBLE) AS weight
    FROM read_parquet('{{mobsco_path}}')
    WHERE TRIM(AGEREV10) = '18'
),
iris_commune_share AS (
    SELECT
        p.commune_code,
        p.department_code,
        p.region_code,
        p.iris_code,
        ce.epci_code,
        SUM(p.population) AS iris_pop,
        SUM(SUM(p.population)) OVER (PARTITION BY p.commune_code) AS commune_pop
    FROM population p
    LEFT JOIN commune_epci ce ON p.commune_code = ce.commune_code
    WHERE p.iris_code <> '{IRIS_SENTINEL_NO_GEO}'
      AND RIGHT(p.iris_code, 4) <> '{IRIS_SENTINEL_MASKED_SUFFIX}'
      AND LENGTH(p.iris_code) = 9
    GROUP BY p.commune_code, p.department_code, p.region_code,
             p.iris_code, ce.epci_code
),
study_iris AS (
    SELECT
        m.department_code,
        ics.department_code AS iris_dept,
        ics.region_code,
        ics.commune_code,
        ics.iris_code,
        ics.epci_code,
        m.sex,
        SUM(m.weight * CASE WHEN ics.commune_pop > 0
            THEN ics.iris_pop / ics.commune_pop ELSE 0 END) AS study_pop
    FROM mobsco_raw m
    JOIN iris_commune_share ics ON m.study_commune = ics.commune_code
    GROUP BY m.department_code, ics.department_code, ics.region_code,
             ics.commune_code, ics.iris_code, ics.epci_code, m.sex
),
dept_totals AS (
    SELECT department_code, sex, SUM(study_pop) AS dept_total
    FROM study_iris
    GROUP BY department_code, sex
)
SELECT
    si.department_code,
    si.region_code,
    si.commune_code,
    si.iris_code,
    si.epci_code,
    si.sex,
    CASE WHEN dt.dept_total > 0 THEN si.study_pop / dt.dept_total ELSE 0
    END AS study_geo_ratio
FROM study_iris si
JOIN dept_totals dt
    ON si.department_code = dt.department_code AND si.sex = dt.sex
"""

# Blend study-based geo ratios into census-based ones for student age bands (IRIS).
# For bands 15_19 and 20_24: corrected = (1-w)*base + w*study, renormalized.
# Uses per-department blend weights from the mobility_weights table.
# Other bands: unchanged from base.
CREATE_CORRECTED_GEO_RATIOS_IRIS = f"""
CREATE OR REPLACE TABLE geo_ratios_iris AS
WITH student_bands AS (
    {_STUDENT_BANDS_SQL}
),
non_student AS (
    SELECT department_code, region_code, commune_code, iris_code,
           epci_code, age_band, sex, geo_ratio
    FROM geo_ratios_iris_base
    WHERE age_band NOT IN (SELECT age_band FROM student_bands)
),
blended_raw AS (
    SELECT
        COALESCE(b.department_code, sf.department_code) AS department_code,
        COALESCE(b.region_code, sf.region_code) AS region_code,
        COALESCE(b.commune_code, sf.commune_code) AS commune_code,
        COALESCE(b.iris_code, sf.iris_code) AS iris_code,
        COALESCE(b.epci_code, sf.epci_code) AS epci_code,
        sb.age_band,
        COALESCE(b.sex, sf.sex) AS sex,
        (1.0 - mw.blend_weight) * COALESCE(b.geo_ratio, 0)
            + mw.blend_weight * COALESCE(sf.study_geo_ratio, 0) AS raw_ratio
    FROM student_bands sb
    CROSS JOIN (
        SELECT DISTINCT department_code, sex FROM geo_ratios_iris_base
    ) ds
    JOIN mobility_weights mw ON mw.department_code = ds.department_code
    LEFT JOIN geo_ratios_iris_base b
        ON b.department_code = ds.department_code
        AND b.sex = ds.sex
        AND b.age_band = sb.age_band
    LEFT JOIN student_flows_iris sf
        ON sf.department_code = ds.department_code
        AND sf.sex = ds.sex
        AND sf.iris_code = COALESCE(b.iris_code, sf.iris_code)
    WHERE COALESCE(b.geo_ratio, 0) > 0
       OR COALESCE(sf.study_geo_ratio, 0) > 0
),
blended_totals AS (
    SELECT department_code, age_band, sex, SUM(raw_ratio) AS total_ratio
    FROM blended_raw
    GROUP BY department_code, age_band, sex
),
blended AS (
    SELECT
        br.department_code, br.region_code, br.commune_code,
        br.iris_code, br.epci_code, br.age_band, br.sex,
        CASE WHEN bt.total_ratio > 0 THEN br.raw_ratio / bt.total_ratio
             ELSE 0 END AS geo_ratio
    FROM blended_raw br
    JOIN blended_totals bt
        ON br.department_code = bt.department_code
        AND br.age_band = bt.age_band
        AND br.sex = bt.sex
)
SELECT * FROM non_student
UNION ALL
SELECT * FROM blended
"""

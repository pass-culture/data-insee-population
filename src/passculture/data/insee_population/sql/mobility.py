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

# Compute per-department, per-age-band inter-departmental student mobility rate.
#
# Each age band uses its own MOBSCO AGEREV10 group:
#   15_19 -> primary AGEREV10='15' (lycee students, ages 15-17)
#            secondary AGEREV10='18' (higher-ed, for the ~25% that are ages 18-19
#            in higher education); effective_rate = 0.75*lycee + 0.25*higher_ed
#   20_24 -> AGEREV10='18' (higher-education students, ages 18-24)
#
# blend_weight = min(effective_rate, per-band cap), with per-band fallback default
# for departments not in MOBSCO.
#
# Format parameters (supplied by projections.py from constants):
#   {mobsco_path}       — path to MOBSCO parquet
#   {band_config_sql}   — VALUES rows: (age_band, agerev10, blend_cap, blend_default,
#                          secondary_agerev10, secondary_weight)
CREATE_MOBILITY_WEIGHTS = """
CREATE OR REPLACE TABLE mobility_weights AS
WITH band_config AS (
    {band_config_sql}
),
mobsco_primary AS (
    SELECT
        bc.age_band,
        CASE
            WHEN LEFT(TRIM(m.COMMUNE), 2) = '97' THEN LEFT(TRIM(m.COMMUNE), 3)
            ELSE LEFT(TRIM(m.COMMUNE), 2)
        END AS residence_dept,
        CASE
            WHEN LEFT(TRIM(m.DCETUF), 2) = '97' THEN LEFT(TRIM(m.DCETUF), 3)
            ELSE LEFT(TRIM(m.DCETUF), 2)
        END AS study_dept,
        CAST(m.IPONDI AS DOUBLE) AS weight
    FROM read_parquet('{mobsco_path}') m
    JOIN band_config bc ON TRIM(m.AGEREV10) = bc.agerev10
),
mobsco_secondary AS (
    SELECT
        bc.age_band,
        CASE
            WHEN LEFT(TRIM(m.COMMUNE), 2) = '97' THEN LEFT(TRIM(m.COMMUNE), 3)
            ELSE LEFT(TRIM(m.COMMUNE), 2)
        END AS residence_dept,
        CASE
            WHEN LEFT(TRIM(m.DCETUF), 2) = '97' THEN LEFT(TRIM(m.DCETUF), 3)
            ELSE LEFT(TRIM(m.DCETUF), 2)
        END AS study_dept,
        CAST(m.IPONDI AS DOUBLE) AS weight
    FROM read_parquet('{mobsco_path}') m
    JOIN band_config bc ON TRIM(m.AGEREV10) = bc.secondary_agerev10
    WHERE bc.secondary_agerev10 IS NOT NULL AND bc.secondary_weight > 0
),
primary_rates AS (
    SELECT
        m.age_band,
        m.residence_dept AS department_code,
        SUM(m.weight) AS total_students,
        SUM(CASE WHEN m.residence_dept <> m.study_dept THEN m.weight ELSE 0 END)
            AS inter_dept_students
    FROM mobsco_primary m
    GROUP BY m.age_band, m.residence_dept
),
secondary_rates AS (
    SELECT
        m.age_band,
        m.residence_dept AS department_code,
        SUM(m.weight) AS total_students,
        SUM(CASE WHEN m.residence_dept <> m.study_dept THEN m.weight ELSE 0 END)
            AS inter_dept_students
    FROM mobsco_secondary m
    GROUP BY m.age_band, m.residence_dept
),
-- Effective rate = (1-sec_weight)*primary_rate + sec_weight*secondary_rate.
-- For 20_24 sec_weight=0 so this reduces to primary_rate unchanged.
rates AS (
    SELECT
        pr.age_band,
        pr.department_code,
        (1.0 - bc.secondary_weight)
            * CASE WHEN pr.total_students > 0
                   THEN pr.inter_dept_students / pr.total_students ELSE 0 END
            + bc.secondary_weight
            * CASE WHEN sr.total_students > 0
                   THEN sr.inter_dept_students / sr.total_students ELSE 0 END
            AS mobility_rate,
        bc.blend_cap,
        bc.blend_default
    FROM primary_rates pr
    JOIN band_config bc ON pr.age_band = bc.age_band
    LEFT JOIN secondary_rates sr
        ON sr.age_band = pr.age_band AND sr.department_code = pr.department_code
)
SELECT
    age_band,
    department_code,
    mobility_rate,
    LEAST(mobility_rate, blend_cap) AS blend_weight
FROM rates
UNION ALL
SELECT
    bc.age_band,
    d.department_code,
    bc.blend_default AS mobility_rate,
    bc.blend_default AS blend_weight
FROM (SELECT DISTINCT department_code FROM population) d
CROSS JOIN band_config bc
LEFT JOIN rates r
    ON r.department_code = d.department_code AND r.age_band = bc.age_band
WHERE r.department_code IS NULL
"""

# Compute study-destination EPCI distribution from MOBSCO parquet, per age band.
# For bands with a secondary AGEREV10 (e.g. 15_19 has higher-ed secondary),
# the study_geo_ratio is a weighted mix of primary and secondary flow distributions:
#   study_geo_ratio = (1 - secondary_weight) * primary_ratio
#                   + secondary_weight * secondary_ratio
# This correctly models that ~25% of 15_19 (ages 18-19 in higher-ed) follow
# higher-ed destination patterns rather than lycée destination patterns.
# Format parameters: {mobsco_path}, {band_config_sql}
CREATE_STUDENT_FLOWS_EPCI = """
CREATE OR REPLACE TABLE student_flows_epci AS
WITH band_config AS (
    {band_config_sql}
),
-- Primary raw flows: column transforms only, no aggregation yet
mobsco_primary AS (
    SELECT
        bc.age_band,
        CASE
            WHEN LEFT(TRIM(m.COMMUNE), 2) = '97' THEN LEFT(TRIM(m.COMMUNE), 3)
            ELSE LEFT(TRIM(m.COMMUNE), 2)
        END AS department_code,
        TRIM(m.DCETUF) AS study_commune,
        CASE m.SEXE WHEN '1' THEN 'male' WHEN '2' THEN 'female' END AS sex,
        CAST(m.IPONDI AS DOUBLE) AS weight
    FROM read_parquet('{mobsco_path}') m
    JOIN band_config bc ON TRIM(m.AGEREV10) = bc.agerev10
),
-- Secondary raw flows (higher-ed for 15_19; empty for 20_24)
mobsco_secondary AS (
    SELECT
        bc.age_band,
        CASE
            WHEN LEFT(TRIM(m.COMMUNE), 2) = '97' THEN LEFT(TRIM(m.COMMUNE), 3)
            ELSE LEFT(TRIM(m.COMMUNE), 2)
        END AS department_code,
        TRIM(m.DCETUF) AS study_commune,
        CASE m.SEXE WHEN '1' THEN 'male' WHEN '2' THEN 'female' END AS sex,
        CAST(m.IPONDI AS DOUBLE) AS weight
    FROM read_parquet('{mobsco_path}') m
    JOIN band_config bc ON TRIM(m.AGEREV10) = bc.secondary_agerev10
    WHERE bc.secondary_agerev10 IS NOT NULL AND bc.secondary_weight > 0
),
-- EPCI aggregation for primary flows
study_epci_primary AS (
    SELECT m.age_band, m.department_code, ce.epci_code, m.sex,
           SUM(m.weight) AS study_pop
    FROM mobsco_primary m
    INNER JOIN commune_epci ce ON m.study_commune = ce.commune_code
    WHERE ce.epci_code IS NOT NULL
    GROUP BY m.age_band, m.department_code, ce.epci_code, m.sex
),
dept_totals_primary AS (
    SELECT age_band, department_code, sex, SUM(study_pop) AS dept_total
    FROM study_epci_primary
    GROUP BY age_band, department_code, sex
),
primary_ratios AS (
    SELECT se.age_band, se.department_code, se.epci_code, se.sex,
           CASE WHEN dt.dept_total > 0 THEN se.study_pop / dt.dept_total ELSE 0 END
               AS geo_ratio
    FROM study_epci_primary se
    JOIN dept_totals_primary dt
        ON se.age_band = dt.age_band AND se.department_code = dt.department_code
        AND se.sex = dt.sex
),
-- EPCI aggregation for secondary flows
study_epci_secondary AS (
    SELECT m.age_band, m.department_code, ce.epci_code, m.sex,
           SUM(m.weight) AS study_pop
    FROM mobsco_secondary m
    INNER JOIN commune_epci ce ON m.study_commune = ce.commune_code
    WHERE ce.epci_code IS NOT NULL
    GROUP BY m.age_band, m.department_code, ce.epci_code, m.sex
),
dept_totals_secondary AS (
    SELECT age_band, department_code, sex, SUM(study_pop) AS dept_total
    FROM study_epci_secondary
    GROUP BY age_band, department_code, sex
),
secondary_ratios AS (
    SELECT se.age_band, se.department_code, se.epci_code, se.sex,
           CASE WHEN dt.dept_total > 0 THEN se.study_pop / dt.dept_total ELSE 0 END
               AS geo_ratio
    FROM study_epci_secondary se
    JOIN dept_totals_secondary dt
        ON se.age_band = dt.age_band AND se.department_code = dt.department_code
        AND se.sex = dt.sex
),
-- Mix primary and secondary distributions using per-band secondary_weight.
-- FULL OUTER JOIN ensures EPCIs in secondary-only or primary-only are included.
-- Renormalization handles edge cases where only primary or only secondary exists
-- (e.g. a dept with no AGEREV10='15' data in MOBSCO).
flow_mix AS (
    SELECT
        COALESCE(pr.age_band, sr.age_band) AS age_band,
        COALESCE(pr.department_code, sr.department_code) AS department_code,
        COALESCE(pr.epci_code, sr.epci_code) AS epci_code,
        COALESCE(pr.sex, sr.sex) AS sex,
        (1.0 - bc.secondary_weight) * COALESCE(pr.geo_ratio, 0)
            + bc.secondary_weight * COALESCE(sr.geo_ratio, 0) AS raw_ratio
    FROM primary_ratios pr
    FULL OUTER JOIN secondary_ratios sr
        ON sr.age_band = pr.age_band AND sr.department_code = pr.department_code
        AND sr.epci_code = pr.epci_code AND sr.sex = pr.sex
    JOIN band_config bc ON bc.age_band = COALESCE(pr.age_band, sr.age_band)
),
flow_mix_totals AS (
    SELECT age_band, department_code, sex, SUM(raw_ratio) AS total_ratio
    FROM flow_mix
    GROUP BY age_band, department_code, sex
)
SELECT
    fm.age_band, fm.department_code, fm.epci_code, fm.sex,
    CASE WHEN fmt.total_ratio > 0 THEN fm.raw_ratio / fmt.total_ratio ELSE 0 END
        AS study_geo_ratio
FROM flow_mix fm
JOIN flow_mix_totals fmt
    ON fm.age_band = fmt.age_band AND fm.department_code = fmt.department_code
    AND fm.sex = fmt.sex
"""

# Blend study-based geo ratios into census-based ones for student age bands.
# For bands 15_19 and 20_24: corrected = (1-w)*base + w*study, renormalized.
# Uses per-department PER-BAND blend weights from the mobility_weights table.
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
-- Blended ratios for student bands (per-department PER-BAND blend weights)
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
    -- JOIN on both department_code AND age_band for per-band weights
    JOIN mobility_weights mw
        ON mw.department_code = ds.department_code
        AND mw.age_band = sb.age_band
    LEFT JOIN geo_ratios_epci_base b
        ON b.department_code = ds.department_code
        AND b.sex = ds.sex
        AND b.age_band = sb.age_band
    LEFT JOIN student_flows_epci sf
        ON sf.department_code = ds.department_code
        AND sf.sex = ds.sex
        AND sf.age_band = sb.age_band
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

# Compute study-destination IRIS distribution from MOBSCO parquet, per age band.
# Distributes commune-level MOBSCO flows to IRIS using census population
# proportions within each study commune.
# For bands with a secondary AGEREV10 (15_19), mixes primary + secondary flows
# exactly as in CREATE_STUDENT_FLOWS_EPCI.
# Format parameters: {mobsco_path}, {band_config_sql}
CREATE_STUDENT_FLOWS_IRIS = f"""
CREATE OR REPLACE TABLE student_flows_iris AS
WITH band_config AS (
    {{band_config_sql}}
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
-- Primary raw flows: column transforms only
mobsco_iris_primary AS (
    SELECT
        bc.age_band,
        CASE
            WHEN LEFT(TRIM(m.COMMUNE), 2) = '97' THEN LEFT(TRIM(m.COMMUNE), 3)
            ELSE LEFT(TRIM(m.COMMUNE), 2)
        END AS department_code,
        TRIM(m.DCETUF) AS study_commune,
        CASE m.SEXE WHEN '1' THEN 'male' WHEN '2' THEN 'female' END AS sex,
        CAST(m.IPONDI AS DOUBLE) AS weight
    FROM read_parquet('{{mobsco_path}}') m
    JOIN band_config bc ON TRIM(m.AGEREV10) = bc.agerev10
),
-- Secondary raw flows (higher-ed for 15_19; empty for 20_24)
mobsco_iris_secondary AS (
    SELECT
        bc.age_band,
        CASE
            WHEN LEFT(TRIM(m.COMMUNE), 2) = '97' THEN LEFT(TRIM(m.COMMUNE), 3)
            ELSE LEFT(TRIM(m.COMMUNE), 2)
        END AS department_code,
        TRIM(m.DCETUF) AS study_commune,
        CASE m.SEXE WHEN '1' THEN 'male' WHEN '2' THEN 'female' END AS sex,
        CAST(m.IPONDI AS DOUBLE) AS weight
    FROM read_parquet('{{mobsco_path}}') m
    JOIN band_config bc ON TRIM(m.AGEREV10) = bc.secondary_agerev10
    WHERE bc.secondary_agerev10 IS NOT NULL AND bc.secondary_weight > 0
),
-- IRIS aggregation for primary flows
study_iris_primary AS (
    SELECT
        m.age_band, m.department_code,
        ics.department_code AS iris_dept, ics.region_code,
        ics.commune_code, ics.iris_code, ics.epci_code, m.sex,
        SUM(m.weight * CASE WHEN ics.commune_pop > 0
            THEN ics.iris_pop / ics.commune_pop ELSE 0 END) AS study_pop
    FROM mobsco_iris_primary m
    JOIN iris_commune_share ics ON m.study_commune = ics.commune_code
    GROUP BY m.age_band, m.department_code, ics.department_code, ics.region_code,
             ics.commune_code, ics.iris_code, ics.epci_code, m.sex
),
dept_totals_primary AS (
    SELECT age_band, department_code, sex, SUM(study_pop) AS dept_total
    FROM study_iris_primary
    GROUP BY age_band, department_code, sex
),
primary_ratios AS (
    SELECT si.age_band, si.department_code, si.iris_dept, si.region_code,
           si.commune_code, si.iris_code, si.epci_code, si.sex,
           CASE WHEN dt.dept_total > 0 THEN si.study_pop / dt.dept_total ELSE 0 END
               AS geo_ratio
    FROM study_iris_primary si
    JOIN dept_totals_primary dt
        ON si.age_band = dt.age_band AND si.department_code = dt.department_code
        AND si.sex = dt.sex
),
-- IRIS aggregation for secondary flows
study_iris_secondary AS (
    SELECT
        m.age_band, m.department_code,
        ics.department_code AS iris_dept, ics.region_code,
        ics.commune_code, ics.iris_code, ics.epci_code, m.sex,
        SUM(m.weight * CASE WHEN ics.commune_pop > 0
            THEN ics.iris_pop / ics.commune_pop ELSE 0 END) AS study_pop
    FROM mobsco_iris_secondary m
    JOIN iris_commune_share ics ON m.study_commune = ics.commune_code
    GROUP BY m.age_band, m.department_code, ics.department_code, ics.region_code,
             ics.commune_code, ics.iris_code, ics.epci_code, m.sex
),
dept_totals_secondary AS (
    SELECT age_band, department_code, sex, SUM(study_pop) AS dept_total
    FROM study_iris_secondary
    GROUP BY age_band, department_code, sex
),
secondary_ratios AS (
    SELECT si.age_band, si.department_code, si.iris_dept, si.region_code,
           si.commune_code, si.iris_code, si.epci_code, si.sex,
           CASE WHEN dt.dept_total > 0 THEN si.study_pop / dt.dept_total ELSE 0 END
               AS geo_ratio
    FROM study_iris_secondary si
    JOIN dept_totals_secondary dt
        ON si.age_band = dt.age_band AND si.department_code = dt.department_code
        AND si.sex = dt.sex
),
-- Mix primary and secondary distributions using per-band secondary_weight.
-- Renormalization handles edge cases where only primary or only secondary exists.
flow_mix AS (
    SELECT
        COALESCE(pr.age_band, sr.age_band) AS age_band,
        COALESCE(pr.department_code, sr.department_code) AS department_code,
        COALESCE(pr.iris_dept, sr.iris_dept) AS iris_dept,
        COALESCE(pr.region_code, sr.region_code) AS region_code,
        COALESCE(pr.commune_code, sr.commune_code) AS commune_code,
        COALESCE(pr.iris_code, sr.iris_code) AS iris_code,
        COALESCE(pr.epci_code, sr.epci_code) AS epci_code,
        COALESCE(pr.sex, sr.sex) AS sex,
        (1.0 - bc.secondary_weight) * COALESCE(pr.geo_ratio, 0)
            + bc.secondary_weight * COALESCE(sr.geo_ratio, 0) AS raw_ratio
    FROM primary_ratios pr
    FULL OUTER JOIN secondary_ratios sr
        ON sr.age_band = pr.age_band AND sr.department_code = pr.department_code
        AND sr.iris_code = pr.iris_code AND sr.sex = pr.sex
    JOIN band_config bc ON bc.age_band = COALESCE(pr.age_band, sr.age_band)
),
flow_mix_totals AS (
    SELECT age_band, department_code, sex, SUM(raw_ratio) AS total_ratio
    FROM flow_mix
    GROUP BY age_band, department_code, sex
)
SELECT
    fm.age_band, fm.department_code, fm.iris_dept, fm.region_code,
    fm.commune_code, fm.iris_code, fm.epci_code, fm.sex,
    CASE WHEN fmt.total_ratio > 0 THEN fm.raw_ratio / fmt.total_ratio ELSE 0 END
        AS study_geo_ratio
FROM flow_mix fm
JOIN flow_mix_totals fmt
    ON fm.age_band = fmt.age_band AND fm.department_code = fmt.department_code
    AND fm.sex = fmt.sex
"""

# Blend study-based geo ratios into census-based ones for student age bands (IRIS).
# For bands 15_19 and 20_24: uses per-department, per-band blend weights.
# Other bands: unchanged from base.
#
# Design: IRIS codes are department-specific, so cross-departmental study destinations
# (e.g. a Val d'Oise student going to study in a Paris IRIS) cannot appear in the
# origin department's geo_ratios_iris. Only intra-departmental study flows are usable.
#
# To avoid inflating local IRIS ratios when students leave for another department,
# the effective census weight is scaled by the intra-dept study fraction p:
#   effective_census_weight = 1 - w * p
#   raw_i = (1 - w*p) * census_i + w * intra_study_geo_ratio_i
#
# where p = SUM(study_geo_ratio for intra-dept IRIS only) per (dept, band, sex).
# This guarantees SUM(raw_i) = 1.0 regardless of cross-dept outbound flow fraction.
# For departments with 100% cross-dept outbound (IDF suburbs): p=0, ratios unchanged.
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
-- Intra-dept study fraction p: fraction of study flows that stay within origin dept.
-- SUM(intra_study_geo_ratio for local IRIS) = p per (dept, band, sex).
-- Departments with purely cross-dept outbound flows get p=0 → no IRIS correction.
intra_dept_fractions AS (
    SELECT
        age_band,
        department_code,
        sex,
        SUM(CASE WHEN iris_dept = department_code THEN study_geo_ratio ELSE 0 END)
            AS intra_frac
    FROM student_flows_iris
    GROUP BY age_band, department_code, sex
),
blended_raw AS (
    SELECT
        b.department_code,
        b.region_code,
        b.commune_code,
        b.iris_code,
        b.epci_code,
        sb.age_band,
        b.sex,
        -- Effective census weight: 1 - w*p (reduces only by intra-dept share)
        -- Study weight: w * intra_study_geo_ratio (0 for cross-dept IRIS)
        (1.0 - mw.blend_weight * COALESCE(idf.intra_frac, 0)) * b.geo_ratio
            + mw.blend_weight * COALESCE(sf.study_geo_ratio, 0) AS raw_ratio
    FROM student_bands sb
    CROSS JOIN (
        SELECT DISTINCT department_code, sex FROM geo_ratios_iris_base
    ) ds
    -- Per-band weights (separate weights for 15_19 and 20_24)
    JOIN mobility_weights mw
        ON mw.department_code = ds.department_code
        AND mw.age_band = sb.age_band
    -- Intra-dept fraction for scaling census weight
    LEFT JOIN intra_dept_fractions idf
        ON idf.department_code = ds.department_code
        AND idf.age_band = sb.age_band
        AND idf.sex = ds.sex
    -- Only base IRIS (census rows) drive the iteration; cross-dept IRIS are excluded
    JOIN geo_ratios_iris_base b
        ON b.department_code = ds.department_code
        AND b.sex = ds.sex
        AND b.age_band = sb.age_band
    -- Only intra-dept study flows (iris_dept = origin dept) contribute to study weight
    LEFT JOIN student_flows_iris sf
        ON sf.department_code = ds.department_code
        AND sf.sex = ds.sex
        AND sf.age_band = sb.age_band
        AND sf.iris_code = b.iris_code
        AND sf.iris_dept = sf.department_code
),
-- blended_raw sums to 1.0 by construction (see comment above); renorm is a safety net
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

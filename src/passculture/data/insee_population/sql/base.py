"""Base table creation and data registration templates."""

__all__ = [
    "CREATE_BASE_TABLE",
    "INSERT_MAYOTTE",
    "REGISTER_CANTON_WEIGHTS",
    "REGISTER_COMMUNE_EPCI",
    "REGISTER_MONTHLY_BIRTHS",
    "REGISTER_QUINQUENNAL",
]

CREATE_BASE_TABLE = """
CREATE OR REPLACE TABLE population AS
WITH raw_data AS (
    SELECT
        CAST(AGEREV AS INT) AS age,
        CASE SEXE WHEN '1' THEN 'male' WHEN '2' THEN 'female' END AS sex,
        CAST(IPONDI AS DOUBLE) AS weight,
        TRIM(DEPT) AS department_code,
        TRIM(REGION) AS region_code,
        TRIM(CANTVILLE) AS canton_code,
        CASE
            WHEN TRIM(IRIS) = 'ZZZZZZZZZ' OR TRIM(IRIS) IS NULL THEN ''
            ELSE LEFT(TRIM(IRIS), 5)
        END AS commune_code,
        TRIM(IRIS) AS iris_code
    FROM read_parquet('{parquet_path}')
    {where_clause}
)
SELECT
    {year} AS year,
    department_code,
    region_code,
    canton_code,
    commune_code,
    iris_code,
    age,
    sex,
    SUM(weight) AS population
FROM raw_data
WHERE age IS NOT NULL AND weight IS NOT NULL
GROUP BY department_code, region_code, canton_code, commune_code, iris_code, age, sex
ORDER BY department_code, commune_code, iris_code, age, sex
"""

REGISTER_COMMUNE_EPCI = (
    "CREATE OR REPLACE TABLE commune_epci AS SELECT * FROM commune_epci_df"
)
REGISTER_CANTON_WEIGHTS = (
    "CREATE OR REPLACE TABLE canton_weights AS SELECT * FROM canton_weights_df"
)

INSERT_MAYOTTE = "INSERT INTO population SELECT * FROM mayotte_df"

REGISTER_QUINQUENNAL = (
    "CREATE OR REPLACE TABLE quinquennal AS SELECT * FROM quinquennal_df"
)
REGISTER_MONTHLY_BIRTHS = (
    "CREATE OR REPLACE TABLE monthly_births AS SELECT * FROM monthly_births_df"
)

"""Constants for INSEE data imports."""

# INSEE INDCVI file URLs
# Main file: France hors Mayotte (metro + DOM)
# COM file: Saint-Barthélemy, Saint-Martin, Saint-Pierre-et-Miquelon

INDCVI_BASE_URL = "https://www.insee.fr/fr/statistiques/fichier"

# URL patterns by year (INSEE changes URLs with each release)
# Each year has a unique statistics page ID
# Format: RP{YEAR}_indcvi.parquet or RP{YEAR}_indcvi.zip
INDCVI_URLS = {
    # Note: Only 2021-2022 have verified working URLs
    # Older years need page ID research on insee.fr
    2022: {
        "page_id": "8647104",
        "france": f"{INDCVI_BASE_URL}/8647104/RP2022_indcvi.zip",
        "france_parquet": f"{INDCVI_BASE_URL}/8647104/RP2022_indcvi.parquet",
        "zone_e": f"{INDCVI_BASE_URL}/8647104/RP2022_indcvize.zip",
    },
    2021: {
        "page_id": "8268848",
        "france": f"{INDCVI_BASE_URL}/8268848/RP2021_indcvi.zip",
        "france_parquet": f"{INDCVI_BASE_URL}/8268848/RP2021_indcvi.parquet",
        "zone_e": f"{INDCVI_BASE_URL}/8268848/RP2021_indcvize.zip",
    },
}

# Population estimates URL (for projections beyond census year)
# These are department-level estimates updated annually
# Page 8721456 has 1975-2026 estimates (updated Jan 2026)
POPULATION_ESTIMATES_URL = "https://www.insee.fr/fr/statistiques/fichier/8721456/estim-pop-dep-sexe-gca-1975-2026.xlsx"

# Birth data URLs for cohort-based projections
# More accurate for specific age groups (15-20) since we track actual birth cohorts
BIRTH_DATA_URLS = {
    # Births by department and year (for cohort tracking)
    "by_dept_year": "https://www.insee.fr/fr/statistiques/fichier/2381380/T_nais_dep.xlsx",
    # Births by department and month (for monthly distribution)
    "by_dept_month": "https://www.insee.fr/fr/statistiques/fichier/2381380/T_nais_dep_mois.xlsx",
}

# Official reference populations ("populations légales") for validation
# These are the official census figures by commune/department
REFERENCE_POPULATION_URLS = {
    2022: "https://www.insee.fr/fr/statistiques/fichier/8290591/ensemble.xlsx",
    2021: "https://www.insee.fr/fr/statistiques/fichier/7739582/ensemble.xlsx",
}

# MOBSCO (Mobilités Scolaires) — student commuting flows
# Contains residence commune (COMMUNE) and study commune (DCETUF)
MOBSCO_URL = (
    "https://www.insee.fr/fr/statistiques/fichier/8589945/RP2022_mobsco.parquet"
)

# Student mobility correction constants for EPCI/IRIS geo ratios
# corrected = (1 - w) * census_ratio + w * study_ratio, for 15_19/20_24 bands
# Default blend weight (fallback for departments not in MOBSCO)
STUDENT_MOBILITY_BLEND_DEFAULT = 0.3
# Maximum blend weight to prevent over-correction in high-mobility departments
STUDENT_MOBILITY_BLEND_CAP = 0.6

# Population estimates by age quinquennial (5-year bands) by department
# Better for validating specific age ranges like 15-20
# Contains columns for each 5-year age group: 0-4, 5-9, 10-14, 15-19, 20-24, etc.
AGE_PYRAMID_URL = "https://www.insee.fr/fr/statistiques/fichier/8721456/estim-pop-dep-sexe-aq-1975-2026.xlsx"

# Birth data by department and month
# Source: https://www.insee.fr/fr/statistiques/6041515?sommaire=5348638
# File: monthly births by department of residence, 2 years per file
BIRTH_DATA_URLS = {
    "by_month_dept": "https://www.insee.fr/fr/statistiques/fichier/6041515/naissances_dep_decembre_2021.xlsx",
}

# Columns to extract from INDCVI files
INDCVI_COLUMNS = [
    "IRIS",  # IRIS code (9 chars)
    "DEPT",  # Department code (2-3 chars)
    "REGION",  # Region code (2 chars)
    "AGEREV",  # Age in completed years (0-120)
    "SEXE",  # Sex (1=M, 2=F)
    "IPONDI",  # Individual weight (15 decimal precision)
]

# IRIS geographic correspondence table URLs
IRIS_GEO_BASE_URL = "https://www.insee.fr/fr/statistiques/fichier/7708995"
IRIS_GEO_URL_PATTERN = f"{IRIS_GEO_BASE_URL}/reference_IRIS_geo{{year}}.zip"

# Population reference (official figures) URLs
POPULATION_REF_BASE_URL = "https://www.insee.fr/fr/statistiques/fichier"
POPULATION_REF_URL_PATTERN = f"{POPULATION_REF_BASE_URL}/8680726/ensemble.csv"

# Department codes
DEPARTMENTS_METRO = [f"{i:02d}" for i in range(1, 96) if i != 20] + ["2A", "2B"]
# DOM departments available in INDCVI census data
DEPARTMENTS_DOM = [
    "971",
    "972",
    "973",
    "974",
]  # Guadeloupe, Martinique, Guyane, Réunion
# 976 (Mayotte) has separate census - not in standard INDCVI files
DEPARTMENTS_MAYOTTE = ["976"]
DEPARTMENTS_COM = ["975", "977", "978"]  # Saint-Pierre, Saint-Barth, Saint-Martin

# Age bucket definitions (for compatibility with existing data)
AGE_BUCKETS = {
    "0_4": range(0, 5),
    "5_9": range(5, 10),
    "10_14": range(10, 15),
    "15_19": range(15, 20),
    "20_24": range(20, 25),
    "25_29": range(25, 30),
    "30_34": range(30, 35),
    "35_39": range(35, 40),
    "40_44": range(40, 45),
    "45_49": range(45, 50),
    "50_54": range(50, 55),
    "55_59": range(55, 60),
    "60_64": range(60, 65),
    "65_69": range(65, 70),
    "70_74": range(70, 75),
    "75_79": range(75, 80),
    "80_84": range(80, 85),
    "85_89": range(85, 90),
    "90_94": range(90, 95),
    "95_plus": range(95, 121),
}

# Confidence interval parameters (error estimates by census offset)
# Based on historical analysis of model vs official INSEE data
CI_BASE_NEAR = 0.02  # 0-1 years from census
CI_BASE_MID = 0.03  # 2-3 years from census
CI_PER_YEAR = 0.01  # per year of offset beyond 3 years
CI_EXTRA_CANTON = 0.05  # additional uncertainty for canton geo_ratio
CI_EXTRA_EPCI = 0.03  # additional uncertainty for EPCI geo_ratio
CI_EXTRA_IRIS = 0.10  # additional uncertainty for IRIS geo_ratio

# Maximum years of CAGR extrapolation beyond the pipeline's valid range.
# The pipeline produces reliable projections up to census_year + min_age
# (the point where cohort-shifted age ratios remain valid). Beyond that,
# CAGR computed on final projected output extends the series.
MAX_CAGR_EXTENSION = 10

# BigQuery schema for population table
POPULATION_SCHEMA = [
    {"name": "year", "type": "INTEGER", "description": "Census year"},
    {
        "name": "department_code",
        "type": "STRING",
        "description": "Department code (2-3 chars)",
    },
    {"name": "region_code", "type": "STRING", "description": "Region code"},
    {"name": "commune_code", "type": "STRING", "description": "Commune INSEE code"},
    {"name": "iris_code", "type": "STRING", "description": "IRIS code (9 chars)"},
    {"name": "epci_code", "type": "STRING", "description": "EPCI SIREN code"},
    {"name": "epci_name", "type": "STRING", "description": "EPCI name"},
    {"name": "age", "type": "INTEGER", "description": "Age in completed years (0-120)"},
    {"name": "sex", "type": "STRING", "description": "Sex (male/female)"},
    {"name": "population", "type": "INTEGER", "description": "Population count"},
    {
        "name": "confidence_pct",
        "type": "FLOAT",
        "description": "Estimated error margin (0-1)",
    },
    {
        "name": "population_low",
        "type": "FLOAT",
        "description": "Population lower bound (population * (1 - confidence_pct))",
    },
    {
        "name": "population_high",
        "type": "FLOAT",
        "description": "Population upper bound (population * (1 + confidence_pct))",
    },
]

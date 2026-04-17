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

# Population estimates URL (used for Mayotte synthesis)
# Department-level estimates, page 8721456, 1975-2026 (updated Jan 2026)
POPULATION_ESTIMATES_URL = "https://www.insee.fr/fr/statistiques/fichier/8721456/estim-pop-dep-sexe-gca-1975-2026.xlsx"

# INDREG: individus localisés à la région, contains MNAI (month of birth).
# Only big departments (>= MNAI_MIN_DEPT_POPULATION) have DEPT populated;
# small departments are recoverable only at REGION level.
INDREG_URLS = {
    2022: {
        "page_id": "8590183",
        "france_parquet": (
            "https://www.insee.fr/fr/statistiques/fichier/8590183/RP2022_indreg.parquet"
        ),
    },
}

# Population threshold used by INSEE to publish department-level figures in
# INDREG. Below this, only the REGION is published; we fall back to a regional
# month-of-birth distribution for those departments.
MNAI_MIN_DEPT_POPULATION = 700_000

# Mayotte 2017 census (POP1B: population by sex and age). Page 4199233.
# INSEE publishes the full POP1B (+other tables) as a single zip archive.
# We extract the POP1B xls and sum communes to get the Mayotte-wide pyramid.
# Fallback to synthesis if unreachable.
MAYOTTE_CENSUS_YEAR = 2017
MAYOTTE_POP1B_URL = (
    "https://www.insee.fr/fr/statistiques/fichier/4199233/"
    "td_Mayotte_Population_2017.zip"
)
MAYOTTE_POP1B_MEMBER = "BTX_TD_POP1B_2017.xls"

# Birth data URLs for monthly distribution (fallback when MNAI unavailable)
BIRTH_DATA_URLS = {
    # N4D: naissances vivantes par mois et département (page 8582142)
    # Columns: REGDEP_DOMI_MERE (region+dept), MNAIS (01-12/AN), NBNAIS
    "by_dept_month": "https://www.insee.fr/fr/statistiques/fichier/8582142/N4D.csv",
    # Monthly births by department of residence (fallback)
    # Source: https://www.insee.fr/fr/statistiques/6041515?sommaire=5348638
    "by_month_dept": "https://www.insee.fr/fr/statistiques/fichier/6041515/naissances_dep_decembre_2021.xlsx",
}

# MOBSCO (Mobilités Scolaires) — student commuting flows
# Contains residence commune (COMMUNE) and study commune (DCETUF)
MOBSCO_URL = (
    "https://www.insee.fr/fr/statistiques/fichier/8589945/RP2022_mobsco.parquet"
)

# Student mobility correction constants for EPCI/IRIS geo ratios
# corrected = (1 - w) * census_ratio + w * study_ratio, for 15_19/20_24 bands

# MOBSCO AGEREV10 code used per age band.
# AGEREV10='15' covers lycée-age students (~15-17, seconde/première/terminale).
# AGEREV10='18' covers higher-education students (~18-24).
# Using '18' for both bands (old behaviour) over-corrects lycée ages.
STUDENT_BAND_AGEREV10: dict[str, str] = {
    "15_19": "15",
    "20_24": "18",
}

# Secondary AGEREV10 for mixed-population bands.
# Ages 18-19 within the 15_19 band (~25% of band population) are in higher
# education and follow higher-ed mobility patterns (AGEREV10='18'), not lycée.
# The secondary weight controls how much of the 15_19 study flow uses higher-ed
# patterns vs lycée patterns. None = no secondary component.
STUDENT_BAND_AGEREV10_SECONDARY: dict[str, str | None] = {
    "15_19": "18",  # 18-19 year olds in higher education
    "20_24": None,  # pure higher-ed band, no secondary needed
}

# Fraction of the 15_19 band population that follows higher-ed mobility patterns.
# ~2/5 ages are 18-19; ~60% of those are in higher education -> ~25%.
STUDENT_BAND_SECONDARY_WEIGHT: dict[str, float] = {
    "15_19": 0.25,
    "20_24": 0.0,
}

# Per-band blend caps: lycée mobility is nearly zero across departments;
# higher-ed reaches 60% in IDF suburbs.
STUDENT_MOBILITY_BLEND_CAP_BY_BAND: dict[str, float] = {
    "15_19": 0.25,
    "20_24": 0.60,
}

# Per-band defaults for departments absent from MOBSCO.
STUDENT_MOBILITY_BLEND_DEFAULT_BY_BAND: dict[str, float] = {
    "15_19": 0.10,
    "20_24": 0.30,
}

# Quinquennal age pyramid URL (used for Mayotte synthesis)
AGE_PYRAMID_URL = "https://www.insee.fr/fr/statistiques/fichier/8721456/estim-pop-dep-sexe-aq-1975-2026.xlsx"

# Columns to extract from INDCVI files
INDCVI_COLUMNS = [
    "IRIS",  # IRIS code (9 chars)
    "DEPT",  # Department code (2-3 chars)
    "REGION",  # Region code (2 chars)
    "AGEREV",  # Age in completed years (0-120)
    "SEXE",  # Sex (1=M, 2=F)
    "IPONDI",  # Individual weight (15 decimal precision)
]

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


# Age bands affected by student mobility correction (MOBSCO).
# Order must match STUDENT_BAND_AGEREV10 keys.
STUDENT_AGE_BANDS = ("15_19", "20_24")

# IRIS sentinel values in INDCVI census data
IRIS_SENTINEL_NO_GEO = "ZZZZZZZZZ"  # commune has no IRIS coverage
IRIS_SENTINEL_MASKED_SUFFIX = "XXXX"  # IRIS masked (< 200 inhabitants)

# Maximum individual age in census data (AGEREV 0-120)
MAX_AGE = 120


# BigQuery schema for population tables (per geographic level)
# Common columns shared by all levels
_COMMON_SCHEMA = [
    {"name": "year", "type": "INTEGER", "description": "Projection year"},
    {
        "name": "month",
        "type": "INTEGER",
        "description": "Snapshot month (1-12, or 1 in yearly mode)",
    },
    {
        "name": "birth_month",
        "type": "INTEGER",
        "description": "Estimated birth month (1-12)",
    },
    {
        "name": "snapshot_month",
        "type": "DATE",
        "description": "First day of observation month",
    },
    {
        "name": "born_date",
        "type": "DATE",
        "description": "First day of estimated birth month/year",
    },
    {
        "name": "decimal_age",
        "type": "FLOAT",
        "description": "Age in years derived from snapshot_month and born_date",
    },
    {
        "name": "department_code",
        "type": "STRING",
        "description": "Department code (2-3 chars)",
    },
    {"name": "region_code", "type": "STRING", "description": "Region code"},
    {"name": "age", "type": "INTEGER", "description": "Age in completed years (0-120)"},
    {"name": "sex", "type": "STRING", "description": "Sex (male/female)"},
    {
        "name": "geo_precision",
        "type": "STRING",
        "description": "Geographic precision indicator",
    },
    {"name": "population", "type": "FLOAT", "description": "Population estimate"},
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

# Per-level schemas matching SQL output column order
POPULATION_SCHEMA_DEPARTMENT = list(_COMMON_SCHEMA)

POPULATION_SCHEMA_EPCI = [
    *_COMMON_SCHEMA[:8],  # up to region_code
    {"name": "epci_code", "type": "STRING", "description": "EPCI SIREN code"},
    *_COMMON_SCHEMA[8:],  # age onward
]

POPULATION_SCHEMA_CANTON = [
    *_COMMON_SCHEMA[:8],  # up to region_code
    {"name": "canton_code", "type": "STRING", "description": "Canton code"},
    *_COMMON_SCHEMA[8:],  # age onward
]

POPULATION_SCHEMA_IRIS = [
    *_COMMON_SCHEMA[:8],  # up to region_code
    {"name": "epci_code", "type": "STRING", "description": "EPCI SIREN code"},
    {"name": "commune_code", "type": "STRING", "description": "Commune INSEE code"},
    {"name": "iris_code", "type": "STRING", "description": "IRIS code (9 chars)"},
    *_COMMON_SCHEMA[8:],  # age onward
]

# Lookup dict for bigquery.py
POPULATION_SCHEMAS = {
    "department": POPULATION_SCHEMA_DEPARTMENT,
    "epci": POPULATION_SCHEMA_EPCI,
    "canton": POPULATION_SCHEMA_CANTON,
    "iris": POPULATION_SCHEMA_IRIS,
}

# Backward-compatible alias (most complete schema)
POPULATION_SCHEMA = POPULATION_SCHEMA_IRIS

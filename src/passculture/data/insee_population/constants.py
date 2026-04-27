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

# INSEE region (REGION) → constituent department codes. Used to fall back to
# a regional month-of-birth distribution for small departments whose DEPT is
# pooled under '99' in INDREG. COM territories (975, 977, 978) are not part
# of any INSEE region and fall back to the metropolitan aggregate.
REGION_TO_DEPARTMENTS: dict[str, list[str]] = {
    "11": ["75", "77", "78", "91", "92", "93", "94", "95"],  # Île-de-France
    "24": ["18", "28", "36", "37", "41", "45"],  # Centre-Val de Loire
    "27": ["21", "25", "39", "58", "70", "71", "89", "90"],  # Bourgogne-FC
    "28": ["14", "27", "50", "61", "76"],  # Normandie
    "32": ["02", "59", "60", "62", "80"],  # Hauts-de-France
    "44": ["08", "10", "51", "52", "54", "55", "57", "67", "68", "88"],  # Grand Est
    "52": ["44", "49", "53", "72", "85"],  # Pays de la Loire
    "53": ["22", "29", "35", "56"],  # Bretagne
    "75": [
        "16",
        "17",
        "19",
        "23",
        "24",
        "33",
        "40",
        "47",
        "64",
        "79",
        "86",
        "87",
    ],  # Nouvelle-Aquitaine
    "76": [
        "09",
        "11",
        "12",
        "30",
        "31",
        "32",
        "34",
        "46",
        "48",
        "65",
        "66",
        "81",
        "82",
    ],  # Occitanie
    "84": [
        "01",
        "03",
        "07",
        "15",
        "26",
        "38",
        "42",
        "43",
        "63",
        "69",
        "73",
        "74",
    ],  # Auvergne-Rhône-Alpes
    "93": ["04", "05", "06", "13", "83", "84"],  # Provence-Alpes-Côte d'Azur
    "94": ["2A", "2B"],  # Corse
    "01": ["971"],  # Guadeloupe
    "02": ["972"],  # Martinique
    "03": ["973"],  # Guyane
    "04": ["974"],  # La Réunion
    "06": ["976"],  # Mayotte
}

DEPARTMENT_TO_REGION: dict[str, str] = {
    dept: region for region, depts in REGION_TO_DEPARTMENTS.items() for dept in depts
}

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

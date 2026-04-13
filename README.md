# passculture-data-insee-population

Approximate monthly population estimates by age, sex, and geographic level (department / EPCI / IRIS) for metropolitan France, DOM, and COM. Built from open INSEE datasets; no individual-level data is redistributed.

Part of the `passculture.data.*` namespace — reusable as a library in other ETLs.

## How it works

**This tool is not a demographic projection model.** It combines several official INSEE publications, each authoritative at a different granularity, and multiplies compatible ratios to produce a single consistent table. Every number in the output can be traced back to a published INSEE file.

It is designed to extend recent trends over a short horizon (~5 years beyond the last INSEE estimates) and provide department- and EPCI-level detail for tracking population movements. Beyond that horizon, accuracy degrades quickly. For real long-term population projections, use the [official INSEE projection tool](https://www.insee.fr/fr/outil-interactif/5896897/pyramide.htm) or the [Regional interactive pyramids](https://www.insee.fr/fr/outil-interactif/5014911/pyramide.html)

### Source files

| File | What it provides | Granularity | Updated |
|------|-----------------|-------------|---------|
| [INDCVI](https://www.insee.fr/fr/statistiques/8647104) | Census microdata (individual weights) | person-level, by IRIS/commune/dept | Every year (latest: 2022) |
| [Quinquennal estimates](https://www.insee.fr/fr/statistiques/8721456) | Population by 5-year age band, dept, sex, year | dept x band x sex x year | Annually (1975-2026) |
| [Monthly births](https://www.insee.fr/fr/statistiques/6041515) | Births by department and month | dept x month | Annually |
| [MOBSCO](https://www.insee.fr/fr/statistiques/8589945) | Student commuting flows (residence vs study commune) | person-level | With census |
| Commune-EPCI / Canton-EPCI | Geographic correspondence tables | commune or canton level | With COG updates |

### Calculation

The processor uses **simple census aging** to project population forward:

```
pop(year, age, sex, dept) = census_pop(census_year, age - (year - census_year), sex, dept)
```

Each census cohort is aged forward by shifting: a person counted as age 15 in 2022 becomes age 18 in 2025. No mortality or migration adjustment is applied (~0.2% error over 4 years for ages 15–24).

Sub-department and monthly estimates are derived by applying ratio tables:

```
pop(year, month, age, sex, geo) =
    census_aged(year, age, sex, dept)             [A]
  x month_ratio(month | dept)                     [B]
  x geo_ratio(geo | dept, age_band, sex)          [C]
```

| Factor | What it does | Source | Constraint |
|--------|-------------|--------|------------|
| **A** `census_aged` | Population at each individual age, shifted from census | INDCVI census | Exact at census year |
| **B** `month_ratio` | Distributes yearly population across 12 months | INSEE monthly birth data | Sums to 1 within each dept |
| **C** `geo_ratio` | Distributes department population to sub-dept units (EPCI, canton, IRIS) | INDCVI census | Sums to <=1 within each dept |

#### Geographic ratios and the structure-repeats hypothesis

Geographic ratios use the **structure-repeats hypothesis**: the sub-department distribution observed in the census at the *target* age is assumed to hold for future years. This is because where people of a given age live changes slowly.

#### Student mobility correction (MOBSCO)

The census records people at their *residence* commune, but students aged 15-24 often live in a different city from where they study. This causes the EPCI geo_ratios for the `15_19` and `20_24` bands to undercount university cities and overcount family-home departments.

When `correct_student_mobility=True` (the default), the pipeline blends census-based geo_ratios with study-destination ratios from the MOBSCO commuting file:

```
corrected = (1 - w) * census_ratio + w * study_ratio
```

Blend weights are **per-department and per-age-band**, derived from actual inter-departmental mobility rates using MOBSCO AGEREV10 groups:

| Band | MOBSCO group used | Typical `w` range | Cap |
|------|------------------|--------------------|-----|
| `15_19` | `AGEREV10='15'` (lycée, ~15–17) | 0–15% | 0.25 |
| `20_24` | `AGEREV10='18'` (higher-ed, ~18–24) | 5–60% | 0.60 |

Departments absent from MOBSCO use per-band defaults (0.10 for `15_19`, 0.30 for `20_24`). After blending, ratios are renormalized to sum to 1. Other age bands are unchanged.

## Accuracy and known limits

Output numbers are **indicative, not authoritative**. Every value can be traced to a published INSEE file, but the model makes assumptions that introduce error, especially at sub-department level and beyond 2–3 years from the census.

Quick summary (ages 15–24, validated against INSEE pyramids):

| Level | What is exact | What drifts |
|-------|--------------|-------------|
| Department | Individual ages exact at census year; ~0.2% drift/4yr from simple aging | No mortality/migration modelling |
| EPCI | Aggregates to dept exactly; 625 EPCIs (100% coverage) | Sub-dept distribution (MOBSCO corrected, direction validated) |
| IRIS | 100% pop coverage; ~60% has sub-commune spatial resolution | Same as EPCI + larger CI |

Confidence intervals are included in all outputs (`confidence_pct`, `population_low`, `population_high`):

| Census offset | Department | EPCI (+3%) | IRIS (+10%) |
|---------------|-----------|------------|-------------|
| 0–1 years | 2% | 5% | 12% |
| 2–3 years | 3% | 6% | 13% |
| 4+ years | 1%/yr | +3% | +10% |

**For the 18–25 age range at EPCI level** (Pass Culture's primary use case), error is highest: student mobility peaks here and geographic distributions shift fastest.

→ Full analysis of biases, validation methodology, and empirical findings: **[docs/accuracy_and_biases.md](docs/accuracy_and_biases.md)**

### Data source catalog

| Source | INSEE page ID | File | Granularity | Update frequency |
|--------|--------------|------|-------------|-----------------|
| INDCVI census microdata | [8647104](https://www.insee.fr/fr/statistiques/8647104) | `RP2022_indcvi.parquet` | Person-level, by IRIS/commune/dept | Yearly (latest: 2022) |
| Quinquennal age pyramid estimates | [8721456](https://www.insee.fr/fr/statistiques/8721456) | `estim-pop-dep-sexe-aq-1975-2026.xlsx` | Dept x 5yr band x sex x year | Annually (1975-2026) |
| Monthly births by department | [6041515](https://www.insee.fr/fr/statistiques/6041515) | `naissances_dep_decembre_2021.xlsx` | Dept x month | Annually |
| MOBSCO student commuting | [8589945](https://www.insee.fr/fr/statistiques/8589945) | `RP2022_mobsco.parquet` | Person-level (residence + study commune) | With census |
| Commune-EPCI correspondence | COG | Via `geo_mappings.py` | Commune level | With COG updates |
| Population estimates (dept total) | [8721456](https://www.insee.fr/fr/statistiques/8721456) | `estim-pop-dep-sexe-gca-1975-2026.xlsx` | Dept x sex x year | Annually |

## Quick start

```bash
make install   # Install dependencies
make test      # Run unit tests
make run       # Run full pipeline (ages 15-24, 2015-2030) → data/output/
```

## Usage

### Default pipeline

`make run` exports multi-year monthly projections for ages 15-24 over 2015-2030 to `data/output/`.

### Custom exports

```bash
# Different age range and year span
uv run insee-population population --year 2022 --min-age 0 --max-age 120 \
    --start-year 2020 --end-year 2026 -o data/output

# Dry run (preview only, no file output)
uv run insee-population population --year 2022 --dry-run

# Disable student mobility correction
uv run insee-population population --year 2022 --min-age 15 --max-age 24 \
    --start-year 2015 --end-year 2030 --no-student-mobility -o data/output

# Exclude DOM/COM/Mayotte
uv run insee-population population --year 2022 --no-dom --no-com --no-mayotte -o data/output

# Show available years and schema
make info
```

### Python API

```python
from passculture.data.insee_population import PopulationProcessor, export_to_bigquery

processor = PopulationProcessor(
    year=2022, min_age=15, max_age=24,
    start_year=2015, end_year=2030,
)
processor.download_and_process()
processor.create_multi_level_tables()

# Get DataFrame for custom handling
df = processor.to_pandas("epci")

# Or export parquet files
processor.save_multi_level("output/")

# Or direct to BigQuery
export_to_bigquery(
    processor,
    level="epci",
    project_id="my-project",
    dataset="population",
    table="epci_2022",
)
```

## Output tables

Three parquet files are produced at department, EPCI, and IRIS levels.

### Common columns (all levels)

| Column | Type | Description |
|--------|------|-------------|
| year | INTEGER | Calendar year |
| month | INTEGER | Month (1-12) |
| current_date | DATE | First of month (year-month-01) |
| born_date | DATE | Estimated birth date ((year-age)-month-01) |
| decimal_age | FLOAT | Age with monthly precision (age + (month-1)/12) |
| department_code | STRING | Department code |
| region_code | STRING | Region code |
| age | INTEGER | Age in completed years |
| sex | STRING | Sex (male/female) |
| geo_precision | STRING | 'exact' |
| population | FLOAT | Projected population (product of ratios) |
| confidence_pct | FLOAT | Estimated error margin (0-1), grows with census offset and geographic level |
| population_low | FLOAT | Lower bound: population * (1 - confidence_pct) |
| population_high | FLOAT | Upper bound: population * (1 + confidence_pct) |

### Additional columns by level

| Column | Type | Levels | Description |
|--------|------|--------|-------------|
| epci_code | STRING | EPCI, IRIS | EPCI SIREN code |
| commune_code | STRING | IRIS | Commune INSEE code |
| iris_code | STRING | IRIS | IRIS code, 9 chars |

## Geographic coverage

### Metropolitan France
- All 96 departments (01-95, 2A, 2B)
- ~35,000 communes
- ~16,000 IRIS

### DOM (Departements d'Outre-Mer)
| Code | Territory |
|------|-----------|
| 971 | Guadeloupe |
| 972 | Martinique |
| 973 | Guyane |
| 974 | La Reunion |
| 976 | Mayotte (synthesized from population estimates, not census) |

### COM (Collectivites d'Outre-Mer)
| Code | Territory |
|------|-----------|
| 975 | Saint-Pierre-et-Miquelon |
| 977 | Saint-Barthelemy |
| 978 | Saint-Martin |

## Dashboard

An interactive dashboard for exploring the population data (maps, age pyramids, time series).

```bash
make run                # Generate population data
make dashboard-prepare  # Prepare dashboard data (split IRIS, download GeoJSON)
make dashboard-up       # Serve dashboard locally
```

Features: summary stats, age pyramids, department/EPCI/IRIS choropleth maps, year-over-year trends, DOM-TOM seasonality, data quality checks.

## Development

```bash
make install           # Install dependencies
make dev               # Install dev dependencies (includes jupyter, geopandas, etc.)
make test              # Run unit tests
make test-integration  # Run integration tests (requires data/cache/ populated)
make lint              # Run ruff linter
make format            # Format code
make clean             # Remove cached and generated files
```

CI runs ruff and pytest on every push/PR to `main` via GitHub Actions. Pre-commit hooks are configured for local use (`pre-commit install`).

## Environment variables

| Variable | Description | Required |
|----------|-------------|----------|
| GCP_PROJECT_ID | Google Cloud project ID | Yes (for BigQuery export) |
| ENV_SHORT_NAME | Environment (dev/stg/prod) | No |

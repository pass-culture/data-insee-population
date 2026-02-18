# passculture-data-insee-population

Approximate monthly population estimates by age, sex, and geographic level (department / EPCI / IRIS) for metropolitan France, DOM, and COM. Built from open INSEE datasets; no individual-level data is redistributed.

Part of the `passculture.data.*` namespace — reusable as a library in other ETLs.

## How it works

**This tool is not a demographic projection model.** It combines several official INSEE publications, each authoritative at a different granularity, and multiplies compatible ratios to produce a single consistent table. Every number in the output can be traced back to a published INSEE file.

It is designed to extend recent trends over a short horizon (~5 years beyond the last INSEE estimates) and provide department- and EPCI-level detail for tracking population movements. Beyond that horizon, accuracy degrades quickly. For real long-term population projections, use the official INSEE projection tool: https://www.insee.fr/fr/outil-interactif/5896897/pyramide.htm

### Source files

| File | What it provides | Granularity | Updated |
|------|-----------------|-------------|---------|
| [INDCVI](https://www.insee.fr/fr/statistiques/8647104) | Census microdata (individual weights) | person-level, by IRIS/commune/dept | Every year (latest: 2022) |
| [Quinquennal estimates](https://www.insee.fr/fr/statistiques/8721456) | Population by 5-year age band, dept, sex, year | dept x band x sex x year | Annually (1975-2026) |
| [Monthly births](https://www.insee.fr/fr/statistiques/6041515) | Births by department and month | dept x month | Annually |
| [MOBSCO](https://www.insee.fr/fr/statistiques/8589945) | Student commuting flows (residence vs study commune) | person-level | With census |
| Commune-EPCI / Canton-EPCI | Geographic correspondence tables | commune or canton level | With COG updates |

### Calculation

The processor builds monthly estimates by multiplying four ratio tables derived from the sources above:

```
pop(year, month, age, sex, geo) =
    quinquennal(year, age_band, sex, dept)        [A]
  x age_ratio(year, age | age_band, sex, dept)    [B]
  x month_ratio(month | dept)                     [C]
  x geo_ratio(geo | dept, age_band, sex)          [D]
```

| Factor | What it does | Source | Constraint |
|--------|-------------|--------|------------|
| **A** `quinquennal` | Anchors total population per 5-year band / dept / sex / year | INSEE age pyramid estimates | — |
| **B** `age_ratio` | Splits a 5-year band into individual ages | INDCVI census, cohort-shifted | Sums to 1 within each band |
| **C** `month_ratio` | Distributes yearly population across 12 months | INSEE monthly birth data | Sums to 1 within each dept |
| **D** `geo_ratio` | Distributes department population to sub-dept units (EPCI or IRIS) | INDCVI census | Sums to <=1 within each dept |

Because **B** and **C** each sum to 1, the department-level yearly total equals the quinquennal value exactly.

#### Cohort-shifted age ratios

Rather than freezing age ratios at census-year values, we shift by birth cohort:

```
census_age = target_age + (census_year - projection_year)
```

For example, to estimate the distribution of 18-year-olds in 2025 using a 2022 census, we look at 15-year-olds in the census (the same birth cohort). This captures cohort-specific patterns (e.g., a baby boom year) that a frozen ratio would miss.

#### Geographic ratios and the structure-repeats hypothesis

Geographic ratios use the **structure-repeats hypothesis**: the sub-department distribution observed in the census at the *target* age is assumed to hold for future years. This is because where people of a given age live changes slowly and is better approximated by the target age's spatial pattern than by the shifted census age's pattern.

#### Student mobility correction (MOBSCO)

The census records people at their *residence* commune, but students aged 15-24 often live in a different city from where they study. This causes the EPCI geo_ratios for the `15_19` and `20_24` bands to undercount university cities and overcount family-home departments.

When `correct_student_mobility=True` (the default), the pipeline blends census-based geo_ratios with study-destination ratios from the MOBSCO commuting file, using **per-department blend weights** derived from actual inter-departmental student mobility rates:

```
corrected = (1 - w_dept) * census_ratio + w_dept * study_ratio
```

Where `w_dept = min(mobility_rate, 0.6)` and `mobility_rate` is the fraction of students in that department who study in a different department. Departments not in MOBSCO use a default weight of 0.3. This gives higher correction weights to Ile-de-France suburbs (50-62% inter-dept mobility) and lower weights to university cities (5-7%).

Then renormalizes so ratios still sum to 1 per (dept, band, sex). Other age bands are unchanged.

## Known biases and accuracy

Estimates will not match official INSEE projections exactly. INSEE publishes age-by-age regional projections (https://www.insee.fr/fr/outil-interactif/5014911/pyramide.htm) that use demographic models and data not publicly available at this granularity. For real local estimates, use official INSEE reports. The extrapolations produced here represent the best approximation possible given the openly available data, but should be treated as indicative, not authoritative.

The numbers below are validated by the integration test suite (`tests/test_integration.py`) using 2022 census vs 2022 quinquennal estimates.

### Quantified accuracy for ages 15-24

Comparison of model projections vs official INSEE pyramide data (`donnees_pyramide_act.csv`, national, M+F):

#### 2026 per-age errors

| Age | Model | Official | Error |
|-----|------:|--------:|------:|
| 15 | 858,697 | 885,211 | -3.0% |
| 16 | 863,200 | 871,489 | -1.0% |
| 17 | 864,491 | 868,144 | -0.4% |
| 18 | 862,183 | 841,073 | +2.5% |
| 19 | 861,066 | 833,790 | +3.3% |
| 20 | 810,281 | 796,876 | +1.7% |
| 21 | 816,153 | 784,282 | +4.1% |
| 22 | 809,732 | 772,575 | +4.8% |
| 23 | 792,602 | 781,668 | +1.4% |
| 24 | 767,502 | 799,895 | -4.0% |
| **Total** | **8,305,908** | **8,235,003** | **+0.9%** |

#### Yearly totals (ages 15-24)

| Year | Error | Notes |
|------|------:|-------|
| 2015-2022 | 0.0% | Exact match (quinquennal data available) |
| 2023 | +0.1% | |
| 2024 | +0.2% | |
| 2025 | +0.3% | |
| 2026 | +0.9% | Furthest from census year |

#### Band-level accuracy (2026)

| Band | Model vs official | Notes |
|------|------------------:|-------|
| 15_19 | +0.2% | Quinquennal anchoring keeps error very low |
| 20_24 | +1.5% | Age-ratio drift from cohort-shifting over 4 years |

The quinquennal estimates are used as-is for band totals (no census replacement). Census data provides only the age-ratio *shape* within each 5-year band. This limits national-level band error to ~1.5%. Individual age errors (up to ~5%) come from cohort-shifted age ratios drifting over time.

#### Improvement from removing census-derived replacement

The model previously replaced quinquennal band totals with census-derived cohort sums. Removing this and trusting quinquennal estimates directly improved accuracy significantly:

| Year | Old model error | Current model error |
|------|----------------:|--------------------:|
| 2024 | +1.2% (MAE 2.9%) | +0.2% (MAE 2.9%) |
| 2025 | +1.8% (MAE 3.8%) | +0.3% (MAE 3.0%) |
| 2026 | +2.4% (MAE 4.3%) | +0.9% (MAE 2.6%) |

The 20_24 band error dropped from +5.8% to +1.5%, and individual age errors for 20-22 dropped from +7-9% to +2-5%.

### Census vs quinquennal discrepancy by age band

At the department level, census aggregates and quinquennal estimates disagree for structural reasons (different survey timing, methodology, rounding). The discrepancy varies by age band:

| Age band | Max dept-level discrepancy | Cause |
|----------|---------------------------|-------|
| 0_4, 5_9, 10_14 | ~10% | Sampling variance in small rural departments (e.g., Cantal, Jura) |
| 15_19 | ~12% | Transitional — some lycee-related mobility |
| 20_24 | ~20% | Strong student mobility: census counts residence, not study location |

At the **national** level (summing all departments), discrepancy drops below **1.5%** for all bands, confirming the errors are spatially distributed, not systematic.

The `20_24` band has the highest *median* department-level discrepancy — more than 2x the median of other bands — confirming student mobility as the dominant source of geographic misallocation.

### Known biases

1. **Structure-repeats hypothesis**: geographic ratios are frozen from the census year. Safe horizon is ~2-3 years; breaks near university towns, ANRU urban renewal zones, and areas with major residential developments.

2. **Month ratio from births**: the monthly distribution is derived from birth data, which is accurate for ages 0-4 but increasingly approximate for older ages where seasonal patterns differ (e.g., student migration in September).

3. **Student mobility correction**: uses MOBSCO commuting flows with per-department blend weights (capped at 0.6). Ile-de-France suburbs have the highest inter-departmental mobility (~50-62%) while university cities (Lyon, Toulouse, Bordeaux, Montpellier) have the lowest (~5-7%). The correction improves EPCI-level accuracy for 15_19 and 20_24 bands but cannot capture individual EPCI-level flows.

4. **Mayotte (976)**: synthesized from quinquennal population estimates, not census microdata. Age distribution uses quinquennal band structure rather than individual-age census counts.

5. **CAGR extension**: beyond the quinquennal data range, population is extended using compound annual growth rates clamped to +/-5%/year. Growth rates are computed from the last 5 years of pipeline output.

### Pass Culture note

The **18-25 age range at EPCI level** is the highest-risk zone for this model:
- Student mobility is concentrated here (ages 18-24 move for university)
- Structure-repeats hypothesis is weakest for this age range (high residential turnover)
- EPCI adds ~3% geographic uncertainty on top of the temporal uncertainty
- Confidence intervals reflect this: EPCI-level CI = department CI + 3%

### Confidence intervals

All output tables include `confidence_pct`, `population_low`, and `population_high` columns. The confidence percentage grows with distance from the census year and geographic granularity:

| Census offset | Department | EPCI (+3%) | IRIS (+10%) |
|---------------|-----------|------------|-------------|
| 0-1 years | 2% | 5% | 12% |
| 2-3 years | 3% | 6% | 13% |
| 4+ years | 1% per year | +3% | +10% |

Example: for census year 2022 projecting to 2028 (offset=6), department CI = 6%, EPCI CI = 9%, IRIS CI = 16%.

### Geographic coverage

| Level | Coverage | Detail |
|-------|----------|--------|
| Department | 100% | 96 metro + 4 DOM + Mayotte (synthesized from estimates) |
| EPCI | ~100% | Direct commune-EPCI join + canton-weighted distribution for unmatched communes |
| IRIS | ~60% | Only communes with IRIS subdivisions; urban depts (75, 69, 13) >70%, rural depts (23, 15, 46) <50% |

### Projection consistency

- Department yearly totals equal quinquennal values to within **0.1%** (ratio tables sum to 1 by construction)
- Year-over-year national population changes are **< 3%** (catches scaling errors)
- No NULL values in any critical column (year, month, department_code, age, sex, population)
- All population values are strictly positive
- Individual cells (month/dept/age/sex) stay within plausible bounds: max < 5,000, mean in [50, 1000]

### EPCI and IRIS consistency

- EPCI population summed by department is within **5%** of department totals
- IRIS population summed by department never exceeds department totals by more than **1%**

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

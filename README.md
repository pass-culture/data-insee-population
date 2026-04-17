# passculture-data-insee-population

Approximate monthly population estimates by age, sex, and geographic
level (department / EPCI / canton / IRIS) for metropolitan France, DOM,
and COM. Built from open INSEE datasets; no individual-level data is
redistributed.

Part of the `passculture.data.*` namespace — reusable as a library in
other ETLs.

## How it works

The pipeline ages the INDCVI census forward one cohort at a time and
splits it by month of birth and by sub-department geography:

```
pop(year, month, age, sex, geo) =
    census_aged(year, age, sex, dept)         [A]
  × month_ratio(month | dept)                 [B]
  × geo_ratio(geo | dept, age_band, sex)      [C]
```

- **[A]** comes from the 2022 INDCVI census, shifted by
  `year − census_year`. Mayotte is stitched in from its 2017 POP1B
  census, aged forward the same way.
- **[B]** comes from MNAI in INDREG — month of birth of the living
  population. Regional fallback for departments < 700 k population.
- **[C]** comes from INDCVI shares within each department, with a
  MOBSCO student-mobility correction for 15-19 and 20-24 bands.

No mortality, no migration — this is a simple aging model, not a
demographic projection. Projections are safe up to
`census_year + min_age` (e.g. 2037 for 2022 + 15).

## Documentation

- **[docs/method.md](docs/method.md)** — step-by-step pipeline reference (what we do, sources, outputs).
- **[docs/design.md](docs/design.md)** — why each choice, trade-offs, limits, confidence intervals.
- **[docs/findings.md](docs/findings.md)** — running log of validation results (numeric).

## Quick start

```bash
make install   # Install dependencies
make test      # Run unit tests
make run       # Run full pipeline (ages 15-24, 2019-2026) → data/output/
```

### Common CLI variants

```bash
# Custom age range and projection window
uv run insee-population population --min-age 0 --max-age 120 \
    --start-year 2020 --end-year 2026 -o data/output

# Dry run (preview, no files written)
uv run insee-population population --dry-run

# Disable the MNAI month-of-birth source (use N4D instead)
uv run insee-population population --no-mnai

# Disable MOBSCO student-mobility correction
uv run insee-population population --no-student-mobility

# Exclude DOM / COM / Mayotte
uv run insee-population population --no-dom --no-com --no-mayotte
```

### Python API

```python
from passculture.data.insee_population import PopulationProcessor

processor = PopulationProcessor(
    year=2022, min_age=15, max_age=24,
    start_year=2019, end_year=2026,
)
processor.download_and_process()
processor.create_multi_level_tables()

df = processor.to_pandas("epci")          # DataFrame
processor.save_multi_level("data/output") # parquet files
```

BigQuery export via `export_to_bigquery(...)` — see `src/passculture/data/insee_population/bigquery.py`.

## Outputs

Four parquet files under `data/output/`, each with one row per
`(year, month, birth_month, geography, age, sex)`. Schema covers
`population`, `confidence_pct`, `population_low`, `population_high`,
plus derived `snapshot_month`, `born_date`, `decimal_age`. Full
schema in [docs/method.md](docs/method.md).

## Dashboard

Interactive exploration of the output (maps, age pyramids, time series):

```bash
make run                # Generate population data
make dashboard-prepare  # Split IRIS, download GeoJSON
make dashboard-up       # Serve dashboard locally
```

## Development

```bash
make install           # Install dependencies
make dev               # Add dev deps (jupyter, geopandas…)
make test              # Unit tests
make test-integration  # Integration tests (requires data/cache/ populated)
make lint              # Ruff linter
make format            # Ruff format
make clean             # Remove cached and generated files
```

CI runs ruff and pytest on every push/PR to `main` via GitHub Actions.
Pre-commit hooks are configured for local use (`pre-commit install`).

## Environment variables

| Variable | Description | Required |
|---|---|---|
| `GCP_PROJECT_ID` | Google Cloud project ID | Yes (BigQuery export only) |
| `ENV_SHORT_NAME` | Environment (dev/stg/prod) | No |

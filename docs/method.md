# Method

Reference for what the pipeline computes, from raw INSEE inputs to the
four output parquet files. For the *why* behind each choice (plus
error bars and known limits), see [`design.md`](./design.md). For
validated numeric results from the last run, see
[`findings.md`](./findings.md).

## Outputs

Four parquet files under `data/output/`, each with one row per
`(year, month, birth_month, geography, age, sex)`:

| File | Geographic grain | Rows | Coverage |
|---|---|---|---|
| `population_department.parquet` | 101 departments | ~16k per year | 100% |
| `population_epci.parquet` | 625 EPCIs | ~102k per year | 100% |
| `population_canton.parquet` | 2,033 cantons | ~325k per year | 100% |
| `population_iris.parquet` | 15,671 IRIS | ~2.5M per year | 100% (pop); ~60% sub-commune |

Each row carries `population`, `confidence_pct`, `population_low`,
`population_high`, plus derived `snapshot_month`, `born_date`, and
`decimal_age`.

## Inputs

"Page ID" refers to the INSEE statistics page on
`https://www.insee.fr/fr/statistiques/{id}`.

| Source | Page | Use |
|---|---|---|
| RP2022 INDCVI | [8647104](https://www.insee.fr/fr/statistiques/8647104) | Census base: population by dept × canton × commune × IRIS × age × sex |
| RP2022 INDREG | [8590183](https://www.insee.fr/fr/statistiques/8590183) | Month-of-birth (MNAI) per region + per dept ≥ 700k pop |
| Mayotte 2017 POP1B | [4199233](https://www.insee.fr/fr/statistiques/4199233) | Mayotte age pyramid (aged forward) |
| Dept annual estimates (`estim-pop-dep-sexe-aq-*.xlsx`) | [8721456](https://www.insee.fr/fr/statistiques/8721456) | Per-dept × sex × age band annual anchor 1975→current year |
| Dept × sex total estimates (`estim-pop-dep-sexe-gca-*.xlsx`) | [8721456](https://www.insee.fr/fr/statistiques/8721456) | Dept totals per sex per year |
| N4D births | [8582142](https://www.insee.fr/fr/statistiques/8582142) | Monthly birth counts — **fallback** when MNAI is unavailable |
| RP2022 MOBSCO | [8589945](https://www.insee.fr/fr/statistiques/8589945) | Student commuting for the 15-19 / 20-24 correction |
| COG commune↔EPCI mapping | [COG](https://www.insee.fr/fr/information/2560452) | Sub-dept geography |

## Steps

1. **Base census.** Load RP2022 INDCVI. Aggregate per
   `(dept, region, canton, commune, iris, age, sex)` with the
   `IPONDI` weight summed. This becomes the `population` base table
   for the census year.

2. **Add Mayotte.** INDCVI does not cover 976. Download POP1B (zip),
   parse the wide commune sheet, sum communes to get a Mayotte-wide
   age pyramid at 2017. Shift ages forward by `census_year - 2017` and
   rescale per sex to match the current-year INSEE dept estimate.
   Fallback path (used only if POP1B is unreachable): synthesise the
   age distribution from the DOM quinquennal pyramid and apply the
   current-year estimate as the total.

3. **Age the cohorts.** For each projection year `Y`, population at
   age `A` equals census population at age `A - (Y - census_year)`.
   No mortality, no migration. Valid for `Y ≤ census_year + min_age`.

4. **Month-of-birth distribution.** Load INDREG and compute, per
   department, the share of population born in each month:
   - Departments with population ≥ 700 000 use their own MNAI
     distribution.
   - Smaller departments inherit their region's MNAI distribution.
   - Mayotte (absent from INDREG) inherits the metropolitan aggregate.
   - If INDREG is unavailable, fall back to N4D birth counts, then to
     uniform `1/12`.
   Every cohort row is expanded at export time into 12 birth-month
   sub-rows whose populations sum to the cohort total.

5. **Sub-department geography ratios.** For each
   `(dept, age band, sex)`, compute the census share of every EPCI,
   canton, or IRIS inside that department. Age bands are used (not
   individual ages) to keep coverage close to 100% even where small
   IRIS are masked. Ratios are then applied on top of the
   dept-level projection.

6. **Student-mobility correction (MOBSCO) for 15-19 and 20-24.**
   Blend the census ratio with a study-destination ratio derived
   from MOBSCO, using a per-department blend weight capped at
   `STUDENT_MOBILITY_BLEND_CAP_BY_BAND` (0.25 for lycée, 0.60 for
   higher-ed). The IRIS variant uses only intra-department study
   flows. Ratios are renormalised to sum to 1 per `(dept, band, sex)`.

7. **Confidence intervals.** A per-row `confidence_pct` is computed
   from the census offset (`|Y - census_year|`) plus a geography-
   specific extra (`+3%` EPCI, `+5%` canton, `+10%` IRIS).
   `population_low` and `population_high` apply that to `population`.

## Running the pipeline

```bash
# Default run: ages 15-24, 2019-2026, MNAI + POP1B active
uv run insee-population population --min-age 15 --max-age 24 \
    --start-year 2019 --end-year 2026 -o data/output

# Disable the MNAI month-of-birth source (use N4D instead)
uv run insee-population population --no-mnai ...

# Disable MOBSCO correction (keep raw census ratios at EPCI/IRIS)
uv run insee-population population --no-student-mobility ...

# Skip Mayotte
uv run insee-population population --no-mayotte ...
```

Caches under `data/cache/` (re-used across runs).

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

3. **Project the cohorts.** Two methods are available via `--method`:

   - **`cohort-stable`** (default, from the INSEE spec doc). For each
     projection year `Y`, current age `A`, sex `S`, dept `D`:
     ```
     pop(Y, D, A, S) = effectif(Y − A, S) × pct_dept(D | S, A)
     ```
     where `effectif(B, S)` is the national total of people born in
     year `B`, sex `S`, in the census (equivalently: sum over depts of
     census population at age `census_year − B`, sex `S`); and
     `pct_dept(D | S, A)` is the census share of dept `D` among people
     of sex `S`, age `A` at census. The age-specific dept distribution
     is frozen at the census pattern and applied afresh each year. No
     mortality, no net migration balance.

   - **`cohort-aging`** (legacy). For each projection year `Y`,
     population at age `A` equals census population at age
     `A − (Y − census_year)` in the same dept: each cohort is aged
     forward while staying in place.

   Both methods preserve national cohort totals and degenerate to the
   census at `Y = census_year`. They differ in how the geographic
   distribution evolves for `Y ≠ census_year`:
   - `cohort-stable` keeps *age-specific* dept shares constant, so the
     share of 18-year-olds in dept D is the same every year. This
     implicitly captures post-bac migration (18-year-old distribution
     reflects where 18-year-olds live, not where they lived at 14).
   - `cohort-aging` keeps *cohort-specific* dept shares constant, so
     the 2022 14-year-old distribution becomes the 2026 18-year-old
     distribution.

   Both methods are valid for `Y ≤ census_year + min_age`.

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
# Default: cohort-stable, 2019-2027, all ages
uv run insee-population population

# Narrow to ages 15-24
uv run insee-population population --min-age 15 --max-age 24 \
    --start-year 2019 --end-year 2027 -o data/output

# Switch to the legacy cohort-aging method
uv run insee-population population --method cohort-aging ...

# Monthly snapshots (12 per year instead of a single Jan 1st snapshot)
uv run insee-population population --monthly ...

# Disable the MNAI month-of-birth source (use N4D instead)
uv run insee-population population --no-mnai ...

# Disable MOBSCO correction (keep raw census ratios at EPCI/IRIS)
uv run insee-population population --no-student-mobility ...

# Skip Mayotte
uv run insee-population population --no-mayotte ...
```

Caches under `data/cache/` (re-used across runs).

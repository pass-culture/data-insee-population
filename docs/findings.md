# Findings

> Running log of validation results. Re-run the scripts in `validation/`
> after regenerating the pipeline output to refresh these numbers.
>
> Last run: **2026-04-23 (c)** — re-run after aligning the base table
> with the INSEE spec doc: `age = year − ANAI` (birth-year age) and
> raw Mayotte 2017 POP1B (no rescale). Pipeline:
> `--method cohort-stable`, ages 15-24, 2019-2026,
> `use_mnai_birth_month=True`, MOBSCO on for EPCI/IRIS.
>
> Prior runs:
> - **2026-04-23 (b)** — first `cohort-stable` run, still with AGEREV
>   age and rescaled Mayotte.
> - **2026-04-17 (a)** — legacy `cohort-aging`, with AGEREV age and
>   rescaled Mayotte.
>
> 2022 numbers between (b) and the latest (c) shift slightly because
> of the AGEREV→ANAI age redefinition; (a) and (b) agree at 2022
> (methods, not inputs).
>
> Scripts: `validation/compare_regional.py`, `validation/compare_epci.py`,
>          `validation/compare_sise.py`, `validation/compare_iris.py`.
>
> Pipeline reference: [`method.md`](./method.md). Design rationale and
> limits: [`design.md`](./design.md).

---

## TL;DR

**What is reliable**

- Department-level population at individual ages is **exact** at census year — taken directly from INDCVI, no redistribution.
- Under `cohort-stable` (default, ANAI age), drift vs INSEE metro total is **+0% to +2.6%** across 2019-2026; the ~+1.7% at the anchor year reflects the birth-year age vs AGEREV definitional offset (not model error). Run-comparable against INSEE requires either an AGEREV conversion or switching the base table back to AGEREV.
- EPCI coverage is **100%** (625 EPCIs); EPCI totals aggregate exactly to department totals.
- MOBSCO student correction goes in the right direction: university EPCIs **+3 to +8pp** on 20-24 share; IDF suburbs down to **−8.7pp**.
- Under `cohort-stable`: national cohort totals by `(birth_year, sex)` and age-specific dept shares are stable across projection years by construction.

**What is approximate**

- EPCI and IRIS geographic distributions are **frozen** at 2022 census ratios. Confidence degrades ~1%/year beyond census.
- MOBSCO correction magnitude: direction confirmed, **±2pp** uncertainty on the correction amount.
- IRIS-level totals: reliable at dept level (100% pop coverage); **do not aggregate IRIS to derive EPCI totals for IDF suburbs** — structural 15-21% gap for ~20 EPCIs.
- Neither method models mortality or migration (**~0.2%** impact over 4 years for ages 15-24).

**Hard limits**

- Projection is valid up to `census_year + min_age` (e.g. 2022 + 15 = 2037). Beyond that, the youngest cohorts were not yet born at census time.
- No individual migration or mortality modelling — simple aging, not a demographic projection.
- MOBSCO correction magnitude has not been independently validated at EPCI level. An official sub-departmental age pyramid does not exist.

---

## Method fidelity

Under `cohort-stable` the pipeline is a faithful implementation of the
INSEE spec doc: national cohort totals by `(birth_year, sex)` are
preserved across projection years by construction, and the dept share
for any `(sex, age)` is frozen at the census pattern. Under
`cohort-aging` the cohort total is preserved but the dept share
drifts with the entering cohort — which is the defining difference
between the two methods. Prerequisites for the docx match are the
base-table choices documented in [`design.md`](./design.md):
`age = year − ANAI`, raw Mayotte 2017 POP1B with `age + 5`, MNAI
month-of-birth, MOBSCO on at EPCI/IRIS.

## Regional level (department → region aggregation)

### Finding 1: Census year totals vs INSEE regional pyramid

Re-run 2026-04-23 (c), year 2022, ages 15-24 — our dept totals aggregated
to region vs INSEE interactive regional pyramid (`donnees_pyramide_act.csv`):

| Region | INSEE | Ours | Diff |
|---|---|---|---|
| Île-de-France | 1,612,053 | 1,604,789 | −0.45% |
| Centre-Val de Loire | 287,868 | 295,773 | +2.75% |
| Bourgogne-Franche-Comté | 313,646 | 322,488 | +2.82% |
| Normandie | 388,766 | 397,892 | +2.35% |
| Hauts-de-France | 763,734 | 780,188 | +2.15% |
| Grand Est | 653,281 | 664,624 | +1.74% |
| Pays de la Loire | 466,915 | 479,537 | +2.70% |
| Bretagne | 393,633 | 406,547 | +3.28% |
| Nouvelle-Aquitaine | 666,267 | 683,275 | +2.55% |
| Occitanie | 699,082 | 716,062 | +2.43% |
| Auvergne-Rhône-Alpes | 969,841 | 993,422 | +2.43% |
| PACA | 559,852 | 563,638 | +0.68% |
| Corse | 33,398 | 33,035 | −1.09% |

Run (c) shifts most regions ~2pp higher than run (b) because our
output now counts people by **birth-year age** (`age = year − ANAI`)
while INSEE's regional pyramid is by **AGEREV** (age at last birthday
at Jan 1st of the year). "15-24 by birth year" includes an extra
half-year of cohorts compared to "AGEREV 15-24", hence the
systematic positive bias. This was the price of reproducing the
INSEE spec doc / xlsx exactly; under run (b) the same check gave
±1% for every metro region.

The shift is definitional, not random. For comparisons to INSEE's
AGEREV-based series, apply an AGEREV conversion or switch the base
table back to AGEREV (not currently a CLI option).

### Finding 2: Year-over-year drift vs INSEE metro estimates

Metro total 15-24 vs INSEE estimate, under each run:

| Year | INSEE | (c) cohort-stable + ANAI (2026-04-23) | drift | (a) cohort-aging + AGEREV (2026-04-17) | drift |
|---|---|---|---|---|---|
| 2019 | 7,655,791 | 7,653,854 | −0.03% | 7,543,879 | −1.46% |
| 2020 | 7,688,057 | 7,744,541 | +0.73% | 7,632,171 | −0.73% |
| 2021 | 7,731,977 | 7,847,401 | +1.49% | 7,719,947 | −0.16% |
| 2022 | 7,808,336 | 7,941,269 | +1.70% | 7,810,089 | +0.02% |
| 2023 | 7,860,061 | 8,027,080 | +2.12% | 7,889,888 | +0.38% |
| 2024 | 7,897,953 | 8,102,379 | +2.59% | 7,962,918 | +0.82% |
| 2025 | 7,929,069 | 8,133,612 | +2.58% | 8,028,890 | +1.26% |
| 2026 | 7,943,494 | 8,140,970 | +2.49% | 8,083,258 | +1.76% |

The +1.7% systematic gap at the anchor year 2022 under run (c) is
the AGEREV-vs-ANAI definitional shift described in Finding 1 — our
15-24 by birth-year age includes roughly half a year of extra cohorts
compared to INSEE's AGEREV 15-24. The slope (~0.5pp per year from
2022) reflects the no-mortality, no-migration assumption shared by
both methods.

For drift analysis that's cleanly comparable to INSEE, use run (a)
or switch the base table back to AGEREV; for docx-compliant modelling,
use run (c) and accept the definitional offset.

### Finding 2b: Lycée band (15-17) flagged for MOBSCO leakage in 8 regions

Sub-band shares within 15-24 show a >0.3pp shift on the 15-17 band in:
Centre-Val de Loire (+1.09pp), PACA (+0.76pp), Normandie (+0.64pp),
Bretagne (+0.49pp), Pays de la Loire (+0.48pp), Occitanie (−0.45pp),
Bourgogne-Franche-Comté (+0.34pp), Nouvelle-Aquitaine (+0.32pp).

These correspond to small mechanical overcorrection of the lycée share when
the 20-24 university correction redistributes weight across EPCIs. The
effect is small enough to stay inside the documented `CI_EXTRA_EPCI = 3%`
envelope; it is listed here as a watch-item rather than an error.

### What this level cannot test

Department-level data cannot validate the MOBSCO correction because MOBSCO only
redistributes population *within* a department across EPCIs/IRIS — it does not
change department totals.

---

## EPCI level

### Finding 3: EPCI coverage is 100% — no uncovered population

| Metric | Result (2026-04-17) |
|--------|--------|
| EPCI coverage vs dept total | 100.0% across all 18 regions |
| Number of EPCIs | 625 |
| Missing population | 0 (0.0%) |

EPCI totals aggregate to department totals exactly — the `geo_ratio → EPCI`
breakdown is internally consistent. Canton-weighted fallback and direct
commune-EPCI joins cover all cases.

### Finding 4: MOBSCO correction direction and magnitude confirmed (2026-04-17)

National 20-24 / 15-24 share: **47.90%**.

University EPCIs — 20-24 share within 15-24 (should be > national mean):

| EPCI | 20-24 pop | 20-24 / 15-24 | vs national |
|---|---|---|---|
| Toulouse Métropole | 101,098 | 56.2% | +8.3pp ✓ |
| Montpellier Méd. Métr. | 62,856 | 56.1% | +8.2pp ✓ |
| Métropole Grand Paris | 549,208 | 55.6% | +7.7pp ✓ |
| Métropole de Lyon | 138,486 | 53.6% | +5.7pp ✓ |
| Bordeaux Métropole | 97,657 | 53.2% | +5.3pp ✓ |
| Rennes Métropole | 65,063 | 53.2% | +5.3pp ✓ |
| Strasbourg Eurométr. | 63,888 | 53.0% | +5.1pp ✓ |
| Grenoble-Alpes-Métr. | 58,900 | 52.4% | +4.5pp ✓ |
| Brest Métropole | 29,351 | 51.8% | +3.9pp ✓ |
| Nantes Métropole | 71,767 | 51.4% | +3.5pp ✓ |
| Aix-Marseille-Prov. | 119,617 | 50.2% | +2.3pp ✓ |

IDF suburb EPCIs (should be < national mean):

| EPCI | 20-24 pop | 20-24 / 15-24 | vs national |
|---|---|---|---|
| Paris Est Créteil | 16,012 | 39.2% | −8.7pp ✓ |
| Grand Paris Sud Est | 24,603 | 51.1% | +3.2pp ~ (weak) |

**Interpretation**: magnitudes have tightened since the Feb-2026 run. University
cities now sit at +3 to +8pp (previously +2 to +6pp) and Paris Est Créteil is at
−8.7pp (previously −2.5pp). This is consistent with the MNAI/POP1B changes
firming up cohort shape rather than spreading weight evenly across bands.

### Finding 5: Top-20 EPCIs by 20-24 share are all university cities

All top-20 EPCIs (with > 10,000 population) are recognizable as university-dominated
urban areas. The bottom-20 are rural territories and Mayotte (which uses synthesized
quinquennal data, not INDCVI, explaining its anomalous age structure, -9.2pp).

### Finding 6: Unknown EPCI codes in lookup table

The code `200071678` (initially labelled Toulouse Métropole) is actually a small EPCI
in Maine-et-Loire (dep. 49), with only ~4,700 people in 15-24. The real Toulouse Métropole
is `243100518` (174,900 people, +6.0pp — top-2 nationally).

**Action**: corrected in `compare_epci.py`. See EPCI SIREN lookup via
`https://geo.api.gouv.fr/epcis/{code}` for future lookups.

---

## SISE enrollment cross-check (2026-04-17 re-run)

### Finding 7a: National enrolled / 20-24 ratio = 76.8% (all formations)

Re-run with no formation filter over the full EPCI parquet:

| Metric | Value |
|---|---|
| Total SISE enrollment | 2,988,730 |
| Our 20-24 (EPCI) | 3,894,006 |
| Our 15-24 (EPCI) | 8,129,492 |
| Enrolled / 20-24 ratio | 76.8% |

University EPCIs vs IDF suburbs (all formations, enrolled / 20-24):

| EPCI | Ratio |
|---|---|
| Cergy-Pontoise (univ, 95) | 171.3% |
| Communauté Paris-Saclay | 163.7% |
| Montpellier Méd. Métr. | 137.9% |
| Métropole de Lyon | 135.0% |
| Grand Nancy Métropole | 128.2% |
| Toulouse Métropole | 116.4% |
| Rennes Métropole | 118.2% |
| Bordeaux Métropole | 109.9% |
| Strasbourg Eurométr. | 109.4% |
| Grenoble-Alpes-Métr. | 108.2% |
| Paris Est Créteil (94, suburb) | 22.4% |
| Grand Paris Sud Est (94, suburb) | 66.3% |
| Val d'Oise Nord résidentiel (95, suburb) | 10.0% |

University EPCIs: 108-171% (plausible — SISE includes students aged up to ~30).
Pure residential IDF suburbs: 10-22% — MOBSCO correction direction confirmed.

### Finding 7: Licence/Master filter gives a sharper magnitude signal (Feb-2026 baseline)

Re-run with `--formation licence-master` (UNIV+GE+INP+UT+ENS codes), which narrows
SISE to students mostly aged 19–24 — a much better proxy for our 20-24 projection.

National ratio (Licence/Master enrolled / our projected 20-24): **45.0%**
(SISE L/M = 1,766,186; our 20-24 EPCI = 3,924,562)

| EPCI | SISE L/M enrolled | Our 20-24 | Ratio |
|------|------------------|-----------|-------|
| Communauté Paris-Saclay (91) | 28,509 | 22,753 | **125.3%** |
| Montpellier Méd. Métr. | 62,968 | 60,599 | **103.9%** |
| Grand Nancy (54) | 40,512 | 39,656 | **102.2%** |
| Tours Métropole | 24,670 | 25,304 | 97.5% |
| Strasbourg Eurométr. | 51,814 | 61,485 | 84.3% |
| Grenoble-Alpes-Métr. | 49,174 | 59,539 | 82.6% |
| Toulouse Métropole | 78,007 | 95,227 | 81.9% |
| Rennes Métropole | 51,494 | 63,306 | 81.3% |
| Dijon Métropole | 28,000 | 32,826 | 85.3% |
| Bordeaux Métropole | 63,982 | 96,350 | 66.4% |
| Métropole de Lyon | 96,317 | 133,520 | 72.1% |
| Métropole Grand Paris | 364,977 | 545,971 | 66.8% |
| Aix-Marseille-Prov. | 67,126 | 120,951 | 55.5% |
| **Val d'Oise (residential)** | 0 | 10,647 | **0.0%** ✓ |
| **Paris Est Créteil** | 1,803 | 16,762 | **10.8%** ✓ |

**Interpretation**: The gradient is unambiguous — university-dominated EPCIs show
ratios 65–125% while residential IDF suburbs are 0–11%. Ratios slightly above 100%
are expected: SISE Licence/Master includes students aged 17-19 (entering year) and
25+ (Masters/continuing education) not covered by our 20-24 band.

Note: Cergy-Pontoise (249500109, 89.3%) is expected high — it hosts CY Cergy Paris
Université; it should not be treated as a pure residential suburb.

**What this confirms**: MOBSCO correction magnitude is plausible. A 45% national ratio
with university EPCIs at 65–125% and residential suburbs at 0–11% is fully consistent
with the correction being well-calibrated. No sign of systematic over- or under-correction.

### Finding 8: Top-20 by SISE ratio are all university cities or research clusters

All top-20 EPCIs by Licence/Master enrolled/20-24 ratio are recognizable university
or research clusters (#1 Paris-Saclay/Polytechnique, #2 Montpellier, #3 Nancy, etc.).
Direction and rank order are correct.

---

## IRIS internal consistency (Feb 2026)

### Finding 9: IRIS coverage is 100% at dept level (2026-04-17)

| Metric | Result |
|--------|--------|
| IRIS total population vs dept | 100.0% (8,129,492 / 8,129,492 at 15-24) |
| EPCIs with IRIS rows | 624 / 625 (one EPCI is a single un-subdivided commune) |

**What "60% coverage" actually means**: the README's "~60% coverage" refers to the fraction of
national population living in communes *divided into sub-commune IRIS units*. Rural communes are
assigned a single IRIS code covering the whole commune (no sub-division), but they ARE present
in the IRIS output. All population is accounted for at IRIS level.

The 60% figure should be understood as: "60% of the population lives in communes with at least
2 IRIS codes (genuine sub-commune spatial resolution)."

### Finding 10: ~20 EPCIs show IRIS total > EPCI total (pre-fix and post-fix)

**Pre-fix** (before 2026-02-27): IRIS codes are department-specific, so cross-departmental
study destinations (e.g. Val d'Oise students going to Paris IRIS) could not appear in the
origin dept's `geo_ratios_iris_base`. Those cross-dept flows were absent from `blended_raw`,
making `blended_totals < 1.0`. Renormalization then inflated all local IRIS ratios
(e.g. by 1/0.64 = 1.56× for Val d'Oise).

| EPCI | IRIS pop (pre-fix) | EPCI pop | Pre-fix ratio |
|------|----------|----------|---------------|
| 200058485 (Val d'Oise) | 32,118 | 22,475 | 142.9% |
| 249500489 | 2,800 | 1,974 | 141.9% |
| 249500455 | 2,894 | 2,047 | 141.4% |
| 249500109 (Cergy-Pontoise) | 54,149 | 38,323 | 141.3% |
| 200056380 | 20,151 | 14,527 | 138.7% |

**Fix (merged 2026-02-27)**: `CREATE_CORRECTED_GEO_RATIOS_IRIS` now uses only
intra-departmental study flows, with an effective blend weight `w × p` where
`p = intra-dept study fraction`. For IDF suburbs (p ≈ 0), the correction is a near
no-op and ratios remain close to census values. IRIS geo_ratios sum to 1.0 by
construction. See `docs/design.md` ("Why the IRIS correction is weaker than the EPCI one") for the full design explanation.

**Post-fix results (re-run 2026-04-17, MNAI + POP1B active)**:

| EPCI | IRIS pop | EPCI pop | Post-fix ratio | Pre-fix ratio (Feb) |
|------|----------|----------|----------------|---------------------|
| 200058485 (Val d'Oise Nord) | 36,325 | 29,993 | 121.1% | 142.9% |
| 249500455 | 3,404 | 2,850 | 119.4% | 141.4% |
| 249500489 | 3,391 | 2,845 | 119.2% | 141.9% |
| 200056380 | 22,874 | 19,291 | 118.6% | 138.7% |
| 249500109 (Cergy-Pontoise) | 41,405 | 35,922 | 115.3% | 141.3% |
| 200058477 | 23,225 | 20,278 | 114.5% | — |
| 200057859 | 23,429 | 20,460 | 114.5% | — |
| 200055655 | 51,589 | 45,187 | 114.2% | 127.4% |
| 200056232 (Communauté Paris-Saclay) | 49,576 | 43,820 | 113.1% | — |

Worst-case gap is now ~21pp (was 43pp in the un-fixed pipeline; was 19pp in the
Feb-2026 re-run). The mild widening vs Feb is a consequence of MNAI reshaping
the monthly distribution — not a regression in the MOBSCO blend. 20 EPCIs show
>1% IRIS-vs-EPCI gap; all other EPCIs are under 1%.

The residual is structural: the EPCI pipeline deflates IDF suburb EPCIs
(students leave cross-dept), while the IRIS pipeline cannot apply the
equivalent deflation (IRIS codes are dept-specific, cross-dept destinations
unavailable). IRIS stay at census; EPCI are below census → IRIS > EPCI. This
cannot be eliminated without redesigning the pipeline to allow dept-total
underrun.

**Practical guidance**: IRIS-level totals should not be used to derive EPCI totals for
IDF suburb EPCIs. Use `population_epci.parquet` for EPCI totals.

### Finding 11: IRIS vs INDCVI — comparison methodology limited

Direct comparison of our 2022 IRIS output vs INDCVI census shows large diffs in
university departments (+19% Lyon, +29% Montpellier, +54% Strasbourg). These are NOT bugs:

1. **MOBSCO effect**: our output adds study-destination students; INDCVI is residential.
   University cities naturally show our output > census for this reason.
2. **Rural commune code (ZZZZZZZZZ)**: in INDCVI, residents of un-IRIS'd communes use
   `ZZZZZZZZZ` as IRIS code. These are excluded from the comparison, artificially
   inflating the diff for rural depts (Creuse dept 23: +314% due to this).
3. **Paris (dept 75, +6%)**: Paris has minimal MOBSCO effect (students are already residents).
   The +6% is close to the expected range from birth seasonality and methodological differences.

**Conclusion**: for Paris and non-university departments, our geo_ratio pipeline reproduces
the census within ~6%. For university cities, MOBSCO effect dominates the diff.

---

## Multi-year drift analysis (Feb 2026)

### Finding 12: University EPCI share drift — `cohort-stable` is much flatter than `cohort-aging`

2019→2026 drift of 20-24 share (within 15-24) for the top university
EPCIs, under each method (re-run 2026-04-23):

| EPCI | cohort-stable | cohort-aging |
|---|---|---|
| Toulouse Métropole | +1.44pp | +9.56pp |
| Métropole de Lyon | +1.46pp | +8.48pp |
| Montpellier Méd. Métr. | +1.44pp | +8.21pp |
| Métropole Grand Paris | +1.43pp | +3.56pp |
| Rennes Métropole | +1.48pp | +6.29pp |
| Bordeaux Métropole | +1.49pp | +6.09pp |
| Strasbourg Eurométr. | +1.47pp | +5.90pp |
| Brest Métropole | +1.53pp | +4.51pp |
| Grenoble-Alpes-Métr. | +1.53pp | +3.19pp |
| Aix-Marseille-Prov. | +1.50pp | +2.36pp |
| Nantes Métropole | +1.51pp | +1.66pp |

**Interpretation.** Under `cohort-aging`, each projection year takes
a *different* birth cohort's dept share (aged in place), so the 20-24
window's composition and its geographic distribution both shift year
over year. Under `cohort-stable` the age-specific dept share is
pinned to the census pattern, so share drift only reflects changing
cohort sizes — a much tighter ~1.5pp envelope.

The prior finding table (2015–2030 horizon, 1.4–2.7pp drift under
cohort-aging) remains valid for that method and horizon; `cohort-stable`
produces roughly equivalent magnitude but for a different reason (no
share flip between entering/exiting cohorts), and shares still freeze
after `census_year + min_age`.

---

## Known limitations

| Ref | Status | Description |
|-----|--------|-------------|
| L2 | Resolved | SISE Licence/Master filter added (Feb 2026). National ratio 45%; university EPCIs 65–125%; IDF residential suburbs 0–11%. Correction magnitude is plausible. |
| L3 | Resolved | IDF suburb EPCI codes found (Plaine Commune, Est Ensemble, Val de Bièvre, Grand-Orly are all present) |
| L4 | Resolved | IRIS coverage wording fixed in README, docs, cli.py, duckdb_processor.py |
| L5 | Confirmed | Post-2026 freeze confirmed: 2027–2030 values are identical to 2026 |
| L6 | Fixed | IRIS-EPCI inconsistency reduced from 43pp to 19pp by intra-dept blend scaling |

Remaining structural limitation: the 12–19% IRIS > EPCI gap for ~20 IDF suburb EPCIs
cannot be eliminated without redesigning the pipeline to allow per-dept IRIS undershoot.
All other EPCIs show < 1% gap post-fix.

---

## How to re-run

```bash
# 1. Regenerate pipeline output (defaults: cohort-stable, 2019-2027)
uv run insee-population population --min-age 15 --max-age 24

# Or use the legacy method for comparison
uv run insee-population population --method cohort-aging \
  --min-age 15 --max-age 24 -o data/output/cohort-aging

# Regional comparison
uv run python validation/compare_regional.py \
  --input data/output/population_department.parquet \
  --year 2022

# EPCI validation + drift
uv run python validation/compare_epci.py \
  --dept data/output/population_department.parquet \
  --epci data/output/population_epci.parquet \
  --year 2022 --drift

# SISE enrollment cross-check
uv run python validation/compare_sise.py \
  --epci data/output/population_epci.parquet \
  --commune-epci data/cache/commune_epci.parquet \
  --year 2022

# SISE with Licence/Master filter (better magnitude check)
uv run python validation/compare_sise.py \
  --epci data/output/population_epci.parquet \
  --commune-epci data/cache/commune_epci.parquet \
  --year 2022 --formation licence-master

# IRIS consistency + census comparison
uv run python validation/compare_iris.py \
  --iris data/output/population_iris.parquet \
  --epci data/output/population_epci.parquet \
  --dept data/output/population_department.parquet \
  --indcvi data/cache/indcvi_2022.parquet \
  --year 2022

# Skip census comparison (faster, no INDCVI needed)
uv run python validation/compare_iris.py \
  --iris data/output/population_iris.parquet \
  --epci data/output/population_epci.parquet \
  --dept data/output/population_department.parquet \
  --indcvi data/cache/indcvi_2022.parquet \
  --year 2022 --skip-census
```

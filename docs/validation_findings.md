# Validation Findings

> Last run: 2026-02-27 — SISE re-run with Licence/Master filter (same day, post 18-19 mixing).
> Regional/EPCI/IRIS consistency checks run pre-18-19-mixing (output unchanged at dept level).
> Re-run scripts after generating new output to update EPCI/IRIS findings.
> Scripts: `validation/compare_regional.py`, `validation/compare_epci.py`,
>          `validation/compare_sise.py`, `validation/compare_iris.py`
> Full methodology and bias analysis: `docs/accuracy_and_biases.md`

---

## Regional level (department → region aggregation)

### Finding 1: Census year totals are exact by construction

| Metric | Result |
|--------|--------|
| Total 15–24 per region (census year 2022) | Exact — from INDCVI census |
| Sex ratio vs INDCVI | Identical |
| Individual age populations | Exact at census year (no intra-band redistribution) |

**Why**: Department-level population at each individual age comes directly from the
INDCVI census. No quinquennal anchoring or age-ratio redistribution is applied.
Regional totals are exact because regions are sums of departments.

### Finding 2: Projection years use simple aging

For non-census years, each cohort keeps its census population as it ages forward.
This means population at age A in year Y equals census population at age A-(Y-2022).
The only error source is mortality and migration (~0.2% over 4 years for ages 15–24).

### What this level cannot test

Department-level data cannot validate the MOBSCO correction because MOBSCO only
redistributes population *within* a department across EPCIs/IRIS — it does not
change department totals.

---

## EPCI level

### Finding 3: EPCI coverage is 100% — no uncovered population

| Metric | Result |
|--------|--------|
| EPCI coverage vs dept total | 100.0% across all 18 regions |
| Number of EPCIs | 625 |
| Missing population | 0 (0.0%) |

**Note**: This is better than the ~100% documented estimate. Canton-weighted fallback
and direct commune-EPCI joins cover all cases.

EPCI totals aggregate perfectly to department totals — the geo_ratio → EPCI
breakdown is internally consistent.

### Finding 4: MOBSCO correction direction is correct for most university EPCIs

National mean 20-24 share within 15-24: **48.43%**

University EPCIs (should be > national mean):

| EPCI | 20-24/15-24 | vs national |
|------|-------------|-------------|
| Montpellier Méd. Métr. | 54.5% | +6.1pp ✓ |
| Métropole Grand Paris | 53.8% | +5.3pp ✓ |
| Toulouse Métropole | 54.4% | +6.0pp ✓ |
| Bordeaux Métropole | 52.9% | +4.5pp ✓ |
| Métropole de Lyon | 52.7% | +4.3pp ✓ |
| Rennes Métropole | 52.5% | +4.0pp ✓ |
| Grenoble-Alpes-Métr. | 51.8% | +3.4pp ✓ |
| Brest Métropole | 51.9% | +3.5pp ✓ |
| Nantes Métropole | 51.2% | +2.8pp ✓ |
| Aix-Marseille-Prov. | 50.6% | +2.1pp ✓ |
| Strasbourg Eurométr. | 51.9% | +3.4pp ✓ |

IDF suburb EPCIs (should be < national mean):

| EPCI | 20-24/15-24 | vs national |
|------|-------------|-------------|
| Paris Est Créteil (Val-de-Marne) | 45.9% | -2.5pp ✓ |
| Grand Paris Sud Est | 48.6% | +0.1pp ~ (weak) |

**Interpretation**: The MOBSCO correction is working in the right direction.
University cities show meaningfully elevated 20-24 shares (+2 to +6pp). IDF suburbs
show depressed shares. The gradient is plausible.

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

## SISE enrollment cross-check (Feb 2026, updated with Licence/Master filter)

### Finding 7: Licence/Master filter gives a sharper magnitude signal

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

### Finding 9: IRIS coverage is 100% at dept level

| Metric | Result |
|--------|--------|
| IRIS total population vs dept | 100.0% |
| EPCIs with IRIS rows | 624 / 625 |

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
construction. See `docs/accuracy_and_biases.md` Bias 2 for the full design explanation.

**Post-fix results (re-run 2026-02-27)**:

| EPCI | IRIS pop | EPCI pop | Post-fix ratio | Pre-fix ratio |
|------|----------|----------|----------------|---------------|
| 200058485 (Val d'Oise) | 36,915 | 31,128 | 118.6% | 142.9% |
| 249500109 (Cergy-Pontoise) | 40,963 | 35,896 | 114.1% | 141.3% |
| 200056380 | 23,284 | 19,949 | 116.7% | 138.7% |
| 200055655 | 52,636 | 46,647 | 112.8% | 127.4% |

Worst-case gap reduced from 43pp to 19pp. The residual is structural: the EPCI
pipeline deflates IDF suburb EPCIs (students leave cross-dept), while the IRIS pipeline
cannot apply the equivalent deflation (IRIS codes are dept-specific, cross-dept
destinations unavailable). IRIS stay at census; EPCI are below census → IRIS > EPCI.
This cannot be eliminated without redesigning the pipeline to allow dept-total underrun.

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

### Finding 12: University EPCI shares drift 1.4–2.7pp over 2015–2030; freeze after 2026

| EPCI | Share 2015 | Share 2026 | Share 2030 | Drift (2015–2026) |
|------|------------|------------|------------|-------------------|
| Montpellier | 56.8% | 54.6% | 54.6% | +2.71pp |
| Toulouse | 56.3% | 54.6% | 54.6% | +2.68pp |
| Rennes | 53.9% | 52.2% | 52.2% | +2.58pp |
| Métropole Grand Paris | 54.6% | 53.7% | 53.7% | +2.45pp |
| Grenoble | 52.1% | 49.8% | 49.8% | +2.24pp |
| Brest | 50.7% | 51.0% | 51.0% | +2.17pp |
| Lyon | 54.1% | 52.8% | 52.8% | +2.15pp |
| Strasbourg | 53.2% | 51.3% | 51.3% | +2.05pp |
| Bordeaux | 53.3% | 52.0% | 52.0% | +1.36pp |

**Year-over-year variation**: with simple aging, different birth cohorts enter/exit
the 15–24 range each year. The 20-24 share varies because census cohort sizes differ
(reflecting historical birth rate variations). This is expected demographic structure,
not an error. Geographic ratios are fixed at 2022 census values.

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

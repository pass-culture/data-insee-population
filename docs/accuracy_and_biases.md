# Accuracy and Biases

> Methodology reference for the `passculture-data-insee-population` pipeline.
> Covers what the model produces, what it is calibrated to, known failure modes,
> and empirical validation findings.
>
> **Empirical validation results and stats**: `docs/validation_findings.md`
> **Validation scripts**: `validation/` — re-run at any time (see `docs/validation_findings.md`)

---

## Quick reference for new users

**What is reliable:**
- Department-level population at individual ages is exact at census year — taken directly from the INDCVI census with no redistribution or approximation.
- For projection years, population uses simple cohort aging (no mortality/migration adjustment). Each cohort keeps its census population as it ages forward.
- EPCI coverage is 100% (625 EPCIs). EPCI totals aggregate to department totals with no rounding loss.
- MOBSCO student correction goes in the right direction: university EPCIs gain +2 to +6pp on 20-24 share; IDF suburbs lose share.

**What is approximate:**
- EPCI geographic distribution: frozen at 2022 census ratios. Confidence degrades ~1%/year beyond census.
- MOBSCO correction magnitude: direction confirmed, ±2pp uncertainty on the correction amount.
- IRIS-level estimates: reliable at dept level (100% population coverage); do not aggregate IRIS to derive EPCI totals for IDF suburbs (structural 12–19% gap for ~20 EPCIs).
- Simple aging ignores mortality and migration (~0.2% impact over 4 years for ages 15–24).

**Hard limits:**
- Projection is valid up to census_year + min_age (e.g. 2022 + 15 = 2037). Beyond that, the youngest cohorts were not yet born at census time.
- No individual migration or mortality modelling — this is a simple aging model, not a demographic projection.
- MOBSCO correction magnitude has not been independently validated at EPCI level. An official sub-departmental age pyramid does not exist.

---

## What this model produces and for what use

The pipeline outputs monthly population estimates by age, sex, and geographic unit
(department, EPCI, canton, IRIS) for configurable age ranges and projection years. It is designed for:

- **Tracking youth population (15–25) at EPCI level** — primary use case at Pass Culture
- **Estimating territorial reach** for cultural programs (how many potential users in a given zone)
- **Interpolating between census years** at sub-departmental granularity

It is **not** a demographic projection model. It does not model fertility, mortality, or
migration explicitly. It ages census population forward by shifting cohorts, assuming
zero mortality and zero net migration. For official demographic projections, use
INSEE's Omphale tool.

---

## What is calibrated vs what is estimated

| Component | Source | Accuracy |
|-----------|--------|----------|
| Department population by individual age | INDCVI 2022 census, cohort-aged | Exact at census year; ~0.2% drift over 4 years (no mortality/migration) |
| Monthly distribution | Monthly birth data per dept | Approximate |
| EPCI geographic split | INDCVI 2022 census | Frozen at census year |
| EPCI student correction | MOBSCO 2022 commuting data | Direction validated only |
| IRIS geographic split | INDCVI 2022 census, commune-distributed | 100% pop; ~60% sub-commune resolution |
| Mayotte | Quinquennal estimates + total population estimates | Approximate (synthesized, not census) |

**Key implication**: department-level population at each individual age is exact at census
year. For projection years, the only error source is the simple aging assumption (no
mortality/migration). All other uncertainty is in how population is distributed across
months and sub-geographies.

---

## Empirical validation findings (Feb 2026)

### Regional level

Comparison: our dept-level projections aggregated to region vs INSEE interactive pyramid
(`donnees_pyramide_act.csv`) for all 13 metropolitan regions, ages 15–24.

- **Census year (2022)**: exact match by construction — population comes directly from INDCVI.
- **Projection years**: differ from INSEE quinquennal estimates because simple aging does
  not account for mortality or migration. Expected drift is ~0.2% over 4 years for ages 15–24.
- **Sex ratio**: identical to INSEE at census year.

### EPCI level — internal consistency confirmed

- **Coverage**: 100% (625 EPCIs), confirmed 2026-02-27. No population is lost in the
  department → EPCI disaggregation. The README's "~100%" estimate should be read as 100%.
- **Consistency**: EPCI totals aggregate exactly to department totals.

### EPCI level — MOBSCO correction direction validated

National mean 20-24 / 15-24 share: **48.4%**

University EPCIs (confirmed elevated, as expected):

| EPCI | Share | vs national |
|------|-------|-------------|
| Montpellier Méditerranée | 54.5% | +6.1pp |
| Toulouse Métropole (243100518) | 54.4% | +6.0pp |
| Métropole Grand Paris | 53.8% | +5.3pp |
| Bordeaux Métropole | 52.9% | +4.5pp |
| Métropole de Lyon | 52.7% | +4.3pp |
| Rennes Métropole | 52.5% | +4.0pp |
| Strasbourg Eurométropole | 51.9% | +3.4pp |
| Brest Métropole | 51.9% | +3.5pp |
| Nantes Métropole | 51.2% | +2.8pp |

IDF suburb EPCI (confirmed depressed): Paris Est Créteil (Val-de-Marne) at 45.9%, -2.5pp.

The correction goes in the right direction. The magnitude has not been independently
validated (no official EPCI pyramid exists for comparison).

### EPCI level — SISE cross-check (Feb 2026)

SISE enrollment (2022-23) with Licence/Master filter (UNIV+GE+INP+UT+ENS codes,
covering students mostly aged 19–24):

- National L/M enrolled/projected-20-24 ratio: **45.0%** (1,766K enrolled / 3,925K projected)
- University EPCIs: 65–125% — consistently above national mean
- Residential IDF suburb (Val d'Oise): **0%**, Paris Est Créteil: **10.8%** — confirmed low
- Top-20 EPCIs by ratio are all university or research clusters (#1 Paris-Saclay 125%, #2 Montpellier 104%)

**MOBSCO magnitude validated**: the 45% national baseline with university EPCIs at 65–125%
and residential suburbs at 0–11% is fully consistent with the correction being well-calibrated.
No sign of systematic over- or under-correction.

→ Full findings: `docs/validation_findings.md` Findings 7 & 8

### IRIS level — coverage and consistency (Feb 2026)

- **Coverage**: 100% at dept level. All population is in the IRIS output, including rural
  communes (which receive a single IRIS code per commune, not sub-divided).
- **IRIS → EPCI inconsistency**: ~20 EPCIs show IRIS totals 1–43% above EPCI parquet totals.
  EPCI parquet is authoritative. IRIS-level EPCI aggregation should not be used directly.
- **Census comparison (Paris, non-university)**: our 2022 IRIS output is ~6% above INDCVI
  for Paris (dept 75), consistent with expected methodological differences.
- **Census comparison (university cities)**: large diffs (+19 to +54pp) reflect MOBSCO
  effect — our output adds study-destination students; INDCVI captures residential counts.

### Multi-year behavior

With simple aging, year-over-year population changes reflect different birth cohorts
entering/leaving the 15–24 age range. Geographic ratios are fixed at 2022 census values.
University EPCI 20-24 shares vary across years as cohort sizes differ.

---

## Known biases

### Bias 1 — Structure-repeats hypothesis: geographic distributions are frozen

**What happens**: geographic ratios (R_geo) are computed from the 2022 INDCVI census.
They represent, for each department × age_band × sex, which fraction of the population
lives in each EPCI or IRIS. These fractions are assumed constant for all projection years.

**When it breaks**:
- Near large new student residences or campus openings
- In ANRU urban renewal zones (major demographic turnover)
- When EPCI boundaries change
- For fast-growing suburban areas

**Empirical horizon**: assumed safe for ~2–3 years. Confidence intervals grow at 1%/year
beyond that (plus EPCI +3%, IRIS +10% flat penalty) to reflect this degradation.

**Projection horizon**: simple aging is valid up to `census_year + min_age` (e.g.
2022 + 15 = 2037). Beyond that, the youngest cohorts were not yet born at census time
and cannot be projected. Year-over-year changes in EPCI and IRIS population reflect
different cohorts entering/leaving the age range, applied with fixed geographic ratios.

**Cannot be fixed without**: a time-series of EPCI-level population data, which INSEE
does not publish at this granularity.

---

### Bias 2 — MOBSCO correction: direction right, magnitude uncertain

**What happens**: the correction blends census-based residential ratios with study-destination
ratios using per-department, per-age-band blend weights. Lycée (15_19) and higher-ed (20_24)
are treated separately since their mobility patterns differ fundamentally.

**IRIS-specific design**: IRIS codes are department-specific, so cross-departmental study
destinations (e.g. a Val d'Oise student studying at a Sorbonne IRIS in Paris) cannot appear
in the origin department's IRIS geo_ratios. The IRIS correction uses only intra-departmental
study flows. The effective blend weight is `w × p` where `p` is the fraction of study flows
that stay within the origin department. For IDF suburbs with near-100% cross-dept outbound
flows (p ≈ 0), the IRIS correction is a no-op and ratios remain at census values.

**Confirmed**:
- University EPCIs gain 2–6pp on 20-24 share vs national mean.
- IDF suburbs lose share (Paris Est Créteil: -2.5pp). Direction is correct.
- IRIS→EPCI inconsistency (finding #10) is reduced by intra-dept scaling, but not eliminated.

**Not confirmed**:
- Whether +6pp for Montpellier is the right magnitude (could be ±2pp).
- Whether the lycée band (15_19) correction is well-calibrated (new since 2026-02).

**What blend weights mean in practice** (effective rate = 0.75×lycée_rate + 0.25×higher_ed_rate):

| Department type | 15_19 effective weight | 20_24 weight | Source |
|-----------------|----------------------|-------------|--------|
| IDF suburbs (Val-de-Marne, Seine-Saint-Denis) | ~15.6% (0.75×0% + 0.25×62.5%) | 50–60% → capped at 0.60 | MOBSCO |
| University cities (Lyon, Bordeaux…) | ~2.5% (0.75×0% + 0.25×~10%) | 5–15% → direct | MOBSCO |
| Rural departments not in MOBSCO | default 10% (same for both components) | default 30% | Constants |

The 0.60 cap on 20_24 was set to match the observed maximum IDF suburb mobility rate
(~62%). The 0.25 cap on 15_19 is conservative: lycée inter-dept mobility is below 10%
in all observed departments. The 0.25 secondary_weight reflects that ~2/5 ages in
the 15_19 band are 18-19, of which ~60% are in higher education (→ ~25% of the band).

---

### Bias 3 — Simple aging ignores mortality and migration

**What happens**: population at each age is taken directly from the census and aged
forward by shifting cohorts. No mortality or migration adjustment is applied. A person
counted as age 15 in 2022 is assumed to still be alive and in the same department at
age 18 in 2025.

**Impact**: for ages 15–24, mortality is very low (~0.05%/year) and net migration is
small at department level. The total error is estimated at ~0.2% over 4 years
(2022→2026) for the 15–24 age range.

**Advantage over previous approach**: individual ages are exact at census year (no
redistribution within 5-year bands). There is no intra-band approximation error.

---

### Bias 4 — Monthly distribution uses birth seasonality, not population seasonality

**What happens**: the `month_ratio` factor distributes a year's population across 12 months
using historical birth counts per department. For newborns (age 0), this is accurate. For
15–24 year-olds, population varies by month due to student migration (September arrivals,
June departures) — a pattern not captured by birth data.

**Impact**: monthly snapshots for 15–24 year-olds are slightly off in September and June
(student migration season). The annual total is unaffected.

**Known magnitude**: not quantified. Effect is believed to be < 2% on monthly values.

---

### Bias 5 — IRIS sub-commune resolution: 100% population coverage, ~60% spatial resolution

**What the IRIS output contains**: all national population is in the IRIS parquet
(100% population coverage, confirmed in validation). Rural communes are assigned a
single IRIS code covering the whole commune — they are NOT absent from the output.

**What "60% coverage" actually means**: about 60% of the national population lives in
communes divided into multiple IRIS codes (genuine sub-commune spatial resolution).
The remaining 40% lives in small communes that each have exactly one IRIS code, so
the IRIS estimate is identical to the commune estimate for those areas.

Urban departments (75, 69, 13): > 70% of population has sub-commune resolution.
Rural departments (23, 15, 46): < 50% of population has sub-commune resolution.

**Cannot be fixed without**: a change to INSEE's IRIS framework.

---

### Bias 6 — Mayotte uses synthesized data, not census microdata

**What happens**: Mayotte (department 976) is not included in the standard INDCVI census
parquet. Its population is synthesized from quinquennal estimates with a simplified age
structure (5-year band totals distributed uniformly within each band).

**Impact**: Mayotte estimates are less precise than metropolitan departments. Age
distribution within each band is uniform rather than census-derived.

---

### Bias 7 — Age composition varies across projection years

**What happens**: with simple aging, different birth cohorts enter and exit the 15–24
age range each year. The 20-24 share within 15-24 fluctuates because census cohort
sizes vary (reflecting historical birth rate variations).

**This is expected behavior**, not an error — it reflects real demographic structure.
Geographic ratios (EPCI, IRIS) are fixed at census values, so year-over-year changes
in EPCI population are driven solely by which cohorts are in the age range that year.

---

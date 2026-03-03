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
- Department-level band totals (15–19 and 20–24) are exact by construction — calibrated to INSEE quinquennal estimates, 0.00% error at regional level.
- EPCI coverage is 100% (625 EPCIs). EPCI totals aggregate to department totals with no rounding loss.
- MOBSCO student correction goes in the right direction: university EPCIs gain +2 to +6pp on 20-24 share; IDF suburbs lose share.

**What is approximate:**
- Individual age splits within 5-year bands: ±3–5% error on single ages (e.g. age 15 is -3%, age 22 is +5%), but band totals are exact.
- EPCI geographic distribution: frozen at 2022 census ratios. Confidence degrades ~1%/year beyond census.
- MOBSCO correction magnitude: direction confirmed, ±2pp uncertainty on the correction amount.
- IRIS-level estimates: reliable at dept level (100% population coverage); do not aggregate IRIS to derive EPCI totals for IDF suburbs (structural 12–19% gap for ~20 EPCIs).

**Hard limits:**
- Quinquennal data ends at 2026. Years 2027–2030 in the output are frozen at 2026 values (no demographic projection beyond that year).
- No individual migration or mortality modelling — this is a ratio-based interpolation, not a demographic model.
- MOBSCO correction magnitude has not been independently validated at EPCI level. An official sub-departmental age pyramid does not exist.

---

## What this model produces and for what use

The pipeline outputs monthly population estimates by age, sex, and geographic unit
(department, EPCI, IRIS) for ages 0–120, years 1975–2036 (extendable). It is designed for:

- **Tracking youth population (15–25) at EPCI level** — primary use case at Pass Culture
- **Estimating territorial reach** for cultural programs (how many potential users in a given zone)
- **Interpolating between census years** at sub-departmental granularity

It is **not** a demographic projection model. It does not model fertility, mortality, or
migration explicitly. It extends known INSEE estimates using stable ratio patterns from
the most recent census. For official demographic projections, use INSEE's Omphale tool.

---

## What is calibrated vs what is estimated

| Component | Calibrated to | Ground truth available |
|-----------|--------------|------------------------|
| Department band totals | INSEE quinquennal estimates (1975–2026) | Yes — same source |
| Individual age splits within bands | INDCVI 2022 census, cohort-shifted | Approximate |
| Monthly distribution | Monthly birth data per dept | Approximate |
| EPCI geographic split | INDCVI 2022 census | Frozen at census year |
| EPCI student correction | MOBSCO 2022 commuting data | Direction validated only |
| IRIS geographic split | INDCVI 2022 census, commune-distributed | 100% pop; ~60% sub-commune resolution |

**Key implication**: department-level band totals are exact by construction. All uncertainty
is in how the total is distributed across individual ages, months, and sub-geographies.

---

## Empirical validation findings (Feb 2026)

### Regional level — exact match by construction

Comparison: our dept-level projections aggregated to region vs INSEE interactive pyramid
(`donnees_pyramide_act.csv`) for all 13 metropolitan regions, ages 15–24, years 2015–2030.

- **Total population per region**: 0.00% error every year, every region.
  This is expected — quinquennal anchoring makes department totals identical to INSEE.
- **20-24 band**: exact match (it is the quinquennal total directly).
- **Sex ratio**: identical to INSEE at regional level.
- **Year drift 2015–2030**: 0.00% — quinquennal anchoring prevents drift entirely at this level.

The 15–17 vs 18–19 sub-band split shows minor variations (< 0.5pp in most regions). This
reflects age_ratio modelling accuracy within the 15–24 band, not a calibration error.

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

### Multi-year drift (Feb 2026)

University EPCI 20-24 shares drift **1.4–2.7pp over 2015–2030**. All EPCIs exceed the
1pp threshold but the direction is stable — shares in 2022 are within ±1pp of 2015.
After 2026, estimates freeze (quinquennal data ends; projection uses constant band totals).

This drift is within the EPCI confidence interval (+3% + 1%/year beyond census year).

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

**Post-2026 freeze**: the quinquennal dataset ends at 2026. For years 2027 onward,
department-level band totals (factor A) are held constant at the 2026 value via a
linear extrapolation of the 2025–2026 trend. Year-over-year changes in EPCI and IRIS
population (2027–2030) reflect only the fixed geographic ratios applied to a frozen
band total — they are **not** a demographic projection. University EPCI 20-24 shares
drift 1.4–2.7pp over 2015–2030 (finding #12), which is within the expected CI growth
rate but should be interpreted with caution.

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

### Bias 3 — Individual age errors from cohort-shifting

**What happens**: within each 5-year band, individual ages (15, 16, 17…) are distributed
using INDCVI age ratios, cohort-shifted. For a 2025 projection, we look at how 15-year-olds
from 2022 are split (i.e., the 12-year-olds from the 2022 census — same birth cohort).
Band totals are exact; the within-band split drifts over time.

**Observed magnitude (national, 2026)**:

| Age | Error vs INSEE |
|-----|---------------|
| 15  | -3.0% |
| 18  | +2.5% |
| 19  | +3.3% |
| 21  | +4.1% |
| 22  | +4.8% |
| 24  | -4.0% |
| **Total 15–24** | **+0.9%** |

The sign alternates (overestimates some ages, underestimates others), confirming this is
not a systematic scaling error but a cohort-shape modelling limitation.

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

### Bias 7 — Age-band composition drift: 20-24 share fluctuates with quinquennal band changes

**What happens**: the 20-24 share within the 15-24 age range fluctuates year to year
because the quinquennal dataset provides separate band totals for 15-19 and 20-24,
and these totals vary independently. The share is not smoothed or interpolated.

**Observed magnitude (2015–2026, university EPCIs)**:

| EPCI | Share 2015 | Share 2026 | Drift |
|------|------------|------------|-------|
| Montpellier | 56.8% | 54.6% | +2.71pp |
| Toulouse | 56.3% | 54.6% | +2.68pp |
| Rennes | 53.9% | 52.2% | +2.58pp |
| Métropole Grand Paris | 54.6% | 53.7% | +2.45pp |

The drift is within the EPCI confidence interval (+3% + 1%/year). Shares in 2022 are
within ±1pp of 2015 values (structure-repeats hypothesis holds reasonably well).

**Post-2026 behavior**: when the quinquennal dataset ends (currently 2026), band totals
beyond that year are extrapolated at constant growth. Year-over-year fluctuations in the
20-24 share stop after 2026 — the share "freezes" at the last extrapolated ratio.
Years 2027–2030 in the output are thus nearly identical to 2026 (finding #12).

---

## What good validation would look like

The validation currently available:

| Test | What it confirms | Script |
|------|-----------------|--------|
| Regional totals vs INSEE pyramids | Calibration correctness (should be exact) | `validation/compare_regional.py` |
| EPCI consistency (EPCI → dept aggregation) | No data loss in geographic disaggregation | `validation/compare_epci.py` |
| MOBSCO direction (university vs suburb EPCIs) | Correction goes the right way | `validation/compare_epci.py` |

What is still missing:

| Test | What it would confirm | External source |
|------|----------------------|----------------|
| 20-24 EPCI magnitude vs student enrollment | MOBSCO correction magnitude | SISE/MESRI enrollment by commune |
| IRIS vs RP2022 IRIS tables | IRIS distribution accuracy | INSEE `BTT_TD_POP1B_2022.csv` |
| Multi-year EPCI drift (2022 vs 2025) | How fast geo_ratios degrade | Re-run with 2023+ quinquennal when available |

---

## Data source catalog

| Source | INSEE page | File | Granularity |
|--------|------------|------|-------------|
| INDCVI census microdata | [8647104](https://www.insee.fr/fr/statistiques/8647104) | `RP2022_indcvi.parquet` | Person-level by IRIS/commune/dept |
| Quinquennal age pyramid estimates | [8721456](https://www.insee.fr/fr/statistiques/8721456) | `estim-pop-dep-sexe-aq-1975-2026.xlsx` | Dept × 5yr band × sex × year |
| Monthly births by department | [6041515](https://www.insee.fr/fr/statistiques/6041515) | various | Dept × month × year |
| MOBSCO student commuting | [8589945](https://www.insee.fr/fr/statistiques/8589945) | `RP2022_mobsco.parquet` | Person-level (residence + study commune) |
| Commune-EPCI correspondence | geo.api.gouv.fr | API JSON | Commune level, updated with COG |
| Regional interactive pyramids | [5014911](https://www.insee.fr/fr/outil-interactif/5014911) | `data/Reg/{code}/donnees_pyramide_act.csv` | Region × age × sex × year |

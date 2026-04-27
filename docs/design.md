# Design choices

For the step-by-step pipeline reference, see
[`method.md`](./method.md). For run-by-run numeric results, see
[`findings.md`](./findings.md).

## What is calibrated vs. what is estimated

| Component | Source | Accuracy |
|---|---|---|
| Department population by individual age | INDCVI 2022 census (redistributed per `--method`) | Exact at census year; ~0.2% drift over 4 years (no mortality/migration) |
| Monthly distribution | INDREG MNAI (with regional + metro fallback); N4D fallback | Reflects month of birth of the living population; regional fallback for depts <700k |
| EPCI geographic split | INDCVI 2022 census | Frozen at census year |
| EPCI student correction | MOBSCO 2022 commuting data | Direction validated only |
| IRIS geographic split | INDCVI 2022 census, commune-distributed | 100% pop; ~60% sub-commune resolution |
| Mayotte | 2017 POP1B census aged forward (quinquennal fallback) | Precise age pyramid; totals rescaled to current INSEE estimate |

**Key implication**: department-level population at each individual age
is exact at census year. For projection years, the only error source is
the simple aging assumption (no mortality/migration). All other
uncertainty is in how population is distributed across months and
sub-geographies.

## Core posture: census redistribution, no demographic model

The pipeline is deliberately not a demographic projection model. It
does not attempt to model fertility, mortality, or migration. It takes
the 2022 census (RP2022 INDCVI) and rebuilds a per-year snapshot from
two assumptions: national cohort sizes are conserved (no deaths, no
net migration) and either age-specific or cohort-specific geographic
patterns are frozen at the census.

Two methods are exposed via `--method`:

- **`cohort-stable`** (default): national cohort totals times
  age-specific census dept shares. For year `Y`, the share of dept `D`
  among people of age `A`, sex `S` is the census share of `(A, S)` in
  `D`. This is the method described in the INSEE internal spec doc.
  It renews the age-specific distribution each year — so the
  distribution of 18-year-olds in 2026 matches the distribution of
  18-year-olds at census, *not* the distribution of 14-year-olds at
  census. This implicitly captures post-bac migration.

- **`cohort-aging`** (legacy): population at age `A` in year `Y` is
  the census population at age `A − (Y − census_year)` in the same
  dept. Each cohort ages in place. Closer to a "follow the cohort"
  view; useful as a reference and for comparing against MOBSCO-based
  corrections.

Both methods agree at `Y = census_year` and preserve national cohort
totals. They differ only in how geographic distribution drifts with
time.

This works because the primary use case (Pass Culture: territorial
reach for 15-24 year-olds) tolerates ~1-2% total drift over a 4-year
horizon. Mortality on this band is ~0.05%/year; net inter-department
migration at these ages is small — but systematic for post-bac cohorts
concentrating in university cities, which is why `cohort-stable` is
the default. Overcomplicating with fertility scenarios would add more
assumption error than it removes. Total error is estimated at ~0.2%
over 4 years (2022→2026) for the 15-24 range under `cohort-aging`;
see [`findings.md`](./findings.md) Finding 2.

The horizon cap is mechanical: with `min_age = 15` and
`census_year = 2022`, projections run safely through 2037. Beyond
that, the youngest cohort needed was not yet born at census time.

**Consequence — age composition varies across projection years.**
Different birth cohorts enter and exit the 15-24 window each year.
The 20-24 share fluctuates because census cohort sizes differ
(reflecting historical birth rate variations). This is expected
demographic structure, not an error — and it is identical under both
methods.

## MNAI over N4D for month-of-birth

Two options for the `birth_month` distribution:

- **MNAI** (from INDREG): month of birth of the *living* population
  observed at census time.
- **N4D**: count of recent births by month × department.

For cohort analysis MNAI is conceptually correct: an 18-year-old in
2022 was born in 2003-2004, so their birth-month distribution should
reflect *their* birth year, not the seasonality of 2022 births.

MNAI is published per-department only for departments ≥ 700 000 pop
(INSEE disclosure threshold). Smaller departments inherit the
regional MNAI distribution; Mayotte inherits the metropolitan
aggregate. N4D stays as a fallback when INDREG is unreachable.

**Limit.** The `month_ratio` still represents *birth* seasonality,
not *population stock* seasonality. Student arrivals / departures
(September / June) are not modelled. Monthly snapshots for 15-24
year-olds are therefore slightly off in those months. The annual
total is unaffected (ratios sum to 1). Effect believed to be < 2%
on monthly values; not independently quantified.


## Mayotte POP1B aged forward, not synthesised

INDCVI does not include Mayotte. Two options:

- **Synthesise** a Mayotte pyramid by applying the DOM quinquennal
  age structure to the Mayotte total estimate.
- **Age forward** the 2017 POP1B census.

POP1B is the real observed age pyramid, so it is preferable. We age
cohorts forward by `census_year − 2017` and rescale per sex to the
current-year dept estimate — keeping the age *shape* from POP1B and
the *total* from the annual estimate.

Residual bias: the rescale treats mortality and Mayotte net migration
between 2017 and 2022 as uniform across ages. For 15-24, that
assumption is acceptable.

## Age bands, not individual ages, at sub-department levels

Computing geo-ratios per *individual age* would give zero coverage in
many rural communes because INSEE masks IRIS with < 200 inhabitants.
Using 5-year bands (the `AGE_BUCKETS` definition in `constants.py`)
keeps coverage close to 100% without hurting the 15-24 use case —
within each band the individual age shape still comes from the
dept-level cohort aging, which is exact.

## Per-geographic-level strategy

Rule of thumb: **the finer the geography, the more approximations
stack, so we bias toward robust priors rather than clever per-cell
estimates.**

- **Department** — direct INDCVI aggregation, Mayotte added via
  POP1B. Exact at census year, simple-aging drift thereafter.
  `CI_BASE_NEAR = 2%`, `+1%/yr` beyond 3yr.
- **EPCI** — dept population × `geo_ratio(EPCI | dept, age band, sex)`,
  with MOBSCO blending for 15-19 / 20-24. Shares frozen at 2022.
  `+CI_EXTRA_EPCI = 3%`.
- **Canton** — same template, weighted by `canton_weights` to handle
  cantons spanning multiple EPCIs. `+CI_EXTRA_CANTON = 5%`.
- **IRIS** — same template but one rung finer. The MOBSCO blend uses
  only intra-department study flows scaled by `w × p` (with `p` =
  intra-dept fraction) to prevent IDF suburb IRIS from inflating.
  `+CI_EXTRA_IRIS = 10%`.

**Structure-repeats limit.** All geographic ratios are computed once
from the 2022 census and held constant for every projection year.
They break near new student residences, ANRU renewal zones, EPCI
boundary changes, and fast-growing suburbs. Empirical horizon:
~2-3 years before the 1%/yr drift term dominates. Cannot be fixed
without a time-series of EPCI-level population data, which INSEE
does not publish at this granularity.

**IRIS coverage semantics.** "100% pop coverage, ~60% sub-commune
resolution" means: 100% of the population appears in
`population_iris.parquet`, but only ~60% lives in communes that are
actually subdivided into multiple IRIS codes. The remaining 40%
lives in small communes with a single IRIS = the whole commune, so
IRIS estimates equal commune estimates for those areas. Urban depts
(75, 69, 13) are >70% sub-commune; rural depts (23, 15, 46) <50%.
Cannot be fixed without a change to INSEE's IRIS framework.

## MOBSCO: direction is strong, magnitude is our best guess

MOBSCO is residence-to-study commuting data. The
pipeline blends census residential ratios with study-destination
ratios for 15-19 and 20-24 bands, with per-department blend weights
capped to reflect observed maxima (~60% in IDF suburbs for 20-24,
~25% for 15-19 lycée).

**MOBSCO composes with either `--method`.** MOBSCO only modifies
*within-department* ratios (`geo_ratios_epci`, `geo_ratios_iris`);
the IRIS variant restricts to intra-dept study flows (see "Why the
IRIS correction is weaker than the EPCI one" below). Under
`cohort-stable`, across-dept post-bac migration is already reflected
in the dept-level age-specific shares, so the two corrections do not
double-count: MOBSCO addresses intra-dept commuting (e.g.
residence-in-suburb, studies-in-city-centre) that census residential
data alone misses. Both methods ship with MOBSCO on by default.

Validation is indirect: university EPCIs show +3 to +8pp on 20-24
share vs. national mean (Toulouse +8.3, Montpellier +8.2, Grand Paris
+7.7), and pure residential IDF suburbs show -8.7 to +3 (Paris Est
Créteil -8.7). Cross-checking against SISE higher-education
enrolment gives a national enrolled/20-24 ratio of 76.8% with
university EPCIs 108-171% and residential suburbs 10-22% — the
gradient is correct.

What we cannot verify: the exact magnitude of the blend. An official
EPCI-level age pyramid would pin this down; INSEE does not publish
one.

**Blend weights in practice** (effective rate =
`0.75 × lycée_rate + 0.25 × higher_ed_rate`):

| Department type | 15-19 effective weight | 20-24 weight | Source |
|---|---|---|---|
| IDF suburbs (94, 93) | ~15.6% (0.75×0 + 0.25×62.5%) | 50-60% → capped at 0.60 | MOBSCO |
| University cities (Lyon, Bordeaux…) | ~2.5% (0.75×0 + 0.25×~10%) | 5-15% → direct | MOBSCO |
| Departments absent from MOBSCO | default 10% | default 30% | Constants |

The 0.60 cap on 20-24 matches the observed maximum IDF suburb
mobility rate (~62%). The 0.25 cap on 15-19 is conservative: lycée
inter-dept mobility is below 10% everywhere observed. The 0.25
secondary weight reflects that ~2/5 ages in the 15-19 band are
18-19, of which ~60% are in higher education (≈ 25% of the band).

## Why the IRIS correction is weaker than the EPCI one

IRIS codes are department-specific. A Val-d'Oise student studying at
a Sorbonne IRIS in Paris cannot appear in Val-d'Oise's
`geo_ratios_iris_base` because those IRIS codes live in department
75, not 95. So the IRIS correction can only use intra-department
study flows. For IDF suburbs the intra-dept fraction `p` is very
small, so the correction is a near-no-op and IRIS stays at census
ratios.

This creates a structural gap: the EPCI pipeline deflates IDF suburb
EPCIs (students leave cross-dept) while IRIS cannot apply the
equivalent deflation. About 20 EPCIs show an IRIS-vs-EPCI gap
>1%, worst case 121%. This is why the guidance is: do not aggregate
IRIS to compute EPCI totals — use `population_epci.parquet` for EPCI.

## Confidence intervals: empirical, not statistical

The `confidence_pct` values (`CI_BASE_NEAR`, `CI_PER_YEAR`,
`CI_EXTRA_*` in `constants.py`) are calibrated against observed
validation drift, not derived from a variance model:

- 2% at census year ±1: the observed regional-total deviation in
  `compare_regional.py`.
- +1%/yr beyond 3 years from census: the observed year-over-year
  drift trend in the same script (−1.5% to +1.8% across 8 years).
- +3% / +5% / +10% at EPCI / canton / IRIS: the worst-case structural
  gaps observed in `compare_iris.py`.

They are bounds on documented bias, not statistical error bars.

## Post-2026 freeze

The quinquennal estimate file publishes 1975-2026. Years beyond 2026
reuse the 2026 values (see `_extrapolate_last_year` in
`downloaders.py`). This means 2027-2030 outputs are numerically
identical to 2026. Treat them as placeholders, not forecasts.

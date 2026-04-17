"""Check whether applying a single national growth rate by year-of-birth
is safe across departments and sexes.

A plausible alternative design would be to extrapolate the census past
2022 using a single national growth rate per year-of-birth cohort, under
the hypothesis that the population evolves the same way in all
departments regardless of sex.

This repo chose a stronger anchor: dept-level annual estimates
(``AGE_PYRAMID_URL``). This script documents why — it quantifies how
much departments actually diverge from the national trend, so we can
show the dept-level anchor is needed for ≥2-digit precision at EPCI and
IRIS levels. See ``docs/design.md`` for the decision record.

Usage:
    uv run python validation/compare_dept_growth.py
    uv run python validation/compare_dept_growth.py --start-year 2017 --end-year 2022

Outputs (stdout):
  - p50 / p90 / p99 of |dept growth - national growth| per age band x sex.
  - Top 5 departments whose growth deviates most from the national trend.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from passculture.data.insee_population.downloaders import (
    download_quinquennal_estimates,
)

DEFAULT_CACHE = Path("data/cache")


def compute_growth_dispersion(
    start_year: int, end_year: int, cache_dir: Path
) -> pd.DataFrame:
    """Return |dept growth rate - national growth rate| per (age_band, sex, dept).

    Growth rate = pop(end_year) / pop(start_year) - 1, aligned by
    ``year_of_birth`` cohort (a cohort in band ``a`` at ``start_year`` is
    in band ``a + (end_year - start_year) / 5`` at ``end_year``). For
    simplicity we compare like-for-like bands (5-year step assumption,
    equivalent to cohorts advancing one full band).

    National growth is computed as the sum over all departments.
    """
    df = download_quinquennal_estimates(start_year, end_year, cache_dir=cache_dir)
    if df.empty:
        raise RuntimeError("No quinquennal estimates downloaded")

    # Keep only endpoint years
    df = df[df["year"].isin([start_year, end_year])].copy()

    # National population per (year, age_band, sex)
    national = (
        df.groupby(["year", "age_band", "sex"])["population"]
        .sum()
        .unstack("year")
        .reset_index()
        .rename(columns={start_year: "nat_start", end_year: "nat_end"})
    )
    national["nat_growth"] = national["nat_end"] / national["nat_start"] - 1

    # Department-level endpoint comparison
    dept = df.pivot_table(
        index=["department_code", "age_band", "sex"],
        columns="year",
        values="population",
        aggfunc="sum",
    ).reset_index()
    dept = dept.rename(columns={start_year: "dept_start", end_year: "dept_end"})
    dept = dept[(dept["dept_start"] > 0) & (dept["dept_end"] > 0)]
    dept["dept_growth"] = dept["dept_end"] / dept["dept_start"] - 1

    merged = dept.merge(
        national[["age_band", "sex", "nat_growth"]],
        on=["age_band", "sex"],
    )
    merged["deviation"] = (merged["dept_growth"] - merged["nat_growth"]).abs()
    return merged


def _print_table(df: pd.DataFrame, cols: list[str], title: str) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    print(df[cols].to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-year", type=int, default=2017)
    parser.add_argument("--end-year", type=int, default=2022)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE)
    args = parser.parse_args()

    merged = compute_growth_dispersion(args.start_year, args.end_year, args.cache_dir)

    # 1. Dispersion summary
    summary = (
        merged.groupby(["age_band", "sex"])["deviation"]
        .agg(
            p50="median",
            p90=lambda s: s.quantile(0.90),
            p99=lambda s: s.quantile(0.99),
        )
        .reset_index()
    )
    for c in ("p50", "p90", "p99"):
        summary[c] = (summary[c] * 100).round(2)
    _print_table(
        summary,
        ["age_band", "sex", "p50", "p90", "p99"],
        f"Dept-vs-national growth deviation ({args.start_year}-{args.end_year}, %)",
    )

    # 2. Most divergent departments overall
    worst = (
        merged.groupby("department_code")["deviation"]
        .mean()
        .sort_values(ascending=False)
        .head(5)
        .mul(100)
        .round(2)
        .rename("mean_dev_pct")
        .reset_index()
    )
    _print_table(
        worst,
        ["department_code", "mean_dev_pct"],
        "Top divergent departments",
    )

    # 3. Interpretation
    national_uniform_ok = summary["p90"].max() < 2.0
    print("\nConclusion:")
    if national_uniform_ok:
        print(
            "  p90 dept deviation < 2pp across all bands — uniform national"
            "  growth would be defensible."
        )
    else:
        print(
            "  p90 dept deviation exceeds 2pp — uniform national growth"
            "  understates regional variation. This justifies the code's"
            "  dept-level anchoring."
        )


if __name__ == "__main__":
    main()

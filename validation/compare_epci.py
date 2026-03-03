"""Validate EPCI-level projections via internal consistency and distribution checks.

No official EPCI-level pyramids exist, so we use four approaches:
  1. Consistency: EPCI totals aggregated to region must match dept totals.
  2. Coverage: what fraction of the 15-24 population is covered by EPCIs?
  3. MOBSCO direction check: known university EPCIs should have elevated 20-24 share;
     known IDF suburbs should have depressed 20-24 share vs national mean.
  4. Top/bottom EPCIs by 20-24 share: inspect whether university
     cities dominate the top.
  5. Multi-year drift: how do university EPCI shares evolve year-over-year?

Usage:
    uv run python validation/compare_epci.py \
        --dept data/output/population_department.parquet \
        --epci data/output/population_epci.parquet \
        --year 2022

    # Run drift analysis across all years in the parquet:
    uv run python validation/compare_epci.py \
        --dept data/output/population_department.parquet \
        --epci data/output/population_epci.parquet \
        --year 2022 --drift
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

# Known university-dominated EPCIs (net student importers)
KNOWN_UNIVERSITY_EPCIS = {
    "243400017": "Montpellier Méd. Métr.",
    "200054781": "Métropole Grand Paris",
    "200046977": "Métropole de Lyon",
    "243300316": "Bordeaux Métropole",
    "243500139": "Rennes Métropole",
    "243100518": "Toulouse Métropole",
    "200040715": "Grenoble-Alpes-Métr.",
    "242900314": "Brest Métropole",
    "244400404": "Nantes Métropole",
    "200054807": "Aix-Marseille-Prov.",
    "246700488": "Strasbourg Eurométr.",
    "245900410": "Lille Métropole",
}

# Known IDF suburban EPCIs (net student exporters — students commute to Paris)
KNOWN_IDF_SUBURB_EPCIS = {
    "200057958": "Grand Paris Sud Est",
    "200058519": "Paris Est Créteil",
    "200057313": "Plaine Commune",
    "200057362": "Est Ensemble",
    "200057990": "Grand-Orly Seine Bièvr",
    "200057412": "Paris Terres d'Envol",
    "200057875": "Val de Bièvre",
}


def load_dept(conn: duckdb.DuckDBPyConnection, path: Path, year: int) -> None:
    conn.execute(f"""
        CREATE OR REPLACE TABLE dept_proj AS
        SELECT region_code, age::INTEGER AS age, sex, SUM(population) AS pop
        FROM read_parquet('{path}')
        WHERE year::INTEGER = {year} AND month = 1
          AND region_code IS NOT NULL AND region_code != ''
        GROUP BY region_code, age, sex
    """)


def load_epci(conn: duckdb.DuckDBPyConnection, path: Path, year: int) -> None:
    conn.execute(f"""
        CREATE OR REPLACE TABLE epci_proj AS
        SELECT epci_code, department_code, region_code,
               age::INTEGER AS age, sex, SUM(population) AS pop
        FROM read_parquet('{path}')
        WHERE year::INTEGER = {year} AND month = 1
          AND epci_code IS NOT NULL AND epci_code != ''
        GROUP BY epci_code, department_code, region_code, age, sex
    """)
    conn.execute("""
        CREATE OR REPLACE TABLE epci_by_region AS
        SELECT region_code, age, sex, SUM(pop) AS pop
        FROM epci_proj
        GROUP BY region_code, age, sex
    """)


def print_table(
    rows: list[dict], columns: list[str], title: str, width: int = 80
) -> None:
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print(f"{'=' * width}")
    if not rows:
        print("  (no data)")
        return
    widths = {
        c: max(len(c), max(len(str(r.get(c, ""))) for r in rows)) for c in columns
    }
    print("  " + "  ".join(c.ljust(widths[c]) for c in columns))
    print("  " + "-" * (sum(widths.values()) + 2 * len(columns)))
    for row in rows:
        print("  " + "  ".join(str(row.get(c, "")).ljust(widths[c]) for c in columns))


def run(dept_path: Path, epci_path: Path, year: int) -> None:
    conn = duckdb.connect()

    print(f"\nLoading dept projections from {dept_path} (year {year})...")
    load_dept(conn, dept_path, year)

    print(f"Loading EPCI projections from {epci_path} (year {year})...")
    load_epci(conn, epci_path, year)

    age_min, age_max = conn.execute(
        "SELECT MIN(age), MAX(age) FROM dept_proj"
    ).fetchone()

    # -----------------------------------------------------------------------
    # Check 1: EPCI aggregated to region vs dept totals (consistency)
    # -----------------------------------------------------------------------
    regions = sorted(
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT region_code FROM dept_proj WHERE region_code IS NOT NULL"
        ).fetchall()
    )

    consist_rows = []
    for reg in regions:
        dept_total = (
            conn.execute(
                f"SELECT SUM(pop) FROM dept_proj WHERE region_code = '{reg}'"
            ).fetchone()[0]
            or 0.0
        )
        epci_total = (
            conn.execute(
                f"SELECT SUM(pop) FROM epci_by_region WHERE region_code = '{reg}'"
            ).fetchone()[0]
            or 0.0
        )
        coverage_pct = epci_total / dept_total * 100 if dept_total else 0.0
        consist_rows.append(
            {
                "region": reg,
                "dept_total": f"{dept_total:,.0f}",
                "epci_total": f"{epci_total:,.0f}",
                "epci_coverage_%": f"{coverage_pct:.1f}%",
            }
        )

    print_table(
        consist_rows,
        ["region", "dept_total", "epci_total", "epci_coverage_%"],
        "Consistency: EPCI vs dept totals by region"
        f" - ages {age_min}-{age_max}, year {year}",
    )

    # -----------------------------------------------------------------------
    # Check 2: Overall EPCI coverage
    # -----------------------------------------------------------------------
    total_dept = conn.execute("SELECT SUM(pop) FROM dept_proj").fetchone()[0] or 1.0
    total_epci = conn.execute("SELECT SUM(pop) FROM epci_proj").fetchone()[0] or 0.0
    n_epcis = conn.execute(
        "SELECT COUNT(DISTINCT epci_code) FROM epci_proj"
    ).fetchone()[0]
    coverage = total_epci / total_dept * 100
    print(f"\n  Overall EPCI coverage: {coverage:.1f}% ({n_epcis} EPCIs)")
    missing = total_dept - total_epci
    missing_pct = missing / total_dept * 100
    print(
        f"  Missing from EPCI: {missing:,.0f} persons "
        f"({missing_pct:.1f}%) - rural communes without EPCI"
    )

    # -----------------------------------------------------------------------
    # Check 3: National 20-24 share benchmark
    # -----------------------------------------------------------------------
    if age_max >= 24 and age_min <= 20:
        national_20_24 = (
            conn.execute(
                "SELECT SUM(pop) FROM dept_proj WHERE age BETWEEN 20 AND 24"
            ).fetchone()[0]
            or 0.0
        )
        national_15_24 = (
            conn.execute(
                "SELECT SUM(pop) FROM dept_proj WHERE age BETWEEN 15 AND 24"
            ).fetchone()[0]
            or 0.0
        )
        nat_share = national_20_24 / national_15_24 * 100
        print(
            "\n  National 20-24 / 15-24 share:"
            f" {nat_share:.2f}%  (reference for EPCI comparison)"
        )

    # -----------------------------------------------------------------------
    # Check 4: University EPCIs — 20-24 share
    # -----------------------------------------------------------------------
    if age_max >= 24 and age_min <= 20:
        univ_rows = []
        for epci_code, epci_name in KNOWN_UNIVERSITY_EPCIS.items():
            row = conn.execute(f"""
                SELECT
                    SUM(CASE WHEN age BETWEEN 20 AND 24 THEN pop ELSE 0 END) AS p2024,
                    SUM(CASE WHEN age BETWEEN 15 AND 24 THEN pop ELSE 0 END) AS p1524,
                    SUM(pop) AS total
                FROM epci_proj WHERE epci_code = '{epci_code}'
            """).fetchone()
            if row[2] and row[2] > 0:
                share_2024_of_1524 = (row[0] / row[1] * 100) if row[1] else 0.0
                vs_national = share_2024_of_1524 - nat_share
                univ_rows.append(
                    {
                        "epci": epci_name,
                        "code": epci_code,
                        "20-24_pop": f"{row[0]:,.0f}",
                        "20-24/15-24_%": f"{share_2024_of_1524:.1f}%",
                        "vs_national": f"{vs_national:+.1f}pp",
                    }
                )
        print_table(
            univ_rows,
            ["epci", "code", "20-24_pop", "20-24/15-24_%", "vs_national"],
            "University EPCIs — 20-24 share within 15-24 (should be > national mean)",
        )

        # -----------------------------------------------------------------------
        # Check 5: IDF suburb EPCIs — 20-24 share (should be < national mean)
        # -----------------------------------------------------------------------
        suburb_rows = []
        for epci_code, epci_name in KNOWN_IDF_SUBURB_EPCIS.items():
            row = conn.execute(f"""
                SELECT
                    SUM(CASE WHEN age BETWEEN 20 AND 24 THEN pop ELSE 0 END) AS p2024,
                    SUM(CASE WHEN age BETWEEN 15 AND 24 THEN pop ELSE 0 END) AS p1524,
                    SUM(pop) AS total
                FROM epci_proj WHERE epci_code = '{epci_code}'
            """).fetchone()
            if row[2] and row[2] > 0:
                share_2024_of_1524 = (row[0] / row[1] * 100) if row[1] else 0.0
                vs_national = share_2024_of_1524 - nat_share
                suburb_rows.append(
                    {
                        "epci": epci_name,
                        "code": epci_code,
                        "20-24_pop": f"{row[0]:,.0f}",
                        "20-24/15-24_%": f"{share_2024_of_1524:.1f}%",
                        "vs_national": f"{vs_national:+.1f}pp",
                    }
                )
        print_table(
            suburb_rows,
            ["epci", "code", "20-24_pop", "20-24/15-24_%", "vs_national"],
            "IDF suburb EPCIs - 20-24 share within 15-24"
            " (MOBSCO: should be < national mean)",
        )

    # -----------------------------------------------------------------------
    # Check 6: Top-20 EPCIs by 20-24 share — should be university cities
    # -----------------------------------------------------------------------
    if age_max >= 24 and age_min <= 20:
        top_rows = conn.execute("""
            SELECT
                epci_code,
                SUM(CASE WHEN age BETWEEN 20 AND 24 THEN pop ELSE 0 END) AS p2024,
                SUM(CASE WHEN age BETWEEN 15 AND 24 THEN pop ELSE 0 END) AS p1524,
                SUM(CASE WHEN age BETWEEN 20 AND 24
                    THEN pop ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN age BETWEEN 15
                    AND 24 THEN pop ELSE 0 END), 0)
                    * 100 AS share_pct
            FROM epci_proj
            GROUP BY epci_code
            HAVING SUM(pop) > 10000
            ORDER BY share_pct DESC
            LIMIT 20
        """).fetchdf()

        print_table(
            [
                {
                    "rank": str(i + 1),
                    "epci_code": r["epci_code"],
                    "name": KNOWN_UNIVERSITY_EPCIS.get(
                        r["epci_code"], KNOWN_IDF_SUBURB_EPCIS.get(r["epci_code"], "")
                    ),
                    "20-24_pop": f"{r['p2024']:,.0f}",
                    "20-24/15-24_%": f"{r['share_pct']:.1f}%",
                    "vs_nat": f"{r['share_pct'] - nat_share:+.1f}pp",
                }
                for i, r in top_rows.iterrows()
            ],
            ["rank", "epci_code", "name", "20-24_pop", "20-24/15-24_%", "vs_nat"],
            "Top-20 EPCIs by 20-24 share within 15-24 (expected: university cities)",
        )

        # Bottom-20
        bot_rows = conn.execute("""
            SELECT
                epci_code,
                SUM(CASE WHEN age BETWEEN 20 AND 24 THEN pop ELSE 0 END) AS p2024,
                SUM(CASE WHEN age BETWEEN 15 AND 24 THEN pop ELSE 0 END) AS p1524,
                SUM(CASE WHEN age BETWEEN 20 AND 24
                    THEN pop ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN age BETWEEN 15
                    AND 24 THEN pop ELSE 0 END), 0)
                    * 100 AS share_pct
            FROM epci_proj
            GROUP BY epci_code
            HAVING SUM(pop) > 10000
            ORDER BY share_pct ASC
            LIMIT 20
        """).fetchdf()

        print_table(
            [
                {
                    "rank": str(i + 1),
                    "epci_code": r["epci_code"],
                    "name": KNOWN_UNIVERSITY_EPCIS.get(
                        r["epci_code"], KNOWN_IDF_SUBURB_EPCIS.get(r["epci_code"], "")
                    ),
                    "20-24_pop": f"{r['p2024']:,.0f}",
                    "20-24/15-24_%": f"{r['share_pct']:.1f}%",
                    "vs_nat": f"{r['share_pct'] - nat_share:+.1f}pp",
                }
                for i, r in bot_rows.iterrows()
            ],
            ["rank", "epci_code", "name", "20-24_pop", "20-24/15-24_%", "vs_nat"],
            "Bottom-20 EPCIs by 20-24 share within 15-24"
            " (expected: rural/retirement areas)",
        )

    # -----------------------------------------------------------------------
    # Check 7: Sex ratio consistency EPCI vs dept within same region
    # -----------------------------------------------------------------------
    sex_rows = []
    for reg in regions[:6]:  # first 6 regions
        dept_mf = (
            conn.execute(f"""
            SELECT
                SUM(CASE WHEN sex='male' THEN pop END) /
                NULLIF(SUM(CASE WHEN sex='female' THEN pop END), 0) AS mf
            FROM dept_proj WHERE region_code = '{reg}'
        """).fetchone()[0]
            or 0.0
        )
        epci_mf = (
            conn.execute(f"""
            SELECT
                SUM(CASE WHEN sex='male' THEN pop END) /
                NULLIF(SUM(CASE WHEN sex='female' THEN pop END), 0) AS mf
            FROM epci_proj WHERE region_code = '{reg}'
        """).fetchone()[0]
            or 0.0
        )
        sex_rows.append(
            {
                "region": reg,
                "dept_M/F": f"{dept_mf:.4f}",
                "epci_M/F": f"{epci_mf:.4f}",
                "diff": f"{epci_mf - dept_mf:+.4f}",
            }
        )
    print_table(
        sex_rows,
        ["region", "dept_M/F", "epci_M/F", "diff"],
        "Sex ratio consistency: EPCI vs dept (should be near 0 diff)",
    )

    print("\n")


def run_drift(epci_path: Path) -> None:
    """Check 5: how stable are university EPCI shares across all years in the parquet?

    The 'structure-repeats' hypothesis (geo_ratios frozen at census year) predicts that
    our projected shares for university EPCIs should stay flat over time. If they drift
    significantly, it suggests that geo_ratios are degrading or that the model is
    sensitive to year-to-year quinquennal changes.
    """
    conn = duckdb.connect()

    print(f"\n{'=' * 80}")
    print("  Check 5: Multi-year drift — university EPCI shares (20-24/15-24) by year")
    print(f"{'=' * 80}")
    print(
        "  Rationale: geo_ratios are frozen at census year. University shares should be"
    )
    print("  stable over time (structure-repeats hypothesis). Large drift indicates")
    print("  the model is sensitive to year-to-year quinquennal changes.")
    print()

    # Get all years in the file
    years = sorted(
        r[0]
        for r in conn.execute(f"""
            SELECT DISTINCT year::INTEGER FROM read_parquet('{epci_path}')
            WHERE month = 1
            ORDER BY year
        """).fetchall()
    )
    print(f"  Years in parquet: {years[0]}-{years[-1]} ({len(years)} years)")

    # Compute 20-24/15-24 share for each university EPCI by year
    epci_codes = list(KNOWN_UNIVERSITY_EPCIS.keys())
    codes_sql = ", ".join(f"'{c}'" for c in epci_codes)

    drift_df = conn.execute(f"""
        SELECT
            year::INTEGER AS year,
            epci_code,
            SUM(CASE WHEN age::INTEGER BETWEEN 20 AND 24
                THEN population ELSE 0 END) AS p2024,
            SUM(CASE WHEN age::INTEGER BETWEEN 15 AND 24
                THEN population ELSE 0 END) AS p1524
        FROM read_parquet('{epci_path}')
        WHERE month = 1
          AND epci_code IN ({codes_sql})
          AND age::INTEGER BETWEEN 15 AND 24
        GROUP BY year, epci_code
        ORDER BY year, epci_code
    """).fetchdf()

    if drift_df.empty:
        print("  (no data for known university EPCIs — check EPCI codes)")
        return

    drift_df["share_pct"] = (
        drift_df["p2024"] / drift_df["p1524"].replace(0, float("nan")) * 100
    )

    # Also compute national share per year for reference
    nat_df = conn.execute(f"""
        SELECT
            year::INTEGER AS year,
            SUM(CASE WHEN age::INTEGER BETWEEN 20 AND 24
                THEN population ELSE 0 END) AS p2024,
            SUM(CASE WHEN age::INTEGER BETWEEN 15 AND 24
                THEN population ELSE 0 END) AS p1524
        FROM read_parquet('{epci_path}')
        WHERE month = 1 AND age::INTEGER BETWEEN 15 AND 24
        GROUP BY year ORDER BY year
    """).fetchdf()
    nat_df["nat_share"] = (
        nat_df["p2024"] / nat_df["p1524"].replace(0, float("nan")) * 100
    )
    nat_by_year = dict(zip(nat_df["year"], nat_df["nat_share"], strict=False))

    # Pivot: one row per year, columns are EPCIs
    pivot_rows = []
    for year in years:
        year_data = drift_df[drift_df["year"] == year]
        nat_share = nat_by_year.get(year, float("nan"))
        row: dict[str, str] = {"year": str(year), "national": f"{nat_share:.1f}%"}
        for code, name in KNOWN_UNIVERSITY_EPCIS.items():
            subset = year_data[year_data["epci_code"] == code]
            if not subset.empty:
                share = subset.iloc[0]["share_pct"]
                delta = share - nat_share
                row[name[:12]] = f"{share:.1f}% ({delta:+.1f})"
            else:
                row[name[:12]] = "-"
        pivot_rows.append(row)

    # Print columns: year, national, then each EPCI (truncated names)
    epci_col_names = [name[:12] for name in KNOWN_UNIVERSITY_EPCIS.values()]
    columns = ["year", "national", *epci_col_names]
    print_table(
        pivot_rows,
        columns,
        "20-24/15-24 share % by year (format: share% (delta vs national))",
        width=120,
    )

    # Summary: show range of drift per EPCI across all years
    drift_summary = []
    for code, name in KNOWN_UNIVERSITY_EPCIS.items():
        subset = drift_df[drift_df["epci_code"] == code]
        if len(subset) < 2:
            continue
        min_share = subset["share_pct"].min()
        max_share = subset["share_pct"].max()
        drift_pp = max_share - min_share
        first_year = int(subset.iloc[0]["year"])
        last_year = int(subset.iloc[-1]["year"])
        drift_summary.append(
            {
                "epci": name,
                f"share_{first_year}": (
                    f"{subset[subset['year'] == first_year].iloc[0]['share_pct']:.1f}%"
                ),
                f"share_{last_year}": (
                    f"{subset[subset['year'] == last_year].iloc[0]['share_pct']:.1f}%"
                ),
                "drift_pp": f"{drift_pp:+.2f}pp",
                "flag": "!" if drift_pp > 1.0 else "",
            }
        )

    if drift_summary:
        first_yr = int(drift_df["year"].min())
        last_yr = int(drift_df["year"].max())
        print_table(
            sorted(
                drift_summary,
                key=lambda r: -float(r["drift_pp"].replace("pp", "").replace("+", "")),
            ),
            ["epci", f"share_{first_yr}", f"share_{last_yr}", "drift_pp", "flag"],
            f"University EPCI drift summary ({first_yr}-{last_yr})",
        )
        print()
        print(
            "  ! = drift > 1pp across projection horizon"
            " (may indicate geo_ratio degradation)."
        )
        print(
            "  drift is expected to be near-zero: geo_ratios are frozen at census year."
        )
        print(
            "  Non-zero drift reflects year-to-year changes in quinquennal band totals"
        )
        print("  propagating differently across EPCIs with different age structures.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dept", required=True, type=Path, help="Dept-level parquet")
    parser.add_argument("--epci", required=True, type=Path, help="EPCI-level parquet")
    parser.add_argument(
        "--year", type=int, default=2022, help="Year to compare (default: 2022)"
    )
    parser.add_argument(
        "--drift",
        action="store_true",
        help="Also run multi-year drift analysis (Check 5) across all years in parquet",
    )
    args = parser.parse_args()

    for p in (args.dept, args.epci):
        if not p.exists():
            print(f"ERROR: file not found: {p}")
            sys.exit(1)

    run(args.dept, args.epci, args.year)

    if args.drift:
        run_drift(args.epci)


if __name__ == "__main__":
    main()

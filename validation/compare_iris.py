"""Validate IRIS-level projections via internal consistency and INDCVI comparison.

Two types of checks:

  Part A — Internal consistency
    Aggregate our IRIS parquet to EPCI and department, compare against our EPCI/dept
    parquets. Tests that no population is lost in the IRIS disaggregation and that
    the IRIS layer sums back to higher-level layers.

  Part B — Census accuracy (2022 only)
    Compare our IRIS 2022 output directly against INDCVI census microdata.
    Since our geo_ratios are derived from INDCVI, near-perfect agreement in 2022
    is expected. Large discrepancies indicate a bug in the geo_ratio computation.

Usage:
    uv run python validation/compare_iris.py \\
        --iris data/output/population_iris.parquet \\
        --epci data/output/population_epci.parquet \\
        --dept data/output/population_department.parquet \\
        --indcvi data/cache/indcvi_2022.parquet \\
        --year 2022

Note: The IRIS parquet is large (~1.7 GB). Expect 30-60 seconds for DuckDB scans.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

# Departments to spot-check in Part B (mix of urban/rural)
SAMPLE_DEPTS = ["75", "69", "13", "31", "34", "44", "67", "23", "48"]


def print_table(
    rows: list[dict], columns: list[str], title: str, width: int = 90
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


def run_part_a(
    conn: duckdb.DuckDBPyConnection,
    iris_path: Path,
    epci_path: Path,
    dept_path: Path,
    year: int,
) -> None:
    """Part A: internal consistency — IRIS sums to EPCI and EPCI sums to dept."""
    print(f"\n{'#' * 70}")
    print("  PART A: Internal consistency (IRIS → EPCI → dept aggregation)")
    print(f"{'#' * 70}")

    # Load dept projections
    print(f"Loading dept projections ({dept_path.name})...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE dept_proj AS
        SELECT
            department_code,
            region_code,
            age::INTEGER AS age,
            sex,
            SUM(population) AS pop
        FROM read_parquet('{dept_path}')
        WHERE year::INTEGER = {year} AND month = 1
          AND department_code IS NOT NULL
        GROUP BY department_code, region_code, age, sex
    """)

    # Load EPCI projections
    print(f"Loading EPCI projections ({epci_path.name})...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE epci_proj AS
        SELECT
            epci_code,
            department_code,
            age::INTEGER AS age,
            sex,
            SUM(population) AS pop
        FROM read_parquet('{epci_path}')
        WHERE year::INTEGER = {year} AND month = 1
          AND epci_code IS NOT NULL
        GROUP BY epci_code, department_code, age, sex
    """)

    # Load IRIS projections aggregated to EPCI and dept
    print(f"Loading IRIS projections ({iris_path.name}) — this may take ~30 seconds...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE iris_agg_epci AS
        SELECT
            epci_code,
            department_code,
            age::INTEGER AS age,
            sex,
            SUM(population) AS pop
        FROM read_parquet('{iris_path}')
        WHERE year::INTEGER = {year} AND month = 1
          AND iris_code IS NOT NULL AND iris_code != ''
        GROUP BY epci_code, department_code, age, sex
    """)

    conn.execute(f"""
        CREATE OR REPLACE TABLE iris_agg_dept AS
        SELECT
            department_code,
            age::INTEGER AS age,
            sex,
            SUM(population) AS pop
        FROM read_parquet('{iris_path}')
        WHERE year::INTEGER = {year} AND month = 1
          AND iris_code IS NOT NULL AND iris_code != ''
        GROUP BY department_code, age, sex
    """)
    print("  Done loading.")

    _age_min, _age_max = conn.execute(
        "SELECT MIN(age), MAX(age) FROM dept_proj"
    ).fetchone()

    # -----------------------------------------------------------------------
    # A1: IRIS to EPCI coverage (what fraction of EPCI pop has IRIS rows?)
    # -----------------------------------------------------------------------
    total_epci = conn.execute("SELECT SUM(pop) FROM epci_proj").fetchone()[0] or 1.0
    total_iris_as_epci = (
        conn.execute("SELECT SUM(pop) FROM iris_agg_epci").fetchone()[0] or 0.0
    )
    n_iris_epcis = conn.execute(
        "SELECT COUNT(DISTINCT epci_code) FROM iris_agg_epci"
    ).fetchone()[0]
    n_epci_epcis = conn.execute(
        "SELECT COUNT(DISTINCT epci_code) FROM epci_proj"
    ).fetchone()[0]

    print("\n  A1: IRIS coverage of EPCI population:")
    print(f"    EPCIs in our EPCI parquet:   {n_epci_epcis}")
    print(f"    EPCIs with IRIS rows:         {n_iris_epcis}")
    print(f"    Population in EPCI parquet:  {total_epci:>14,.0f}")
    print(f"    Population in IRIS (→EPCI):  {total_iris_as_epci:>14,.0f}")
    iris_cov_pct = total_iris_as_epci / total_epci * 100
    print(f"    IRIS coverage:               {iris_cov_pct:>13.1f}%")
    print(
        f"    (remaining ~{100 - iris_cov_pct:.0f}%"
        " is rural pop with no IRIS subdivision)"
    )

    # -----------------------------------------------------------------------
    # A2: Where IRIS rows exist, do they match the EPCI parquet?
    # -----------------------------------------------------------------------
    print("\n  A2: For EPCIs with IRIS coverage — do IRIS totals match EPCI totals?")
    check = conn.execute("""
        SELECT
            i.epci_code,
            SUM(i.pop) AS iris_pop,
            SUM(e.pop) AS epci_pop,
            SUM(i.pop) / NULLIF(SUM(e.pop), 0) * 100 AS coverage_pct
        FROM iris_agg_epci i
        JOIN epci_proj e USING (epci_code, department_code, age, sex)
        GROUP BY i.epci_code
        HAVING ABS(SUM(i.pop) / NULLIF(SUM(e.pop), 0) - 1) > 0.01  -- more than 1% gap
        ORDER BY ABS(SUM(i.pop) / NULLIF(SUM(e.pop), 0) - 1) DESC
        LIMIT 20
    """).fetchdf()

    if len(check) == 0:
        print("    All EPCIs with IRIS rows match EPCI parquet within 1%. Good.")
    else:
        print(
            f"    {len(check)} EPCIs show >1% gap"
            " between IRIS and EPCI totals (top 20):"
        )
        print_table(
            [
                {
                    "epci_code": r["epci_code"],
                    "iris_pop": f"{r['iris_pop']:,.0f}",
                    "epci_pop": f"{r['epci_pop']:,.0f}",
                    "coverage_%": f"{r['coverage_pct']:.1f}%",
                }
                for _, r in check.iterrows()
            ],
            ["epci_code", "iris_pop", "epci_pop", "coverage_%"],
            "EPCIs with >1% IRIS vs EPCI gap",
        )

    # -----------------------------------------------------------------------
    # A3: IRIS to dept coverage per department
    # -----------------------------------------------------------------------
    dept_coverage_rows = conn.execute("""
        SELECT
            d.department_code,
            SUM(d.pop) AS dept_pop,
            SUM(id.pop) AS iris_pop,
            SUM(id.pop) / NULLIF(SUM(d.pop), 0) * 100 AS iris_coverage_pct
        FROM dept_proj d
        LEFT JOIN iris_agg_dept id USING (department_code, age, sex)
        GROUP BY d.department_code
        ORDER BY iris_coverage_pct ASC
        LIMIT 20
    """).fetchdf()

    print_table(
        [
            {
                "dept": r["department_code"],
                "dept_pop": f"{r['dept_pop']:,.0f}",
                "iris_pop": f"{r['iris_pop'] or 0:,.0f}",
                "iris_coverage_%": f"{r['iris_coverage_pct'] or 0:.1f}%",
            }
            for _, r in dept_coverage_rows.iterrows()
        ],
        ["dept", "dept_pop", "iris_pop", "iris_coverage_%"],
        "Bottom-20 departments by IRIS coverage"
        " (rural dept have low coverage - by design)",
    )

    # Top coverage departments
    top_coverage = conn.execute("""
        SELECT
            d.department_code,
            SUM(id.pop) / NULLIF(SUM(d.pop), 0) * 100 AS iris_coverage_pct
        FROM dept_proj d
        LEFT JOIN iris_agg_dept id USING (department_code, age, sex)
        GROUP BY d.department_code
        ORDER BY iris_coverage_pct DESC
        LIMIT 10
    """).fetchdf()

    print("\n  Top-10 departments by IRIS coverage (urban depts should be >70%):")
    for _, r in top_coverage.iterrows():
        bar = "=" * int(r["iris_coverage_pct"] / 2)
        print(
            f"    {r['department_code']:>3}: [{bar:<50}] {r['iris_coverage_pct']:.1f}%"
        )

    # Overall
    total_dept_pop = conn.execute("SELECT SUM(pop) FROM dept_proj").fetchone()[0] or 1
    total_iris_dept = (
        conn.execute("SELECT SUM(pop) FROM iris_agg_dept").fetchone()[0] or 0
    )
    dept_cov = total_iris_dept / total_dept_pop * 100
    print(f"\n  Overall IRIS coverage at dept level: {dept_cov:.1f}%")


def run_part_b(
    conn: duckdb.DuckDBPyConnection,
    iris_path: Path,
    indcvi_path: Path,
    year: int,
) -> None:
    """Part B: compare 2022 IRIS output against INDCVI census."""
    if year != 2022:
        print(
            "\n  PART B skipped - census comparison"
            f" only valid for year=2022 (got {year})."
        )
        return

    print(f"\n{'#' * 70}")
    print("  PART B: Census accuracy — our IRIS 2022 vs INDCVI (ages 15-24)")
    print(f"{'#' * 70}")
    print()
    print("  This compares our IRIS output for 2022 against the INDCVI census.")
    print(
        "  Since geo_ratios are derived from INDCVI, agreement should be near-perfect."
    )
    print("  Discrepancies indicate a bug in the geo_ratio computation pipeline.")

    # Build INDCVI aggregated at IRIS x age x sex for ages 15-24
    # Exclude ZZZZZZZZZ (INSEE code for people in communes without IRIS subdivision —
    # these residents are assigned to specific IRIS in our output, causing false diffs).
    print("\nAggregating INDCVI census data at IRIS level (ages 15-24)...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE indcvi_iris AS
        SELECT
            IRIS AS iris_code,
            AGED::INTEGER AS age,
            CASE WHEN SEXE = '1' THEN 'male' ELSE 'female' END AS sex,
            SUM(IPONDI) AS census_pop
        FROM read_parquet('{indcvi_path}')
        WHERE AGED::INTEGER BETWEEN 15 AND 24
          AND IRIS IS NOT NULL AND IRIS != ''
          AND IRIS != 'ZZZZZZZZZ'
        GROUP BY iris_code, age, sex
    """)

    n_iris_census = conn.execute(
        "SELECT COUNT(DISTINCT iris_code) FROM indcvi_iris"
    ).fetchone()[0]
    total_census = (
        conn.execute("SELECT SUM(census_pop) FROM indcvi_iris").fetchone()[0] or 0
    )
    print(f"  {n_iris_census} IRIS codes, {total_census:,.0f} persons 15-24 in INDCVI.")

    # Load our IRIS output for 2022, year-total
    # (sum over months to get annual, or use month=1).
    # We compare month=1 (January 2022) vs census snapshot
    # - should be same order of magnitude.
    # For a cleaner comparison, sum over all months (annual avg = yearly pop / 12).
    # Better: use month=1 as a single-month snapshot.
    print(f"Aggregating our IRIS output for year {year}, month=1...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE our_iris_2022 AS
        SELECT
            iris_code,
            department_code,
            age::INTEGER AS age,
            sex,
            SUM(population) AS our_pop
        FROM read_parquet('{iris_path}')
        WHERE year::INTEGER = {year} AND month = 1
          AND iris_code IS NOT NULL AND iris_code != ''
          AND age::INTEGER BETWEEN 15 AND 24
        GROUP BY iris_code, department_code, age, sex
    """)

    n_iris_ours = conn.execute(
        "SELECT COUNT(DISTINCT iris_code) FROM our_iris_2022"
    ).fetchone()[0]
    total_ours = (
        conn.execute("SELECT SUM(our_pop) FROM our_iris_2022").fetchone()[0] or 0
    )
    print(f"  {n_iris_ours} IRIS codes, {total_ours:,.0f} persons 15-24 in our output.")

    # -----------------------------------------------------------------------
    # B1: Match rate between our IRIS and census IRIS codes
    # -----------------------------------------------------------------------
    matched = conn.execute("""
        SELECT COUNT(DISTINCT o.iris_code)
        FROM our_iris_2022 o
        INNER JOIN indcvi_iris c USING (iris_code)
    """).fetchone()[0]
    only_ours = conn.execute("""
        SELECT COUNT(DISTINCT o.iris_code)
        FROM our_iris_2022 o
        LEFT JOIN indcvi_iris c USING (iris_code)
        WHERE c.iris_code IS NULL
    """).fetchone()[0]
    only_census = conn.execute("""
        SELECT COUNT(DISTINCT c.iris_code)
        FROM indcvi_iris c
        LEFT JOIN our_iris_2022 o USING (iris_code)
        WHERE o.iris_code IS NULL
    """).fetchone()[0]

    print("\n  B1: IRIS code match rate (excluding ZZZZZZZZZ from INDCVI):")
    print(f"    Both our output and INDCVI:  {matched:>6}")
    print(f"    Only in our output:          {only_ours:>6}")
    print(f"    Only in INDCVI:              {only_census:>6}")
    print("    Note: ZZZZZZZZZ (no-IRIS commune code in INDCVI) excluded from census.")

    # -----------------------------------------------------------------------
    # B2: For matched IRIS, compare totals per department
    # -----------------------------------------------------------------------
    print(
        "\n  B2: Per-department comparison"
        " (our 2022 vs INDCVI census, for sample depts):"
    )
    dept_rows = []
    for dept in SAMPLE_DEPTS:
        our_total = (
            conn.execute(
                "SELECT SUM(our_pop) FROM our_iris_2022"
                f" WHERE department_code = '{dept}'"
            ).fetchone()[0]
            or 0
        )
        census_total = (
            conn.execute(f"""
            SELECT SUM(c.census_pop)
            FROM indcvi_iris c
            INNER JOIN our_iris_2022 o USING (iris_code, age, sex)
            WHERE o.department_code = '{dept}'
        """).fetchone()[0]
            or 0
        )
        if our_total == 0:
            continue
        diff_pct = (
            (our_total - census_total) / census_total * 100
            if census_total
            else float("nan")
        )
        dept_rows.append(
            {
                "dept": dept,
                "our_pop": f"{our_total:,.0f}",
                "census_pop": f"{census_total:,.0f}",
                "diff_%": f"{diff_pct:+.2f}%",
            }
        )

    print_table(
        dept_rows,
        ["dept", "our_pop", "census_pop", "diff_%"],
        "Our 2022 IRIS output vs INDCVI census (ages 15-24, month=1)",
    )
    print()
    print("  Expected: diff < 5% for most departments.")
    print("  Large positive diff → more population in our IRIS than in INDCVI")
    print(
        "  (can happen if our month=1 population > census snapshot, or geo_ratio bug)."
    )
    print()
    print("  Note: diff is not 0.00% because:")
    print(
        "    1. MOBSCO correction: our output adds students"
        " to university depts -> higher than census."
    )
    print(
        "       This is expected - INDCVI captures residential"
        " population, we add study-destination."
    )
    print(
        "    2. Rural communes: INDCVI uses ZZZZZZZZZ"
        " (excluded here). Unmatched INDCVI residents"
    )
    print("       are not in census_pop, causing our_pop > census_pop in rural areas.")
    print("    3. Census uses full-year weights; our month=1 uses birth seasonality.")
    print(
        "  -> Use urban, non-university departments to check"
        " geo_ratio accuracy (e.g., dept 75)."
    )

    # -----------------------------------------------------------------------
    # B3: Top IRIS with largest absolute discrepancy
    # -----------------------------------------------------------------------
    outliers = conn.execute("""
        SELECT
            o.iris_code,
            o.department_code,
            SUM(o.our_pop) AS our_pop,
            SUM(c.census_pop) AS census_pop,
            ABS(SUM(o.our_pop) - SUM(c.census_pop)) AS abs_diff,
            (SUM(o.our_pop) - SUM(c.census_pop))
                / NULLIF(SUM(c.census_pop), 0) * 100 AS pct_diff
        FROM our_iris_2022 o
        INNER JOIN indcvi_iris c USING (iris_code, age, sex)
        GROUP BY o.iris_code, o.department_code
        HAVING SUM(c.census_pop) > 100  -- at least 100 persons in census
        ORDER BY abs_diff DESC
        LIMIT 15
    """).fetchdf()

    print_table(
        [
            {
                "iris_code": r["iris_code"],
                "dept": r["department_code"],
                "our_pop": f"{r['our_pop']:,.0f}",
                "census_pop": f"{r['census_pop']:,.0f}",
                "diff_%": f"{r['pct_diff']:+.1f}%",
            }
            for _, r in outliers.iterrows()
        ],
        ["iris_code", "dept", "our_pop", "census_pop", "diff_%"],
        "Top-15 IRIS by absolute discrepancy (our 2022 vs INDCVI)",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--iris", required=True, type=Path, help="IRIS-level parquet")
    parser.add_argument("--epci", required=True, type=Path, help="EPCI-level parquet")
    parser.add_argument("--dept", required=True, type=Path, help="Dept-level parquet")
    parser.add_argument(
        "--indcvi",
        required=True,
        type=Path,
        help="INDCVI census parquet (data/cache/indcvi_2022.parquet)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2022,
        help="Year to compare (default: 2022; Part B only runs for 2022)",
    )
    parser.add_argument(
        "--skip-census",
        action="store_true",
        help="Skip Part B (census comparison) — faster, no INDCVI needed",
    )
    args = parser.parse_args()

    for p in (args.iris, args.epci, args.dept):
        if not p.exists():
            print(f"ERROR: file not found: {p}")
            sys.exit(1)

    conn = duckdb.connect()

    run_part_a(conn, args.iris, args.epci, args.dept, args.year)

    if not args.skip_census:
        if not args.indcvi.exists():
            print(f"\nWARNING: INDCVI file not found at {args.indcvi}")
            print(
                "  Skipping Part B (census comparison)."
                " Use --skip-census to suppress this warning."
            )
        else:
            run_part_b(conn, args.iris, args.indcvi, args.year)

    print()


if __name__ == "__main__":
    main()

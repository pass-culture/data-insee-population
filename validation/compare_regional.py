"""Compare our department-level projections against INSEE official regional pyramids.

Usage:
    uv run python validation/compare_regional.py \
        --input data/output/population_department.parquet
    uv run python validation/compare_regional.py \
        --input data/output/population_department.parquet \
        --year 2022

The script auto-detects which age range is present in our output and restricts the
INSEE reference to the same range. This handles pipelines run with --min-age/--max-age.

Checks:
  1. Total population in detected age range, per region: ours vs INSEE.
  2. Within-band splits (15-17, 18-19, 20-24) — MOBSCO diagnostic.
  3. Sex ratio per band.

Input columns expected (dept-level parquet):
  year, age, sex (male/female), region_code, population, month
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

REGION_NAMES = {
    "11": "Île-de-France",
    "24": "Centre-Val de Loire",
    "27": "Bourgogne-Franche-Comté",
    "28": "Normandie",
    "32": "Hauts-de-France",
    "44": "Grand Est",
    "52": "Pays de la Loire",
    "53": "Bretagne",
    "75": "Nouvelle-Aquitaine",
    "76": "Occitanie",
    "84": "Auvergne-Rhône-Alpes",
    "93": "PACA",
    "94": "Corse",
}

# Net student exporters: more students leave than arrive
# -> 15-24 should be lower than census
STUDENT_EXPORTER_REGIONS = {
    "11"
}  # Île-de-France (suburbs push students to Paris intra-muros)
# Net student importers: major university clusters → 15-24 should be higher than census
STUDENT_IMPORTER_REGIONS = {"53", "76", "84"}

INSEE_PYRAMID_URL = "https://www.insee.fr/fr/outil-interactif/5014911/data/Reg/{code}/donnees_pyramide_act.csv"


def load_insee_region(
    conn: duckdb.DuckDBPyConnection,
    region_code: str,
    year: int,
    age_min: int,
    age_max: int,
) -> None:
    url = INSEE_PYRAMID_URL.format(code=region_code)
    table = f"insee_reg_{region_code}"
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT
            ANNEE::INTEGER AS year,
            AGE::INTEGER   AS age,
            SEXE           AS sexe,
            POP::DOUBLE    AS pop
        FROM read_csv_auto('{url}', header=true)
        WHERE ANNEE::INTEGER = {year}
          AND AGE::INTEGER BETWEEN {age_min} AND {age_max}
    """)


def load_our_projections(
    conn: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    year: int,
) -> tuple[int, int]:
    """Load our dept-level output, aggregate to region. Returns (age_min, age_max)."""
    conn.execute(f"""
        CREATE OR REPLACE TABLE our_regional AS
        SELECT
            year::INTEGER   AS year,
            region_code,
            age::INTEGER    AS age,
            sex,
            SUM(population) AS population
        FROM read_parquet('{parquet_path}')
        WHERE year::INTEGER = {year}
          AND month = 1
          AND region_code IS NOT NULL
          AND region_code != ''
        GROUP BY year, region_code, age, sex
    """)
    row = conn.execute("SELECT MIN(age), MAX(age) FROM our_regional").fetchone()
    return int(row[0]), int(row[1])


def _fmt_pct(v: float) -> str:
    return f"{v:+.2f}%" if v == v else "n/a"  # nan check


def _fmt_pp(v: float) -> str:
    return f"{v:+.3f}pp" if v == v else "n/a"


def print_table(
    rows: list[dict], columns: list[str], title: str, width: int = 76
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
    header = "  " + "  ".join(c.ljust(widths[c]) for c in columns)
    print(header)
    print("  " + "-" * (sum(widths.values()) + 2 * len(columns)))
    for row in rows:
        print("  " + "  ".join(str(row.get(c, "")).ljust(widths[c]) for c in columns))


def run(parquet_path: Path, year: int) -> None:
    conn = duckdb.connect()

    print(f"\nLoading our projections from {parquet_path} for year {year}...")
    age_min, age_max = load_our_projections(conn, parquet_path, year)
    print(f"  Detected age range in our data: {age_min}-{age_max}")

    our_regions = {
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT region_code FROM our_regional"
        ).fetchall()
    }
    target_regions = sorted(set(REGION_NAMES.keys()) & our_regions)

    if not target_regions:
        print(
            f"ERROR: none of the known region codes found in our data.\n"
            f"Regions present: {sorted(our_regions)}"
        )
        sys.exit(1)

    print(
        f"Fetching INSEE pyramids for {len(target_regions)}"
        f" regions (ages {age_min}-{age_max})..."
    )
    failed = []
    for code in target_regions:
        try:
            load_insee_region(conn, code, year, age_min, age_max)
        except Exception as e:
            print(
                f"  WARNING: failed to load region {code} ({REGION_NAMES[code]}): {e}"
            )
            failed.append(code)

    available = [r for r in target_regions if r not in failed]

    # -----------------------------------------------------------------------
    # Table 1: total population ages age_min-age_max
    # -----------------------------------------------------------------------
    totals = []
    for code in available:
        insee = (
            conn.execute(f"SELECT SUM(pop) FROM insee_reg_{code}").fetchone()[0] or 0.0
        )
        ours = (
            conn.execute(
                f"SELECT SUM(population) FROM our_regional WHERE region_code = '{code}'"
            ).fetchone()[0]
            or 0.0
        )
        diff_pct = (ours - insee) / insee * 100 if insee else float("nan")
        totals.append(
            {
                "region": REGION_NAMES.get(code, code),
                "insee": f"{insee:,.0f}",
                "ours": f"{ours:,.0f}",
                "diff": _fmt_pct(diff_pct),
            }
        )
    print_table(
        totals,
        ["region", "insee", "ours", "diff"],
        f"Total population ages {age_min}-{age_max} per region - year {year}",
    )

    # -----------------------------------------------------------------------
    # Table 2: sub-band breakdown 15-17 / 18-19 / 20-24 (MOBSCO diagnostic)
    # -----------------------------------------------------------------------
    # Only makes sense if we cover the 15-24 range
    if age_min <= 15 and age_max >= 24:
        sub_bands = [
            (15, 17, "15-17 (lycée)"),
            (18, 19, "18-19 (post-bac)"),
            (20, 24, "20-24 (univ)"),
        ]
        band_rows = []
        for code in available:
            insee_total = (
                conn.execute(f"SELECT SUM(pop) FROM insee_reg_{code}").fetchone()[0]
                or 1.0
            )
            our_total = (
                conn.execute(
                    "SELECT SUM(population) FROM our_regional"
                    f" WHERE region_code = '{code}'"
                ).fetchone()[0]
                or 1.0
            )
            for lo, hi, label in sub_bands:
                insee_band = (
                    conn.execute(
                        f"SELECT SUM(pop) FROM insee_reg_{code}"
                        f" WHERE age BETWEEN {lo} AND {hi}"
                    ).fetchone()[0]
                    or 0.0
                )
                our_band = (
                    conn.execute(
                        f"SELECT SUM(population) FROM our_regional "
                        f"WHERE region_code = '{code}' AND age BETWEEN {lo} AND {hi}"
                    ).fetchone()[0]
                    or 0.0
                )
                insee_share = insee_band / insee_total * 100
                our_share = our_band / our_total * 100
                diff_pp = our_share - insee_share
                flag = ""
                if abs(diff_pp) > 0.3 and label.startswith("15-17"):
                    flag = " !"  # unexpected — lycée should be unaffected by MOBSCO
                band_rows.append(
                    {
                        "region": REGION_NAMES.get(code, code)[:22],
                        "band": label,
                        "insee_%": f"{insee_share:.2f}%",
                        "ours_%": f"{our_share:.2f}%",
                        "diff": f"{diff_pp:+.3f}pp{flag}",
                    }
                )
        print_table(
            band_rows,
            ["region", "band", "insee_%", "ours_%", "diff"],
            "Sub-band shares within 15-24 — MOBSCO correction diagnostic",
        )

    # -----------------------------------------------------------------------
    # Table 3: sex ratio 15-24 (M/F should be ~0.95-1.05)
    # -----------------------------------------------------------------------
    sex_rows = []
    for code in available:
        insee_m = (
            conn.execute(
                f"SELECT SUM(pop) FROM insee_reg_{code} WHERE sexe = 'M'"
            ).fetchone()[0]
            or 0.0
        )
        insee_f = (
            conn.execute(
                f"SELECT SUM(pop) FROM insee_reg_{code} WHERE sexe = 'F'"
            ).fetchone()[0]
            or 1.0
        )
        our_m = (
            conn.execute(
                f"SELECT SUM(population) FROM our_regional "
                f"WHERE region_code = '{code}' AND sex = 'male'"
            ).fetchone()[0]
            or 0.0
        )
        our_f = (
            conn.execute(
                f"SELECT SUM(population) FROM our_regional "
                f"WHERE region_code = '{code}' AND sex = 'female'"
            ).fetchone()[0]
            or 1.0
        )
        sex_rows.append(
            {
                "region": REGION_NAMES.get(code, code)[:22],
                "insee_M/F": f"{insee_m / insee_f:.4f}",
                "ours_M/F": f"{our_m / our_f:.4f}",
                "diff": f"{(our_m / our_f) - (insee_m / insee_f):+.4f}",
            }
        )
    print_table(
        sex_rows,
        ["region", "insee_M/F", "ours_M/F", "diff"],
        f"Sex ratio (M/F) ages {age_min}-{age_max}",
    )

    # -----------------------------------------------------------------------
    # Table 4: per-year accuracy (how does drift look as we move from 2022?)
    # -----------------------------------------------------------------------
    years_in_data = sorted(
        {
            r[0]
            for r in conn.execute(
                f"SELECT DISTINCT year FROM read_parquet('{parquet_path}') "
                f"WHERE month = 1"
            ).fetchall()
        }
    )
    if len(years_in_data) > 1:
        drift_rows = []
        for y in years_in_data:
            # Reload at year y for all regions combined
            conn.execute(f"""
                CREATE OR REPLACE TABLE our_year AS
                SELECT region_code, age::INTEGER AS age, SUM(population) AS pop
                FROM read_parquet('{parquet_path}')
                WHERE year::INTEGER = {y} AND month = 1
                  AND region_code IS NOT NULL AND region_code != ''
                GROUP BY region_code, age
            """)
            # Reload INSEE for year y
            total_insee = 0.0
            total_ours = 0.0
            for code in available:
                try:
                    conn.execute(f"""
                        CREATE OR REPLACE TABLE insee_year_{code} AS
                        SELECT AGE::INTEGER AS age, SUM(POP::DOUBLE) AS pop
                        FROM read_csv_auto(
                            '{INSEE_PYRAMID_URL.format(code=code)}',
                            header=true)
                        WHERE ANNEE::INTEGER = {y}
                          AND AGE::INTEGER BETWEEN {age_min} AND {age_max}
                        GROUP BY AGE
                    """)
                    total_insee += (
                        conn.execute(
                            f"SELECT SUM(pop) FROM insee_year_{code}"
                        ).fetchone()[0]
                        or 0.0
                    )
                    total_ours += (
                        conn.execute(
                            "SELECT SUM(pop) FROM our_year"
                            f" WHERE region_code = '{code}'"
                        ).fetchone()[0]
                        or 0.0
                    )
                except Exception:
                    pass
            if total_insee > 0:
                diff_pct = (total_ours - total_insee) / total_insee * 100
                drift_rows.append(
                    {
                        "year": str(y),
                        "insee_total": f"{total_insee:,.0f}",
                        "ours_total": f"{total_ours:,.0f}",
                        "diff": _fmt_pct(diff_pct),
                    }
                )
        print_table(
            drift_rows,
            ["year", "insee_total", "ours_total", "diff"],
            "Year-by-year drift - all metro regions"
            f" combined (ages {age_min}-{age_max})",
        )

    print(f"\n{'=' * 76}")
    print("  Legend")
    print(f"{'=' * 76}")
    print("  diff > 0  → our projection is HIGHER than INSEE estimate")
    print("  diff < 0  → our projection is LOWER than INSEE estimate")
    print("  ! in band rows → lycée band (15-17) shifted by > 0.3pp,")
    print("    which suggests MOBSCO correction is leaking into lycée ages.")
    print("  Expected pattern: IDF should be slightly lower (student exporter),")
    print("    Bretagne/Occitanie/AURA slightly higher (university importers).")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        type=Path,
        help="Path to our department-level projections parquet",
    )
    parser.add_argument(
        "--year", "-y", type=int, default=2022, help="Reference year (default: 2022)"
    )
    args = parser.parse_args()

    if not args.input.exists():
        print(f"ERROR: file not found: {args.input}")
        sys.exit(1)

    run(args.input, args.year)


if __name__ == "__main__":
    main()

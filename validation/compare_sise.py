"""Validate 20-24 EPCI projections against SISE higher-education enrollment data.

SISE (Système d'Information sur le Suivi de l'Étudiant) is the Ministry of Higher
Education's enrollment database. It covers all higher-education students by institution
commune, sex, training type, and year.

This script compares:
  - Total SISE enrollment per EPCI (from MESRI open data) for a given academic year
  - Our projected 20-24 population per EPCI for the corresponding calendar year

Key caveats:
  - SISE has no age breakdown by default: it includes all enrolled students (~18-30).
  - Filtering by --formation=licence-master narrows to students more likely to be 19-24.
  - Our projection covers residents aged 20-24, not just students.
  - Enrolled/projected ratio tests whether MOBSCO pushed enough
    students to university EPCIs.

Usage:
    uv run python validation/compare_sise.py \\
        --epci data/output/population_epci.parquet \\
        --commune-epci data/cache/commune_epci.parquet \\
        --year 2022

    # Filter to Licence + Master formations only (closer to 19-24 age range)
    uv run python validation/compare_sise.py \\
        --epci data/output/population_epci.parquet \\
        --commune-epci data/cache/commune_epci.parquet \\
        --year 2022 --formation licence-master

Data source:
    MESRI Atlas regional des effectifs etudiants
    (fr-esr-atlas_regional-effectifs-d-etudiants-inscrits_agregeables)
    https://data.enseignementsup-recherche.gouv.fr
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

_SISE_BASE_URL = (
    "https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets"
    "/fr-esr-atlas_regional-effectifs-d-etudiants-inscrits_agregeables/exports/csv"
)

# Full SISE export: all formations, no age restriction
SISE_CSV_URL = (
    f"{_SISE_BASE_URL}"
    "?select=annee_universitaire,com_id,regroupement,effectif"
    "&limit=500000"
)

# Formation group values in the regroupement column for Licence + Master filter.
# The atlas dataset uses code-based groups, not plain text labels:
#   UNIV  = Universités (Licence, Master, Doctorat, BUT/IUT) — core 20-24 population
#   GE    = Grandes Écoles (engineering, business, post-CPGE) — core 20-24
#   INP   = Instituts Nationaux Polytechniques
#   UT    = Universités de Technologie
#   ENS   = Écoles Normales Supérieures
# Excluded: CPGE (17-20, pre-grandes écoles), STS (18-20, BTS 2-year post-bac)
_LICENCE_MASTER_GROUPS = ("'UNIV'", "'GE'", "'INP'", "'UT'", "'ENS'")
LICENCE_MASTER_FILTER = f"regroupement IN ({', '.join(_LICENCE_MASTER_GROUPS)})"

# Known university EPCI names for display
EPCI_NAMES = {
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
    "246300765": "Clermont Auvergne Métr.",
    "200069854": "Tours Métropole",
    "243700754": "Poitiers",
    "241300445": "Pays d'Aix",
    "200040956": "Annecy",
    "242500410": "Besançon",
    "242100410": "Dijon Métropole",
    "244200741": "Saint-Etienne Métr.",
    "243400476": "Hérault - Béziers",
    "247200132": "Le Mans Métropole",
    "243300813": "Arcachon",
    "200056232": "Communauté Paris-Saclay (91)",
    "245400676": "Grand Nancy Métropole (54)",
}

# IDF suburb EPCIs - expected below national average
# (students leave for university cities)
# Note: Cergy-Pontoise (249500109) hosts CY Cergy Paris Universite
# - high ratio is expected,
# not a sign of under-correction. It is kept here for reference but flagged separately.
IDF_SUBURB_EPCI_NAMES = {
    "200057958": "Grand Paris Sud Est (94)",
    "200058519": "Paris Est Créteil (94)",
    "200057313": "Plaine Commune (93)",
    "200057362": "Est Ensemble (93)",
    "200057990": "Grand-Orly S. Bièvr. (94)",
    "200058485": "Val d'Oise Nord résidentiel (95)",
    "249500109": "Cergy-Pontoise [univ] (95)",
    "200056976": "Grand Paris Seine Ouest (92)",
    "200057982": "Val de Bièvre (94)",
    "200073650": "Boucle Nord de Seine (92)",
}


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


def _academic_year_label(calendar_year: int) -> str:
    """Map calendar year to SISE academic year string, e.g. 2022 → '2022-23'."""
    return f"{calendar_year}-{str(calendar_year + 1)[-2:]}"


def run(
    epci_path: Path,
    commune_epci_path: Path,
    year: int,
    formation: str = "all",
) -> None:
    conn = duckdb.connect()
    academic_year = _academic_year_label(year)
    formation_label = (
        "all formations" if formation == "all" else "Licence + Master only"
    )

    # -----------------------------------------------------------------------
    # Load SISE data
    # -----------------------------------------------------------------------
    print(f"\nFetching SISE enrollment data for academic year {academic_year}...")
    print(f"  Formation filter: {formation_label}")
    print(f"  Source: {SISE_CSV_URL[:80]}...")
    try:
        formation_clause = ""
        if formation == "licence-master":
            formation_clause = f"AND {LICENCE_MASTER_FILTER}"

        conn.execute(f"""
            CREATE OR REPLACE TABLE sise_raw AS
            SELECT
                com_id,
                SUM(effectif::INTEGER) AS enrolled
            FROM read_csv_auto('{SISE_CSV_URL}', header=true, delim=';')
            WHERE annee_universitaire = '{academic_year}'
              AND com_id IS NOT NULL
              AND com_id != ''
              {formation_clause}
            GROUP BY com_id
        """)
        n_sise = conn.execute("SELECT COUNT(*), SUM(enrolled) FROM sise_raw").fetchone()
        total = n_sise[1] or 0
        print(f"  Loaded {n_sise[0]} communes, {total:,.0f} total enrolled students.")
        if total == 0 and formation == "licence-master":
            # Show available regroupement values to diagnose filter mismatch
            available = conn.execute(
                "SELECT DISTINCT regroupement FROM read_csv_auto("
                f"'{SISE_CSV_URL}', header=true, delim=';')"
                f" WHERE annee_universitaire = '{academic_year}'"
                " ORDER BY regroupement LIMIT 30"
            ).fetchdf()
            print("  WARNING: zero rows matched Licence/Master filter.")
            print("  Available regroupement values for this academic year:")
            for v in available["regroupement"].tolist():
                print(f"    {v!r}")
            sys.exit(1)
    except Exception as e:
        print(f"  ERROR fetching SISE data: {e}")
        print("  Check network connection and API availability.")
        sys.exit(1)

    # -----------------------------------------------------------------------
    # Load commune-EPCI mapping and join
    # -----------------------------------------------------------------------
    print(f"Loading commune-EPCI mapping from {commune_epci_path}...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE commune_epci AS
        SELECT commune_code, epci_code, department_code
        FROM read_parquet('{commune_epci_path}')
    """)

    # Commune codes in SISE are 5-char INSEE codes; commune_epci has same format
    conn.execute("""
        CREATE OR REPLACE TABLE sise_epci AS
        SELECT
            ce.epci_code,
            ce.department_code,
            SUM(s.enrolled) AS enrolled
        FROM sise_raw s
        JOIN commune_epci ce ON ce.commune_code = s.com_id
        GROUP BY ce.epci_code, ce.department_code
    """)

    n_matched = conn.execute(
        "SELECT COUNT(DISTINCT epci_code) FROM sise_epci"
    ).fetchone()[0]
    total_matched = (
        conn.execute("SELECT SUM(enrolled) FROM sise_epci").fetchone()[0] or 0
    )
    total_sise = conn.execute("SELECT SUM(enrolled) FROM sise_raw").fetchone()[0] or 1
    print(
        f"  Joined to {n_matched} EPCIs"
        f" ({total_matched / total_sise * 100:.1f}%"
        " of enrolled students)"
    )

    # -----------------------------------------------------------------------
    # Load our EPCI projections (20-24 age range)
    # -----------------------------------------------------------------------
    print(f"Loading our EPCI projections from {epci_path} (year {year}, ages 20-24)...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE epci_proj AS
        SELECT
            epci_code,
            department_code,
            age::INTEGER AS age,
            SUM(population) AS pop
        FROM read_parquet('{epci_path}')
        WHERE year::INTEGER = {year} AND month = 1
          AND epci_code IS NOT NULL AND epci_code != ''
        GROUP BY epci_code, department_code, age
    """)

    age_min, age_max = conn.execute(
        "SELECT MIN(age), MAX(age) FROM epci_proj"
    ).fetchone()

    if age_max < 20 or age_min > 24:
        print(
            f"  WARNING: 20-24 age range not in our data (range: {age_min}-{age_max}). "
            f"Skipping SISE comparison."
        )
        sys.exit(1)

    # -----------------------------------------------------------------------
    # Table 1: Major university EPCIs — enrollment vs projected 20-24 pop
    # -----------------------------------------------------------------------
    epci_rows = []
    for epci_code, epci_name in EPCI_NAMES.items():
        sise_row = (
            conn.execute(
                f"SELECT SUM(enrolled) FROM sise_epci WHERE epci_code = '{epci_code}'"
            ).fetchone()[0]
            or 0
        )
        proj_20_24 = (
            conn.execute(f"""
            SELECT SUM(pop) FROM epci_proj
            WHERE epci_code = '{epci_code}' AND age BETWEEN 20 AND 24
        """).fetchone()[0]
            or 0
        )
        if proj_20_24 == 0:
            continue
        ratio_pct = sise_row / proj_20_24 * 100
        flag = " (!>100%)" if ratio_pct > 100 else (" (?low)" if ratio_pct < 10 else "")
        epci_rows.append(
            {
                "epci": epci_name,
                "code": epci_code,
                "sise_enrolled": f"{sise_row:,.0f}",
                "our_20-24": f"{proj_20_24:,.0f}",
                "enrolled/20-24_%": f"{ratio_pct:.1f}%{flag}",
            }
        )

    print_table(
        sorted(
            epci_rows,
            key=lambda r: -float(r["sise_enrolled"].replace(",", "")),
        ),
        [
            "epci",
            "code",
            "sise_enrolled",
            "our_20-24",
            "enrolled/20-24_%",
        ],
        "SISE enrollment vs our 20-24 projection"
        f" - university EPCIs - {academic_year}"
        f" ({formation_label})",
    )

    # -----------------------------------------------------------------------
    # Table 2: IDF suburb EPCIs — should show low enrolled/20-24 ratio
    # -----------------------------------------------------------------------
    suburb_rows = []
    for epci_code, epci_name in IDF_SUBURB_EPCI_NAMES.items():
        sise_row = (
            conn.execute(
                f"SELECT SUM(enrolled) FROM sise_epci WHERE epci_code = '{epci_code}'"
            ).fetchone()[0]
            or 0
        )
        proj_20_24 = (
            conn.execute(f"""
            SELECT SUM(pop) FROM epci_proj
            WHERE epci_code = '{epci_code}' AND age BETWEEN 20 AND 24
        """).fetchone()[0]
            or 0
        )
        if proj_20_24 == 0:
            continue
        ratio_pct = sise_row / proj_20_24 * 100
        flag = " (!high)" if ratio_pct > 30 else ""
        suburb_rows.append(
            {
                "epci": epci_name,
                "code": epci_code,
                "sise_enrolled": f"{sise_row:,.0f}",
                "our_20-24": f"{proj_20_24:,.0f}",
                "enrolled/20-24_%": f"{ratio_pct:.1f}%{flag}",
            }
        )

    print_table(
        sorted(
            suburb_rows,
            key=lambda r: float(r["enrolled/20-24_%"].split("%")[0]),
        ),
        [
            "epci",
            "code",
            "sise_enrolled",
            "our_20-24",
            "enrolled/20-24_%",
        ],
        "SISE enrollment vs our 20-24 projection"
        f" - IDF suburbs - {academic_year}"
        f" ({formation_label})",
    )
    print()
    print("  Expected: IDF suburb EPCIs should show enrolled/20-24 < national average.")
    print(
        "  A low ratio confirms MOBSCO correction is"
        " depressing student counts in IDF suburbs."
    )
    print(
        "  High ratio (!high) suggests the MOBSCO correction may be under-correcting."
    )

    # -----------------------------------------------------------------------
    # Table 3: National aggregate
    # -----------------------------------------------------------------------
    total_enrolled = (
        conn.execute("SELECT SUM(enrolled) FROM sise_epci").fetchone()[0] or 0
    )
    total_20_24 = (
        conn.execute(
            "SELECT SUM(pop) FROM epci_proj WHERE age BETWEEN 20 AND 24"
        ).fetchone()[0]
        or 1
    )
    total_15_24 = (
        conn.execute(
            "SELECT SUM(pop) FROM epci_proj WHERE age BETWEEN 15 AND 24"
        ).fetchone()[0]
        or 1
    )
    print(f"\n  National summary ({formation_label}):")
    print(f"    Total SISE enrollment:                   {total_enrolled:>12,.0f}")
    print(f"    Our projected 20-24 pop (EPCI level):    {total_20_24:>12,.0f}")
    print(f"    Our projected 15-24 pop (EPCI level):    {total_15_24:>12,.0f}")
    print(
        "    Enrolled / 20-24 ratio:                 "
        f" {total_enrolled / total_20_24 * 100:>11.1f}%"
    )
    print()
    if formation == "all":
        print("  Interpretation (all formations):")
        print(
            "    ~50-80% ratio nationally is plausible:"
            " SISE includes all students (up to ~30),"
        )
        print(
            "    while our 20-24 band covers only 5 years."
            " University EPCIs should exceed national mean."
        )
    else:
        print("  Interpretation (Licence + Master only):")
        print(
            "    Licence/Master skews toward ages 19-24;"
            " ratio should be lower than 'all formations'."
        )
        print("    University EPCIs still expected to exceed national mean.")

    # -----------------------------------------------------------------------
    # Table 4: Top-20 EPCIs by enrolled/20-24 ratio
    # -----------------------------------------------------------------------
    top_rows = conn.execute("""
        SELECT
            s.epci_code,
            s.enrolled,
            p.pop_20_24,
            s.enrolled / NULLIF(p.pop_20_24, 0) * 100 AS ratio_pct
        FROM sise_epci s
        JOIN (
            SELECT epci_code, SUM(pop) AS pop_20_24
            FROM epci_proj
            WHERE age BETWEEN 20 AND 24
            GROUP BY epci_code
        ) p ON p.epci_code = s.epci_code
        WHERE p.pop_20_24 > 5000
        ORDER BY ratio_pct DESC
        LIMIT 20
    """).fetchdf()

    print_table(
        [
            {
                "rank": str(i + 1),
                "epci_code": r["epci_code"],
                "name": EPCI_NAMES.get(
                    r["epci_code"], IDF_SUBURB_EPCI_NAMES.get(r["epci_code"], "")
                ),
                "enrolled": f"{r['enrolled']:,.0f}",
                "our_20-24": f"{r['pop_20_24']:,.0f}",
                "ratio_%": f"{r['ratio_pct']:.1f}%",
            }
            for i, r in top_rows.iterrows()
        ],
        ["rank", "epci_code", "name", "enrolled", "our_20-24", "ratio_%"],
        "Top-20 EPCIs by SISE/20-24 ratio (expected: university cities dominate)",
    )

    print()
    print(
        "  Note: SISE has no age filter by default"
        " (use --formation=licence-master to narrow)."
    )
    print(
        "  Ratios > 100% in university cities are expected:"
        " SISE age range exceeds 20-24."
    )
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--epci", required=True, type=Path, help="EPCI-level parquet")
    parser.add_argument(
        "--commune-epci", required=True, type=Path, help="Commune-EPCI mapping parquet"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2022,
        help="Calendar year to compare"
        " (maps to academic year year/year+1, default: 2022)",
    )
    parser.add_argument(
        "--formation",
        choices=["all", "licence-master"],
        default="all",
        help=(
            "Formation filter: 'all' = all higher-ed students (default); "
            "'licence-master' = Licence + Master only (closer to 19-24 age range)"
        ),
    )
    args = parser.parse_args()

    for p in (args.epci, args.commune_epci):
        if not p.exists():
            print(f"ERROR: file not found: {p}")
            sys.exit(1)

    run(args.epci, args.commune_epci, args.year, formation=args.formation)


if __name__ == "__main__":
    main()

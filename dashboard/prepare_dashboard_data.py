"""Prepare dashboard data files.

Copies parquet files, splits IRIS by department, downloads GeoJSON boundaries,
computes census-vs-quinquennal bias comparison for the precision dashboard tab.
Run with: uv run python dashboard/prepare_dashboard_data.py
"""

import json
import shutil
from pathlib import Path

import duckdb
import requests

ROOT = Path(__file__).resolve().parent.parent
DATA_INPUT = ROOT / "data" / "output"
DATA_CACHE = ROOT / "data" / "cache"
DATA_OUTPUT = Path(__file__).resolve().parent / "data"

# GeoJSON sources
DEPT_GEOJSON_URL = (
    "https://raw.githubusercontent.com/gregoiredavid/france-geojson/"
    "master/departements-version-simplifiee.geojson"
)
# Opendatasoft IRIS API (per-department)
IRIS_GEOJSON_API = (
    "https://data.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "georef-france-iris-millesime@public/exports/geojson"
    "?refine=dep_code%3A{dept}"
    "&limit=-1"
)
# Opendatasoft EPCI
EPCI_GEOJSON_URL = (
    "https://data.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "georef-france-epci@public/exports/geojson"
    "?limit=-1"
)
# Opendatasoft cantons (per-department)
CANTON_GEOJSON_API = (
    "https://data.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "georef-france-canton@public/exports/geojson"
    "?refine=dep_code%3A{dept}"
    "&limit=-1"
)


def copy_parquet():
    """Copy department and EPCI parquet files."""
    for name in [
        "population_department.parquet",
        "population_epci.parquet",
        "population_canton.parquet",
    ]:
        src = DATA_INPUT / name
        dst = DATA_OUTPUT / name
        if not src.exists():
            print(f"  SKIP {name} (not found)")
            continue
        shutil.copy2(src, dst)
        print(f"  Copied {name} ({dst.stat().st_size / 1e6:.1f} MB)")


def split_iris():
    """Split IRIS parquet by department."""
    iris_file = DATA_INPUT / "population_iris.parquet"
    if not iris_file.exists():
        print("  SKIP population_iris.parquet (not found)")
        return

    iris_dir = DATA_OUTPUT / "iris"
    iris_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()

    # Get distinct department codes
    depts = conn.execute(
        "SELECT DISTINCT department_code "
        f"FROM read_parquet('{iris_file}') ORDER BY department_code"
    ).fetchall()

    print(f"  Splitting IRIS into {len(depts)} department files...")

    for (dept_code,) in depts:
        out_path = iris_dir / f"population_iris_{dept_code}.parquet"
        conn.execute(
            f"""
            COPY (
                SELECT * FROM read_parquet('{iris_file}')
                WHERE department_code = '{dept_code}'
            ) TO '{out_path}' (FORMAT PARQUET)
            """
        )
        size = out_path.stat().st_size / 1e6
        print(f"    {dept_code}: {size:.1f} MB")

    conn.close()


def split_canton():
    """Split canton parquet by department."""
    canton_file = DATA_INPUT / "population_canton.parquet"
    if not canton_file.exists():
        print("  SKIP population_canton.parquet (not found)")
        return

    canton_dir = DATA_OUTPUT / "canton"
    canton_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()

    depts = conn.execute(
        "SELECT DISTINCT department_code "
        f"FROM read_parquet('{canton_file}') ORDER BY department_code"
    ).fetchall()

    print(f"  Splitting canton into {len(depts)} department files...")

    for (dept_code,) in depts:
        out_path = canton_dir / f"population_canton_{dept_code}.parquet"
        conn.execute(
            f"""
            COPY (
                SELECT * FROM read_parquet('{canton_file}')
                WHERE department_code = '{dept_code}'
            ) TO '{out_path}' (FORMAT PARQUET)
            """
        )
        size = out_path.stat().st_size / 1e6
        print(f"    {dept_code}: {size:.1f} MB")

    conn.close()


def download_dept_geojson():
    """Download department boundaries GeoJSON."""
    geo_dir = DATA_OUTPUT / "geo"
    geo_dir.mkdir(parents=True, exist_ok=True)
    out_path = geo_dir / "departements.geojson"

    if out_path.exists():
        print("  SKIP departements.geojson (already exists)")
        return

    print("  Downloading department boundaries...")
    resp = requests.get(DEPT_GEOJSON_URL, timeout=60)
    resp.raise_for_status()
    out_path.write_bytes(resp.content)
    print(f"  Saved departements.geojson ({out_path.stat().st_size / 1e6:.1f} MB)")


def download_epci_geojson():
    """Download and clean EPCI boundaries GeoJSON."""
    geo_dir = DATA_OUTPUT / "geo"
    geo_dir.mkdir(parents=True, exist_ok=True)
    out_path = geo_dir / "epci_france.geojson"

    if out_path.exists():
        print("  SKIP epci_france.geojson (already exists)")
        return

    print("  Downloading EPCI boundaries (this may take a minute)...")
    resp = requests.get(EPCI_GEOJSON_URL, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    # Clean: flatten array-valued properties (e.g. ["200067981"] -> "200067981")
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        for key, val in list(props.items()):
            if isinstance(val, list) and len(val) == 1:
                props[key] = val[0]

    out_path.write_text(json.dumps(data), encoding="utf-8")
    print(f"  Saved epci_france.geojson ({out_path.stat().st_size / 1e6:.1f} MB)")


def download_iris_geojson():
    """Download per-department IRIS boundaries from Opendatasoft."""
    iris_dir = DATA_OUTPUT / "geo" / "iris"
    iris_dir.mkdir(parents=True, exist_ok=True)

    # Get department codes from IRIS parquet
    iris_file = DATA_INPUT / "population_iris.parquet"
    if not iris_file.exists():
        print("  SKIP IRIS GeoJSON (no IRIS parquet)")
        return

    conn = duckdb.connect()
    depts = conn.execute(
        "SELECT DISTINCT department_code "
        f"FROM read_parquet('{iris_file}') ORDER BY department_code"
    ).fetchall()
    conn.close()

    for (dept_code,) in depts:
        out_path = iris_dir / f"iris_{dept_code}.geojson"
        if out_path.exists():
            continue

        url = IRIS_GEOJSON_API.format(dept=dept_code)
        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            out_path.write_bytes(resp.content)
            size = out_path.stat().st_size / 1e6
            print(f"    {dept_code}: {size:.1f} MB")
        except Exception as e:
            print(f"    {dept_code}: FAILED ({e})")


def download_canton_geojson():
    """Download per-department canton boundaries from Opendatasoft."""
    canton_dir = DATA_OUTPUT / "geo" / "canton"
    canton_dir.mkdir(parents=True, exist_ok=True)

    canton_file = DATA_INPUT / "population_canton.parquet"
    if not canton_file.exists():
        print("  SKIP canton GeoJSON (no canton parquet)")
        return

    conn = duckdb.connect()
    depts = conn.execute(
        "SELECT DISTINCT department_code "
        f"FROM read_parquet('{canton_file}') ORDER BY department_code"
    ).fetchall()
    conn.close()

    for (dept_code,) in depts:
        out_path = canton_dir / f"canton_{dept_code}.geojson"
        if out_path.exists():
            continue

        url = CANTON_GEOJSON_API.format(dept=dept_code)
        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            out_path.write_bytes(resp.content)
            size = out_path.stat().st_size / 1e6
            print(f"    {dept_code}: {size:.1f} MB")
        except Exception as e:
            print(f"    {dept_code}: FAILED ({e})")


def compute_bias_comparison():
    """Compute census vs quinquennal bias per (department, age_band).

    Also computes IRIS coverage ratios per department.
    Exports to dashboard/data/bias_comparison.parquet.
    Requires cached INDCVI and quinquennal parquets from a prior pipeline run.
    """
    indcvi_path = DATA_CACHE / "indcvi_2022.parquet"
    quint_path = DATA_CACHE / "quinquennal_estimates.parquet"

    if not indcvi_path.exists() or not quint_path.exists():
        print("  SKIP bias comparison (missing cached INDCVI or quinquennal)")
        return

    conn = duckdb.connect()

    # AGE_BUCKETS from constants — rebuild the CASE for DuckDB
    age_band_case = """
    CASE
        WHEN age BETWEEN 0 AND 4 THEN '0_4'
        WHEN age BETWEEN 5 AND 9 THEN '5_9'
        WHEN age BETWEEN 10 AND 14 THEN '10_14'
        WHEN age BETWEEN 15 AND 19 THEN '15_19'
        WHEN age BETWEEN 20 AND 24 THEN '20_24'
        WHEN age BETWEEN 25 AND 29 THEN '25_29'
        WHEN age BETWEEN 30 AND 34 THEN '30_34'
        WHEN age BETWEEN 35 AND 39 THEN '35_39'
        WHEN age BETWEEN 40 AND 44 THEN '40_44'
        WHEN age BETWEEN 45 AND 49 THEN '45_49'
        WHEN age BETWEEN 50 AND 54 THEN '50_54'
        WHEN age BETWEEN 55 AND 59 THEN '55_59'
        WHEN age BETWEEN 60 AND 64 THEN '60_64'
        WHEN age BETWEEN 65 AND 69 THEN '65_69'
        WHEN age BETWEEN 70 AND 74 THEN '70_74'
        WHEN age BETWEEN 75 AND 79 THEN '75_79'
        WHEN age BETWEEN 80 AND 84 THEN '80_84'
        WHEN age BETWEEN 85 AND 89 THEN '85_89'
        WHEN age BETWEEN 90 AND 94 THEN '90_94'
        ELSE '95_plus'
    END"""

    # 1. Census vs quinquennal comparison per (dept, age_band) — both sexes combined
    # Exclude Mayotte (976) — synthesized, not from INDCVI
    print("  Computing census vs quinquennal bias...")
    bias_sql = f"""
    WITH census AS (
        SELECT
            TRIM(DEPT) AS department_code,
            CAST(AGEREV AS INTEGER) AS age,
            CAST(IPONDI AS DOUBLE) AS weight
        FROM read_parquet('{indcvi_path}')
        WHERE TRIM(DEPT) != '976'
    ),
    census_by_band AS (
        SELECT
            department_code,
            {age_band_case} AS age_band,
            ROUND(SUM(weight), 0) AS census_pop
        FROM census
        WHERE age IS NOT NULL AND weight IS NOT NULL
        GROUP BY department_code, age_band
    ),
    quint AS (
        SELECT department_code, age_band, SUM(population) AS quint_pop
        FROM read_parquet('{quint_path}')
        WHERE year = 2022 AND department_code != '976'
        GROUP BY department_code, age_band
    )
    SELECT
        c.department_code,
        c.age_band,
        c.census_pop,
        COALESCE(q.quint_pop, 0) AS quint_pop,
        ROUND(100.0 * ABS(c.census_pop - COALESCE(q.quint_pop, 0))
              / NULLIF(COALESCE(q.quint_pop, 0), 0), 2) AS abs_bias_pct,
        ROUND(100.0 * (c.census_pop - COALESCE(q.quint_pop, 0))
              / NULLIF(COALESCE(q.quint_pop, 0), 0), 2) AS signed_bias_pct
    FROM census_by_band c
    LEFT JOIN quint q
        ON c.department_code = q.department_code
        AND c.age_band = q.age_band
    ORDER BY c.department_code, c.age_band
    """
    out_path = DATA_OUTPUT / "bias_comparison.parquet"
    conn.execute(f"COPY ({bias_sql}) TO '{out_path}' (FORMAT PARQUET)")
    rows = conn.execute(f"SELECT count(*) FROM read_parquet('{out_path}')").fetchone()[
        0
    ]
    print(
        f"  Saved bias_comparison.parquet "
        f"({rows} rows, {out_path.stat().st_size / 1e3:.1f} KB)"
    )

    # 2. Geographic coverage from raw INDCVI (census-mode reality)
    # IRIS coverage = % of population with precise IRIS code (not ZZZZZZZZZ)
    # EPCI direct = % with commune→EPCI mapping; rest uses canton weighting → ~100%
    commune_epci_path = DATA_CACHE / "commune_epci.parquet"
    print("  Computing geographic coverage from INDCVI...")
    geo_coverage_sql = f"""
    WITH raw AS (
        SELECT
            TRIM(DEPT) AS department_code,
            TRIM(IRIS) AS iris_code,
            CASE
                WHEN TRIM(IRIS) = 'ZZZZZZZZZ' OR TRIM(IRIS) IS NULL THEN ''
                ELSE LEFT(TRIM(IRIS), 5)
            END AS commune_code,
            CAST(IPONDI AS DOUBLE) AS weight
        FROM read_parquet('{indcvi_path}')
        WHERE TRIM(DEPT) != '976'
    ),
    dept_totals AS (
        SELECT department_code, ROUND(SUM(weight), 0) AS dept_pop
        FROM raw GROUP BY department_code
    ),
    iris_pop AS (
        SELECT department_code, ROUND(SUM(weight), 0) AS iris_pop
        FROM raw
        WHERE iris_code != 'ZZZZZZZZZ' AND iris_code IS NOT NULL
        GROUP BY department_code
    ),
    epci_direct AS (
        SELECT r.department_code, ROUND(SUM(r.weight), 0) AS epci_direct_pop
        FROM raw r
        INNER JOIN read_parquet('{commune_epci_path}') ce
            ON r.commune_code = ce.commune_code
        WHERE r.commune_code != ''
        GROUP BY r.department_code
    )
    SELECT
        d.department_code,
        d.dept_pop,
        COALESCE(ed.epci_direct_pop, 0) AS epci_direct_pop,
        COALESCE(i.iris_pop, 0) AS iris_pop,
        ROUND(100.0 * COALESCE(ed.epci_direct_pop, 0) / NULLIF(d.dept_pop, 0), 2)
              AS epci_direct_pct,
        ROUND(100.0 * COALESCE(i.iris_pop, 0)
              / NULLIF(d.dept_pop, 0), 2) AS iris_coverage_pct
    FROM dept_totals d
    LEFT JOIN epci_direct ed ON d.department_code = ed.department_code
    LEFT JOIN iris_pop i ON d.department_code = i.department_code
    ORDER BY d.department_code
    """
    out_geo = DATA_OUTPUT / "geo_coverage.parquet"
    conn.execute(f"COPY ({geo_coverage_sql}) TO '{out_geo}' (FORMAT PARQUET)")
    rows = conn.execute(f"SELECT count(*) FROM read_parquet('{out_geo}')").fetchone()[0]
    avg_epci_direct = conn.execute(
        f"SELECT ROUND(AVG(epci_direct_pct), 1) FROM read_parquet('{out_geo}')"
    ).fetchone()[0]
    avg_iris = conn.execute(
        f"SELECT ROUND(AVG(iris_coverage_pct), 1) FROM read_parquet('{out_geo}')"
    ).fetchone()[0]
    print(
        f"  Saved geo_coverage.parquet ({rows} depts, "
        f"avg EPCI direct={avg_epci_direct}%, avg IRIS={avg_iris}%)"
    )

    conn.close()


def main():
    DATA_OUTPUT.mkdir(parents=True, exist_ok=True)

    print("=== Step 1: Copy parquet files ===")
    copy_parquet()

    print("\n=== Step 2: Split IRIS by department ===")
    split_iris()

    print("\n=== Step 2b: Split canton by department ===")
    split_canton()

    print("\n=== Step 3: Download department GeoJSON ===")
    download_dept_geojson()

    print("\n=== Step 4: Download EPCI GeoJSON ===")
    download_epci_geojson()

    print("\n=== Step 5: Download IRIS GeoJSON (per-department) ===")
    download_iris_geojson()

    print("\n=== Step 5b: Download canton GeoJSON (per-department) ===")
    download_canton_geojson()

    print("\n=== Step 6: Compute bias comparison ===")
    compute_bias_comparison()

    print("\n=== Done ===")
    print(f"Data prepared in {DATA_OUTPUT}")


if __name__ == "__main__":
    main()

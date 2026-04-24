"""Data downloaders for INSEE sources.

Handles HTTP downloads and Excel parsing for:
- Census INDCVI parquet files
- INDREG (MNAI month-of-birth) parquet files
- MOBSCO student commuting parquet
- Mayotte 2017 POP1B census
"""

from __future__ import annotations

import io
import tempfile
from pathlib import Path

import pandas as pd
import requests
from loguru import logger
from rich.progress import Progress

from passculture.data.insee_population.constants import (
    DEPARTMENTS_METRO,
    INDCVI_URLS,
    INDREG_URLS,
    IRIS_SENTINEL_NO_GEO,
    MAYOTTE_CENSUS_YEAR,
    MAYOTTE_POP1B_MEMBER,
    MAYOTTE_POP1B_URL,
    MNAI_MIN_DEPT_POPULATION,
    MOBSCO_URL,
)

# HTTP timeouts
DOWNLOAD_TIMEOUT = 600  # 10 minutes for large census files
ESTIMATES_TIMEOUT = 120  # 2 minutes for smaller files
CHUNK_SIZE = 131072  # 128KB chunks


def _cached_parquet(url: str, filename: str, cache_dir: Path | None) -> Path:
    """Return a local path to the parquet at ``url``, caching by filename."""
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = cache_dir / filename
        if parquet_path.exists():
            logger.debug("Using cached: {}", parquet_path)
            return parquet_path
    else:
        parquet_path = Path(tempfile.mkdtemp()) / filename

    _download_file(url, parquet_path)
    return parquet_path


def download_indcvi(year: int, cache_dir: Path | None) -> Path:
    """Download INDCVI census parquet file."""
    if year not in INDCVI_URLS:
        raise ValueError(
            f"Year {year} not available. Available: {list(INDCVI_URLS.keys())}"
        )
    url = INDCVI_URLS[year].get("france_parquet") or INDCVI_URLS[year]["france"]
    return _cached_parquet(url, f"indcvi_{year}.parquet", cache_dir)


def download_indreg(year: int, cache_dir: Path | None) -> Path:
    """Download INDREG (individus localisés à la région) parquet file.

    INDREG contains MNAI (month of birth). Large departments (population
    ≥ MNAI_MIN_DEPT_POPULATION) have DEPT populated; small departments are
    only available at REGION level.
    """
    if year not in INDREG_URLS:
        raise ValueError(
            f"Year {year} not available for INDREG. "
            f"Available: {list(INDREG_URLS.keys())}"
        )
    return _cached_parquet(
        INDREG_URLS[year]["france_parquet"], f"indreg_{year}.parquet", cache_dir
    )


def download_mobsco(cache_dir: Path | None) -> Path:
    """Download MOBSCO (student commuting) parquet file."""
    return _cached_parquet(MOBSCO_URL, "mobsco_2022.parquet", cache_dir)


# -----------------------------------------------------------------------------
# Month-of-birth distribution from MNAI (RP2022_indreg)
# -----------------------------------------------------------------------------


def download_mnai_birth_distribution(
    year: int,
    cache_dir: Path | None = None,
) -> pd.DataFrame:
    """Compute month-of-birth distribution from INDREG MNAI.

    For each department with ``population >= MNAI_MIN_DEPT_POPULATION`` we use
    its own MNAI distribution; smaller departments inherit the distribution of
    their region (INSEE only publishes DEPT for large departments in INDREG).
    Mayotte, absent from the file, inherits the metropolitan aggregate.

    Doc-faithful rule: month of birth by region and big departments, small
    departments use the regional distribution, Mayotte uses metropolitan
    France. See ``docs/design.md`` for the rationale.

    Returns a DataFrame with columns ``department_code, month, month_ratio``
    compatible with the existing ``monthly_births`` table. Returns an empty
    DataFrame if INDREG cannot be fetched/parsed.
    """
    cache_path = (
        cache_dir / f"monthly_birth_distribution_mnai_{year}.parquet"
        if cache_dir
        else None
    )
    if cache_path and cache_path.exists():
        logger.debug("Using cached MNAI birth distribution: {}", cache_path)
        return pd.read_parquet(cache_path)

    try:
        parquet_path = download_indreg(year, cache_dir)
    except Exception as e:
        logger.warning("INDREG unavailable: {}", e)
        return pd.DataFrame()

    try:
        df = _read_indreg_mnai(parquet_path)
    except Exception as e:
        logger.warning("Could not parse INDREG MNAI: {}", e)
        return pd.DataFrame()

    if df.empty:
        return df

    result = _build_mnai_distribution(df)

    if cache_path and cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        result.to_parquet(cache_path, index=False)

    return result


def _read_indreg_mnai(parquet_path: Path) -> pd.DataFrame:
    """Load the MNAI-relevant columns from INDREG.

    INDREG columns of interest: DEPT (3-char, may be empty for small depts),
    REGION (2-char), MNAI (01-12 or "XX" for unknown), IPONDI (weight).
    """
    df = pd.read_parquet(parquet_path, columns=["DEPT", "REGION", "MNAI", "IPONDI"])
    df["DEPT"] = df["DEPT"].astype(str).str.strip()
    df["REGION"] = df["REGION"].astype(str).str.strip()
    df["MNAI"] = df["MNAI"].astype(str).str.strip()
    df["IPONDI"] = pd.to_numeric(df["IPONDI"], errors="coerce").fillna(0.0)
    df["month"] = pd.to_numeric(df["MNAI"], errors="coerce")
    return df[df["month"].between(1, 12) & (df["IPONDI"] > 0)].copy()


def _build_mnai_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Turn raw MNAI rows into a (department_code, month, month_ratio) table.

    - Department distribution for departments with total weight >= the INSEE
      publication threshold.
    - Region distribution as the fallback; small departments inherit it.
    - Mayotte (976) and any department absent from INDREG inherit the
      metropolitan-wide distribution.
    """
    dept_totals = df.groupby("DEPT")["IPONDI"].sum()
    big_depts = set(dept_totals[dept_totals >= MNAI_MIN_DEPT_POPULATION].index) - {""}

    dept_rows = _month_ratios(df[df["DEPT"].isin(big_depts)], ["DEPT", "month"], "DEPT")
    region_rows = _month_ratios(df, ["REGION", "month"], "REGION")
    metro_rows = _month_ratios(
        df[df["DEPT"].isin(set(DEPARTMENTS_METRO))], ["month"], None
    )

    # Map region → department list using the populated (DEPT, REGION) pairs
    # in INDREG to fill small departments from their region's distribution.
    dept_region = (
        df[df["DEPT"] != ""][["DEPT", "REGION"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    rows: list[dict] = []
    for dept in dept_region["DEPT"].unique():
        if dept in big_depts:
            for _, r in dept_rows[dept_rows["DEPT"] == dept].iterrows():
                rows.append(
                    {
                        "department_code": dept,
                        "month": int(r["month"]),
                        "month_ratio": float(r["month_ratio"]),
                    }
                )
            continue
        region = dept_region[dept_region["DEPT"] == dept]["REGION"].iloc[0]
        region_dist = region_rows[region_rows["REGION"] == region]
        if region_dist.empty:
            # no region data - fall back to metro aggregate below
            continue
        for _, r in region_dist.iterrows():
            rows.append(
                {
                    "department_code": dept,
                    "month": int(r["month"]),
                    "month_ratio": float(r["month_ratio"]),
                }
            )

    # Mayotte (absent from INDREG) + any dept missing above uses the
    # metropolitan aggregate.
    metro_dict = {
        int(r["month"]): float(r["month_ratio"]) for _, r in metro_rows.iterrows()
    }
    existing_depts = {r["department_code"] for r in rows}
    for dept in {"976", *existing_depts} - existing_depts:
        for month, ratio in metro_dict.items():
            rows.append({"department_code": dept, "month": month, "month_ratio": ratio})

    return (
        pd.DataFrame(rows)
        .sort_values(["department_code", "month"])
        .reset_index(drop=True)
    )


def _month_ratios(
    df: pd.DataFrame, group_cols: list[str], normalize_col: str | None
) -> pd.DataFrame:
    """Aggregate IPONDI by ``group_cols`` and normalise per ``normalize_col``.

    If ``normalize_col`` is None, normalises globally (e.g. for the metro-wide
    distribution).
    """
    if df.empty:
        return pd.DataFrame(columns=[*group_cols, "month_ratio"])
    agg = df.groupby(group_cols)["IPONDI"].sum().reset_index()
    if normalize_col is None:
        total = agg["IPONDI"].sum()
        agg["month_ratio"] = agg["IPONDI"] / total if total > 0 else 0.0
    else:
        totals = agg.groupby(normalize_col)["IPONDI"].sum().rename("_total")
        agg = agg.merge(totals, left_on=normalize_col, right_index=True)
        agg["month_ratio"] = agg["IPONDI"] / agg["_total"].where(agg["_total"] > 0, 1.0)
        agg = agg.drop(columns=["_total"])
    return agg.drop(columns=["IPONDI"])


# -----------------------------------------------------------------------------
# Mayotte 2017 census (POP1B)
# -----------------------------------------------------------------------------


def download_mayotte_pop1b(
    cache_dir: Path | None = None,
) -> pd.DataFrame:
    """Download Mayotte 2017 census (POP1B: population by sex x age).

    INSEE ships POP1B inside a zip archive that bundles all 2017 Mayotte
    census tables. We fetch the zip, extract ``MAYOTTE_POP1B_MEMBER``, and
    parse the wide COM sheet, aggregating communes to get the Mayotte-wide
    pyramid. Returns a DataFrame with columns ``age, sex, population``.
    Returns empty DataFrame when the file cannot be parsed so callers can
    fall back to the synthesised path.
    """
    cache_path = cache_dir / "mayotte_pop1b_2017.parquet" if cache_dir else None
    if cache_path and cache_path.exists():
        logger.debug("Using cached Mayotte POP1B: {}", cache_path)
        return pd.read_parquet(cache_path)

    try:
        logger.info("Downloading Mayotte 2017 POP1B from {}", MAYOTTE_POP1B_URL)
        response = requests.get(MAYOTTE_POP1B_URL, timeout=ESTIMATES_TIMEOUT)
        response.raise_for_status()
        xls_bytes = _extract_zip_member(response.content, MAYOTTE_POP1B_MEMBER)
        df = _parse_mayotte_pop1b_wide(xls_bytes)
    except Exception as e:
        logger.warning("Could not fetch Mayotte POP1B: {}", e)
        return pd.DataFrame()

    if df.empty:
        return df

    if cache_path and cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_path, index=False)

    return df


def _extract_zip_member(zip_bytes: bytes, member_name: str) -> bytes:
    """Return the raw bytes for a single file inside a zip archive."""
    import zipfile

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # Tolerate extra directory prefixes INSEE may add in the future.
        candidates = [n for n in zf.namelist() if n.endswith(member_name)]
        if not candidates:
            raise FileNotFoundError(
                f"{member_name} not found in zip (have: {zf.namelist()})"
            )
        with zf.open(candidates[0]) as f:
            return f.read()


def _parse_mayotte_pop1b_wide(xls_bytes: bytes) -> pd.DataFrame:
    """Parse the wide POP1B layout: one row per commune, columns per (sex, age).

    Header layout (COM sheet):
      row 5, col 1: ``SEXE``
      row 5, cols 2-102: ``1`` (male), cols 103-203: ``2`` (female)
      row 6, col 1: ``AGED100``
      row 6, cols 2-102: ages ``000``..``100`` (male), cols 103-203: ages
      ``000``..``100`` (female)
      row 10, col 0: ``CODGEO``
      rows 11+: commune code followed by 202 population counts

    We sum across all communes to get the Mayotte-wide age pyramid.
    """
    xls = pd.ExcelFile(io.BytesIO(xls_bytes))
    candidates = [s for s in xls.sheet_names if s.upper().startswith("COM")]
    if not candidates:
        candidates = xls.sheet_names
    for sheet in candidates:
        try:
            raw = pd.read_excel(xls, sheet_name=sheet, header=None)
        except Exception:
            continue
        parsed = _extract_pop1b_wide_rows(raw)
        if not parsed.empty:
            return parsed
    return pd.DataFrame()


def _extract_pop1b_wide_rows(raw: pd.DataFrame) -> pd.DataFrame:
    """Extract (age, sex, population) from the wide POP1B commune table."""
    # Locate header rows by scanning the first ~12 rows.
    sexe_row = aged_row = codgeo_row = None
    for i in range(min(len(raw), 15)):
        first = str(raw.iloc[i, 0]).strip().lower()
        second = str(raw.iloc[i, 1]).strip().upper() if raw.shape[1] > 1 else ""
        if first == "codgeo":
            codgeo_row = i
        if second == "SEXE":
            sexe_row = i
        if second == "AGED100":
            aged_row = i
    if sexe_row is None or aged_row is None or codgeo_row is None:
        return pd.DataFrame()

    sex_header = raw.iloc[sexe_row].tolist()
    age_header = raw.iloc[aged_row].tolist()

    # Build a (sex, age, col_index) map across all columns 2+.
    col_map: list[tuple[str, int, int]] = []
    for col in range(2, raw.shape[1]):
        sex_raw = str(sex_header[col]).strip()
        age_raw = age_header[col]
        age = _parse_age_label(age_raw)
        if age is None or sex_raw not in {"1", "2"}:
            continue
        sex = "male" if sex_raw == "1" else "female"
        col_map.append((sex, age, col))

    if not col_map:
        return pd.DataFrame()

    # Sum population across commune rows.
    totals: dict[tuple[str, int], float] = {}
    for i in range(codgeo_row + 1, len(raw)):
        codgeo = raw.iloc[i, 0]
        if pd.isna(codgeo):
            continue
        for sex, age, col in col_map:
            value = _safe_float(raw.iloc[i, col])
            key = (sex, age)
            totals[key] = totals.get(key, 0.0) + value

    rows = [
        {"age": age, "sex": sex, "population": pop}
        for (sex, age), pop in totals.items()
        if pop > 0
    ]
    return pd.DataFrame(rows).sort_values(["age", "sex"]).reset_index(drop=True)


def _parse_age_label(raw: object) -> int | None:
    """Parse INSEE age labels: ``"0"``, ``"17"``, ``"100"``, ``"100 ou plus"``."""
    if pd.isna(raw):
        return None
    text = str(raw).strip().lower()
    if not text:
        return None
    if "plus" in text:
        digits = "".join(c for c in text if c.isdigit())
        return int(digits) if digits else None
    try:
        return int(float(text.split()[0]))
    except (ValueError, IndexError):
        return None


def _safe_float(value: object) -> float:
    """Parse a POP1B cell: accepts numerics, French-formatted strings, or NaN."""
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace(" ", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return 0.0


# -----------------------------------------------------------------------------
# Private helpers
# -----------------------------------------------------------------------------


def _download_file(url: str, dest: Path) -> None:
    """Download file with progress indicator."""
    logger.info("Downloading from {}...", url)
    response = requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT)
    response.raise_for_status()

    total = int(response.headers.get("content-length", 0))

    with Progress(transient=True) as progress:
        task = progress.add_task("Downloading", total=total or None)
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)
                progress.advance(task, len(chunk))


# -----------------------------------------------------------------------------
# Mayotte population synthesis
# -----------------------------------------------------------------------------


def synthesize_mayotte_population(
    year: int,
    cache_dir: Path | None = None,
) -> pd.DataFrame:
    """Build Mayotte (976) population rows for the given census year.

    Loads the Mayotte 2017 POP1B census (population by sex x age) and
    ages it forward by ``year - MAYOTTE_CENSUS_YEAR``. Raw counts are
    used as-is — this matches the INSEE spec doc convention of adding
    Mayotte 2017 with ``age + 5`` for a 2022 reference.
    """
    logger.info("Adding Mayotte (976) for year {}...", year)

    pop1b = download_mayotte_pop1b(cache_dir)
    if pop1b.empty:
        logger.warning("  Could not load Mayotte POP1B, skipping Mayotte")
        return pd.DataFrame()

    offset = year - MAYOTTE_CENSUS_YEAR
    if offset < 0:
        logger.warning(
            "  Asked for Mayotte {} before POP1B reference year {}; skipping",
            year,
            MAYOTTE_CENSUS_YEAR,
        )
        return pd.DataFrame()

    aged = pop1b.copy()
    aged["age"] = aged["age"] + offset
    aged = aged[aged["population"] > 0]
    if aged.empty:
        return pd.DataFrame()

    rows: list[dict] = [
        {
            "year": year,
            "department_code": "976",
            "region_code": "06",
            "canton_code": "9799",
            "commune_code": "",
            "iris_code": IRIS_SENTINEL_NO_GEO,
            "age": int(r["age"]),
            "sex": r["sex"],
            "population": float(r["population"]),
        }
        for _, r in aged.iterrows()
    ]
    df = pd.DataFrame(rows)
    logger.debug(
        "  Added {} Mayotte rows ({:,.0f} population)", len(df), df["population"].sum()
    )
    return df

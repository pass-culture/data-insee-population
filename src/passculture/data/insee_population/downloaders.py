"""Data downloaders for INSEE sources.

Handles HTTP downloads and Excel parsing for:
- Census INDCVI parquet files
- Monthly birth distribution by department
- MOBSCO student commuting parquet
- Mayotte synthesis (population estimates + quinquennal age pyramid)
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
    AGE_BUCKETS,
    AGE_PYRAMID_URL,
    BIRTH_DATA_URLS,
    DEPARTMENTS_METRO,
    INDCVI_URLS,
    INDREG_URLS,
    IRIS_SENTINEL_NO_GEO,
    MAYOTTE_CENSUS_YEAR,
    MAYOTTE_POP1B_MEMBER,
    MAYOTTE_POP1B_URL,
    MNAI_MIN_DEPT_POPULATION,
    MOBSCO_URL,
    POPULATION_ESTIMATES_URL,
)

# HTTP timeouts
DOWNLOAD_TIMEOUT = 600  # 10 minutes for large census files
ESTIMATES_TIMEOUT = 120  # 2 minutes for smaller files
CHUNK_SIZE = 131072  # 128KB chunks


def download_indcvi(year: int, cache_dir: Path | None) -> Path:
    """Download INDCVI census parquet file.

    Args:
        year: Census year
        cache_dir: Directory for caching, None for temp file

    Returns:
        Path to local parquet file
    """
    if year not in INDCVI_URLS:
        raise ValueError(
            f"Year {year} not available. Available: {list(INDCVI_URLS.keys())}"
        )

    url = INDCVI_URLS[year].get("france_parquet") or INDCVI_URLS[year]["france"]

    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = cache_dir / f"indcvi_{year}.parquet"

        if parquet_path.exists():
            logger.debug("Using cached: {}", parquet_path)
            return parquet_path

        _download_file(url, parquet_path)
        return parquet_path

    tmpdir = tempfile.mkdtemp()
    parquet_path = Path(tmpdir) / f"indcvi_{year}.parquet"
    _download_file(url, parquet_path)
    return parquet_path


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

    url = INDREG_URLS[year]["france_parquet"]

    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = cache_dir / f"indreg_{year}.parquet"

        if parquet_path.exists():
            logger.debug("Using cached: {}", parquet_path)
            return parquet_path

        _download_file(url, parquet_path)
        return parquet_path

    tmpdir = tempfile.mkdtemp()
    parquet_path = Path(tmpdir) / f"indreg_{year}.parquet"
    _download_file(url, parquet_path)
    return parquet_path


def download_mobsco(cache_dir: Path | None) -> Path:
    """Download MOBSCO (student commuting) parquet file.

    Args:
        cache_dir: Directory for caching, None for temp file

    Returns:
        Path to local parquet file
    """
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = cache_dir / "mobsco_2022.parquet"

        if parquet_path.exists():
            logger.debug("Using cached: {}", parquet_path)
            return parquet_path

        _download_file(MOBSCO_URL, parquet_path)
        return parquet_path

    tmpdir = tempfile.mkdtemp()
    parquet_path = Path(tmpdir) / "mobsco_2022.parquet"
    _download_file(MOBSCO_URL, parquet_path)
    return parquet_path


def download_estimates(
    extrapolate_to: int | None = None,
    cache_dir: Path | None = None,
) -> pd.DataFrame:
    """Download INSEE population estimates by department/sex/year.

    Args:
        extrapolate_to: If beyond available data, extrapolate using trends
        cache_dir: Directory for caching the downloaded file

    Returns:
        DataFrame with columns: year, department_code, sex, population
    """
    cache_path = cache_dir / "population_estimates.parquet" if cache_dir else None
    if cache_path and cache_path.exists():
        logger.debug("Using cached estimates: {}", cache_path)
        df = pd.read_parquet(cache_path)
        if extrapolate_to and extrapolate_to > df["year"].max():
            df = _extrapolate_last_year(
                df, df["year"].max(), extrapolate_to, "estimates"
            )
        return df

    try:
        logger.info("Downloading estimates from {}", POPULATION_ESTIMATES_URL)
        response = requests.get(POPULATION_ESTIMATES_URL, timeout=ESTIMATES_TIMEOUT)
        response.raise_for_status()

        xls = pd.ExcelFile(io.BytesIO(response.content))
        results = []
        max_available_year = 0

        for sheet_name in xls.sheet_names:
            if not sheet_name.isdigit():
                continue

            year = int(sheet_name)
            max_available_year = max(max_available_year, year)
            results.extend(_parse_estimates_sheet(xls, sheet_name, year))

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)

        if cache_path and cache_dir:
            cache_dir.mkdir(parents=True, exist_ok=True)
            df.to_parquet(cache_path, index=False)

        if extrapolate_to and extrapolate_to > max_available_year:
            df = _extrapolate_last_year(
                df, max_available_year, extrapolate_to, "estimates"
            )

        return df

    except Exception as e:
        logger.warning("Could not download estimates: {}", e)
        return pd.DataFrame()


def download_quinquennal_estimates(
    start_year: int,
    end_year: int,
    cache_dir: Path | None = None,
) -> pd.DataFrame:
    """Download population by department/sex/5-year age band for multiple years.

    Used for Mayotte synthesis. Parses the AGE_PYRAMID_URL Excel file
    (one sheet per year). For years beyond available data, repeats the
    last available year.

    Args:
        start_year: First year to include
        end_year: Last year to include
        cache_dir: Directory for caching the downloaded file

    Returns:
        DataFrame with columns: year, department_code, sex, age_band, population
    """
    cache_path = cache_dir / "quinquennal_estimates.parquet" if cache_dir else None
    if cache_path and cache_path.exists():
        logger.debug("Using cached quinquennal estimates: {}", cache_path)
        df = pd.read_parquet(cache_path)
        if end_year > df["year"].max():
            df = _extrapolate_last_year(df, df["year"].max(), end_year, "quinquennal")
        return df[(df["year"] >= start_year) & (df["year"] <= end_year)]

    logger.info("Downloading quinquennal age pyramid from {}", AGE_PYRAMID_URL)
    response = requests.get(AGE_PYRAMID_URL, timeout=ESTIMATES_TIMEOUT)
    response.raise_for_status()

    xls = pd.ExcelFile(io.BytesIO(response.content))
    results = []
    max_available_year = 0

    for sheet_name in xls.sheet_names:
        if not sheet_name.isdigit():
            continue
        year = int(sheet_name)
        max_available_year = max(max_available_year, year)
        results.extend(_parse_quinquennal_sheet(xls, sheet_name, year))

    if not results:
        logger.warning("No quinquennal data parsed")
        return pd.DataFrame()

    df = pd.DataFrame(results)

    if end_year > max_available_year:
        df = _extrapolate_last_year(df, max_available_year, end_year, "quinquennal")

    if cache_path and cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_path, index=False)

    return df[(df["year"] >= start_year) & (df["year"] <= end_year)]


def download_monthly_birth_distribution(
    cache_dir: Path | None = None,
) -> pd.DataFrame:
    """Download birth data by department and month, compute monthly ratios.

    Downloads the INSEE file with births by month/department, averages across
    available years, and computes each month's share of annual births per dept.

    Args:
        cache_dir: Directory for caching

    Returns:
        DataFrame with columns: department_code, month (1-12), month_ratio
    """
    cache_path = (
        cache_dir / "monthly_birth_distribution_n4d.parquet" if cache_dir else None
    )
    if cache_path and cache_path.exists():
        logger.debug("Using cached monthly birth distribution: {}", cache_path)
        return pd.read_parquet(cache_path)

    url = BIRTH_DATA_URLS.get("by_dept_month") or BIRTH_DATA_URLS.get("by_month_dept")
    if not url:
        logger.warning("No birth-by-month URL configured")
        return pd.DataFrame()

    try:
        logger.info("Downloading birth data by month from {}", url)
        response = requests.get(url, timeout=ESTIMATES_TIMEOUT)
        response.raise_for_status()

        if url.endswith(".csv"):
            df = _parse_n4d_birth_csv(response.text)
        else:
            xls = pd.ExcelFile(io.BytesIO(response.content))
            df = _parse_monthly_birth_excel(xls)

        if df.empty:
            return df

        if cache_path and cache_dir:
            cache_dir.mkdir(parents=True, exist_ok=True)
            df.to_parquet(cache_path, index=False)

        return df

    except Exception as e:
        logger.warning("Could not download monthly birth data: {}", e)
        return pd.DataFrame()


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


def _parse_estimates_sheet(xls: pd.ExcelFile, sheet_name: str, year: int) -> list[dict]:
    """Parse a single year's estimates sheet."""
    df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=3, header=None)
    results = []

    for _, row in df.iterrows():
        dept = str(row[0]).strip() if pd.notna(row[0]) else ""
        if not dept or len(dept) > 3 or not dept[0].isdigit():
            continue
        if len(dept) == 1:
            dept = f"0{dept}"

        try:
            # Columns: 0=dept, 1=name, Ensemble(2-7), Hommes(8-13), Femmes(14-19)
            male_total = row[13] if pd.notna(row[13]) else 0
            female_total = row[19] if pd.notna(row[19]) else 0

            if male_total > 0:
                results.append(
                    {
                        "year": year,
                        "department_code": dept,
                        "sex": "male",
                        "population": float(male_total),
                    }
                )
            if female_total > 0:
                results.append(
                    {
                        "year": year,
                        "department_code": dept,
                        "sex": "female",
                        "population": float(female_total),
                    }
                )
        except (ValueError, TypeError, IndexError):
            continue

    return results


def _extrapolate_last_year(
    df: pd.DataFrame, last_year: int, target_year: int, label: str
) -> pd.DataFrame:
    """Repeat last year of data for years beyond available range.

    Args:
        df: DataFrame with a 'year' column
        last_year: Last available year in the data
        target_year: Year to extend to
        label: Human-readable label for the print message (e.g. "estimates")
    """
    logger.info(
        "Extending {} with last known year ({}) to {}...",
        label,
        last_year,
        target_year,
    )

    last_year_data = df[df["year"] == last_year]
    if last_year_data.empty:
        return df

    extended = []
    for year in range(last_year + 1, target_year + 1):
        year_copy = last_year_data.copy()
        year_copy["year"] = year
        extended.append(year_copy)

    if extended:
        return pd.concat([df, *extended], ignore_index=True)

    return df


# Age band column indices in the quinquennal Excel sheets
# The sheet layout is: col0=dept, col1=name,
#   Ensemble: cols 2..21 (20 age bands), col 22 (total)   = 21 cols
#   Hommes:   cols 23..42 (20 age bands), col 43 (total)  = 21 cols
#   Femmes:   cols 44..63 (20 age bands), col 64 (total)  = 21 cols
# Age bands in order: 0-4, 5-9, 10-14, ... 90-94, 95+
_QUINQUENNAL_AGE_BANDS = list(AGE_BUCKETS.keys())  # 20 bands
_MALE_OFFSET = 23
_FEMALE_OFFSET = 44


def _parse_quinquennal_sheet(
    xls: pd.ExcelFile, sheet_name: str, year: int
) -> list[dict]:
    """Parse a single year's quinquennal age pyramid sheet."""
    df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=3, header=None)
    results = []

    for _, row in df.iterrows():
        dept = str(row[0]).strip() if pd.notna(row[0]) else ""
        if not dept or len(dept) > 3 or not dept[0].isdigit():
            continue
        if len(dept) == 1:
            dept = f"0{dept}"

        try:
            for band_idx, age_band in enumerate(_QUINQUENNAL_AGE_BANDS):
                male_col = _MALE_OFFSET + band_idx
                female_col = _FEMALE_OFFSET + band_idx
                male_pop = float(row[male_col]) if pd.notna(row[male_col]) else 0
                female_pop = float(row[female_col]) if pd.notna(row[female_col]) else 0

                if male_pop > 0:
                    results.append(
                        {
                            "year": year,
                            "department_code": dept,
                            "sex": "male",
                            "age_band": age_band,
                            "population": male_pop,
                        }
                    )
                if female_pop > 0:
                    results.append(
                        {
                            "year": year,
                            "department_code": dept,
                            "sex": "female",
                            "age_band": age_band,
                            "population": female_pop,
                        }
                    )
        except (ValueError, TypeError, IndexError):
            continue

    return results


# Month name mapping for the INSEE birth data Excel files
_MONTH_NAMES = {
    "janvier": 1,
    "février": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "août": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "décembre": 12,
}


def _parse_n4d_birth_csv(content: str) -> pd.DataFrame:
    """Parse INSEE N4D CSV (births by department and month) into monthly ratios.

    Format: semicolon-separated with columns:
    - REGDEP_DOMI_MERE: geographic code — two formats:
        * 4-char "RRDD": region(2) + metro dept(2), e.g. "1175" → dept "75"
        * 3-char "DDD": bare DOM dept code, e.g. "971", "972", "976"
      Aggregate codes containing "X" ("11XX", "97XX", "FR", "FM"…) are skipped.
    - MNAIS: "01"-"12" for months, "AN" for annual total (skipped)
    - NBNAIS: number of births
    """
    df = pd.read_csv(io.StringIO(content), sep=";", dtype=str)

    # Drop annual totals and aggregate codes (contain "X" or are national codes)
    df = df[df["MNAIS"] != "AN"].copy()
    df = df[~df["REGDEP_DOMI_MERE"].str.contains("X", na=True)]
    df = df[~df["REGDEP_DOMI_MERE"].isin(["FR", "FM", "FR_ENR", "FM_ENR"])]

    # Extract department code:
    # - 3-char codes are bare DOM dept codes (971, 972, 973, 974, 976)
    # - 4-char codes are region(2) + metro dept(2), strip the region prefix
    def _extract_dept(regdep: str) -> str:
        return regdep if len(regdep) == 3 else regdep[2:]

    df["department_code"] = df["REGDEP_DOMI_MERE"].apply(_extract_dept)
    df["month"] = pd.to_numeric(df["MNAIS"], errors="coerce")
    df["births"] = pd.to_numeric(df["NBNAIS"], errors="coerce").fillna(0)
    df = df[df["month"].notna() & (df["births"] > 0)]

    result = df.groupby(["department_code", "month"])["births"].sum().reset_index()

    totals = result.groupby("department_code")["births"].sum().reset_index()
    totals.columns = ["department_code", "annual_births"]
    result = result.merge(totals, on="department_code")
    result["month_ratio"] = result["births"] / result["annual_births"]

    n_depts = result["department_code"].nunique()
    logger.debug("  Computed monthly ratios for {} departments (N4D CSV)", n_depts)
    return result[["department_code", "month", "month_ratio"]].copy()


def _parse_monthly_birth_excel(xls: pd.ExcelFile) -> pd.DataFrame:
    """Parse birth-by-month-department Excel into monthly ratios.

    The Excel file has multiple sheets (one per year), each with:
    - First column: department code
    - Month columns: Janvier, Février, ..., Décembre

    We average across years, then compute each month's share per department.
    """
    all_data = []

    for sheet_name in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=3)
        except Exception:
            continue

        df.columns = df.columns.astype(str).str.strip()
        dept_col = df.columns[0]

        # Find month columns by matching French month names
        month_cols = {}
        for col in df.columns:
            col_lower = col.lower().strip()
            for month_name, month_num in _MONTH_NAMES.items():
                if col_lower.startswith(month_name):
                    month_cols[col] = month_num
                    break

        if not month_cols:
            continue

        for _, row in df.iterrows():
            dept = str(row[dept_col]).strip() if pd.notna(row[dept_col]) else ""
            if (
                not dept
                or len(dept) > 3
                or dept.lower() in ["total", "france", "métropole"]
            ):
                continue
            if not dept[0].isdigit():
                continue
            if len(dept) == 1:
                dept = f"0{dept}"

            for col, month_num in month_cols.items():
                try:
                    births = row[col]
                    if pd.notna(births):
                        val = float(str(births).replace(" ", "").replace(",", "."))
                        all_data.append(
                            {
                                "department_code": dept,
                                "month": month_num,
                                "births": val,
                            }
                        )
                except (ValueError, TypeError):
                    continue

    if not all_data:
        return pd.DataFrame()

    df_births = pd.DataFrame(all_data)

    # Average across years (multiple sheets), then compute monthly ratio per dept
    avg = df_births.groupby(["department_code", "month"])["births"].mean().reset_index()
    totals = avg.groupby("department_code")["births"].sum().reset_index()
    totals.columns = ["department_code", "annual_births"]

    avg = avg.merge(totals, on="department_code")
    avg["month_ratio"] = avg["births"] / avg["annual_births"]

    result = avg[["department_code", "month", "month_ratio"]].copy()
    n_depts = result["department_code"].nunique()
    logger.debug("  Computed monthly ratios for {} departments", n_depts)
    return result


# -----------------------------------------------------------------------------
# Mayotte population synthesis
# -----------------------------------------------------------------------------


def synthesize_mayotte_population(
    year: int,
    min_age: int,
    max_age: int,
    cache_dir: Path | None = None,
    prefer_pop1b: bool = True,
) -> pd.DataFrame:
    """Build the Mayotte (976) population rows for a given census year.

    Primary method (``prefer_pop1b``): load the Mayotte 2017 POP1B census
    (population by sex x age), age cohorts forward by ``year -
    MAYOTTE_CENSUS_YEAR``, and scale the total to the department-level
    estimate for ``year`` to keep totals consistent with INSEE. See
    ``docs/design.md`` for the rationale.

    Fallback (used when POP1B is unavailable): synthesise from the
    department-level population estimate and a DOM quinquennal age
    distribution.
    """
    logger.info("Adding Mayotte (976) for year {}...", year)

    if prefer_pop1b:
        rows = _build_mayotte_from_pop1b(year, min_age, max_age, cache_dir)
        if rows:
            df = pd.DataFrame(rows)
            total = df["population"].sum()
            logger.debug(
                "  Added {} Mayotte rows from POP1B ({:,.0f} population)",
                len(df),
                total,
            )
            return df
        logger.info("  POP1B unavailable, falling back to synthesis")

    # Fallback: age distribution from DOM departments
    dom_age_dist = _get_dom_age_distribution(year, cache_dir)
    if not dom_age_dist:
        logger.warning("  Could not compute DOM age distribution, skipping Mayotte")
        return pd.DataFrame()

    estimates_df = download_estimates(cache_dir=cache_dir)
    mayotte_estimates = _get_mayotte_estimates(estimates_df, year)
    if mayotte_estimates.empty:
        logger.warning("  No Mayotte estimates found, skipping")
        return pd.DataFrame()

    data = _build_population_rows(
        mayotte_estimates, dom_age_dist, year, min_age, max_age
    )

    if data:
        df = pd.DataFrame(data)
        total = df["population"].sum()
        logger.debug(
            "  Added {} Mayotte rows (synthesis) ({:,} population)",
            len(data),
            total,
        )
        return df

    return pd.DataFrame()


def _build_mayotte_from_pop1b(
    year: int,
    min_age: int,
    max_age: int,
    cache_dir: Path | None,
) -> list[dict]:
    """Age the Mayotte 2017 POP1B census forward to ``year`` and scale totals.

    Cohorts are shifted by ``year - MAYOTTE_CENSUS_YEAR``. Ages above
    ``MAX_AGE`` get dropped (old cohorts die out of the window). The result
    is then rescaled per sex to match the department-level population
    estimate for ``year``, so regional totals still anchor to INSEE.
    """
    pop1b = download_mayotte_pop1b(cache_dir)
    if pop1b.empty:
        return []

    offset = year - MAYOTTE_CENSUS_YEAR
    if offset < 0:
        # Asked for a census year before the POP1B reference; we can't age
        # backwards safely.
        return []

    aged = pop1b.copy()
    aged["age"] = aged["age"] + offset
    aged = aged[(aged["age"] >= min_age) & (aged["age"] <= max_age)]
    if aged.empty:
        return []

    scaling = _mayotte_scaling_factors(aged, year, cache_dir)
    rows: list[dict] = []
    for _, r in aged.iterrows():
        pop = float(r["population"]) * scaling.get(r["sex"], 1.0)
        if pop <= 0:
            continue
        rows.append(
            {
                "year": year,
                "department_code": "976",
                "region_code": "06",
                "canton_code": "9799",
                "commune_code": "",
                "iris_code": IRIS_SENTINEL_NO_GEO,
                "age": int(r["age"]),
                "sex": r["sex"],
                "population": pop,
            }
        )
    return rows


def _mayotte_scaling_factors(
    aged: pd.DataFrame,
    year: int,
    cache_dir: Path | None,
) -> dict[str, float]:
    """Per-sex factor that rescales aged POP1B to the dept-level estimate.

    Leaves factor = 1.0 when no estimate is available (keep raw POP1B).
    """
    estimates_df = download_estimates(cache_dir=cache_dir)
    mayotte_estimates = _get_mayotte_estimates(estimates_df, year)
    if mayotte_estimates.empty:
        return {}

    aged_totals = aged.groupby("sex")["population"].sum()
    factors: dict[str, float] = {}
    for _, r in mayotte_estimates.iterrows():
        sex = r["sex"]
        target = float(r["population"])
        observed = float(aged_totals.get(sex, 0.0))
        if observed > 0 and target > 0:
            factors[sex] = target / observed
    return factors


def _get_dom_age_distribution(
    year: int, cache_dir: Path | None = None
) -> dict[int, float]:
    """Get age distribution for Mayotte population synthesis.

    Uses Mayotte's own quinquennal estimates (available from 2014).
    Returns empty dict when Mayotte data is not available.

    Uses quinquennal estimates (20 five-year bands) and distributes
    uniformly within each band using AGE_BUCKETS.

    Returns:
        Dict mapping age -> percentage of total population
    """
    df = download_quinquennal_estimates(year, year, cache_dir)
    if df.empty:
        return {}

    source = df[df["department_code"] == "976"]
    if source.empty:
        logger.warning("  No quinquennal data for Mayotte (976) -- skipping")
        return {}

    # Sum population across sexes per age band
    band_totals = source.groupby("age_band")["population"].sum()

    # Distribute uniformly within each band
    age_totals: dict[int, float] = {}
    for band_name, ages in AGE_BUCKETS.items():
        band_pop = band_totals.get(band_name, 0)
        per_year = band_pop / len(ages)
        for age in ages:
            age_totals[age] = age_totals.get(age, 0) + per_year

    total = sum(age_totals.values())
    if total == 0:
        return {}

    return {age: count / total for age, count in age_totals.items()}


def _get_mayotte_estimates(estimates_df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Get Mayotte population estimates, using closest year if needed."""
    mayotte = estimates_df[
        (estimates_df["department_code"] == "976") & (estimates_df["year"] == year)
    ]

    if mayotte.empty:
        mayotte = estimates_df[estimates_df["department_code"] == "976"]
        if not mayotte.empty:
            closest_year = mayotte["year"].max()
            mayotte = mayotte[mayotte["year"] == closest_year]
            logger.debug("  Using estimates from {} for Mayotte", closest_year)

    return mayotte


def _build_population_rows(
    estimates: pd.DataFrame,
    age_dist: dict[int, float],
    year: int,
    min_age: int,
    max_age: int,
) -> list[dict]:
    """Build Mayotte population rows from estimates and age distribution."""
    data = []
    for _, row in estimates.iterrows():
        sex = row["sex"]
        total_pop = row["population"]

        for age in range(min_age, max_age + 1):
            age_pct = age_dist.get(age, 0)
            pop = total_pop * age_pct
            if pop > 0:
                data.append(
                    {
                        "year": year,
                        "department_code": "976",
                        "region_code": "06",
                        "canton_code": "9799",  # Mayotte single canton
                        "commune_code": "",
                        "iris_code": IRIS_SENTINEL_NO_GEO,
                        "age": age,
                        "sex": sex,
                        "population": pop,
                    }
                )
    return data

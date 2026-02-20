"""Data downloaders for INSEE sources.

Handles HTTP downloads and Excel parsing for:
- Census INDCVI parquet files
- Population estimates by department/year
- Quinquennal age pyramid by department/sex/age band
- Birth data by department/month
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
    INDCVI_URLS,
    IRIS_SENTINEL_NO_GEO,
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


def download_birth_data() -> pd.DataFrame:
    """Download INSEE birth data by department/year.

    Returns:
        DataFrame with columns: birth_year, department_code, births
    """
    url = BIRTH_DATA_URLS.get("by_month_dept")
    if not url:
        return pd.DataFrame()

    try:
        logger.info("Downloading birth data from {}", url)
        response = requests.get(url, timeout=ESTIMATES_TIMEOUT)
        response.raise_for_status()

        xls = pd.ExcelFile(io.BytesIO(response.content))
        sheet_name = next(
            (name for name in xls.sheet_names if "dep" in name.lower()),
            xls.sheet_names[0],
        )
        df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=3)
        return _parse_birth_data(df)

    except Exception as e:
        logger.warning("Could not download birth data: {}", e)
        return pd.DataFrame()


def download_quinquennal_estimates(
    start_year: int,
    end_year: int,
    cache_dir: Path | None = None,
) -> pd.DataFrame:
    """Download population by department/sex/5-year age band for multiple years.

    Parses the AGE_PYRAMID_URL Excel file (one sheet per year).
    For years beyond available data, extrapolates using CAGR.

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
    cache_path = cache_dir / "monthly_birth_distribution.parquet" if cache_dir else None
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


def _parse_birth_data(df: pd.DataFrame) -> pd.DataFrame:
    """Parse birth data Excel into standardized format."""
    df.columns = df.columns.astype(str).str.strip()
    dept_col = df.columns[0]
    year_cols = [c for c in df.columns if c.isdigit() and len(c) == 4]

    results = []
    for _, row in df.iterrows():
        dept = str(row[dept_col]).strip() if pd.notna(row[dept_col]) else ""
        if (
            not dept
            or len(dept) > 3
            or dept.lower() in ["total", "france", "métropole"]
        ):
            continue
        if len(dept) == 1:
            dept = f"0{dept}"

        for col in year_cols:
            try:
                births = row[col]
                if pd.notna(births):
                    results.append(
                        {
                            "birth_year": int(col),
                            "department_code": dept,
                            "births": float(
                                str(births).replace(" ", "").replace(",", ".")
                            ),
                        }
                    )
            except (ValueError, TypeError):
                continue

    return pd.DataFrame(results)


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
) -> pd.DataFrame:
    """Synthesize Mayotte population data from estimates.

    Mayotte uses a separate census methodology and is not included in INDCVI
    files. This function synthesizes Mayotte population data using:
    1. Population estimates from INSEE
    2. Age distribution from Mayotte's own quinquennal estimates (available
       from 2014). Years before 2014 will have zero population.

    Args:
        year: Census year
        min_age: Minimum age to include
        max_age: Maximum age to include
        cache_dir: Directory for caching

    Returns:
        DataFrame with Mayotte population rows ready for insertion
    """
    logger.info("Adding Mayotte (976) from population estimates...")

    # Get age distribution from DOM departments
    dom_age_dist = _get_dom_age_distribution(year, cache_dir)
    if not dom_age_dist:
        logger.warning("  Could not compute DOM age distribution, skipping Mayotte")
        return pd.DataFrame()

    # Get Mayotte population estimates
    estimates_df = download_estimates(cache_dir=cache_dir)
    mayotte_estimates = _get_mayotte_estimates(estimates_df, year)
    if mayotte_estimates.empty:
        logger.warning("  No Mayotte estimates found, skipping")
        return pd.DataFrame()

    # Build population data
    data = _build_population_rows(
        mayotte_estimates, dom_age_dist, year, min_age, max_age
    )

    if data:
        df = pd.DataFrame(data)
        total = df["population"].sum()
        logger.debug("  Added {} Mayotte rows ({:,} population)", len(data), total)
        return df

    return pd.DataFrame()


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

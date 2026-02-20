"""Geographic mapping data for EPCI distribution.

Downloads and caches:
- Commune → EPCI mapping from geo.api.gouv.fr
- Canton → EPCI weights from INSEE COG
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests
from loguru import logger

# API endpoints
GEO_API_COMMUNES_URL = "https://geo.api.gouv.fr/communes?fields=code,nom,codeDepartement,codeEpci,population&format=json"
COG_COMMUNES_URL = (
    "https://www.insee.fr/fr/statistiques/fichier/7766585/v_commune_2024.csv"
)

# Cache filenames
COMMUNE_EPCI_CACHE = "commune_epci.parquet"
CANTON_EPCI_CACHE = "canton_epci_weights.parquet"

DOWNLOAD_TIMEOUT = 120

# Paris, Lyon, Marseille arrondissements → parent commune EPCI
# These are sub-divisions not in standard commune list
ARRONDISSEMENT_EPCI = {
    # Paris arrondissements → Métropole du Grand Paris
    **{f"751{i:02d}": "200054781" for i in range(1, 21)},
    # Lyon arrondissements → Métropole de Lyon
    **{f"6938{i}": "200046977" for i in range(1, 10)},
    # Marseille arrondissements → Métropole d'Aix-Marseille-Provence
    **{f"132{i:02d}": "200054807" for i in range(1, 17)},
}


def download_commune_epci_mapping(cache_dir: Path | None = None) -> pd.DataFrame:
    """Download commune → EPCI mapping from geo API.

    Returns:
        DataFrame with columns: commune_code, epci_code, commune_name, department_code
    """
    if cache_dir:
        cache_path = cache_dir / COMMUNE_EPCI_CACHE
        if cache_path.exists():
            return pd.read_parquet(cache_path)

    logger.info("Downloading commune -> EPCI mapping from geo.api.gouv.fr...")
    response = requests.get(GEO_API_COMMUNES_URL, timeout=DOWNLOAD_TIMEOUT)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(
        [
            {
                "commune_code": c["code"],
                "epci_code": c.get("codeEpci"),
                "commune_name": c.get("nom"),
                "department_code": c.get("codeDepartement"),
                "commune_population": c.get("population", 0),
            }
            for c in data
        ]
    )

    # Filter to communes with EPCI
    df = df[df["epci_code"].notna()].copy()

    # Add Paris/Lyon/Marseille arrondissements
    arrondissements = pd.DataFrame(
        [
            {
                "commune_code": code,
                "epci_code": epci,
                "commune_name": f"Arrondissement {code}",
                "department_code": code[:2],
                "commune_population": 0,
            }
            for code, epci in ARRONDISSEMENT_EPCI.items()
        ]
    )
    df = pd.concat([df, arrondissements], ignore_index=True)

    logger.debug(
        "  Loaded {} communes with EPCI mapping (including arrondissements)",
        len(df),
    )

    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_path, index=False)

    return df


def download_canton_epci_weights(cache_dir: Path | None = None) -> pd.DataFrame:
    """Download canton → EPCI weights based on commune population distribution.

    For cantons with multiple EPCIs, weights are based on the population
    of communes within each EPCI.

    Returns:
        DataFrame with columns: canton_code, epci_code, weight
    """
    if cache_dir:
        cache_path = cache_dir / CANTON_EPCI_CACHE
        if cache_path.exists():
            return pd.read_parquet(cache_path)

    logger.info("Building canton -> EPCI weights...")

    # Get COG with canton codes
    logger.info("  Downloading COG communes...")
    cog = pd.read_csv(COG_COMMUNES_URL, dtype=str)
    cog = cog[["COM", "DEP", "CAN"]].rename(
        columns={
            "COM": "commune_code",
            "DEP": "department_code",
            "CAN": "canton_code",
        }
    )

    # Get commune → EPCI mapping
    commune_epci = download_commune_epci_mapping(cache_dir)

    # Join to get canton + EPCI for each commune
    merged = cog.merge(
        commune_epci[["commune_code", "epci_code", "commune_population"]],
        on="commune_code",
        how="inner",
    )
    merged["commune_population"] = pd.to_numeric(
        merged["commune_population"], errors="coerce"
    ).fillna(0)

    # Aggregate by canton + EPCI
    canton_epci = (
        merged.groupby(["canton_code", "epci_code"])
        .agg({"commune_population": "sum"})
        .reset_index()
    )

    # Calculate weights within each canton
    canton_totals = (
        canton_epci.groupby("canton_code")["commune_population"].sum().reset_index()
    )
    canton_totals.columns = ["canton_code", "canton_total"]

    canton_epci = canton_epci.merge(canton_totals, on="canton_code")
    canton_epci["weight"] = (
        canton_epci["commune_population"] / canton_epci["canton_total"]
    )
    canton_epci["weight"] = canton_epci["weight"].fillna(0)

    result = canton_epci[["canton_code", "epci_code", "weight"]].copy()

    n_cantons = result["canton_code"].nunique()
    n_epcis = result["epci_code"].nunique()
    logger.debug("  Built weights for {} cantons -> {} EPCIs", n_cantons, n_epcis)

    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        result.to_parquet(cache_path, index=False)

    return result


def get_geo_mappings(
    cache_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Get both mapping tables.

    Returns:
        Tuple of (commune_epci, canton_epci_weights) DataFrames
    """
    commune_epci = download_commune_epci_mapping(cache_dir)
    canton_weights = download_canton_epci_weights(cache_dir)
    return commune_epci, canton_weights

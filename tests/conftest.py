"""Pytest configuration and fixtures."""

import pytest


def pytest_addoption(parser):
    """Add --run-integration CLI option."""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests (downloads INSEE data)",
    )


def pytest_collection_modifyitems(config, items):
    """Auto-skip integration tests unless --run-integration is passed."""
    if not config.getoption("--run-integration"):
        skip_marker = pytest.mark.skip(reason="need --run-integration to run")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_marker)


@pytest.fixture
def mock_env(monkeypatch):
    """Set up mock environment variables."""
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("ENV_SHORT_NAME", "dev")


@pytest.fixture
def sample_raw_indcvi_data():
    """Sample raw INDCVI data for testing."""
    import pandas as pd

    return pd.DataFrame(
        {
            "IRIS": [
                "751010101",  # Paris 1er, IRIS 0101
                "751010102",  # Paris 1er, IRIS 0102
                "130550101",  # Marseille, IRIS 0101
                "971050101",  # Guadeloupe
                "75101XXXX",  # Masked IRIS (< 200 inhabitants)
                "ZZZZZZZZZ",  # Non-IRIS commune
            ],
            "DEPT": ["75", "75", "13", "971", "75", "01"],
            "REGION": ["11", "11", "93", "01", "11", "84"],
            "AGEREV": ["018", "019", "018", "020", "015", "025"],
            "SEXE": ["1", "2", "1", "2", "1", "2"],
            "IPONDI": [
                "150.123456789012345",
                "145.987654321098765",
                "200.555555555555555",
                "50.111111111111111",
                "25.222222222222222",
                "75.333333333333333",
            ],
        }
    )


@pytest.fixture
def sample_geo_table():
    """Sample geographic correspondence table."""
    import pandas as pd

    return pd.DataFrame(
        {
            "iris_code": ["751010101", "751010102", "130550101", "971050101"],
            "commune_code": ["75101", "75101", "13055", "97105"],
            "commune_name": ["Paris 1er", "Paris 1er", "Marseille", "Pointe-à-Pitre"],
            "department_code": ["75", "75", "13", "971"],
            "region_code": ["11", "11", "93", "01"],
            "epci_code": ["200054781", "200054781", "200054807", "200040244"],
            "epci_name": [
                "Métropole du Grand Paris",
                "Métropole du Grand Paris",
                "Métropole d'Aix-Marseille-Provence",
                "CA Cap Excellence",
            ],
        }
    )


@pytest.fixture
def sample_aggregated_population():
    """Sample aggregated population data."""
    import pandas as pd

    return pd.DataFrame(
        {
            "year": [2022] * 12,
            "department_code": ["75"] * 6 + ["13"] * 6,
            "region_code": ["11"] * 6 + ["93"] * 6,
            "commune_code": ["75101"] * 3
            + ["75102"] * 3
            + ["13055"] * 3
            + ["13001"] * 3,
            "iris_code": [
                "751010101",
                "751010101",
                "751010101",
                "751020101",
                "751020101",
                "751020101",
                "130550101",
                "130550101",
                "130550101",
                "130010101",
                "130010101",
                "130010101",
            ],
            "age": [15, 16, 17] * 4,
            "sex": ["male", "female", "male"] * 4,
            "population": [
                1000,
                980,
                950,  # 75101
                800,
                780,
                750,  # 75102
                500,
                490,
                480,  # 13055
                300,
                290,
                280,  # 13001
            ],
            "epci_code": [None] * 12,
            "epci_name": [None] * 12,
        }
    )

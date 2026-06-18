"""Tests for the cohort-estimates INSEE anchor correction (Fix B).

The correction rescales métropole + 5 DOM department totals to INSEE's annual
estimates per (year, génération, sex), leaving TOM untouched and preserving the
RP2022 spatial/age/sex distribution.
"""

from __future__ import annotations

import duckdb
import pytest

from passculture.data.insee_population import sql
from passculture.data.insee_population.constants import (
    DEPARTMENTS_TOM,
    INSEE_ESTIMATES_LAST_YEAR,
)

_COLS = (
    "year,month,department_code,region_code,age,sex,geo_precision,"
    "population,confidence_pct,population_low,population_high"
)


@pytest.fixture
def conn():
    c = duckdb.connect()
    # 2 métropole depts + 1 TOM (988), gen-2006 (age 20 @2026), male
    c.execute(f"""
        CREATE TABLE population_department AS SELECT * FROM (VALUES
          (2026,1,'75','11',20,'male','exact',600000.0,0.05,570000.0,630000.0),
          (2026,1,'13','93',20,'male','exact',263306.0,0.05,250000.0,276000.0),
          (2026,1,'988','98',20,'male','exact',4000.0,0.05,3800.0,4200.0)
        ) AS t({_COLS})
    """)
    return c


def _correct(c):
    tom = ", ".join(f"'{d}'" for d in DEPARTMENTS_TOM)
    c.execute(
        sql.CORRECT_DEPARTMENT_WITH_INSEE.format(
            tom_codes=tom, insee_last_year=INSEE_ESTIMATES_LAST_YEAR
        )
    )


def test_anchor_rescales_metropole_to_insee_total(conn):
    conn.execute(
        "CREATE TABLE insee_estimates AS SELECT * FROM (VALUES "
        "(2026,2006,'male',430000.0)) AS t(year,naissance,sex,population)"
    )
    _correct(conn)
    metro = conn.execute(
        "SELECT SUM(population) FROM population_department "
        "WHERE department_code NOT IN ('986','988')"
    ).fetchone()[0]
    assert metro == pytest.approx(430000.0)


def test_anchor_preserves_internal_distribution(conn):
    conn.execute(
        "CREATE TABLE insee_estimates AS SELECT * FROM (VALUES "
        "(2026,2006,'male',430000.0)) AS t(year,naissance,sex,population)"
    )
    _correct(conn)
    # 75:13 ratio preserved (600000:263306)
    p75 = conn.execute(
        "SELECT population FROM population_department WHERE department_code='75'"
    ).fetchone()[0]
    p13 = conn.execute(
        "SELECT population FROM population_department WHERE department_code='13'"
    ).fetchone()[0]
    assert p75 / p13 == pytest.approx(600000.0 / 263306.0)
    # CI bounds scale with the same factor
    low75 = conn.execute(
        "SELECT population_low FROM population_department WHERE department_code='75'"
    ).fetchone()[0]
    assert low75 == pytest.approx(570000.0 * (p75 / 600000.0))


def test_anchor_leaves_tom_untouched(conn):
    conn.execute(
        "CREATE TABLE insee_estimates AS SELECT * FROM (VALUES "
        "(2026,2006,'male',430000.0)) AS t(year,naissance,sex,population)"
    )
    _correct(conn)
    tom = conn.execute(
        "SELECT population FROM population_department WHERE department_code='988'"
    ).fetchone()[0]
    assert tom == 4000.0


def test_anchor_missing_estimate_keeps_frozen(conn):
    # No matching INSEE row -> factor coalesces to 1.0 (unchanged)
    conn.execute(
        "CREATE TABLE insee_estimates AS SELECT * FROM (VALUES "
        "(2026,1999,'male',1.0)) AS t(year,naissance,sex,population)"
    )
    _correct(conn)
    p75 = conn.execute(
        "SELECT population FROM population_department WHERE department_code='75'"
    ).fetchone()[0]
    assert p75 == 600000.0


def test_anchor_correct_in_monthly_mode(conn):
    # Monthly mode: each cohort has 12 snapshot-month rows, each the full total.
    # The factor must be per-month, not summed across months (else ~12x off).
    conn.execute(f"""
        INSERT INTO population_department SELECT * FROM (VALUES
          (2026,2,'75','11',20,'male','exact',600000.0,0.05,570000.0,630000.0),
          (2026,2,'13','93',20,'male','exact',263306.0,0.05,250000.0,276000.0),
          (2026,2,'988','98',20,'male','exact',4000.0,0.05,3800.0,4200.0)
        ) AS t({_COLS})
    """)
    conn.execute(
        "CREATE TABLE insee_estimates AS SELECT * FROM (VALUES "
        "(2026,2006,'male',430000.0)) AS t(year,naissance,sex,population)"
    )
    _correct(conn)
    # each month's métropole total == INSEE (not INSEE/12)
    for m in (1, 2):
        metro = conn.execute(
            "SELECT SUM(population) FROM population_department "
            f"WHERE month={m} AND department_code NOT IN ('986','988')"
        ).fetchone()[0]
        assert metro == pytest.approx(430000.0)


def test_anchor_holds_last_year_for_future(conn):
    # Future projection year (2027) beyond INSEE last year reuses last-year total.
    conn.execute(f"""
        INSERT INTO population_department SELECT * FROM (VALUES
          (2027,1,'75','11',21,'male','exact',600000.0,0.05,570000.0,630000.0),
          (2027,1,'13','93',21,'male','exact',263306.0,0.05,250000.0,276000.0)
        ) AS t({_COLS})
    """)
    # gen-2006 is age 21 in 2027; INSEE only has up to 2026 -> held
    conn.execute(
        "CREATE TABLE insee_estimates AS SELECT * FROM (VALUES "
        "(2026,2006,'male',430000.0)) AS t(year,naissance,sex,population)"
    )
    _correct(conn)
    metro_2027 = conn.execute(
        "SELECT SUM(population) FROM population_department "
        "WHERE year=2027 AND department_code NOT IN ('986','988')"
    ).fetchone()[0]
    assert metro_2027 == pytest.approx(430000.0)

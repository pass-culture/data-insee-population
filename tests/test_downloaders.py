"""Tests for the download helpers in ``downloaders``.

Focus on cache robustness: an interrupted or truncated transfer must never
leave a corrupt file at the cache path for a later run to reuse. INSEE serves
the large census parquets gzip-encoded and chunked with no ``Content-Length``,
so completeness is validated via the trailing ``PAR1`` magic and bad downloads
are retried.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import requests

from passculture.data.insee_population import downloaders

PARQUET = b"PAR1" + b"....payload...." + b"PAR1"


class _FakeResponse:
    """Minimal stand-in for ``requests.Response`` in streaming mode."""

    def __init__(self, chunks: list[bytes], content_length: int | None = None):
        self._chunks = chunks
        self.headers: dict[str, str] = {}
        if content_length is not None:
            self.headers["content-length"] = str(content_length)

    def raise_for_status(self) -> None:
        pass

    def iter_content(self, chunk_size: int):
        yield from self._chunks


def _patch_get(monkeypatch, *responses) -> None:
    """Patch requests.get to return/raise the given responses in sequence.

    A response that is an Exception instance is raised; the last response is
    reused for any further calls.
    """
    seq = list(responses)

    def fake_get(*_a, **_k):
        item = seq.pop(0) if len(seq) > 1 else seq[0]
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(downloaders.requests, "get", fake_get)


def test_is_valid_parquet(tmp_path: Path):
    good = tmp_path / "good.parquet"
    good.write_bytes(PARQUET)
    assert downloaders._is_valid_parquet(good)

    truncated = tmp_path / "bad.parquet"
    truncated.write_bytes(b"PAR1....no trailing magic")
    assert not downloaders._is_valid_parquet(truncated)

    tiny = tmp_path / "tiny.parquet"
    tiny.write_bytes(b"PAR")
    assert not downloaders._is_valid_parquet(tiny)


def test_download_file_writes_complete_file(monkeypatch, tmp_path: Path):
    _patch_get(monkeypatch, _FakeResponse([PARQUET[:8], PARQUET[8:]]))

    dest = tmp_path / "indcvi.parquet"
    downloaders._download_file("http://x", dest, validate=downloaders._is_valid_parquet)

    assert dest.read_bytes() == PARQUET
    assert not dest.with_suffix(".parquet.part").exists()


def test_download_file_truncated_fails_and_leaves_no_file(monkeypatch, tmp_path: Path):
    # Server advertises 100 bytes but only delivers 10 (every attempt).
    _patch_get(monkeypatch, _FakeResponse([b"0123456789"], content_length=100))

    dest = tmp_path / "indcvi.parquet"
    with pytest.raises(OSError, match=r"after .* attempts"):
        downloaders._download_file("http://x", dest, retries=2)

    assert not dest.exists()
    assert not dest.with_suffix(".parquet.part").exists()


def test_download_file_invalid_magic_fails(monkeypatch, tmp_path: Path):
    # Full transfer (no length mismatch) but not a valid parquet.
    _patch_get(monkeypatch, _FakeResponse([b"PAR1...truncated mid-file"]))

    dest = tmp_path / "indcvi.parquet"
    with pytest.raises(OSError, match=r"after .* attempts"):
        downloaders._download_file(
            "http://x", dest, validate=downloaders._is_valid_parquet, retries=2
        )

    assert not dest.exists()
    assert not dest.with_suffix(".parquet.part").exists()


def test_download_file_retries_then_succeeds(monkeypatch, tmp_path: Path):
    # First attempt drops the connection; second delivers a valid parquet.
    _patch_get(
        monkeypatch,
        requests.ConnectionError("network dropped"),
        _FakeResponse([PARQUET]),
    )

    dest = tmp_path / "indcvi.parquet"
    downloaders._download_file(
        "http://x", dest, validate=downloaders._is_valid_parquet, retries=3
    )

    assert dest.read_bytes() == PARQUET
    assert not dest.with_suffix(".parquet.part").exists()


def test_cached_parquet_discards_corrupt_cache(monkeypatch, tmp_path: Path):
    # A pre-existing corrupt cache file must be re-downloaded, not reused.
    cache = tmp_path
    corrupt = cache / "indcvi_2099.parquet"
    corrupt.write_bytes(b"PAR1 partial junk")  # no trailing magic
    _patch_get(monkeypatch, _FakeResponse([PARQUET]))

    result = downloaders._cached_parquet("http://x", "indcvi_2099.parquet", cache)

    assert result == corrupt
    assert corrupt.read_bytes() == PARQUET


# -----------------------------------------------------------------------------
# INSEE estimates parser (POP3) and TOM eligibility
# -----------------------------------------------------------------------------


def _make_pop3_bytes() -> bytes:
    """Build a minimal 2-year POP3 workbook matching the real layout."""
    import io

    import openpyxl

    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    # cols: 0 naissance, 1 age, 2-4 métro (Ens/H/F), 5-7 France (Ens/H/F)
    data = {
        "2022": [
            (2006, 16, 800, 410, 390, 850, 434, 416),
            (2007, 15, 790, 405, 385, 840, 430, 410),
        ],
        "2026": [
            (2006, 20, 770, 395, 375, 820, 418, 402),
            (2007, 19, 760, 390, 370, 810, 414, 396),
        ],
    }
    for year, rows in data.items():
        ws = wb.create_sheet(year)
        ws.append([f"POP3 - ... Année {year}"])
        ws.append([])
        ws.append([])
        ws.append([None, None, "France métropolitaine", None, None, "France"])
        ws.append(["Année de naissance", "Âge", "Ens", "H", "F", "Ens", "H", "F"])
        ws.append([])
        for r in rows:
            ws.append(list(r))
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_parse_pop3_returns_long_table_france_entiere():
    df = downloaders._parse_pop3(_make_pop3_bytes())
    assert set(df.columns) == {"year", "naissance", "sex", "population"}
    assert set(df.sex.unique()) == {"male", "female"}
    # France entière (cols 5-7): gen-2006 @2022 = H 434 + F 416 = 850
    g = df[(df.year == 2022) & (df.naissance == 2006)].population.sum()
    assert g == 850
    # uses France entière, NOT métropole (which would be 800)
    assert df[(df.year == 2026) & (df.naissance == 2006)].population.sum() == 820


def test_synthesize_tom_defaults_to_eligible_only(monkeypatch):
    import pandas as pd

    calls = []

    def fake(name):
        def _dl(cache_dir=None):
            calls.append(name)
            return pd.DataFrame(
                {
                    "age": [15, 16],
                    "sex": ["male", "female"],
                    "population": [100.0, 90.0],
                }
            )

        return _dl

    monkeypatch.setattr(downloaders, "download_wlf_census", fake("986"))
    monkeypatch.setattr(downloaders, "download_pyf_census", fake("987"))
    monkeypatch.setattr(downloaders, "download_ncl_census", fake("988"))

    # year 2022 is BEFORE Wallis census (2023): offset floored, 986 still included
    df = downloaders.synthesize_tom_population(2022, cache_dir=None)
    depts = set(df.department_code.unique())
    assert depts == {"986", "988"}  # eligible only
    assert "987" not in depts  # Polynésie excluded by default
    assert "986" in calls  # Wallis NOT skipped despite 2023 census

import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

# Ensure src is importable
sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))

from finmodel.scripts import adv_fullstats_import_flat


def test_main_runs_without_nameerror(monkeypatch):
    df = pd.DataFrame([{"id": "1", "Организация": "Org", "Token_WB": "token"}])

    monkeypatch.setattr(
        adv_fullstats_import_flat, "load_organizations", lambda sheet=None: df, raising=False
    )
    monkeypatch.setattr(
        adv_fullstats_import_flat,
        "get_campaign_ids_from_api",
        lambda token: [1],
        raising=False,
    )
    monkeypatch.setattr(
        adv_fullstats_import_flat,
        "get_local_eligible_ids",
        lambda conn, org_id, ids, begin, end: [],
        raising=False,
    )
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE AdvCampaignsDetailsFlat (
            org_id TEXT,
            advertId TEXT,
            status TEXT,
            type TEXT,
            startTime TEXT,
            endTime TEXT,
            changeTime TEXT
        )
        """
    )
    monkeypatch.setattr(sqlite3, "connect", lambda *args, **kwargs: conn)

    fake_get = MagicMock(
        return_value=SimpleNamespace(status_code=200, json=lambda: {"adverts": []}, text="")
    )
    fake_post = MagicMock(return_value=SimpleNamespace(status_code=200, json=lambda: [], text=""))
    monkeypatch.setattr("requests.get", fake_get)
    monkeypatch.setattr("requests.post", fake_post)

    try:
        adv_fullstats_import_flat.main()
    except NameError as err:
        pytest.fail(f"main raised NameError: {err}")

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Ensure src is importable
sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))

from finmodel.scripts import finotchet_import


def test_finotchet_respects_sheet_env(monkeypatch, caplog):
    monkeypatch.setenv("ORG_SHEET", "OrgSheet")
    monkeypatch.setenv("SETTINGS_SHEET", "SettingsSheet")

    load_org = MagicMock(return_value=pd.DataFrame())
    load_period = MagicMock(return_value=("2024-01-01", "2024-01-02"))
    monkeypatch.setattr(finotchet_import, "load_organizations", load_org)
    monkeypatch.setattr(finotchet_import, "load_period", load_period)
    monkeypatch.setattr(finotchet_import, "parse_date", lambda x: pd.Timestamp(x))
    monkeypatch.setattr(finotchet_import.sqlite3, "connect", MagicMock())

    with caplog.at_level("INFO"):
        with pytest.raises(SystemExit):
            finotchet_import.main()

    load_org.assert_called_once_with(sheet="OrgSheet")
    load_period.assert_called_once_with(sheet="SettingsSheet")
    assert "Using organizations sheet OrgSheet" in caplog.text
    assert "Using settings sheet SettingsSheet" in caplog.text


def test_finotchet_inserts_rows_and_stops_on_empty_response():
    env = {"ORG_SHEET": "OrgSheet", "SETTINGS_SHEET": "SettingsSheet"}
    df_orgs = pd.DataFrame([{"id": 1, "Организация": "Org", "Token_WB": "t"}])

    with (
        patch.dict(os.environ, env, clear=True),
        patch(
            "finmodel.scripts.finotchet_import.load_organizations", return_value=df_orgs
        ) as load_orgs,
        patch(
            "finmodel.scripts.finotchet_import.load_period",
            return_value=("2021-01-01", "2021-01-31"),
        ) as load_period,
        patch("finmodel.scripts.finotchet_import.WB_FIELDS", ["rrd_id"]),
        patch("finmodel.scripts.finotchet_import.requests.Session.get") as mock_get,
        patch("finmodel.scripts.finotchet_import.sqlite3.connect") as mock_connect,
        patch("finmodel.scripts.finotchet_import.time.sleep"),
    ):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        def make_resp(data):
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = data
            return resp

        page_size = 100_000
        page1 = [{"rrd_id": i} for i in range(1, page_size + 1)]
        page2 = [{"rrd_id": i} for i in range(page_size + 1, 2 * page_size + 1)]
        mock_get.side_effect = [make_resp(page1), make_resp(page2), make_resp([])]

        finotchet_import.main()

        load_orgs.assert_called_once_with(sheet="OrgSheet")
        load_period.assert_called_once_with(sheet="SettingsSheet")

        fin_calls = [c for c in mock_cursor.executemany.call_args_list if "FinOtchet" in c.args[0]]
        assert len(fin_calls) == 2
        for c in fin_calls:
            assert c.args[1]

        assert mock_get.call_count == 3

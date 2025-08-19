import pandas as pd

from finmodel.utils.db_load import load_wb_tokens


def test_load_wb_tokens(tmp_path):
    df = pd.DataFrame(
        {"id": [1, 2, 3], "Организация": ["A", "B", "C"], "Token_WB": ["AAA", None, "BBB"]}
    )
    xls = tmp_path / "orgs.xlsx"
    with pd.ExcelWriter(xls) as writer:
        df.to_excel(writer, sheet_name="НастройкиОрганизаций", index=False)
    tokens = load_wb_tokens(xls)
    assert tokens == [(1, "AAA"), (3, "BBB")]

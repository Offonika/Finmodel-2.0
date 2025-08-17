from __future__ import annotations

import importlib
import shutil
import sys
from importlib import resources
from pathlib import Path


def test_get_project_root_development():
    from finmodel.utils.paths import get_project_root

    root = get_project_root()
    assert (root / "pyproject.toml").exists()


def test_get_project_root_installed(tmp_path, monkeypatch):
    package_src = Path(__file__).resolve().parents[2] / "src" / "finmodel"
    site_packages = tmp_path / "site-packages"
    shutil.copytree(package_src, site_packages / "finmodel")

    monkeypatch.syspath_prepend(str(site_packages))
    monkeypatch.delenv("FINMODEL_PROJECT_ROOT", raising=False)

    sys.modules.pop("finmodel", None)
    sys.modules.pop("finmodel.utils", None)
    sys.modules.pop("finmodel.utils.paths", None)

    mod = importlib.import_module("finmodel.utils.paths")
    importlib.reload(mod)

    root = mod.get_project_root()
    assert root == Path(resources.files("finmodel")).resolve()

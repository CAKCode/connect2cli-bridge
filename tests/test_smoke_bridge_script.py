from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "smoke_bridge.sh"


def test_smoke_bridge_script_disables_external_pytest_plugin_autoload() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert "PYTEST_DISABLE_PLUGIN_AUTOLOAD=1" in content
    assert "-p pytest_asyncio.plugin" in content
    assert "-p no:rerunfailures" in content

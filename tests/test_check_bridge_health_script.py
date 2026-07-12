from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_NAME = "check_bridge_health.sh"
BRIDGE_ENV_NAME = "bridge_env.sh"
RUNTIME_CONFIG_NAME = "bridge_runtime_config.py"


def _copy_script_fixture(target_dir: Path) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    for name in (SCRIPT_NAME, BRIDGE_ENV_NAME, RUNTIME_CONFIG_NAME):
        source = REPO_ROOT / name
        (target_dir / name).write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    return target_dir / SCRIPT_NAME


def test_check_bridge_health_script_expands_tilde_shared_runtime_root(tmp_path: Path) -> None:
    script_dir = tmp_path / "scripts"
    script = _copy_script_fixture(script_dir)
    home_dir = tmp_path / "home"
    schedule_root = home_dir / "shared-runtime" / ".scheduled-messages"
    processing_dir = schedule_root / "processing"
    pending_dir = schedule_root / "pending"
    processing_dir.mkdir(parents=True, exist_ok=True)
    pending_dir.mkdir(parents=True, exist_ok=True)
    (processing_dir / "job-1.json").write_text("{}", encoding="utf-8")
    (pending_dir / "job-2.json").write_text("{}", encoding="utf-8")
    python_hooks = tmp_path / "python-hooks"
    python_hooks.mkdir(parents=True, exist_ok=True)
    (python_hooks / "sitecustomize.py").write_text(
        "\n".join(
            [
                "import json",
                "import urllib.request",
                "",
                "class _Response:",
                "    def __init__(self, payload):",
                "        self._payload = payload",
                "",
                "    def __enter__(self):",
                "        return self",
                "",
                "    def __exit__(self, exc_type, exc, tb):",
                "        return False",
                "",
                "    def read(self):",
                "        return self._payload",
                "",
                "def _fake_urlopen(req, timeout=0):",
                "    url = getattr(req, 'full_url', req)",
                "    if url.endswith('/api/bots'):",
                "        payload = [{'name': 'default', 'id': 'bot-1', 'status': 'running', 'enabled': True, 'sessions': []}]",
                "    elif url.endswith('/livez'):",
                "        payload = {'ok': True}",
                "    else:",
                "        raise RuntimeError(f'unexpected url: {url}')",
                "    return _Response(json.dumps(payload).encode('utf-8'))",
                "",
                "urllib.request.urlopen = _fake_urlopen",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env["HOME"] = str(home_dir)
    env["BRIDGE_BIND"] = "127.0.0.1:19399"
    env["BRIDGE_SHARED_RUNTIME_ROOT"] = "~/shared-runtime"
    env["HEALTH_TIMEOUT_SEC"] = "2"
    env["PYTHONPATH"] = f"{python_hooks}:{env.get('PYTHONPATH', '')}" if env.get("PYTHONPATH") else str(python_hooks)

    result = subprocess.run(
        ["sh", str(script)],
        capture_output=True,
        text=True,
        cwd=script_dir,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert "bridge root: ok" in result.stdout
    assert "bots: 1" in result.stdout
    assert "scheduled processing=1 pending=1" in result.stdout


def test_check_bridge_health_script_uses_livez_not_root_or_healthz(tmp_path: Path) -> None:
    script_dir = tmp_path / "scripts"
    script = _copy_script_fixture(script_dir)
    python_hooks = tmp_path / "python-hooks"
    python_hooks.mkdir(parents=True, exist_ok=True)
    (python_hooks / "sitecustomize.py").write_text(
        "\n".join(
            [
                "import json",
                "import urllib.request",
                "",
                "class _Response:",
                "    def __init__(self, payload):",
                "        self._payload = payload",
                "",
                "    def __enter__(self):",
                "        return self",
                "",
                "    def __exit__(self, exc_type, exc, tb):",
                "        return False",
                "",
                "    def read(self):",
                "        return self._payload",
                "",
                "def _fake_urlopen(req, timeout=0):",
                "    url = getattr(req, 'full_url', req)",
                "    if url.endswith('/livez'):",
                "        payload = {'ok': False}",
                "    elif url.endswith('/healthz'):",
                "        payload = {'ok': True}",
                "    elif url.endswith('/'):",
                "        payload = {'ok': True}",
                "    elif url.endswith('/api/bots'):",
                "        payload = [{'name': 'default', 'id': 'bot-1', 'status': 'running', 'enabled': True, 'sessions': []}]",
                "    else:",
                "        raise RuntimeError(f'unexpected url: {url}')",
                "    return _Response(json.dumps(payload).encode('utf-8'))",
                "",
                "urllib.request.urlopen = _fake_urlopen",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env["BRIDGE_BIND"] = "127.0.0.1:19399"
    env["HEALTH_TIMEOUT_SEC"] = "1"
    env["PYTHONPATH"] = f"{python_hooks}:{env.get('PYTHONPATH', '')}" if env.get("PYTHONPATH") else str(python_hooks)

    result = subprocess.run(
        ["sh", str(script)],
        capture_output=True,
        text=True,
        cwd=script_dir,
        env=env,
        check=False,
    )

    assert result.returncode == 1
    assert "bridge health failed:" in result.stderr


def test_check_bridge_health_script_reports_api_bot_listing_failure_cleanly(tmp_path: Path) -> None:
    script_dir = tmp_path / "scripts"
    script = _copy_script_fixture(script_dir)
    python_hooks = tmp_path / "python-hooks"
    python_hooks.mkdir(parents=True, exist_ok=True)
    (python_hooks / "sitecustomize.py").write_text(
        "\n".join(
            [
                "import json",
                "import urllib.request",
                "",
                "class _Response:",
                "    def __init__(self, payload):",
                "        self._payload = payload",
                "",
                "    def __enter__(self):",
                "        return self",
                "",
                "    def __exit__(self, exc_type, exc, tb):",
                "        return False",
                "",
                "    def read(self):",
                "        return self._payload",
                "",
                "def _fake_urlopen(req, timeout=0):",
                "    url = getattr(req, 'full_url', req)",
                "    if url.endswith('/livez'):",
                "        payload = {'ok': True}",
                "        return _Response(json.dumps(payload).encode('utf-8'))",
                "    if url.endswith('/api/bots'):",
                "        raise RuntimeError('boom')",
                "    raise RuntimeError(f'unexpected url: {url}')",
                "",
                "urllib.request.urlopen = _fake_urlopen",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env["BRIDGE_BIND"] = "127.0.0.1:19399"
    env["HEALTH_TIMEOUT_SEC"] = "1"
    env["PYTHONPATH"] = f"{python_hooks}:{env.get('PYTHONPATH', '')}" if env.get("PYTHONPATH") else str(python_hooks)

    result = subprocess.run(
        ["sh", str(script)],
        capture_output=True,
        text=True,
        cwd=script_dir,
        env=env,
        check=False,
    )

    assert result.returncode == 1
    assert result.stderr.strip() == "bridge bot listing failed: boom"

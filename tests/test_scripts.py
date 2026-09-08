"""The run scripts have to parse and take their arguments before a stack exists.

A syntax error in `smoke.sh` would otherwise surface only in the compose smoke,
which is opt-in on a pull request and takes tens of minutes — so it would reach
`main` and fail there. `bash -n` and the two argument paths that need no Docker
are the whole of what can be checked without a stack, and they are the cheap
half of every mistake actually made in a shell script.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
SMOKE = SCRIPTS / "smoke.sh"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")


def test_every_script_parses() -> None:
    scripts = sorted(SCRIPTS.glob("*.sh"))
    assert scripts, f"no shell scripts under {SCRIPTS}"
    for script in scripts:
        check = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
        assert check.returncode == 0, f"{script.name}: {check.stderr}"


def test_smoke_is_executable() -> None:
    assert os.access(SMOKE, os.X_OK), f"{SMOKE} is not executable"


def test_help_needs_no_stack() -> None:
    """`--help` has to answer before the host-tool check, on any machine.

    It is the one thing a reader runs first, and refusing it for a missing `yq`
    would be refusing to say what the script does.
    """
    out = subprocess.run([str(SMOKE), "--help"], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "--external-ready-file" in out.stdout


def test_an_unknown_argument_is_refused() -> None:
    out = subprocess.run([str(SMOKE), "--warmup"], capture_output=True, text=True)
    assert out.returncode == 2, out.stdout
    assert "unknown argument --warmup" in out.stderr

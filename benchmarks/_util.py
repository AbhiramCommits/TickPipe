"""Shared helpers for the tickpipe benchmark suite."""

from __future__ import annotations

import json
import platform
import subprocess
import sys
from pathlib import Path

RESULTS_PATH = Path(__file__).parent / "results.json"


def hardware_description() -> str:
    if sys.platform == "darwin":
        brand = subprocess.check_output(
            ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
        ).strip()
        model = subprocess.check_output(["sysctl", "-n", "hw.model"], text=True).strip()
        return f"{model} ({brand})"
    return platform.platform()


def python_version() -> str:
    return sys.version.split()[0]


def load_results() -> dict:
    if RESULTS_PATH.exists():
        return json.loads(RESULTS_PATH.read_text())
    return {}


def save_results(results: dict) -> None:
    RESULTS_PATH.write_text(json.dumps(results, indent=2, sort_keys=True))


def report(key: str, value: float, unit: str, description: str) -> None:
    print(f"{description}: {value:,.0f} {unit}" if unit else f"{description}: {value}")
    results = load_results()
    results[key] = value
    results["hardware"] = hardware_description()
    results["python"] = python_version()
    save_results(results)

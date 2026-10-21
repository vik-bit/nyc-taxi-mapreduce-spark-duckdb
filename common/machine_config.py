"""Machine configuration shared by all timed experiments."""

from __future__ import annotations

import platform
import sys
from pathlib import Path

RUN_ID = "local-laptop"

# Recorded once for this laptop (Windows Task Manager / WMI).
MACHINE = {
    "os": "Windows 11 24H2",
    "cpu_model": "12th Gen Intel Core i7-1255U",
    "cpu_base_ghz": 1.70,
    "cpu_physical_cores": 10,
    "cpu_logical_processors": 12,
    "ram_gb": 16,
    "storage_size_gb": 1024,
    "storage_type": "NVMe SSD",
    "gpu_model": "Intel Iris Xe Graphics (Integrated)",
    "gpu_clock_mhz": "NA",
    "gpu_memory_gb": 0,
}

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RESULTS_DIR = PROJECT_ROOT / "results"
TIMINGS_CSV = RESULTS_DIR / "timings.csv"

TIMINGS_HEADER = [
    "run_id",
    "system",
    "task",
    "condition",
    "dataset_rows",
    "input_format",
    "input_mb",
    "run_number",
    "time_seconds",
    "status",
    "os",
    "cpu_model",
    "cpu_base_ghz",
    "cpu_physical_cores",
    "cpu_logical_processors",
    "ram_gb",
    "storage_size_gb",
    "storage_type",
    "gpu_model",
    "gpu_clock_mhz",
    "gpu_memory_gb",
    "python_version",
    "tool_version",
]


def python_version() -> str:
    return platform.python_version()


def file_size_mb(path: Path) -> float:
    return round(path.stat().st_size / (1024 * 1024), 1)


def ensure_results_dir() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def append_timing_row(row: dict) -> None:
    """Append one timings.csv row; write header if the file is new."""
    import csv

    ensure_results_dir()
    write_header = not TIMINGS_CSV.exists() or TIMINGS_CSV.stat().st_size == 0
    with TIMINGS_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TIMINGS_HEADER)
        if write_header:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in TIMINGS_HEADER})


def base_row(
    *,
    system: str,
    task: str,
    condition: str,
    dataset_rows: int,
    input_format: str,
    input_path: Path,
    run_number: int,
    time_seconds: float,
    status: str,
    tool_version: str,
) -> dict:
    return {
        "run_id": RUN_ID,
        "system": system,
        "task": task,
        "condition": condition,
        "dataset_rows": dataset_rows,
        "input_format": input_format,
        "input_mb": file_size_mb(input_path),
        "run_number": run_number,
        "time_seconds": round(time_seconds, 3),
        "status": status,
        **MACHINE,
        "python_version": python_version(),
        "tool_version": tool_version,
    }

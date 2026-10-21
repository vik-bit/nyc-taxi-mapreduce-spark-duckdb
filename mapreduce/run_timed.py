"""Timed MapReduce Task 1 / Task 4 runner.

On Windows, mrjob --runner local fails (symlink privileges / fcntl). Timed
runs therefore invoke the same job via WSL Ubuntu with --runner local, which
is the timed path used here. Fallback: --runner inline on native Windows.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.machine_config import DATA_DIR, append_timing_row, base_row  # noqa: E402

try:
    import mrjob

    MRJOB_VERSION = mrjob.__version__
except Exception:
    MRJOB_VERSION = "unknown"

JOB = ROOT / "mapreduce" / "task1.py"

DATASETS = {
    100_000: "taxi_100k.csv",
    500_000: "taxi_500k.csv",
    1_000_000: "taxi_1m.csv",
    2_000_000: "taxi_2m.csv",
}

WSL_PROJECT = "/mnt/c/Users/vikra/OneDrive/Desktop/Project1_BDA"


def _use_wsl_local() -> bool:
    if os.name != "nt":
        return False
    try:
        r = subprocess.run(
            ["wsl", "-d", "Ubuntu", "--", "bash", "-lc", "python3 -c 'import mrjob'"],
            capture_output=True,
            text=True,
        )
        return r.returncode == 0
    except FileNotFoundError:
        return False


def run_mrjob(csv_path: Path, out_dir: Path, runner: str) -> tuple[float, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    for p in out_dir.glob("*"):
        if p.is_file():
            p.unlink()

    rel_csv = csv_path.relative_to(ROOT).as_posix()
    rel_out = out_dir.relative_to(ROOT).as_posix()
    rel_job = JOB.relative_to(ROOT).as_posix()

    if runner == "local" and _use_wsl_local():
        cmd = [
            "wsl",
            "-d",
            "Ubuntu",
            "--",
            "bash",
            "-lc",
            (
                f"cd '{WSL_PROJECT}' && "
                f"python3 {rel_job} {rel_csv} --runner local "
                f"--output-dir {rel_out} --no-output"
            ),
        ]
    else:
        cmd = [
            sys.executable,
            str(JOB),
            str(csv_path),
            "--runner",
            runner,
            "--output-dir",
            str(out_dir),
            "--no-output",
        ]

    start = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.perf_counter() - start
    if proc.returncode != 0:
        err = ROOT / "results" / "errors.txt"
        err.parent.mkdir(parents=True, exist_ok=True)
        with err.open("a", encoding="utf-8") as f:
            f.write(f"mapreduce {csv_path.name} runner={runner}:\n")
            f.write((proc.stderr or "")[-4000:] + "\n")
            f.write((proc.stdout or "")[-2000:] + "\n")
        return elapsed, "ERROR"
    if elapsed > 900:
        return 900.0, "TIMEOUT_900"
    return elapsed, "OK"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, nargs="+", default=[1_000_000])
    parser.add_argument("--task", default="task1", choices=["task1", "task4"])
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--no-warmup", action="store_true")
    parser.add_argument(
        "--runner",
        default="local",
        choices=["local", "inline"],
        help="Assignment default is local (via WSL on this Windows laptop).",
    )
    args = parser.parse_args()

    for rows in args.rows:
        name = DATASETS[rows]
        csv_path = DATA_DIR / name
        if not csv_path.exists():
            raise FileNotFoundError(csv_path)

        out_base = ROOT / "results" / "mr_out" / f"{args.task}_{rows}"
        if not args.no_warmup:
            print(f"Warm-up mapreduce {name} runner={args.runner} ...")
            run_mrjob(csv_path, out_base / "warmup", args.runner)

        for i in range(1, args.runs + 1):
            print(f"Measured run {i} mapreduce {name} ...")
            elapsed, status = run_mrjob(csv_path, out_base / f"run_{i}", args.runner)
            append_timing_row(
                base_row(
                    system="mapreduce",
                    task=args.task,
                    condition="aggregation",
                    dataset_rows=rows,
                    input_format="csv",
                    input_path=csv_path,
                    run_number=i,
                    time_seconds=elapsed,
                    status=status,
                    tool_version=MRJOB_VERSION,
                )
            )
            print(f"  time={elapsed:.3f}s status={status}")
            if status == "TIMEOUT_900":
                break


if __name__ == "__main__":
    main()

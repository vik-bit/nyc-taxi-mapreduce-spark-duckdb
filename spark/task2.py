"""Task 2 — Spark Parquet filter + projection on taxi_2m.parquet."""

from __future__ import annotations

import csv
import platform
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
TIMINGS_CSV = ROOT / "results" / "timings.csv"
RUN_ID = "local-laptop"
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
HEADER = [
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


def append_timing_row(row: dict) -> None:
    TIMINGS_CSV.parent.mkdir(parents=True, exist_ok=True)
    write_header = not TIMINGS_CSV.exists() or TIMINGS_CSV.stat().st_size == 0
    with TIMINGS_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        if write_header:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in HEADER})


def base_row(**kwargs) -> dict:
    path: Path = kwargs["input_path"]
    return {
        "run_id": RUN_ID,
        "system": kwargs["system"],
        "task": kwargs["task"],
        "condition": kwargs["condition"],
        "dataset_rows": kwargs["dataset_rows"],
        "input_format": kwargs["input_format"],
        "input_mb": round(path.stat().st_size / (1024 * 1024), 1),
        "run_number": kwargs["run_number"],
        "time_seconds": round(kwargs["time_seconds"], 3),
        "status": kwargs["status"],
        **MACHINE,
        "python_version": platform.python_version(),
        "tool_version": kwargs["tool_version"],
    }


def timed_call(fn):
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    if elapsed > 900:
        return 900.0, result, "TIMEOUT_900"
    return elapsed, result, "OK"


def warm_up_then_measure(fn, n_runs: int = 3):
    fn()
    measured = []
    for _ in range(n_runs):
        measured.append(timed_call(fn))
        if measured[-1][2] == "TIMEOUT_900":
            break
    return measured


def query_a(df):
    return df.filter((F.col("PULocationID") == 161) & (F.col("fare_amount") > 30.0)&(F.col("tip_amount") > 0)).collect()


def query_b(df):
    return (
        df.filter((F.col("PULocationID") == 161) & (F.col("fare_amount") > 30.0))
        .select(
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "fare_amount",
        )
        .collect()
    )


def main() -> None:
    parquet_path = DATA_DIR / "taxi_2m.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Missing input: {parquet_path}")

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("BDA_A1_Task2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    tool_version = spark.version

    df = spark.read.parquet(str(parquet_path))

    explain_path = Path(__file__).resolve().parent / "task2_explain.txt"

    def explain_formatted(frame) -> str:
        from io import StringIO
        import contextlib

        buf = StringIO()
        with contextlib.redirect_stdout(buf):
            try:
                frame.explain(mode="formatted")
            except TypeError:
                frame.explain(True)
        text = buf.getvalue().strip()
        if text:
            return text
        return str(frame._jdf.queryExecution().explainString("formatted"))

    qa = df.filter((F.col("PULocationID") == 161) & (F.col("fare_amount") > 30.0))
    qb = qa.select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
    )
    explain_path.write_text(
        "=== Query A (all columns) ===\n"
        + explain_formatted(qa)
        + "\n\n=== Query B (four columns) ===\n"
        + explain_formatted(qb),
        encoding="utf-8",
    )
    print(f"Wrote {explain_path}")

    for condition, fn in [("query_a_all_cols", lambda: query_a(df)), ("query_b_projection", lambda: query_b(df))]:
        measured = warm_up_then_measure(fn, n_runs=3)
        for i, (elapsed, rows, status) in enumerate(measured, start=1):
            append_timing_row(
                base_row(
                    system="spark",
                    task="task2",
                    condition=condition,
                    dataset_rows=2_000_000,
                    input_format="parquet",
                    input_path=parquet_path,
                    run_number=i,
                    time_seconds=elapsed,
                    status=status,
                    tool_version=tool_version,
                )
            )
            print(f"{condition} run={i} time={elapsed:.3f}s status={status} rows={len(rows) if rows else 0}")
            if rows:
                print("sample:", rows[:3])

    spark.stop()


if __name__ == "__main__":
    main()

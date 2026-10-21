"""Task 3 — Spark join with taxi-zone lookup; top-5 zones by total fare."""

from __future__ import annotations

import csv
import platform
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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


def run_join_top5(spark: SparkSession, trips_path: Path, zones_path: Path):
    trips = spark.read.parquet(str(trips_path))
    zones = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(zones_path))
    )

    joined = (
        trips.filter(F.col("fare_amount") >= 0)
        .join(zones, trips.PULocationID == zones.LocationID, "inner")
        .groupBy("Borough", "Zone")
        .agg(F.sum("fare_amount").alias("total_fare"))
    )

    w = Window.orderBy(F.col("total_fare").desc(), F.col("Zone").asc())
    result = (
        joined.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") <= 5)
        .select("Borough", "Zone", "total_fare")
        .collect()
    )
    return result, joined


def main() -> None:
    trips_path = DATA_DIR / "taxi_2m.parquet"
    zones_path = DATA_DIR / "taxi_zone_lookup.csv"
    for p in (trips_path, zones_path):
        if not p.exists():
            raise FileNotFoundError(f"Missing input: {p}")

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("BDA_A1_Task3")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    tool_version = spark.version

    trips = spark.read.parquet(str(trips_path))
    zones = spark.read.option("header", True).option("inferSchema", True).csv(str(zones_path))
    plan_df = (
        trips.filter(F.col("fare_amount") >= 0)
        .join(zones, trips.PULocationID == zones.LocationID, "inner")
        .groupBy("Borough", "Zone")
        .agg(F.sum("fare_amount").alias("total_fare"))
    )
    from io import StringIO
    import contextlib

    buf = StringIO()
    with contextlib.redirect_stdout(buf):
        try:
            plan_df.explain(mode="formatted")
        except TypeError:
            plan_df.explain(True)
    plan_text = buf.getvalue()
    plan_path = Path(__file__).resolve().parent / "task3_explain.txt"
    plan_path.write_text(plan_text, encoding="utf-8")
    print(f"Wrote join plan to {plan_path}")
    if "Broadcast" in plan_text:
        print("Observed: broadcast-related operator present in plan.")
    else:
        print("Observed: no BroadcastHashJoin/BroadcastExchange string in formatted plan.")

    def once():
        rows, _ = run_join_top5(spark, trips_path, zones_path)
        return rows

    measured = warm_up_then_measure(once, n_runs=3)
    for i, (elapsed, rows, status) in enumerate(measured, start=1):
        append_timing_row(
            base_row(
                system="spark",
                task="task3",
                condition="join_top5",
                dataset_rows=2_000_000,
                input_format="parquet",
                input_path=trips_path,
                run_number=i,
                time_seconds=elapsed,
                status=status,
                tool_version=tool_version,
            )
        )
        print(f"run={i} time={elapsed:.3f}s status={status}")
        if rows:
            for r in rows:
                print(r)

    spark.stop()


if __name__ == "__main__":
    main()

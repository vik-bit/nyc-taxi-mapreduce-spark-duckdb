"""Task 5 — Spark repeated aggregation on taxi_1m.parquet (uncached vs cached)."""

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


def aggregate(df):
    result = (
        df.filter(F.col("PULocationID").isNotNull())
        .filter(F.col("fare_amount").isNotNull() & (F.col("fare_amount") >= 0))
        .groupBy("PULocationID")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
        )
    )
    return result.collect()


def main() -> None:
    parquet_path = DATA_DIR / "taxi_1m.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Missing input: {parquet_path}")

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("BDA_A1_Task5")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    tool_version = spark.version

    df_uncached = spark.read.parquet(str(parquet_path))
    print("=== Spark uncached (5 runs) ===")
    for i in range(1, 6):
        elapsed, rows, status = timed_call(lambda: aggregate(df_uncached))
        append_timing_row(
            base_row(
                system="spark",
                task="task5",
                condition="spark_uncached",
                dataset_rows=1_000_000,
                input_format="parquet",
                input_path=parquet_path,
                run_number=i,
                time_seconds=elapsed,
                status=status,
                tool_version=tool_version,
            )
        )
        print(f"uncached run={i} time={elapsed:.3f}s n={len(rows) if rows else 0}")

    df_cached = spark.read.parquet(str(parquet_path)).cache()
    t0 = time.perf_counter()
    _ = df_cached.count()
    cache_build = time.perf_counter() - t0
    append_timing_row(
        base_row(
            system="spark",
            task="task5",
            condition="spark_cache_build",
            dataset_rows=1_000_000,
            input_format="parquet",
            input_path=parquet_path,
            run_number=0,
            time_seconds=cache_build,
            status="OK",
            tool_version=tool_version,
        )
    )
    
    print(f"cache_build time={cache_build:.3f}s")

    print("=== Spark cached (5 runs) ===")
    for i in range(1, 6):
        elapsed, rows, status = timed_call(lambda: aggregate(df_cached))
        append_timing_row(
            base_row(
                system="spark",
                task="task5",
                condition="spark_cached",
                dataset_rows=1_000_000,
                input_format="parquet",
                input_path=parquet_path,
                run_number=i,
                time_seconds=elapsed,
                status=status,
                tool_version=tool_version,
            )
        )
        print(f"cached run={i} time={elapsed:.3f}s n={len(rows) if rows else 0}")

    df_cached.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

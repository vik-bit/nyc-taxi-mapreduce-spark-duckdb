"""Task 4 scaling: same aggregation on 100k/500k/1m/2m CSV for Spark and DuckDB.
MapReduce is run via mapreduce/run_timed.py --task task4.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.machine_config import DATA_DIR, append_timing_row, base_row  # noqa: E402
from common.timing import warm_up_then_measure  # noqa: E402

DATASETS = {
    100_000: "taxi_100k.csv",
    500_000: "taxi_500k.csv",
    1_000_000: "taxi_1m.csv",
    2_000_000: "taxi_2m.csv",
}

DUCK_SQL = """
SELECT
  PULocationID,
  COUNT(*) AS trip_count,
  AVG(fare_amount) AS avg_fare
FROM read_csv_auto(?, header = true)
WHERE PULocationID IS NOT NULL
  AND fare_amount IS NOT NULL
  AND fare_amount >= 0
GROUP BY PULocationID
"""


def spark_agg(spark: SparkSession, csv_path: Path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(str(csv_path))
    result = (
        df.filter(F.col("PULocationID").isNotNull())
        .filter(F.col("fare_amount").isNotNull() & (F.col("fare_amount") >= 0))
        .groupBy("PULocationID")
        .agg(F.count(F.lit(1)).alias("trip_count"), F.avg("fare_amount").alias("avg_fare"))
    )
    return result.collect()


def run_spark() -> None:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("BDA_A1_Task4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    tool_version = spark.version

    for rows, name in DATASETS.items():
        csv_path = DATA_DIR / name
        print(f"Spark scaling {name}")
        measured = warm_up_then_measure(lambda p=csv_path: spark_agg(spark, p), n_runs=3)
        for i, (elapsed, _, status) in enumerate(measured, start=1):
            append_timing_row(
                base_row(
                    system="spark",
                    task="task4",
                    condition="aggregation",
                    dataset_rows=rows,
                    input_format="csv",
                    input_path=csv_path,
                    run_number=i,
                    time_seconds=elapsed,
                    status=status,
                    tool_version=tool_version,
                )
            )
            print(f"  run={i} {elapsed:.3f}s {status}")
            if status == "TIMEOUT_900":
                break

    spark.stop()


def run_duckdb() -> None:
    con = duckdb.connect()
    tool_version = duckdb.__version__
    for rows, name in DATASETS.items():
        csv_path = DATA_DIR / name
        print(f"DuckDB scaling {name}")

        def once(p=str(csv_path)):
            return con.execute(DUCK_SQL, [p]).fetchall()

        measured = warm_up_then_measure(once, n_runs=3)
        for i, (elapsed, _, status) in enumerate(measured, start=1):
            append_timing_row(
                base_row(
                    system="duckdb",
                    task="task4",
                    condition="aggregation",
                    dataset_rows=rows,
                    input_format="csv",
                    input_path=csv_path,
                    run_number=i,
                    time_seconds=elapsed,
                    status=status,
                    tool_version=tool_version,
                )
            )
            print(f"  run={i} {elapsed:.3f}s {status}")
            if status == "TIMEOUT_900":
                break
    con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--system", choices=["spark", "duckdb", "both"], default="both")
    args = parser.parse_args()
    if args.system in ("spark", "both"):
        run_spark()
    if args.system in ("duckdb", "both"):
        run_duckdb()


if __name__ == "__main__":
    main()

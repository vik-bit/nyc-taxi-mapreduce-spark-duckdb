"""Generate spark/task2_explain.txt without timed runs."""
from io import StringIO
import contextlib
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.master("local[*]")
    .appName("explain")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet("data/taxi_2m.parquet")
qa = df.filter((F.col("PULocationID") == 161) & (F.col("fare_amount") > 30.0))
qb = qa.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
)


def expl(frame) -> str:
    buf = StringIO()
    with contextlib.redirect_stdout(buf):
        frame.explain(mode="formatted")
    return buf.getvalue()


out = Path("spark/task2_explain.txt")
out.write_text(
    "=== Query A (all columns) ===\n"
    + expl(qa)
    + "\n=== Query B (four columns) ===\n"
    + expl(qb),
    encoding="utf-8",
)
print("wrote", out, "bytes", out.stat().st_size)
spark.stop()

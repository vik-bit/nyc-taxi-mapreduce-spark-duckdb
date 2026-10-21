"""Verify MapReduce output group count vs DuckDB."""
import ast
from pathlib import Path

import duckdb

mr_dir = Path("results/mr_wsl_smoke")
keys = set()
for p in mr_dir.glob("part-*"):
    for line in p.read_text(encoding="utf-8").splitlines():
        if line.strip():
            k, _ = line.split("\t", 1)
            keys.add(ast.literal_eval(k))

d = duckdb.connect().execute(
    """
    SELECT COUNT(*) FROM (
      SELECT PULocationID
      FROM read_csv_auto('data/taxi_100k.csv', header=true)
      WHERE PULocationID IS NOT NULL
        AND fare_amount IS NOT NULL
        AND fare_amount >= 0
      GROUP BY PULocationID
    )
    """
).fetchone()[0]
print("mr_groups", len(keys), "duckdb_groups", d, "MATCH" if len(keys) == d else "MISMATCH")

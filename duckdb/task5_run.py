"""DuckDB Task 5 — five repeated aggregations on taxi_1m.parquet."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.machine_config import DATA_DIR, append_timing_row, base_row  # noqa: E402
from common.timing import timed_call  # noqa: E402

SQL = """
SELECT
  PULocationID,
  COUNT(*) AS trip_count,
  AVG(fare_amount) AS avg_fare
FROM read_parquet(?)
WHERE PULocationID IS NOT NULL
  AND fare_amount IS NOT NULL
  AND fare_amount >= 0
GROUP BY PULocationID
"""


def main() -> None:
    parquet_path = DATA_DIR / "taxi_1m.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)

    con = duckdb.connect()
    tool_version = duckdb.__version__
    p = str(parquet_path)

    for i in range(1, 6):
        elapsed, rows, status = timed_call(lambda: con.execute(SQL, [p]).fetchall())
        append_timing_row(
            base_row(
                system="duckdb",
                task="task5",
                condition="duckdb_repeat",
                dataset_rows=1_000_000,
                input_format="parquet",
                input_path=parquet_path,
                run_number=i,
                time_seconds=elapsed,
                status=status,
                tool_version=tool_version,
            )
        )
        print(f"run={i} time={elapsed:.3f}s n={len(rows) if rows else 0}")

    con.close()


if __name__ == "__main__":
    main()

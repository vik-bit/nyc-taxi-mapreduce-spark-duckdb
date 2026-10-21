"""DuckDB Task 1 runner with warm-up + 3 measured runs."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.machine_config import DATA_DIR, append_timing_row, base_row  # noqa: E402
from common.timing import warm_up_then_measure  # noqa: E402

SQL = """
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


def main() -> None:
    csv_path = DATA_DIR / "taxi_1m.csv"
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    con = duckdb.connect()
    tool_version = duckdb.__version__

    def once():
        return con.execute(SQL, [str(csv_path)]).fetchall()

    measured = warm_up_then_measure(once, n_runs=3)
    for i, (elapsed, rows, status) in enumerate(measured, start=1):
        append_timing_row(
            base_row(
                system="duckdb",
                task="task1",
                condition="aggregation",
                dataset_rows=1_000_000,
                input_format="csv",
                input_path=csv_path,
                run_number=i,
                time_seconds=elapsed,
                status=status,
                tool_version=tool_version,
            )
        )
        print(f"run={i} time={elapsed:.3f}s status={status} n={len(rows) if rows else 0}")
        if rows:
            print("sample:", rows[:5])

    con.close()


if __name__ == "__main__":
    main()

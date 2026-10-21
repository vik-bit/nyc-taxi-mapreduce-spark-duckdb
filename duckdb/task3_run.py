"""DuckDB Task 3 runner."""

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
  z.Borough,
  z.Zone,
  SUM(t.fare_amount) AS total_fare
FROM read_parquet(?) AS t
INNER JOIN read_csv_auto(?, header = true) AS z
  ON t.PULocationID = z.LocationID
WHERE t.fare_amount >= 0
GROUP BY z.Borough, z.Zone
ORDER BY total_fare DESC, z.Zone ASC
LIMIT 5
"""


def main() -> None:
    trips = DATA_DIR / "taxi_2m.parquet"
    zones = DATA_DIR / "taxi_zone_lookup.csv"
    for p in (trips, zones):
        if not p.exists():
            raise FileNotFoundError(p)

    con = duckdb.connect()
    tool_version = duckdb.__version__

    def once():
        return con.execute(SQL, [str(trips), str(zones)]).fetchall()

    measured = warm_up_then_measure(once, n_runs=3)
    for i, (elapsed, rows, status) in enumerate(measured, start=1):
        append_timing_row(
            base_row(
                system="duckdb",
                task="task3",
                condition="join_top5",
                dataset_rows=2_000_000,
                input_format="parquet",
                input_path=trips,
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

    con.close()


if __name__ == "__main__":
    main()

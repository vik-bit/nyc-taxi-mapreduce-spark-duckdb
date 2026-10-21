"""DuckDB Task 2 runner + EXPLAIN output."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.machine_config import DATA_DIR, append_timing_row, base_row  # noqa: E402
from common.timing import warm_up_then_measure  # noqa: E402

QUERY_A = """
SELECT *
FROM read_parquet(?)
WHERE PULocationID = 161
  AND fare_amount > 30.00
"""

QUERY_B = """
SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  trip_distance,
  fare_amount
FROM read_parquet(?)
WHERE PULocationID = 161
  AND fare_amount > 30.00
"""


def main() -> None:
    parquet_path = DATA_DIR / "taxi_2m.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)

    con = duckdb.connect()
    tool_version = duckdb.__version__
    p = str(parquet_path)

    explain_a = con.execute(f"EXPLAIN {QUERY_A}", [p]).fetchall()
    explain_b = con.execute(f"EXPLAIN {QUERY_B}", [p]).fetchall()
    explain_path = Path(__file__).resolve().parent / "task2_explain.txt"
    text = ["=== Query A (all columns) ===", ""]
    text.extend(str(r[1] if len(r) > 1 else r[0]) for r in explain_a)
    text.append("")
    text.append("=== Query B (four columns) ===")
    text.append("")
    text.extend(str(r[1] if len(r) > 1 else r[0]) for r in explain_b)
    # DuckDB EXPLAIN returns (explain_key, explain_value) or a single plan string
    # Rebuild more carefully:
    def format_explain(rows):
        out = []
        for r in rows:
            if len(r) == 2:
                out.append(f"{r[0]}\n{r[1]}")
            else:
                out.append(str(r[0]))
        return "\n".join(out)

    explain_path.write_text(
        "=== Query A (all columns) ===\n"
        + format_explain(explain_a)
        + "\n\n=== Query B (four columns) ===\n"
        + format_explain(explain_b),
        encoding="utf-8",
    )
    print(f"Wrote {explain_path}")

    for condition, sql in [("query_a_all_cols", QUERY_A), ("query_b_projection", QUERY_B)]:
        def once(sql=sql):
            return con.execute(sql, [p]).fetchall()

        measured = warm_up_then_measure(once, n_runs=3)
        for i, (elapsed, rows, status) in enumerate(measured, start=1):
            append_timing_row(
                base_row(
                    system="duckdb",
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
            print(f"{condition} run={i} time={elapsed:.3f}s rows={len(rows) if rows else 0}")

    con.close()


if __name__ == "__main__":
    main()

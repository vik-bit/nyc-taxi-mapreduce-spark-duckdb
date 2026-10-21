"""Download TLC January 2026 yellow taxi Parquet + zone lookup, then prepare samples."""

from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DATA.mkdir(parents=True, exist_ok=True)

PARQUET_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def download(url: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"Already present: {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
        return
    print(f"Downloading {url} ...")
    urllib.request.urlretrieve(url, dest)
    print(f"Saved {dest} ({dest.stat().st_size / 1e6:.1f} MB)")


def prepare() -> None:
    src = DATA / "yellow_tripdata_2026-01.parquet"
    if not src.exists():
        raise FileNotFoundError(src)

    sql_path = ROOT / "data_prep" / "prepare_data.sql"
    sql = sql_path.read_text(encoding="utf-8")
    con = duckdb.connect()
    # Run relative to data/
    con.execute(f"SET file_search_path='{DATA.as_posix()}'")
    # DuckDB may not have file_search_path on all versions — use chdir approach
    con.close()

    import os

    os.chdir(DATA)
    con = duckdb.connect()
    print("Running prepare_data.sql ...")
    con.execute(sql)
    expected = {
        "taxi_100k.parquet": 100_000,
        "taxi_100k.csv": 100_000,
        "taxi_500k.parquet": 500_000,
        "taxi_500k.csv": 500_000,
        "taxi_1m.parquet": 1_000_000,
        "taxi_1m.csv": 1_000_000,
        "taxi_2m.parquet": 2_000_000,
        "taxi_2m.csv": 2_000_000,
    }
    for name, n in expected.items():
        path = DATA / name
        if name.endswith(".parquet"):
            got = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path.as_posix()}')").fetchone()[0]
        else:
            got = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{path.as_posix()}', header=true)").fetchone()[0]
        status = "OK" if got == n else "MISMATCH"
        print(f"  {name}: {got} rows [{status}]")
        if got != n:
            raise RuntimeError(f"Row count mismatch for {name}: {got} != {n}")
    con.close()
    print("Data preparation complete.")


def main() -> None:
    download(PARQUET_URL, DATA / "yellow_tripdata_2026-01.parquet")
    download(ZONES_URL, DATA / "taxi_zone_lookup.csv")
    prepare()


if __name__ == "__main__":
    main()

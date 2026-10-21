# NYC Taxi Analytics: MapReduce vs Spark vs DuckDB

End-to-end comparison of **three data engines** on the same NYC Yellow Taxi workload (TLC public data, up to **2 million** trips):

| Engine | Role in this repo |
|--------|-------------------|
| **MapReduce** (`mrjob`) | Classic mapper → shuffle → reducer aggregation |
| **Apache Spark** | DataFrame API, Parquet, broadcast join, cache |
| **DuckDB** | In-process SQL on CSV/Parquet |

The question is not only “which is fastest on a laptop,” but **which system is appropriate** for the data size, query shape, and hardware.

**Hardware used for reported timings:** Windows laptop, Intel i7-1255U (10C/12T), 16 GB RAM, NVMe SSD, Spark `local[*]`, MapReduce via WSL local runner.

---

## Headline results (median of measured runs)

| Workload | MapReduce | Spark | DuckDB |
|----------|-----------|-------|--------|
| Aggregate 1M-row CSV by pickup zone | ~27 s | ~17 s | **~0.9 s** |
| Filter 2M-row Parquet (all columns) | — | ~3.3 s | **~0.36 s** |
| Same filter, 4-column projection | — | ~2.9 s | **~0.09 s** |
| Join 2M trips + zone lookup, top-5 zones | design only | ~6.6 s | **~0.12 s** |
| Scale aggregation 100k → 2M CSV | ~8 s → ~50 s | ~2.7 s → ~19 s | **~0.4 s → ~0.8 s** |

Spark **explicit cache** on a repeated 1M-row Parquet aggregation did not beat a warm OS page cache on this machine (build cache ~17 s; cached repeats still similar to uncached after warmup). DuckDB stays in a different league for single-node SQL.

Full rows: [`results/timings.csv`](results/timings.csv). Scaling chart: [`results/scaling_plot.png`](results/scaling_plot.png). Write-up: [`report/FINDINGS.md`](report/FINDINGS.md).

---

## What the code does

1. **Aggregation** — `COUNT(*)` and `AVG(fare)` by `PULocationID` (non-null location, fare ≥ 0).
2. **Scan vs projection** — Parquet filter; all columns vs four columns (predicate/projection pushdown).
3. **Join** — trips ⋈ zone lookup, top 5 zones by total fare. Spark uses a **broadcast hash join** (tiny dimension table). MapReduce design: broadcast the lookup in the mapper instead of a reduce-side join of 2M keys — see [`mapreduce/task3_design.txt`](mapreduce/task3_design.txt).
4. **Scaling** — same aggregation at 100k / 500k / 1M / 2M rows.
5. **Repeated query** — Spark uncached vs cached vs DuckDB.

```
TLC Parquet ──► DuckDB sample ──► CSV / Parquet slices
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
     mrjob         Spark         DuckDB
   (shuffle)    (DataFrames)     (SQL)
        └─────────────┼─────────────┘
                      ▼
                 timings.csv
```

---

## Setup

```bash
python -m pip install -r requirements.txt
```

Spark needs a JDK 17+ on `PATH`. On Windows, MapReduce **timed** runs use WSL (`mrjob --runner local`); native Windows local runner hits symlink/`fcntl` limits.

**Data** (not in git — NYC TLC):

```bash
python scripts/download_and_prepare.py
```

Expects files under `data/`: `taxi_{100k,500k,1m,2m}.csv` / `.parquet`, `taxi_zone_lookup.csv`.

---

## Run

```bash
# Aggregation (1M CSV)
python mapreduce/run_timed.py --task task1 --rows 1000000
python spark/task1.py
python duckdb/task1_run.py

# Parquet filter / projection (2M)
python spark/task2.py
python duckdb/task2_run.py

# Join + top-5
python spark/task3.py
python duckdb/task3_run.py

# Scaling 100k → 2M
python mapreduce/run_timed.py --task task4 --rows 100000 500000 1000000 2000000
python scripts/run_task4.py --system both
python scripts/plot_scaling.py

# Repeated aggregation
python spark/task5.py
python duckdb/task5_run.py
```

---

## When to use which engine

| Situation | Fit |
|-----------|-----|
| Laptop / single file, SQL analytics, Parquet | **DuckDB** |
| Multi-stage pipelines, cluster, ML later | **Spark** |
| Already-sharded multi-TB batch, fault-tolerant jobs | **MapReduce / Hadoop-style** |
| “We have 2 GB, should we start a cluster?” | Usually **no** — DuckDB or pandas |

Local `mrjob` time is a **model of MapReduce**, not a YARN cluster benchmark.

---

## Layout

```
common/          shared machine config + timing helpers
data_prep/       DuckDB SQL to slice TLC Parquet
mapreduce/       mrjob job + timed WSL runner
spark/           DataFrame jobs + explain plans
duckdb/          SQL + Python runners
scripts/         download, scale, plot
results/         timings.csv, scaling_plot.png
report/          FINDINGS.md
```

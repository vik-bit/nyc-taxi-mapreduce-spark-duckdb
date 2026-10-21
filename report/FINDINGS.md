# Findings

Measured on one laptop (i7-1255U, 16 GB RAM, NVMe). One warmup + three (or five) recorded runs. Medians below are from `results/timings.csv`.

## Engine choice

DuckDB won every **single-node** SQL-shaped job: CSV aggregation, Parquet filter, projection, and a broadcast-style join to a tiny zone table. Spark paid JVM / session overhead that dominates at 1–2M rows. MapReduce (local `mrjob`) paid shuffle and process startup; it scaled roughly with CSV size (~8 s at 100k to ~50 s at 2M) and is the right *model* for distributed batch, not the right *tool* for this laptop.

Projection on Parquet mattered: DuckDB dropped from ~0.36 s (all columns) to ~0.09 s (four columns). Spark improved as well but stayed in the 2–3 s band.

## Join design

The zone lookup is small. A reduce-side join that shuffles 2M trips by `PULocationID` is correct and wasteful. Replicate the lookup to each mapper (Spark `BroadcastHashJoin` is the same idea). That design is written in `mapreduce/task3_design.txt`; Spark/DuckDB implement the join.

## Cache

On this machine, Spark `cache()` of 1M Parquet rows cost ~17 s to build. Later cached queries were not clearly faster than uncached queries after the OS had already warmed the file. Explicit cache is for **lineage reuse across actions**, not a substitute for “the file is already in RAM.”

## Takeaways

- Match the engine to **data distribution and query shape**, not to a brand name.
- Columnar Parquet + projection/predicate pushdown is the largest format win here.
- Startup cost hides true scalability until the working set is large or already on a cluster.
- Broadcast the small side of a join; do not shuffle the fact table by default.

-- Data preparation for BDA Assignment 1
-- Run from the data/ folder that contains yellow_tripdata_2026-01.parquet
-- Example:
--   cd data
--   duckdb < ../data_prep/prepare_data.sql

COPY (
  SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 100000
) TO 'taxi_100k.parquet' (FORMAT PARQUET);

COPY (
  SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 100000
) TO 'taxi_100k.csv' (FORMAT CSV, HEADER TRUE);

COPY (
  SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 500000
) TO 'taxi_500k.parquet' (FORMAT PARQUET);

COPY (
  SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 500000
) TO 'taxi_500k.csv' (FORMAT CSV, HEADER TRUE);

COPY (
  SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 1000000
) TO 'taxi_1m.parquet' (FORMAT PARQUET);

COPY (
  SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 1000000
) TO 'taxi_1m.csv' (FORMAT CSV, HEADER TRUE);

COPY (
  SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 2000000
) TO 'taxi_2m.parquet' (FORMAT PARQUET);

COPY (
  SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 2000000
) TO 'taxi_2m.csv' (FORMAT CSV, HEADER TRUE);

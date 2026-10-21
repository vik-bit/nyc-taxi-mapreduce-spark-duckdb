-- AI-ASSISTED: A1
-- Task 5: same aggregation as Task 1 on taxi_1m.parquet (run 5 times in one session)

SELECT
  PULocationID,
  COUNT(*) AS trip_count,
  AVG(fare_amount) AS avg_fare
FROM read_parquet('taxi_1m.parquet')
WHERE PULocationID IS NOT NULL
  AND fare_amount IS NOT NULL
  AND fare_amount >= 0
GROUP BY PULocationID;

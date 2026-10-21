-- AI-ASSISTED: A1
-- Task 1: aggregation on taxi_1m.csv
-- Columns: PULocationID, trip_count, avg_fare

SELECT
  PULocationID,
  COUNT(*) AS trip_count,
  AVG(fare_amount) AS avg_fare
FROM read_csv_auto('taxi_1m.csv', header = true)
WHERE PULocationID IS NOT NULL
  AND fare_amount IS NOT NULL
  AND fare_amount >= 0
GROUP BY PULocationID;

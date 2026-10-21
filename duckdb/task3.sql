-- AI-ASSISTED: A1
-- Task 3: join with zone lookup; top 5 zones by total fare
-- Tie-break: Zone name ascending

SELECT
  z.Borough,
  z.Zone,
  SUM(t.fare_amount) AS total_fare
FROM read_parquet('taxi_2m.parquet') AS t
INNER JOIN read_csv_auto('taxi_zone_lookup.csv', header = true) AS z
  ON t.PULocationID = z.LocationID
WHERE t.fare_amount >= 0
GROUP BY z.Borough, z.Zone
ORDER BY total_fare DESC, z.Zone ASC
LIMIT 5;

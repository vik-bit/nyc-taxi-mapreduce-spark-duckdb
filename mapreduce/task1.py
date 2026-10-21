#!/usr/bin/env python
"""
Task 1 MapReduce aggregation (mrjob local runner).

Required computation:
  For every non-null PULocationID, compute trip_count and avg_fare
  where fare_amount is non-null and fare_amount >= 0.

MapReduce design:
  Mapper key:   PULocationID
  Mapper value: fare_amount (float)
  Shuffle:      values with the same PULocationID are grouped for the reducer
  Reducer:      emits (PULocationID, [trip_count, avg_fare])

CSV column indices match DuckDB COPY of TLC yellow taxi Parquet
(PULocationID=7, fare_amount=10). Indices are fixed so every local-runner
mapper process works even when only the first split sees the header row.
"""

from __future__ import annotations

import csv
from io import StringIO

from mrjob.job import MRJob
from mrjob.step import MRStep

# Fixed positions from data/taxi_*.csv header produced by data_prep/prepare_data.sql
PU_IDX = 7
FARE_IDX = 10


class TaxiAgg(MRJob):
    def mapper(self, _, line):
        row = next(csv.reader(StringIO(line)))
        if not row:
            return
        # Skip header (only present in the first input split)
        if row[0] == "VendorID" or row[0] == "PULocationID":
            return
        try:
            pu = row[PU_IDX]
            fare_s = row[FARE_IDX]
            if pu == "" or fare_s == "":
                return
            fare = float(fare_s)
            if fare < 0:
                return
            yield str(pu), fare
        except (ValueError, IndexError):
            return

    def reducer(self, pu_location_id, fares):
        total = 0.0
        count = 0
        for f in fares:
            total += float(f)
            count += 1
        if count == 0:
            return
        yield pu_location_id, [count, total / count]

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]


if __name__ == "__main__":
    TaxiAgg.run()

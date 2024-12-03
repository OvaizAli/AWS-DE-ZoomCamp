CREATE OR REPLACE TABLE `nycTaxi.TimeDimension` (
  date DATE,
  day_of_week INT64,
  month INT64,
  year INT64,
  holiday_flag STRING
);


CREATE OR REPLACE TABLE `nycTaxi.LocationDimension` (
  location_id INT64,
  PULocationID INT64,
  DOLocationID INT64
);


CREATE OR REPLACE TABLE `nycTaxi.LocationAggregatedFact` (
  PULocationID INT64,
  total_trips INT64,
  total_revenue FLOAT64,
  total_fare FLOAT64,
  avg_trip_distance FLOAT64,
  avg_fare_per_trip FLOAT64
);


CREATE OR REPLACE TABLE `nycTaxi.TotalAggregatedFact` (
  year INT64,
  month INT64,
  total_trips INT64,
  total_revenue FLOAT64,
  total_fare FLOAT64,
  total_tips FLOAT64,
  total_tolls FLOAT64,
  total_congestion_surcharge FLOAT64,
  total_airport_fee FLOAT64,
  avg_trip_distance FLOAT64,
  avg_fare_per_trip FLOAT64
);
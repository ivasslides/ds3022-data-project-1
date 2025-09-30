
  
  create view "emissions"."main"."yellow_transform__dbt_tmp" as (
    -- buidling table 
WITH yellow_trips AS (
    SELECT * FROM yellow_taxi_trips
),

-- upload emissions table
emissions AS (
    SELECT * FROM "emissions"."main"."vehicle_emissions"
    WHERE vehicle_type = 'yellow_taxi'
)

-- transforming part 
SELECT 
     y.passenger_count, y.trip_distance, y.tpep_pickup_datetime, y.tpep_dropoff_datetime,

     -- calculating total co2 output 
    (e.co2_grams_per_mile * y.trip_distance) / 1000.0 AS trip_co2_kgs,

    -- calculating avg mph
    (y.trip_distance / (y.trip_seconds / 3600.0))  AS avg_mph,

    -- extract hour 
    EXTRACT(HOUR FROM y.tpep_pickup_datetime) AS hour_of_day,

    -- extract day of week
    EXTRACT(DOW FROM y.tpep_pickup_datetime) AS day_of_week,

    -- extract week of year 
    EXTRACT(WEEK FROM y.tpep_pickup_datetime) AS week_of_year,

    -- extract month of year
    EXTRACT(MONTH FROM y.tpep_pickup_datetime) AS month_of_year,

-- reference original table
FROM yellow_trips y

-- join from emissions table
CROSS JOIN emissions e
  );

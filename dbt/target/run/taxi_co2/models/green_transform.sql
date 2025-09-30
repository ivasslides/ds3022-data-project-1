
  
  create view "emissions"."main"."green_transform__dbt_tmp" as (
    -- building table
WITH green_trips AS (
    SELECT * FROM green_taxi_trips
),

-- upload emissions table
emissions AS (
    SELECT * FROM "emissions"."main"."vehicle_emissions"
    WHERE vehicle_type = 'green_taxi'
)

-- transforming part 
SELECT 
    g.passenger_count, g.trip_distance, g.lpep_pickup_datetime, g.lpep_dropoff_datetime,

    -- calculating total co2 output
    (e.co2_grams_per_mile * g.trip_distance) / 1000.0 AS trip_co2_kgs, 

    -- calculating avg mph
    (g.trip_distance / (g.trip_seconds / 3600.0) ) AS avg_mph,

    -- extract hour 
    EXTRACT(HOUR FROM g.lpep_pickup_datetime) AS hour_of_day,

    -- extract day of week
    EXTRACT(DOW FROM g.lpep_pickup_datetime) AS day_of_week,

    -- extract week of year 
    EXTRACT(WEEK FROM g.lpep_pickup_datetime) AS week_of_year,

    -- extract month of year
    EXTRACT(MONTH FROM g.lpep_pickup_datetime) AS month_of_year,

-- reference original table
FROM green_trips g 

-- join from emissions table
CROSS JOIN emissions e
  );

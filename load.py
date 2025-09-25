import duckdb
import os
import logging
import time 

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        # loading vehicle emissions csv file
        con.execute(f"""
            DROP TABLE IF EXISTS vehicle_emissions;
            CREATE TABLE vehicle_emissions AS
            SELECT * FROM read_csv('https://raw.githubusercontent.com/ivasslides/ds3022-data-project-1/refs/heads/main/data/vehicle_emissions.csv');
        """)
        logging.info("Successfully loaded vehicle emissions file")
    

        con = duckdb.connect(database='taxi_trips.duckdb', read_only=False)

        # creating base url for yellow taxi trips
        base = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

        # create list of urls for 2015 - 2024, using base url
        urls = [
            base.format(year = year, month = month)
            for year in range(2015, 2025) # 2025 is exclusive
            for month in range(1, 13) # 13 is exclusive 
            ]

        # dropping and creating main table
        con.execute(f"""
            DROP TABLE IF EXISTS yellow_taxi_trips;
            CREATE TABLE yellow_taxi_trips AS
            SELECT VendorId, passenger_count, trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime
            FROM read_parquet('{urls[0]}');
        """)
        logger.info("Dropped yellow_taxi_trips if exists")

        # looping through urls and loading into table
        for url in urls: 
            con.execute(f"""
                INSERT INTO yellow_taxi_trips
                SELECT VendorId, passenger_count, trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime
                FROM read_parquet('{url}');
            """)
            time.sleep(60)
        logging.info("Successfully loaded all tables for yellow taxis trips")

        # creating base url for green taxi trips
        green_base = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"

        # creating list of urls for 2015 - 2024, using base url
        green_urls = [
            green_base.format(year = year, month = month)
            for year in range(2015, 2025) # 2025 is exclusive
            for month in range(1, 13) # 13 is exclusive
        ]

        # creating main table if doesnt exist 
        con.execute("""
            DROP TABLE IF EXISTS green_taxi_trips;
            CREATE TABLE green_taxi_trips AS
            SELECT VendorId, passenger_count, trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime
            FROM read_parquet('{green_urls[0]}');
            """) 
        
        logger.info("Dropped green_taxi_trips if exists")

        # looping through urls and loading into table
        for url in green_urls:
            con.execute(f"""
                INSERT INTO green_taxi_trips
                SELECT VendorId, passenger_count, trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime
                FROM read_parquet('{url}');
            """)
            time.sleep(60)

        logging.info("Successfully loaded all tables for green taxis trips")        
    
    
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()
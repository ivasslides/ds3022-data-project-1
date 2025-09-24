import duckdb
import logging

# creating connection
con = duckdb.connect("taxi_trips.duckdb")

##### creating logging function
logging.basicConfig(
    filename="app.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level="INFO"
)

##### load yellow taxi trips from 2015 - 2024 
# creating base url 
base = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"

# create list of urls for 2015 - 2024, using base url
urls = [
    base.format(year = year, month = month)
    for year in range(2015, 2025) # 2025 is exclusive
    for month in range(1, 13) # 13 is exclusive
]

# creating main table if doesnt exist 
con.execute("""
    DROP TABLE IF EXISTS yellow_taxi_trips;
            CREATE TABLE yellow_taxi_trips AS
            SELECT * FROM read_parquet('{urls[0]}');
    """) 

# looping through urls and loading into table
try:
    for url in urls:
        con.execute(f"""
            INSERT INTO yellow_taxi_trips
            SELECT * FROM read_parquet('{url}');
        """)
    logging.info("Successfully loaded all tables for yellow taxis trips")
except Exception as e:
    logging.error(f"Failed to load: {e}")
    


##### load green taxi trips from 2015 - 2024 
# creating base url 
green_base = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet"

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
            SELECT * FROM read_parquet('{green_urls[0]}');
    """) 

# looping through urls and loading into table
try:
    for url in green_urls:
        con.execute(f"""
            INSERT INTO green_taxi_trips
            SELECT * FROM read_parquet('{url}');
        """)
    logging.info("Successfully loaded all tables for green taxis trips")
except Exception as e:
    logging.error(f"Failed to load: {e}")

##### loading vehicle_emissions csv file 
try:
    con.execute(f"""
        DROP TABLE IF EXISTS vehicle_emissions;
            CREATE TABLE behicle_emissions AS
            SELECT * FROM read_csv('https://raw.githubusercontent.com/ivasslides/ds3022-data-project-1/refs/heads/main/data/vehicle_emissions.csv');
    """)
    logging.info("Successfully loaded vehicle emissions file")

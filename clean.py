import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)


def cleaning():

    con = None

    try:
        # Connect to local DuckDB
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # remove any duplicate trips
        con.execute(f"""
            CREATE TABLE yellow_clean AS
            SELECT DISTINCT * FROM yellow_taxi_trips; 
            DROP TABLE yellow_taxi_trips;
            ALTER TABLE yellow_clean RENAME TO yellow_taxi_trips; 
        """)
        con.execute(f"""
            CREATE TABLE green_clean AS
            SELECT DISTINCT * FROM green_taxi_trips; 
            DROP TABLE green_taxi_trips;
            ALTER TABLE green_clean RENAME TO green_taxi_trips;
        """)
        logging.info("Duplicates complete")
        # testing
        dup_yellow = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT passenger_count, trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime, COUNT(*) as occurrences 
                FROM yellow_taxi_trips
                GROUP BY passenger_count, trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime
                HAVING COUNT(*) > 1)
        """).fetchone()[0]
        dup_green = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT passenger_count, trip_distance, lpep_pickup_datetime, lpep_dropoff_datetime, COUNT(*) as occurrences 
                FROM green_taxi_trips
                GROUP BY passenger_count, trip_distance, lpep_pickup_datetime, lpep_dropoff_datetime
                HAVING COUNT(*) > 1)
        """).fetchone()[0]

        if dup_yellow == 0 and dup_green == 0:
            logging.info("Yay, no duplicate trips.")
        else:
            logging.info("There is a duplicate trip remaining.")

        # remove any trips with 0 passengers
        con.execute(f"""
            DELETE FROM yellow_taxi_trips
            WHERE passenger_count = 0;

            DELETE FROM green_taxi_trips
            WHERE passenger_count = 0; 
        """)
        logging.info("Passengers complete.")
        # testing
        pass_yellow = con.execute(f"""
            SELECT MIN(passenger_count) FROM yellow_taxi_trips;""").fetchone()[0]
        pass_green = con.execute(f"""
            SELECT MIN(passenger_count) FROM green_taxi_trips;""").fetchone()[0] 
            
        if pass_yellow != 0 and pass_green!= 0:
            logging.info("Yay, all trips have at least 1 passenger.")
        else:
            logging.info("There is a trip with no passengers remaining.")

        # remove any trips 0 miles in length
        con.execute(f"""
            DELETE FROM yellow_taxi_trips
            WHERE trip_distance = 0;

            DELETE FROM green_taxi_trips
            WHERE trip_distance = 0;            
        """)
        logging.info("Distance complete")
        # testing
        min_length_yellow = con.execute(f"""
            SELECT MIN(trip_distance) FROM yellow_taxi_trips;""").fetchone()[0]
        min_length_green = con.execute(f"""
            SELECT MIN(trip_distance) FROM green_taxi_trips;""").fetchone()[0]
            
        if min_length_yellow != 0 and min_length_green != 0:
            logging.info("Yay, all trips are longer than 0 miles.")
        else:
            logging.info("There is a trip that is 0 miles long remaining.")

        # remove any trips longer than 100 miles in length
        con.execute(f"""
            DELETE FROM yellow_taxi_trips
            WHERE trip_distance > 100;

            DELETE FROM green_taxi_trips
            WHERE trip_distance > 100;            
        """)
        logging.info("Distance pt2 complete")
        # testing 
        max_length_yellow = con.execute(f"""
            SELECT MAX(trip_distance) FROM yellow_taxi_trips;""").fetchone()[0]
        max_length_green = con.execute(f"""
            SELECT MAX(trip_distance) FROM green_taxi_trips;""").fetchone()[0]
            
        if max_length_yellow <= 100 and max_length_green <= 100:
            logging.info("Yay, all trips are shorter than 100 miles.")
        else:
            logging.info("There is a trip longer than 100 miles remaining.")

        # remove any trips lasting more than 1 day in length (86400 seconds)
        con.execute(f"""
        -- calculate trip time yellow
            -- ALTER TABLE yellow_taxi_trips 
            -- ADD COLUMN trip_seconds INTEGER;

            UPDATE yellow_taxi_trips 
            SET trip_seconds = date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime);
        -- remove when >86400
            DELETE FROM yellow_taxi_trips
            WHERE trip_seconds > 86400;

        -- calculate trip time green
            --ALTER TABLE green_taxi_trips 
            --ADD COLUMN trip_seconds INTEGER;

            UPDATE green_taxi_trips 
            SET trip_seconds = date_diff('second', lpep_pickup_datetime, lpep_dropoff_datetime);
        -- remove when >86400
            DELETE FROM green_taxi_trips
            WHERE trip_seconds > 86400;            
        """)
        logging.info("Time complete")
        # testing 
        max_time_yellow = con.execute(f"""
            SELECT MAX(trip_seconds) FROM yellow_taxi_trips;""").fetchone()[0]
        max_time_green = con.execute(f"""
            SELECT MAX(trip_seconds) FROM green_taxi_trips;""").fetchone()[0]

        if max_time_yellow <= 86400 and max_time_green <= 86400:
            logging.info("Yay, all trips are shorter than 1 day.")
        else:
            logging.info("There is a trip longer than 1 day remaining.")


    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    cleaning()


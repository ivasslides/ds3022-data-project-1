import duckdb
import logging

def cleaning():

    con = None

    try:
        # Connect to local DuckDB
        con = duckdb.connect(database='taxi_trips.duckdb', read_only=False)

        # remove any duplicate trips
        con.execute(f"""
            CREATE TABLE yellow_clean AS
            SELECT DISTINCT VectorId FROM yellow_taxi_trips; 
            DROP TABLE yellow_taxi_trips;
            ALTER TABLE yellow_clean RENAME TO yellow_taxi_trips; 

            CREATE TABLE green_clean AS
            SELECT DISTINCT VectorId FROM green_taxi_trips; 
            DROP TABLE green_taxi_trips;
            ALTER TABLE green_clean RENAME TO green_taxi_trips;
        """)
        # testing
        dup_yellow = con.execute("""
            SELECT COUNT(*) FROM
                (SELECT VectorID, COUNT(*) AS occurrences
                FROM yellow_taxi_trips
                GROUP BY VectorId
                HAVING COUNT(*) > 1)
        """)
        dup_green = con.execute("""
            SELECT COUNT(*) FROM
                (SELECT VectorID, COUNT(*) AS occurrences
                FROM green_taxi_trips
                GROUP BY VectorId
                HAVING COUNT(*) > 1)
        """)

        if dup_yellow.fetchone()[0] == 0 and dup_green.fetchone()[0] == 0:
            logging.info("No duplicate trips.")
        else:
            logging.info("There is a duplicate trip remaining.")

        # remove any trips with 0 passengers
        con.execute(f"""
            DELETE FROM yellow_taxi_trips
            WHERE passenger_count = 0;

            DELETE FROM green_taxi_trips
            WHERE passenger_count = 0; 
        """)
        # testing
        pass_yellow = con.execute(f"""
            SELECT MIN(passenger_count) FROM yellow_taxi_trips;""")
        pass_green = con.execute(f"""
            SELECT MIN(passenger_count) FROM green_taxi_trips;""")
            
        if pass_yellow.fetchone()[0] != 0 and pass_green.fetchone()[0] != 0:
            logging.info("All trips have at least 1 passenger.")
        else:
            logging.info("There is a trip with no passengers remaining.")

        # remove any trips 0 miles in length
        con.execute(f"""
            DELETE FROM yellow_taxi_trips
            WHERE trip_distance = 0;

            DELETE FROM green_taxi_trips
            WHERE trip_distance = 0;            
        """)
        # testing
        min_length_yellow = con.execute(f"""
            SELECT MIN(trip_distance) FROM yellow_taxi_trips;""")
        min_length_green = con.execute(f"""
            SELECT MIN(trip_distance) FROM green_taxi_trips;""")
            
        if min_length_yellow.fetchone()[0] != 0 and min_length_green.fetchone()[0] != 0:
            logging.info("All trips are longer than 0 miles.")
        else:
            logging.info("There is a trip that is 0 miles long remaining.")

        # remove any trips longer than 100 miles in length
        con.execute(f"""
            DELETE FROM yellow_taxi_trips
            WHERE trip_distance > 100;

            DELETE FROM green_taxi_trips
            WHERE trip_distance > 100;            
        """)
        # testing 
        max_length_yellow = con.execute(f"""
            SELECT MAX(trip_distance) FROM yellow_taxi_trips;""")
        max_length_green = con.execute(f"""
            SELECT MAX(trip_distance) FROM green_taxi_trips;""")
            
        if max_length_yellow.fetchone()[0] <= 100 and max_length_green.fetchone()[0] <= 100:
            logging.info("All trips are shorter than 100 miles.")
        else:
            logging.info("There is a trip longer than 100 miles remaining.")

        # remove any trips lasting more than 1 day in length (86400 seconds)
        con.execute(f"""
        -- calculate trip time yellow
        SELECT tpep_pickup_datetime, tpep_dropoff_datetime, 
            DATE_DIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime)
            AS trip_seconds
        FROM yellow_taxi_trips;
        -- remove when >86400
        DELETE FROM yellow_taxi_trips
        WHERE trip_seconds > 86400;

        -- calculate trip time green
        SELECT tpep_pickup_datetime, tpep_dropoff_datetime, 
            DATE_DIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime)
            AS trip_seconds
        FROM green_taxi_trips;
        -- remove when >86400
        DELETE FROM green_taxi_trips
        WHERE trip_seconds > 86400;            
        """)
        # testing 
        max_time_yellow = con.execute(f"""
            SELECT MAX(trip_seconds) FROM yellow_taxi_trips;""")
        max_time_green = con.execute(f"""
            SELECT MAX(trip_seconds) FROM green_taxi_trips;""")

        if max_time_yellow.fetchone()[0] <= 86400 and max_time_green.fetchone()[0] <= 86400:
            logging.info("All trips are shorter than 1 day.")
        else:
            logging.info("There is a trip longer than 1 day remaining.")


    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    cleaning()


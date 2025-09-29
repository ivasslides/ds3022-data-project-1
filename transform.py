import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)


def transforming():

    con = None

    try:
        # Connect to local DuckDB
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # total yellow co2 output 
        con.execute("""
           ALTER TABLE yellow_taxi_trips 
            ADD COLUMN trip_co2_kgss DOUBLE;

            UPDATE yellow_taxi_trips
            SET trip_co2_kgss = (trip_distance * vehicle_emissions.co2_grams_per_mile) / 1000.0
                FROM vehicle_emissions
                WHERE vehicle_emissions.vehicle_type = 'yellow_taxi';
        """)

         # total green co2 output 
        con.execute("""
           ALTER TABLE green_taxi_trips 
           ADD COLUMN trip_co2_kgss DOUBLE;

            UPDATE green_taxi_trips
            SET trip_co2_kgss = (trip_distance * vehicle_emissions.co2_grams_per_mile) / 1000.0
                FROM vehicle_emissions
                WHERE vehicle_emissions.vehicle_type = 'green_taxi';
        """)

        logging.info("Total co2 output complete")

         # avg mph yellow 
        con.execute("""
            ALTER TABLE yellow_taxi_trips 
            ADD COLUMN avg_mph DOUBLE;

            UPDATE yellow_taxi_trips
            SET avg_mph = (trip_distance / (trip_seconds / 3600.0) );
        """)

        # avg mph green 
        con.execute("""
            ALTER TABLE green_taxi_trips 
            ADD COLUMN avg_mph DOUBLE;

            UPDATE green_taxi_trips
            SET avg_mph = (trip_distance / (trip_seconds / 3600.0) );
        """)

        logging.info("Avg mpg complete")

        # extract hour yellow 
        con.execute("""
            ALTER TABLE yellow_taxi_trips 
            ADD COLUMN hour_of_day INTEGER;

            UPDATE yellow_taxi_trips
            SET hour_of_day = EXTRACT(HOUR FROM tpep_pickup_datetime);
        """)
        
        # extract hour green 
        con.execute("""
            ALTER TABLE green_taxi_trips 
            ADD COLUMN hour_of_day INTEGER;

            UPDATE green_taxi_trips
            SET hour_of_day = EXTRACT(HOUR FROM lpep_pickup_datetime);
        """)

        logging.info("Hour of day complete")

        # extract day of week yellow 
        con.execute("""
            ALTER TABLE yellow_taxi_trips 
            ADD COLUMN day_of_week INTEGER;

            UPDATE yellow_taxi_trips
            SET day_of_week = EXTRACT(DOW FROM tpep_pickup_datetime);
        """)

        # extract day of week green 
        con.execute("""
            ALTER TABLE green_taxi_trips 
            ADD COLUMN day_of_week INTEGER;

            UPDATE green_taxi_trips
            SET day_of_week = EXTRACT(DOW FROM lpep_pickup_datetime);
        """)

        logging.info("Day of week complete")

        # extract week of year yellow 
        con.execute("""
            ALTER TABLE yellow_taxi_trips 
            ADD COLUMN week_of_year INTEGER;

            UPDATE yellow_taxi_trips
            SET week_of_year = EXTRACT(WEEK FROM tpep_pickup_datetime);
        """)

        # extract week of year green 
        con.execute("""
            ALTER TABLE green_taxi_trips 
            ADD COLUMN week_of_year INTEGER;

            UPDATE green_taxi_trips
            SET week_of_year = EXTRACT(WEEK FROM lpep_pickup_datetime);
        """)

        logging.info("Week of year complete")

        # extract month of year yellow 
        con.execute("""
            ALTER TABLE yellow_taxi_trips 
            ADD COLUMN month_of_year INTEGER;

            UPDATE yellow_taxi_trips
            SET month_of_year = EXTRACT(MONTH FROM tpep_pickup_datetime);
        """)

        # extract month of year green 
        con.execute("""
            ALTER TABLE green_taxi_trips 
            ADD COLUMN month_of_year INTEGER;

            UPDATE green_taxi_trips
            SET month_of_year = EXTRACT(MONTH FROM lpep_pickup_datetime);
        """)

        logging.info("Month of year complete")

        
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    transforming()


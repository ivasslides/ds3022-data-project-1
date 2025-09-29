import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)


def analyze():

    con = None

    try:
        # Connect to local DuckDB
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # single largest carbon producing trip of the year yellow
        ylargest_cpt = con.execute("""
            SELECT * FROM yellow_taxi_trips
            ORDER BY trip_co2_kgss DESC
            LIMIT 1;
        """).fetchone()
        logging.info(f"The single largest carbon producing trip of the year for all yellow taxi trips was {ylargest_cpt}")

        # single largest carbon producing trip of the year green
        glargest_cpt = con.execute("""
            SELECT * FROM green_taxi_trips
            ORDER BY trip_co2_kgss DESC
            LIMIT 1;
        """).fetchone()
        logging.info(f"The single largest carbon producing trip of the year for all green taxi trips was {glargest_cpt}")

        # most carbon heavy/light hours of the day yellow
        yheaviest_hour = con.execute("""
            SELECT hour_of_day
            FROM yellow_taxi_trips
                GROUP BY hour_of_day
                ORDER BY AVG(trip_co2_kgss) DESC
            LIMIT 1;
        """).fetchone()[0]
        ylightest_hour = con.execute("""
            SELECT hour_of_day
            FROM yellow_taxi_trips
                GROUP BY hour_of_day
                ORDER BY AVG(trip_co2_kgss) ASC
            LIMIT 1;
        """).fetchone()[0]
        logging.info(f"On average, the most carbon heavy hour of the day for yellow taxi trips is {yheaviest_hour} and the most carbon light hour of the day is {ylightest_hour}.")

        # most carbon heavy/light hours of the day green
        gheaviest_hour = con.execute("""
            SELECT hour_of_day
            FROM green_taxi_trips
                GROUP BY hour_of_day
                ORDER BY AVG(trip_co2_kgss) DESC
            LIMIT 1;
        """).fetchone()[0]
        glightest_hour = con.execute("""
            SELECT hour_of_day
            FROM green_taxi_trips
                GROUP BY hour_of_day
                ORDER BY AVG(trip_co2_kgss) ASC
            LIMIT 1;
        """).fetchone()[0]
        logging.info(f"On average, the most carbon heavy hour of the day for green taxi trips is {gheaviest_hour} and the most carbon light hour of the day is {glightest_hour}.")

        # most carbon heavy/light days of the week yellow
        yheaviest_day = con.execute("""
            SELECT strftime(tpep_pickup_datetime, '%A') AS day_of_week
            FROM yellow_taxi_trips
                GROUP BY strftime(tpep_pickup_datetime, '%A')
                ORDER BY AVG(trip_co2_kgss) DESC
            LIMIT 1;
        """).fetchone()[0]
        ylightest_day = con.execute("""
            SELECT strftime(tpep_pickup_datetime, '%A') AS day_of_week
            FROM yellow_taxi_trips
                GROUP BY strftime(tpep_pickup_datetime, '%A')
                ORDER BY AVG(trip_co2_kgss) ASC
            LIMIT 1;
        """).fetchone()[0]

        logging.info(f"On average, the most carbon heay day of the week for yellow taxi trips is {yheaviest_day} and the most carbon light day of the week is {ylightest_day}.")

        # most carbon heavy/light days of the week green
        gheaviest_day = con.execute("""
            SELECT strftime(lpep_pickup_datetime, '%A') AS day_of_week
            FROM green_taxi_trips
                GROUP BY strftime(lpep_pickup_datetime, '%A')
                ORDER BY AVG(trip_co2_kgss) DESC
            LIMIT 1;
        """).fetchone()[0]
        glightest_day = con.execute("""
            SELECT strftime(lpep_pickup_datetime, '%A') AS day_of_week
            FROM green_taxi_trips
                GROUP BY strftime(lpep_pickup_datetime, '%A')
                ORDER BY AVG(trip_co2_kgss) ASC
            LIMIT 1;
        """).fetchone()[0]
        logging.info(f"On average, the most carbon heavy day of the week for green taxi trips is {gheaviest_day} and the most carbon light day of the week is {glightest_day}.")

        # most carbon heavy/light week of the year yellow
        yheaviest_week = con.execute("""
            SELECT week_of_year
            FROM yellow_taxi_trips
                GROUP BY week_of_year
                ORDER BY AVG(trip_co2_kgss) DESC
            LIMIT 1;
        """).fetchone()[0]
        ylightest_week = con.execute("""
            SELECT week_of_year
            FROM yellow_taxi_trips
                GROUP BY week_of_year
                ORDER BY AVG(trip_co2_kgss) ASC
            LIMIT 1;
        """).fetchone()[0]
        logging.info(f"On average, the most carbon heavy week of the year for yellow taxi trips is {yheaviest_week} and the most carbon light week of the year is {ylightest_week}.")

        # most carbon heavy/light week of the year green
        gheaviest_week = con.execute("""
            SELECT week_of_year
            FROM green_taxi_trips
                GROUP BY week_of_year
                ORDER BY AVG(trip_co2_kgss) DESC
            LIMIT 1;
        """).fetchone()[0]
        glightest_week = con.execute("""
            SELECT week_of_year
            FROM green_taxi_trips
                GROUP BY week_of_year
                ORDER BY AVG(trip_co2_kgss) ASC
            LIMIT 1;
        """).fetchone()[0]
        logging.info(f"On average, the most carbon heavy week of the year for green taxi trips is {gheaviest_week} and the most carbon light week of the year is {glightest_week}.")

        # most carbon heavy/light month of the year yellow
        yheaviest_month = con.execute("""
            SELECT strftime(tpep_pickup_datetime, '%B') 
            FROM yellow_taxi_trips
                GROUP BY strftime(tpep_pickup_datetime, '%B') 
                ORDER BY AVG(trip_co2_kgss) DESC
            LIMIT 1;
        """).fetchone()[0]
        ylightest_month = con.execute("""
            SELECT strftime(tpep_pickup_datetime, '%B') 
            FROM yellow_taxi_trips
                GROUP BY strftime(tpep_pickup_datetime, '%B') 
                ORDER BY AVG(trip_co2_kgss) ASC
            LIMIT 1;
        """).fetchone()[0]
        logging.info(f"On average, the most carbon heavy month of the year for yellow taxi trips is {yheaviest_month} and the most carbon light month of the year is {ylightest_month}.")

        # most carbon heavy/light month of the year green
        gheaviest_month = con.execute("""
            SELECT strftime(lpep_pickup_datetime, '%B')
            FROM green_taxi_trips
                GROUP BY strftime(lpep_pickup_datetime, '%B') 
                ORDER BY AVG(trip_co2_kgss) DESC
            LIMIT 1;
        """).fetchone()[0]
        glightest_month = con.execute("""
            SELECT strftime(lpep_pickup_datetime, '%B')
            FROM green_taxi_trips
                GROUP BY strftime(lpep_pickup_datetime, '%B') 
                ORDER BY AVG(trip_co2_kgss) ASC
            LIMIT 1;
        """).fetchone()[0]
        logging.info(f"On average, the most carbon heavy month of the year for green taxi trips is {gheaviest_month} and the most carbon light month of the year is {glightest_month}.")

        # plotting month and co2 totals yellow
        import pandas as pd 
        import matplotlib.pyplot as plt
        # yellow grouping total co2 output by month and converting to dataframe
        yellow_monthly = con.execute("""
            SELECT strftime(tpep_pickup_datetime, '%B') AS month, 
                CAST(strftime(tpep_pickup_datetime, '%m') AS INTEGER) AS month_num,
                SUM(trip_co2_kgss) AS total_co2
            FROM yellow_taxi_trips
                GROUP BY month, month_num
                ORDER BY month_num
        """).df() 
        # green grouping total co2 output by month and converting to dataframe
        green_monthly = con.execute("""
            SELECT strftime(lpep_pickup_datetime, '%B') AS month,
                CAST(strftime(lpep_pickup_datetime, '%m') AS INTEGER) AS month_num,
                SUM(trip_co2_kgss) AS total_co2
            FROM green_taxi_trips
                GROUP BY month, month_num
                ORDER BY month_num
        """).df()
        # plotting month on x axis and summed co2 outputs on y axis
        plt.figure(figsize=(12,6))
        plt.plot(yellow_monthly['month'], yellow_monthly['total_co2'], color='yellow', label='Yellow Taxis')
        plt.plot(green_monthly['month'], green_monthly['total_co2'], color='green', label='Green Taxis')
        # making it look pretty
        plt.title("Monthly CO2 Totals by Taxi Type in 2024")
        plt.xlabel("Month")
        plt.ylabel("CO2 Emissions (kgs)")
        plt.legend()
        plt.show()
        # saving plot as png image to this repo
        plt.savefig("monthly_co2_totals.png", dpi=300)

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    analyze()

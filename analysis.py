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
            SELECT * FROM yellow_transform
            ORDER BY trip_co2_kgs DESC
            LIMIT 1;
        """).fetchone()
        logging.info(f"The single largest carbon producing trip of the year for all yellow taxi trips was {ylargest_cpt}")

        # single largest carbon producing trip of the year green
        glargest_cpt = con.execute("""
            SELECT * FROM green_transform
            ORDER BY trip_co2_kgs DESC
            LIMIT 1;
        """).fetchone()
        logging.info(f"The single largest carbon producing trip of the year for all green taxi trips was {glargest_cpt}")

        # most carbon heavy/light hours of the day yellow
        yheaviest_hour = con.execute("""
            SELECT hour_of_day
            FROM yellow_transform
                GROUP BY hour_of_day
                ORDER BY AVG(trip_co2_kgs) DESC
            LIMIT 1;
        """).fetchone()[0]
        ylightest_hour = con.execute("""
            SELECT hour_of_day
            FROM yellow_transform
                GROUP BY hour_of_day
                ORDER BY AVG(trip_co2_kgs) ASC
            LIMIT 1;
        """).fetchone()[0]
        logging.info(f"On average, the most carbon heavy hour of the day for yellow taxi trips is {yheaviest_hour} and the most carbon light hour of the day is {ylightest_hour}.")

        # most carbon heavy/light hours of the day green
        gheaviest_hour = con.execute("""
            SELECT hour_of_day
            FROM green_transform
                GROUP BY hour_of_day
                ORDER BY AVG(trip_co2_kgs) DESC
            LIMIT 1;
        """).fetchone()[0]
        glightest_hour = con.execute("""
            SELECT hour_of_day
            FROM green_transform
                GROUP BY hour_of_day
                ORDER BY AVG(trip_co2_kgs) ASC
            LIMIT 1;
        """).fetchone()[0]
        logging.info(f"On average, the most carbon heavy hour of the day for green taxi trips is {gheaviest_hour} and the most carbon light hour of the day is {glightest_hour}.")

        # most carbon heavy/light day of the week yellow
        yheaviest_day = con.execute("""
            SELECT strftime(tpep_pickup_datetime, '%A') AS day_of_week
            FROM yellow_transform
                GROUP BY strftime(tpep_pickup_datetime, '%A')
                ORDER BY AVG(trip_co2_kgs) DESC
            LIMIT 1;
        """).fetchone()[0]
        ylightest_day = con.execute("""
            SELECT strftime(tpep_pickup_datetime, '%A') AS day_of_week
            FROM yellow_transform
                GROUP BY strftime(tpep_pickup_datetime, '%A')
                ORDER BY AVG(trip_co2_kgs) ASC
            LIMIT 1;
        """).fetchone()[0]

        logging.info(f"On average, the most carbon heay day of the week for yellow taxi trips is {yheaviest_day} and the most carbon light day of the week is {ylightest_day}.")

        # most carbon heavy/light day of the week green
        gheaviest_day = con.execute("""
            SELECT strftime(lpep_pickup_datetime, '%A') AS day_of_week
            FROM green_transform
                GROUP BY strftime(lpep_pickup_datetime, '%A')
                ORDER BY AVG(trip_co2_kgs) DESC
            LIMIT 1;
        """).fetchone()[0]
        glightest_day = con.execute("""
            SELECT strftime(lpep_pickup_datetime, '%A') AS day_of_week
            FROM green_transform
                GROUP BY strftime(lpep_pickup_datetime, '%A')
                ORDER BY AVG(trip_co2_kgs) ASC
            LIMIT 1;
        """).fetchone()[0]
        logging.info(f"On average, the most carbon heavy day of the week for green taxi trips is {gheaviest_day} and the most carbon light day of the week is {glightest_day}.")

        # most carbon heavy/light week of the year yellow
        yheaviest_week = con.execute("""
            SELECT week_of_year
            FROM yellow_transform
                GROUP BY week_of_year
                ORDER BY AVG(trip_co2_kgs) DESC
            LIMIT 1;
        """).fetchone()[0]
        ylightest_week = con.execute("""
            SELECT week_of_year
            FROM yellow_transform
                GROUP BY week_of_year
                ORDER BY AVG(trip_co2_kgs) ASC
            LIMIT 1;
        """).fetchone()[0]
        logging.info(f"On average, the most carbon heavy week of the year for yellow taxi trips is {yheaviest_week} and the most carbon light week of the year is {ylightest_week}.")

        # most carbon heavy/light week of the year green
        gheaviest_week = con.execute("""
            SELECT week_of_year
            FROM green_transform
                GROUP BY week_of_year
                ORDER BY AVG(trip_co2_kgs) DESC
            LIMIT 1;
        """).fetchone()[0]
        glightest_week = con.execute("""
            SELECT week_of_year
            FROM green_transform
                GROUP BY week_of_year
                ORDER BY AVG(trip_co2_kgs) ASC
            LIMIT 1;
        """).fetchone()[0]
        logging.info(f"On average, the most carbon heavy week of the year for green taxi trips is {gheaviest_week} and the most carbon light week of the year is {glightest_week}.")

        # most carbon heavy/light month of the year yellow
        yheaviest_month = con.execute("""
            SELECT strftime(tpep_pickup_datetime, '%B') 
            FROM yellow_transform
                GROUP BY strftime(tpep_pickup_datetime, '%B') 
                ORDER BY AVG(trip_co2_kgs) DESC
            LIMIT 1;
        """).fetchone()[0]
        ylightest_month = con.execute("""
            SELECT strftime(tpep_pickup_datetime, '%B') 
            FROM yellow_transform
                GROUP BY strftime(tpep_pickup_datetime, '%B') 
                ORDER BY AVG(trip_co2_kgs) ASC
            LIMIT 1;
        """).fetchone()[0]
        logging.info(f"On average, the most carbon heavy month of the year for yellow taxi trips is {yheaviest_month} and the most carbon light month of the year is {ylightest_month}.")

        # most carbon heavy/light month of the year green
        gheaviest_month = con.execute("""
            SELECT strftime(lpep_pickup_datetime, '%B')
            FROM green_transform
                GROUP BY strftime(lpep_pickup_datetime, '%B') 
                ORDER BY AVG(trip_co2_kgs) DESC
            LIMIT 1;
        """).fetchone()[0]
        glightest_month = con.execute("""
            SELECT strftime(lpep_pickup_datetime, '%B')
            FROM green_transform
                GROUP BY strftime(lpep_pickup_datetime, '%B') 
                ORDER BY AVG(trip_co2_kgs) ASC
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
                SUM(trip_co2_kgs) AS total_co2
            FROM yellow_transform
                GROUP BY month, month_num
                ORDER BY month_num
        """).df() 
        # green grouping total co2 output by month and converting to dataframe
        green_monthly = con.execute("""
            SELECT strftime(lpep_pickup_datetime, '%B') AS month,
                CAST(strftime(lpep_pickup_datetime, '%m') AS INTEGER) AS month_num,
                SUM(trip_co2_kgs) AS total_co2
            FROM green_transform
                GROUP BY month, month_num
                ORDER BY month_num
        """).df()
        # plotting month on x axis and summed co2 outputs on y axis
        fig, ax1 = plt.subplots(figsize=(12,6))
        # yellow as ax1 
        ax1.set_xlabel("Month")
        ax1.set_ylabel("Yellow Taxi CO2 (kgs)")
        ax1.plot(yellow_monthly['month_num'], yellow_monthly['total_co2'], color='yellow', label='Yellow Taxis')
        
        # green as 'twin' of yellow on ax2 
        ax2 = ax1.twinx()
        ax2.set_ylabel('Green Taxi CO2 (kgs)')
        ax2.plot(green_monthly['month_num'], green_monthly['total_co2'], color='green', label='Green Taxis')

        # combining legends from both axes
        lines, labels = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax2.legend(lines + lines2, labels + labels2, loc='best')

        # making x axis labels month names not just numbers
        month_nums= yellow_monthly['month_num'].tolist()
        month_names = yellow_monthly['month'].tolist()
        ax1.set_xticks(month_nums)
        ax1.set_xticklabels(month_names)

        # making it pretty 
        plt.title("Monthly CO2 Outputs from Yellow and Green NYC Taxis")
        fig.tight_layout()
        plt.show()
        
        # saving plot as png image to this repo
        plt.savefig("monthly_co2_totals.png", dpi=300)
        logging.info("Plot complete and saved.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    analyze()

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import date

# Configure logging
# logging.basicConfig(
#     filename='weather_data.log',
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )

spark = SparkSession.builder.appName("WeatherETL").config("spark.jars", "/WeatherETL/postgresql-42.7.6.jar").getOrCreate()
load_dotenv(dotenv_path="WeatherETL/keys.env", override=True)

# Load environment variables
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
db = os.getenv("PG_DB")
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
url = f"jdbc:postgresql://{host}:{port}/{db}"
today = date.today().strftime("%Y-%m-%d")

path = os.getenv("CLEAN_LOCATION")
final_file = f"{path}weather_data_{today}.csv"

# Read the final CSV file
final_df = spark.read.csv(final_file, header=True, inferSchema=True)

# Add ingestion_date column
from pyspark.sql.functions import current_timestamp
final_df = final_df.withColumn("ingestion_date", current_timestamp())
final_df.write.jdbc(
    url=url,
    table="weather_data",
    mode="append",
    properties={
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }
)


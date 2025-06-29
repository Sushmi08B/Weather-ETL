import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import *
from datetime import date
import glob


# Configure logging
# logging.basicConfig(
#     filename='weather_data.log',
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )

# Load environment variables
load_dotenv(dotenv_path= "/WeatherETL/keys.env", override=True)
spark = SparkSession.builder.appName("weatherData").getOrCreate()

# Check if the environment variable is set
if not os.getenv("RAW_LOCATION"):
        raise EnvironmentError("RAW_LOCATION environment variable is not set.")

# Check if the environment variable is set
if not os.getenv("CLEAN_LOCATION"):
        raise EnvironmentError("CLEAN_LOCATION environment variable is not set.")

# Read the raw JSON data
path = os.getenv("RAW_LOCATION")
raw_df = spark.read.option("multiLine", True).json(f"{path}weather_data_raw.json")

# select relevant columns from the raw DataFrame
raw_df = raw_df.withColumn("longitude", col('coord.lon')) \
        .withColumn("latitude", col('coord.lat')) \
        .withColumn("feels_like", col('main.feels_like')) \
        .withColumn("temp_max", col('main.temp_max')) \
        .withColumn("temp_min", col('main.temp_min')) \
        .withColumn("temperature", col('main.temp').cast(FloatType())) \
        .withColumn("humidity", col('main.humidity').cast(IntegerType())) \
        .withColumn("grnd_level", col('main.grnd_level')) \
        .withColumn("pressure", col('main.pressure')) \
        .withColumn("sea_level", col('main.sea_level')) \
        .withColumn("country", col('sys.country')) \
        .withColumn("sunrise", col('sys.sunrise')) \
        .withColumn("sunset", col('sys.sunset')) \
        .withColumn("weather_description", col("weather")[0]["description"]) \
        .withColumn("windspeed", col('wind.speed'))  \
        .withColumn("ingestion_date", current_timestamp())

# finalize the DataFrame by selecting the required columns
final_df = raw_df.select("city", "temperature", "humidity", "weather_description", "ingestion_date")
# final_df.show()
today = date.today().strftime("%Y-%m-%d")
path = os.getenv("CLEAN_LOCATION")
final_df.coalesce(1). \
        write.format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(f"{path}weather_data_{today}")

today = date.today().strftime("%Y-%m-%d")
output_dir = f"{path}weather_data_{today}"
final_file = f"{path}weather_data_{today}.csv"

# Find the part file
part_file = glob.glob(f"{output_dir}/part-*.csv")[0]

# Rename it
os.rename(part_file, final_file)


# Weather-ETL

A simple ETL (Extract, Transform, Load) pipeline for weather data using PySpark and PostgreSQL.

## Project Structure

- `extract.py`: Extracts weather data from the OpenWeather API and saves it as raw JSON.
- `transform.py`: Transforms and cleans the raw weather data using PySpark, outputs a CSV.
- `load.py`: Loads the cleaned CSV data into a PostgreSQL database.
- `keys.env`: Stores API keys and environment variables.
- `postgresql-42.7.6.jar`: PostgreSQL JDBC driver for Spark.
- `readme.md`: Project documentation.

## Prerequisites

- Python 3.x
- PySpark
- PostgreSQL
- `python-dotenv`
- `requests`
- Java (for Spark)
- Download the [PostgreSQL JDBC driver](https://jdbc.postgresql.org/download.html) and place it in the project directory.

## Setup

1. Clone the repository.
2. Install dependencies:
   ```sh
   pip install pyspark python-dotenv requests

3. Update keys.env with your OpenWeather API key and PostgreSQL credentials.
4. Ensure the directories specified in RAW_LOCATION and CLEAN_LOCATION exist and are writable.

Usage
1. Extract:
   Run the extraction script to fetch weather data:
   ```sh
   python extract.py
2. Transform:
   Clean and transform the raw data:
   ```sh
   python transform.py
3. Load:
   Load the cleaned data into PostgreSQL:
   ```sh
   python load.py

Notes
The pipeline currently fetches weather data for London, Paris, Chicago, and New York.
Adjust the cities list in extract.py to fetch data for other cities.
Make sure your PostgreSQL server is running and accessible with the credentials in keys.env.
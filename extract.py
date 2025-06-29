import json
import time
import requests
import os
import logging
from dotenv import load_dotenv
load_dotenv(dotenv_path='/WeatherETL/keys.env', override=True)

# Configure logging
logging.basicConfig(
    filename='weather_data.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# Check if the environment variable is set
if not os.getenv("OPENWEATHER_API_KEY"):
    logging.error("OPENWEATHER_API_KEY environment variable is not set.")
    raise EnvironmentError("OPENWEATHER_API_KEY environment variable is not set.")
# Check if the environment variable is set
if not os.getenv("RAW_LOCATION"):
    logging.error("RAW_LOCATION environment variable is not set.")
    raise EnvironmentError("RAW_LOCATION environment variable is not set.")

# getWeatherData function to fetch weather data for a given city
def getWeatherData(city):
    apiKey = os.getenv("OPENWEATHER_API_KEY")
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city},usa&APPID={apiKey}'
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            logging.info(f"Success for {city}")
            print(response.json())
            return response.json()
        else:
            logging.error(f"API error: {response.status_code} - {response.text}")
            return {"error": "API responded with error"}
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return {"error": "Request failed"}

# List of cities to fetch weather data for
# You can modify this list to include more cities or different countries as needed.
# For example, you can add cities like 'Tokyo', 'Berlin', etc.
# Make sure to use the correct country code in the API URL.
# The country code is set to 'usa' in the URL, but you can change it to the appropriate country code for the cities you want to query.
# Example country codes:
# - 'us' for United States
# - 'uk' for United Kingdom
cities = ['London', 'Paris', 'Chicago', 'New York']

# Initialize an empty list to store weather data
weatherData = []
for city in cities:
    data = getWeatherData(city)
    data['city'] = city
    weatherData.append(data)
    time.sleep(1)

# Save the weather data to a JSON file
path = os.getenv("RAW_LOCATION")
with open(f"{path}weather_data_raw.json", "w") as file:
    json.dump(weatherData, file, indent=4)






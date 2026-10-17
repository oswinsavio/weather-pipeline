import os
import requests
import duckdb
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITIES = ["Dubai", "London", "New York", "Tokyo", "Sydney"]
DUCKDB_PATH = os.path.join("data", "duckdb", "weather_lakehouse.duckdb")

def fetch_weather(city):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for {city}: {response.status_code}")
        return None

def run_ingestion():
    print(f"Starting Weather Data Ingestion at {datetime.now(timezone.utc).isoformat()} UTC")
   
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH)
   
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_weather (
            city VARCHAR,
            country VARCHAR,
            temperature DOUBLE,
            feels_like DOUBLE,
            humidity INTEGER,
            weather_condition VARCHAR,
            timestamp_utc BIGINT,
            ingested_at VARCHAR
        )
    """)
   
    for city in CITIES:
        raw_data = fetch_weather(city)
        if raw_data:
            try:
                city_name = raw_data.get("name")
                country = raw_data.get("sys", {}).get("country")
                main = raw_data.get("main", {})
                weather = raw_data.get("weather", [{}])[0]
               
                temp = main.get("temp")
                feels_like = main.get("feels_like")
                humidity = main.get("humidity")
                condition = weather.get("description")
               
                timestamp_utc = raw_data.get("dt", int(datetime.now(timezone.utc).timestamp()))
                ingested_at = datetime.now(timezone.utc).isoformat()
               
                conn.execute("""
                    INSERT INTO raw_weather VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (city_name, country, temp, feels_like, humidity, condition, timestamp_utc, ingested_at))
               
                print(f"Successfully fetched & validated: {city}")
            except Exception as val_err:
                print(f"Validation error for {city}: {val_err}")
               
    conn.close()
    print("Weather ingestion completed successfully.")

if __name__ == "__main__":
    run_ingestion()
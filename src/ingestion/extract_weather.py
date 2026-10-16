import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import httpx
from dotenv import load_dotenv
from pydantic import BaseModel, Field
import pyarrow as pa
import pyarrow.parquet as pq

# Load environment variables from the .env file
load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# List of cities to track for our data engineering portfolio project
CITIES = [
    "Dubai", "London", "New York", "Tokyo", "Singapore", 
    "Paris", "Sydney", "Toronto", "Mumbai", "São Paulo"
]

# --- 1. Pydantic Data Contract ---
class WeatherResponse(BaseModel):
    city_name: str
    country: Optional[str] = None
    temperature_celsius: float
    feels_like_celsius: float
    humidity_pct: int
    pressure_hpa: int
    wind_speed_ms: float
    weather_condition: str
    timestamp_utc: int
    ingested_at: str

    @classmethod
    def from_api_json(cls, raw_data: dict) -> "WeatherResponse":
        # Safely extract and flatten nested OpenWeatherMap JSON structure
        return cls(
            city_name=raw_data.get("name", "Unknown"),
            country=raw_data.get("sys", {}).get("country"),
            temperature_celsius=raw_data.get("main", {}).get("temp", 0.0),
            feels_like_celsius=raw_data.get("main", {}).get("feels_like", 0.0),
            humidity_pct=raw_data.get("main", {}).get("humidity", 0),
            pressure_hpa=raw_data.get("main", {}).get("pressure", 0),
            wind_speed_ms=raw_data.get("wind", {}).get("speed", 0.0),
            weather_condition=raw_data.get("weather", [{}])[0].get("description", "unknown"),
            timestamp_utc=raw_data.get("dt", int(datetime.utcnow().timestamp())),
            ingested_at=datetime.utcnow().isoformat()
        )


def fetch_city_weather(client: httpx.Client, city: str) -> Optional[dict]:
    """Calls the OpenWeatherMap API for a specific city with error handling."""
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"
    }
    try:
        response = client.get(BASE_URL, params=params, timeout=10.0)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        print(f"❌ HTTP error fetching {city}: {e.response.status_code}")
    except Exception as e:
        print(f"❌ Connection error for {city}: {e}")
    return None


def run_ingestion():
    print(f"🚀 Starting Weather Data Ingestion at {datetime.utcnow().isoformat()} UTC")
    
    raw_records = []
    
    # Use httpx client context manager for connection pooling
    with httpx.Client() as client:
        for city in CITIES:
            data = fetch_city_weather(client, city)
            if data:
                # Validate & parse through Pydantic contract
                try:
                    record = WeatherResponse.from_api_json(data)
                    raw_records.append(record.model_dump())
                    print(f"✅ Successfully fetched & validated: {city}")
                except Exception as val_err:
                    print(f"⚠️ Validation error for {city}: {val_err}")

    if not raw_records:
        print("⚠️ No records fetched. Aborting write.")
        return

    # --- 2. Save as Hive-Partitioned Parquet File ---
    # Structure: data/lake/bronze/weather/dt=YYYY-MM-DD/batch.parquet
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    bronze_dir = Path(f"data/lake/bronze/weather/dt={today_str}")
    bronze_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = bronze_dir / f"weather_batch_{int(datetime.utcnow().timestamp())}.parquet"
    
    # Convert list of dictionaries to PyArrow Table and save compressed Parquet
    table = pa.Table.from_pylist(raw_records)
    pq.write_table(table, file_path, compression="zstd")
    
    print(f"🎉 Successfully saved {len(raw_records)} records to Parquet lake at: {file_path}")


if __name__ == "__main__":
    if not API_KEY or API_KEY == "your_openweather_api_key_here":
        print("❌ Error: OPENWEATHER_API_KEY is missing or not set in your .env file!")
    else:
        run_ingestion()
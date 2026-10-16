import os
import duckdb
import psycopg2

DUCKDB_PATH = "data/duckdb/weather_lakehouse.duckdb"
PG_HOST = "127.0.0.1"  # <--- Make sure this is "127.0.0.1", NOT "localhost"
PG_PORT = 5432
PG_DB = "weather_serving"
PG_USER = "weather_user"
PG_PASSWORD = "weather_pass"

def sync_duckdb_to_postgres():
    print(f"DEBUG: Looking for DuckDB at: {os.path.abspath(DUCKDB_PATH)}")
    if not os.path.exists(DUCKDB_PATH):
        print(f"ERROR: DuckDB file not found at {DUCKDB_PATH}!")
        return

    print("Connecting to DuckDB lakehouse...")
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    
    # Check what tables exist in DuckDB
    tables = con.sql("SHOW TABLES").fetchall()
    print(f"DEBUG: Tables found in DuckDB: {tables}")

    print("Extracting data from DuckDB...")
    try:
        df = con.sql("SELECT * FROM fct_weather_hourly").df()
    except Exception as e:
        print(f"ERROR reading model from DuckDB: {e}")
        con.close()
        return
    
    con.close()
    
    print(f"DEBUG: Fetched {len(df)} rows from fct_weather_hourly.")
    if df.empty:
        print("WARNING: DataFrame is empty! No data found to sync.")
        return

    print("Connecting to PostgreSQL...")
    try:
        pg_conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
    except Exception as e:
        print(f"ERROR connecting to PostgreSQL: {e}")
        return

    pg_cursor = pg_conn.cursor()

    print("Creating table in PostgreSQL if not exists...")
    pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS fct_weather_hourly (
            city_name VARCHAR,
            country VARCHAR,
            temperature_celsius FLOAT,
            feels_like_celsius FLOAT,
            humidity_pct INTEGER,
            pressure_hpa INTEGER,
            wind_speed_ms FLOAT,
            weather_condition VARCHAR,
            observed_at_utc TIMESTAMP,
            ingested_at VARCHAR,
            record_id VARCHAR PRIMARY KEY,
            loaded_to_postgres_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    print("Executing upsert into PostgreSQL...")
    insert_query = """
        INSERT INTO fct_weather_hourly (
            city_name, country, temperature_celsius, feels_like_celsius, 
            humidity_pct, pressure_hpa, wind_speed_ms, weather_condition, 
            observed_at_utc, ingested_at, record_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (record_id) DO UPDATE SET
            temperature_celsius = EXCLUDED.temperature_celsius,
            feels_like_celsius = EXCLUDED.feels_like_celsius,
            humidity_pct = EXCLUDED.humidity_pct,
            pressure_hpa = EXCLUDED.pressure_hpa,
            wind_speed_ms = EXCLUDED.wind_speed_ms,
            weather_condition = EXCLUDED.weather_condition,
            loaded_to_postgres_at = CURRENT_TIMESTAMP;
    """

    records = [tuple(x) for x in df[[
        'city_name', 'country', 'temperature_celsius', 'feels_like_celsius',
        'humidity_pct', 'pressure_hpa', 'wind_speed_ms', 'weather_condition',
        'observed_at_utc', 'ingested_at', 'record_id'
    ]].to_numpy()]

    pg_cursor.executemany(insert_query, records)
    pg_conn.commit()

    pg_cursor.close()
    pg_conn.close()
    print("Successfully synced weather data to PostgreSQL gold layer!")

if __name__ == "__main__":
    sync_duckdb_to_postgres()
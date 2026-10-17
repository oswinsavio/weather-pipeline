from dagster import Definitions, asset, ScheduleDefinition, define_asset_job
import subprocess
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

@asset(group_name="weather_etl")
def raw_weather_data():
    """Ingests live weather data into the raw layer."""
    script_path = os.path.join(ROOT_DIR, "src", "ingestion", "extract_weather.py")
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Ingestion failed: {result.stderr}")
    return "Ingestion complete"

@asset(group_name="weather_etl", deps=[raw_weather_data])
def dbt_duckdb_transformations():
    """Runs dbt models inside DuckDB."""
    dbt_dir = os.path.join(ROOT_DIR, "dbt_weather")
    result = subprocess.run("dbt run", cwd=dbt_dir, capture_output=True, text=True, shell=True)
    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}")
    return "dbt models built"

@asset(group_name="weather_etl", deps=[dbt_duckdb_transformations])
def postgres_serving_sync():
    """Syncs curated DuckDB gold models to PostgreSQL container."""
    script_path = os.path.join(ROOT_DIR, "load_to_postgres.py")
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Postgres sync failed: {result.stderr}")
    return "Synced to PostgreSQL"

# 1. Define job for all assets
weather_etl_job = define_asset_job(
    name="weather_etl_job",
    selection=[raw_weather_data, dbt_duckdb_transformations, postgres_serving_sync]
)

# 2. Define schedule to run every 4 hours (safely within API limits)
every_4_hours_schedule = ScheduleDefinition(
    job=weather_etl_job,
    cron_schedule="0 */4 * * *",  # Runs every 4 hours
    execution_timezone="UTC",
)

# 3. Include it in Definitions
defs = Definitions(
    assets=[raw_weather_data, dbt_duckdb_transformations, postgres_serving_sync],
    jobs=[weather_etl_job],
    schedules=[every_4_hours_schedule],
)
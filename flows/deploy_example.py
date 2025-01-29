import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry
from prefect import flow, task, get_run_logger, runtime
from prefect.blocks.system import Secret
from prefect.artifacts import create_markdown_artifact

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below

@task(log_prints = True)
def get_weather_info(params: dict):
    url = "https://api.open-meteo.com/v1/forecast"
    
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    hourly_data["temperature_2m"] = hourly_temperature_2m

    return pd.DataFrame(data = hourly_data)
    

@task(log_prints = True)
def write_to_csv(df: pd.DataFrame):
    print("writing weather data to weather.csv")
    df.to_csv("weather.csv")


@task(log_prints = True)
def load_secret():
    secret_block = Secret.load("extremely-secret-information")
    return secret_block.get()


@task(log_prints = True)
def _create_markdown_artifact(markdown, key="weather-report", description="Weather Report"):
    
    artifact = create_markdown_artifact(
        key=key,
        markdown=markdown,
        description=description,
    )


@flow
def weather_flow(params: dict):
    run_name = runtime.flow_run.name
    logger = get_run_logger()
    logger.info(f"Starting flow {run_name}")
    logger.info("Getting weather info")
    df = get_weather_info(params)
    logger.info("writing weather info to csv")
    write_to_csv(df)
    secret = load_secret()
    logger.info(f"No longer a secret: {secret}")
    _create_markdown_artifact(markdown=df.to_markdown())



if __name__=="__main__":
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "hourly": "temperature_2m"
    }
    weather_flow(params = params)


    
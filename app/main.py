from typing import Optional
import pandas as pd
from fastapi import FastAPI, HTTPException
from dotenv import dotenv_values
import requests
import uvicorn
from datetime import datetime
from utils.utils import get_day_forecast, check_time_format, proximity_search_buses,\
    get_json_by_field, format_nearby_data_json, get_opendata_json, proximity_search_stops, is_crs\


app = FastAPI()

config = dotenv_values(".env")

MALAGA_CODE = 29067
AEMET_BASE_URL = "https://opendata.aemet.es/opendata/api"
AEMET_DAILY_FORECAST_URL = f"{AEMET_BASE_URL}/prediccion/especifica/municipio/diaria/{MALAGA_CODE}"
AEMET_HOURLY_FORECAST_URL = f"{AEMET_BASE_URL}/prediccion/especifica/municipio/horaria/{MALAGA_CODE}"
BUS_RESOURCES_URL = "https://datosabiertos.malaga.eu/recursos/transporte/EMT"
BUS_LOCATION_GEOJSON_URL = f"{BUS_RESOURCES_URL}/EMTlineasUbicaciones/lineasyubicaciones.geojson"
BUS_STOPS_GEOJSON_URL = f"{BUS_RESOURCES_URL}/EMTLineasYParadas/lineasyparadas.geojson"
BUS_STOPS_CSV_URL = f"{BUS_RESOURCES_URL}/EMTLineasYParadas/lineasyparadas.csv"


def setup_csv_dataframe():
    useless_fields = ["lineas", "codLineaStrSin", "codLineaStr", "observaciones", "avisoSinHorarioEs",
                      "avisoSinHorarioEn", "tagsAccesibilidad", "fechaInicioDemanda", "fechaFinDemanda", "linea", "espera"]

    df = pd.read_csv(BUS_STOPS_CSV_URL, sep=",",
                     usecols=lambda col: col not in useless_fields)
    return df


def setup_geojson_dataframe():
    useless_fields = ["properties", "geometry_name"]

    df = pd.read_json(BUS_LOCATION_GEOJSON_URL)
    df["lastUpdate"] = df["properties"].apply(lambda x: x["last_update"])
    df = df.drop(useless_fields, axis=1)

    return df


def get_forecast(url: str):
    response = get_opendata_json(url, config['AEMET_API_KEY'])
    data_url = "datos"
    return requests.get(response[data_url]).json()


@app.get("/")
def hello_world():
    return {"message": "Hello World!"}


@app.get("/bus-stops")
def get_all_bus_stops():
    return get_opendata_json(BUS_STOPS_GEOJSON_URL)


@app.get("/bus-stops/search/nearby")
def get_nearby_stops(lat: float, lon: float, radius: Optional[int] = 500):
    if not is_crs(lat, lon):
        raise HTTPException(
            status_code=400, detail="Coordinates out of range.")
    df = setup_csv_dataframe()
    df = proximity_search_stops(df, lat, lon, radius)

    return format_nearby_data_json(df, lat, lon, radius)


@app.get("/bus-stops/search")
def get_stop_by_line_code(line_code: str):
    df = setup_csv_dataframe()
    line_code_field = "userCodLinea"

    return get_json_by_field(df, line_code_field, str(line_code))


@app.get("/bus-stops/{stop_code}")
def get_stop_by_stop_code(stop_code: str):
    df = setup_csv_dataframe()
    stop_code_field = "codParada"

    return get_json_by_field(df, stop_code_field, int(stop_code))


@app.get("/bus")
def get_all_bus_location():
    return get_opendata_json(BUS_LOCATION_GEOJSON_URL)


@app.get("/bus/search/nearby")
def get_nearby_buses(lat: float, lon: float, radius: Optional[int] = 500):
    if not is_crs(lat, lon):
        raise HTTPException(
            status_code=400, detail="Coordinates out of range.")
    df = setup_geojson_dataframe()
    df = proximity_search_buses(df, lat, lon, radius)

    return format_nearby_data_json(df, lat, lon, radius)


@app.get("/bus/search")
def get_location_by_line_code(line_code: str):
    df = setup_geojson_dataframe()
    line_code_field = "codLinea"

    return get_json_by_field(df, line_code_field, float(line_code))


@app.get("/bus/{bus_code}")
def get_location_by_bus_code(bus_code: int):
    df = setup_geojson_dataframe()
    bus_code_field = "codBus"

    return get_json_by_field(df, bus_code_field, bus_code)


@app.get("/forecasts")
def get_all_forecasts():
    daily_forecast = get_forecast(AEMET_DAILY_FORECAST_URL)
    return get_day_forecast(daily_forecast)


@app.get("/forecasts/today")
def get_today_forecasts():
    daily_forecast = get_forecast(AEMET_DAILY_FORECAST_URL)
    day = datetime.today().strftime('%Y-%m-%d')
    return get_day_forecast(daily_forecast, day)


@app.get("/forecasts/{day}")
def get_forecast_by_day(day: str):
    try:
        check_time_format(day)
    except ValueError:
        raise HTTPException(status_code=400, detail="Incorrect date format.")

    daily_forecast = get_forecast(AEMET_DAILY_FORECAST_URL)
    return get_day_forecast(daily_forecast, day)


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000)

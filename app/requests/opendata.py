from typing import Optional
import requests
from requests import HTTPError, ConnectionError
from requests.exceptions import Timeout
from dotenv import dotenv_values
from pprint import pprint
import pandas as pd
import json
from math import radians, cos, sin, asin, sqrt


config = dotenv_values(".env")

MALAGA_CODE = 29067
AEMET_BASE_URL = "https://opendata.aemet.es/opendata/api"
AEMET_DAILY_FORECAST_URL = f"{AEMET_BASE_URL}/prediccion/especifica/municipio/diaria/{MALAGA_CODE}"
AEMET_HOURLY_FORECAST_URL = f"{AEMET_BASE_URL}/prediccion/especifica/municipio/horaria/{MALAGA_CODE}"
BUS_LOCATION_GEOJSON_URL = "https://datosabiertos.malaga.eu/recursos/transporte/EMT/EMTlineasUbicaciones/lineasyubicaciones.geojson"
BUS_STOPS_JSON_URL = "https://datosabiertos.malaga.eu/recursos/transporte/EMT/EMTLineasYParadas/lineasyparadas.geojson"
BUS_STOPS_CSV_URL = "https://datosabiertos.malaga.eu/recursos/transporte/EMT/EMTLineasYParadas/lineasyparadas.csv"


def get_opendata_json(url: str, api_key: Optional[str] = None):
    try:
        if api_key:
            req = requests.get(url, headers={"api_key": api_key})
        else:
            req = requests.get(url)
    except HTTPError:
        print("An HTTP error occurred.")
    except Timeout:
        print("Time out exception.")
    except ConnectionError:
        print("Connection error.")

    return req.json()


def get_all_bus_stops():
    return get_opendata_json(BUS_STOPS_JSON_URL)


def _setup_csv_dataframe():
    useless_fields = ["lineas", "codLinea", "codLineaStrSin", "codLineaStr", "observaciones", "avisoSinHorarioEs",
                      "avisoSinHorarioEn", "tagsAccesibilidad", "fechaInicioDemanda", "fechaFinDemanda", "linea", "espera"]

    df = pd.read_csv(BUS_STOPS_CSV_URL, sep=",",
                     usecols=lambda col: col not in useless_fields)
    return df


def _to_dict(row, name):
    res = {}
    value = ""
    for i, r in enumerate(row):
        if (row.index[i] != name):
            res[row.index[i]] = r
        else:
            value = r
    return pd.Series({name: value, "resultados": res})


def _get_json_by_field(df: pd.DataFrame, field_name: str, field_value: str):
    df = df[df[field_name] == field_value]

    df = df.apply(lambda row: _to_dict(row, field_name), axis=1)
    df = df.groupby([field_name], as_index=False).agg(total=pd.NamedAgg(column="resultados", aggfunc="count"),
                                                      datos=pd.NamedAgg(column="resultados", aggfunc=list))
    df = df.to_json(force_ascii=False, orient="records")
    return json.loads(df)


def get_stop_by_line_code(line_code: str):
    df = _setup_csv_dataframe()
    line_code_field = "userCodLinea"

    return _get_json_by_field(df, line_code_field, str(line_code))


def get_stop_by_stop_code(stop_code: str):
    df = _setup_csv_dataframe()
    stop_code_field = "codParada"

    return _get_json_by_field(df, stop_code_field, int(stop_code))


def _setup_geojson_dataframe():
    useless_fields = ["properties", "geometry_name"]

    df = pd.read_json(BUS_LOCATION_GEOJSON_URL)
    df["lastUpdate"] = df["properties"].apply(lambda x: x["last_update"])
    df = df.drop(useless_fields, axis=1)

    return df


def get_all_bus_location():
    return get_opendata_json(BUS_LOCATION_GEOJSON_URL)


def get_location_by_bus_code(bus_code: str):
    df = _setup_geojson_dataframe()
    bus_code_field = "codBus"

    return _get_json_by_field(df, bus_code_field, bus_code)


def get_location_by_line_code(line_code: str):
    df = _setup_geojson_dataframe()
    line_code_field = "codLinea"

    return _get_json_by_field(df, line_code_field, line_code)


def _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float):
    """
    Calculate the great circle distance in meters between two points
    on the earth (specified in decimal degrees)
    """
    METER_TO_KM = 1000
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    r = 6371

    return c * r * METER_TO_KM


def _proximity_search_stops(df: pd.DataFrame, lat: float, lon: float, radius: int):
    return df[df.apply(lambda x: _haversine_distance(
        x["lat"], x["lon"], lat, lon) <= radius, axis=1)]


def _proximity_search_buses(df: pd.DataFrame, lat: float, lon: float, radius: int):
    df = df[df.apply(lambda x: _haversine_distance(
        float(x["geometry"]["coordinates"][1]), float(x["geometry"]["coordinates"][0]), lat, lon) <= radius, axis=1)]
    distance = df.apply(lambda row: _haversine_distance(float(
        row["geometry"]["coordinates"][1]), float(row["geometry"]["coordinates"][0]), lat, lon), axis=1)

    return df.assign(distancia=distance)


def _format_nearby_data_json(df: pd.DataFrame, lat: float, lon: float, radius: int):
    json_data = df.to_json(force_ascii=False, orient="records")
    filtered_data = json.loads(json_data)

    return {"lat": lat, "lon": lon, "radius": radius, "total": len(filtered_data), "datos": filtered_data}


def get_nearby_stops(lat: float, lon: float, radius: int):
    df = _setup_csv_dataframe()
    df = _proximity_search_stops(df, lat, lon, radius)

    return _format_nearby_data_json(df, lat, lon, radius)


def get_nearby_buses(lat: float, lon: float, radius: int):
    df = _setup_geojson_dataframe()
    df = _proximity_search_buses(df, lat, lon, radius)

    return _format_nearby_data_json(df, lat, lon, radius)


def get_forecast(url: str):
    response = get_opendata_json(url, config['AEMET_API_KEY'])
    data_url = "datos"
    return requests.get(response[data_url]).json()


# TODO: Refactor
def get_forecast_by_day(day: str):
    daily_forecast = get_forecast(AEMET_DAILY_FORECAST_URL)
    forecasts = daily_forecast[0]["prediccion"]["dia"]
    result = next(
        (f for f in forecasts if f["fecha"] == f"{day}T00:00:00"), None)

    if not result:
        return

    return result


# TODO: Refactor
def get_forecast_by_day_hour(day: str, hour: str):
    hourly_forecast = get_forecast(AEMET_HOURLY_FORECAST_URL)
    delete_ = ["probNieve", "probPrecipitacion", "probTormenta"]
    forecasts = hourly_forecast[0]["prediccion"]["dia"]
    result = next(
        (f for f in forecasts if f["fecha"] == f"{day}T00:00:00"), None)

    result = {key: value for key,
              value in result.items() if key not in delete_}

    if not result:
        return

    for field in result:
        for f in result[field]:
            if (type(f) == dict):
                for key in f:
                    if (key == "periodo" and int(f[key]) == int(hour)):
                        result[field] = f

    if not result:
        return

    return result

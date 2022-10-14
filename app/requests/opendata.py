from typing import Optional
import requests
from requests import HTTPError, ConnectionError
from requests.exceptions import Timeout
from dotenv import dotenv_values
from pprint import pprint
import pandas as pd
import json

config = dotenv_values(".env")

MALAGA_CODE = 29067
AEMET_BASE_URL = "https://opendata.aemet.es/opendata/api"
AEMET_FORECAST_URL = f"{AEMET_BASE_URL}/prediccion/especifica/municipio/horaria/{MALAGA_CODE}"
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


def get_hourly_forecast():
    response = get_opendata_json(AEMET_FORECAST_URL, config['AEMET_API_KEY'])
    data_url = "datos"
    return requests.get(response[data_url]).json()


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

# TODO: get nearby buses location and stops

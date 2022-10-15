from typing import Optional
from fastapi import APIRouter, HTTPException
import pandas as pd

from utils.utils import format_nearby_data_json, get_json_by_field, get_opendata_json, is_crs, proximity_search_stops

router = APIRouter()

BUS_RESOURCES_URL = "https://datosabiertos.malaga.eu/recursos/transporte/EMT"
BUS_STOPS_CSV_URL = f"{BUS_RESOURCES_URL}/EMTLineasYParadas/lineasyparadas.csv"
BUS_STOPS_GEOJSON_URL = f"{BUS_RESOURCES_URL}/EMTLineasYParadas/lineasyparadas.geojson"


def setup_csv_dataframe():
    useless_fields = ["lineas", "codLineaStrSin", "codLineaStr", "observaciones", "avisoSinHorarioEs",
                      "avisoSinHorarioEn", "tagsAccesibilidad", "fechaInicioDemanda", "fechaFinDemanda", "linea", "espera"]

    df = pd.read_csv(BUS_STOPS_CSV_URL, sep=",",
                     usecols=lambda col: col not in useless_fields)
    return df


@router.get("")
def get_all_bus_stops():
    return get_opendata_json(BUS_STOPS_GEOJSON_URL)


@router.get("/search/nearby")
def get_nearby_stops(lat: float, lon: float, radius: Optional[int] = 500):
    if not is_crs(lat, lon):
        raise HTTPException(
            status_code=400, detail="Coordinates out of range.")
    df = setup_csv_dataframe()
    df = proximity_search_stops(df, lat, lon, radius)

    return format_nearby_data_json(df, lat, lon, radius)


@router.get("/search")
def get_stop_by_line_code(line_code: str):
    df = setup_csv_dataframe()
    line_code_field = "userCodLinea"

    return get_json_by_field(df, line_code_field, str(line_code))


@router.get("/{stop_code}")
def get_stop_by_stop_code(stop_code: str):
    df = setup_csv_dataframe()
    stop_code_field = "codParada"

    return get_json_by_field(df, stop_code_field, int(stop_code))

import requests
from dotenv import dotenv_values
from pprint import pprint

config = dotenv_values(".env")

MALAGA_CODIGO_MUNICIPIO = 29067
AEMET_URL_BASE = "https://opendata.aemet.es/opendata/api"

req_pred_h = requests.get(
    f"{AEMET_URL_BASE}/prediccion/especifica/municipio/horaria/{MALAGA_CODIGO_MUNICIPIO}", headers={"api_key": config["AEMET_API_KEY"]})

link_pred_h = req_pred_h.json()["datos"]

datos_pred_h = requests.get(link_pred_h)

pprint(datos_pred_h.json())

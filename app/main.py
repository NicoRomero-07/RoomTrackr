from fastapi import FastAPI
from app.routers import bus_locations, bus_stops, forecasts

app = FastAPI()

app.include_router(bus_locations.router, prefix="/buses", tags=["buses"])
app.include_router(bus_stops.router, prefix="/bus-stops", tags=["bus_stops"])
app.include_router(forecasts.router, prefix="/forecasts", tags=["forecasts"])

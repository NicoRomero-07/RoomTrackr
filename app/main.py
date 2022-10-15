from fastapi import FastAPI
import uvicorn
from routers import bus_locations, bus_stops, forecasts

app = FastAPI()

app.include_router(bus_locations.router, prefix="/buses", tags=["buses"])
app.include_router(bus_stops.router, prefix="/bus-stops", tags=["bus_stops"])
app.include_router(forecasts.router, prefix="/forecasts", tags=["forecasts"])


@app.get("/")
def hello_world():
    return {"message": "Hello World!"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000)

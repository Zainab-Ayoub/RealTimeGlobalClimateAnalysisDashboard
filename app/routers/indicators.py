from fastapi import APIRouter, HTTPException

from ..schemas import CatalogResponse, ForecastResponse, IndicatorKey, IndicatorMeta, TimeSeriesResponse
from ..state import app_state
from ..forecast import forecast_indicator

router = APIRouter(prefix="/api", tags=["indicators"])


@router.get("/catalog", response_model=CatalogResponse)
async def get_catalog() -> CatalogResponse:
    indicators = [
        IndicatorMeta(key="ao", name="Arctic Oscillation", group="Natural Variability", unit="index", description="Shifts in surface pressure between mid-latitudes and the Arctic."),
        IndicatorMeta(key="sea_ice", name="Arctic Sea Ice", group="Climate Change", unit="million km^2", description="Minimum September Arctic sea ice extent since 1979."),
        IndicatorMeta(key="co2", name="Carbon Dioxide", group="Climate Change", unit="ppm", description="Atmospheric CO₂ monthly average (Mauna Loa / NOAA GML)."),
        IndicatorMeta(key="glaciers", name="Mountain Glaciers", group="Climate Change", unit="m", description="Global glacier mass balance / thickness change."),
        IndicatorMeta(key="ghg", name="Greenhouse Gases", group="Climate Change", unit="index", description="Aggregate heating influence (AGGI)."),
        IndicatorMeta(key="nao", name="North Atlantic Oscillation", group="Natural Variability", unit="index", description="Pressure difference pattern over the North Atlantic."),
        IndicatorMeta(key="ocean_heat", name="Ocean Heat", group="Climate Change", unit="ZJ", description="Global ocean heat content."),
        IndicatorMeta(key="oni", name="El Niño / La Niña (ONI)", group="Natural Variability", unit="°C", description="Niño 3.4 SST anomaly 3‑month mean."),
    ]
    return CatalogResponse(indicators=indicators)


@router.get("/indicators")
async def get_indicators():
    # Return dynamic dictionary mapping indicator -> series
    return await app_state.get_all()


@router.get("/indicators/{indicator}", response_model=TimeSeriesResponse)
async def get_indicator(indicator: str) -> TimeSeriesResponse:
    series = await app_state.get_series(indicator)
    if not series:
        raise HTTPException(status_code=404, detail="No data for indicator")
    return TimeSeriesResponse(indicator=indicator, series=series)


@router.get("/forecast/{indicator}", response_model=ForecastResponse)
async def get_forecast(indicator: str, horizon: int = 12) -> ForecastResponse:
    series = await app_state.get_series(indicator)
    if not series:
        raise HTTPException(status_code=404, detail="No data for indicator")
    forecast = await forecast_indicator(indicator, series, horizon)
    return ForecastResponse(indicator=indicator, horizon=horizon, forecast=forecast)

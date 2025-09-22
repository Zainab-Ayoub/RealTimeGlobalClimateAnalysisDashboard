from pydantic import BaseModel
from typing import List

# General indicator key support for extension
IndicatorKey = str


class TimeSeriesPoint(BaseModel):
    t: str
    v: float


class TimeSeriesResponse(BaseModel):
    indicator: IndicatorKey
    series: List[TimeSeriesPoint]


class IndicatorsBundle(BaseModel):
    co2: List[TimeSeriesPoint]
    temp: List[TimeSeriesPoint]
    oni: List[TimeSeriesPoint]


class ForecastResponse(BaseModel):
    indicator: IndicatorKey
    horizon: int
    forecast: List[TimeSeriesPoint]


class IndicatorMeta(BaseModel):
    key: IndicatorKey
    name: str
    group: str  # e.g., "Climate Change", "Natural Variability"
    unit: str
    description: str


class CatalogResponse(BaseModel):
    indicators: List[IndicatorMeta]

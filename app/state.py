import asyncio
from pathlib import Path
from typing import Dict, List

from .schemas import TimeSeriesPoint
from .services.fetchers import fetch_all_indicators

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

class AppState:
    def __init__(self):
        self._lock = asyncio.Lock()
        self.cache: Dict[str, List[TimeSeriesPoint]] = {
            "ao": [],
            "sea_ice": [],
            "co2": [],
            "glaciers": [],
            "ghg": [],
            "nao": [],
            "ocean_heat": [],
            "oni": [],
            "temp": [],
        }

    async def refresh(self):
        series = await fetch_all_indicators(DATA_DIR)
        async with self._lock:
            for k, v in series.items():
                self.cache[k] = v

    async def get_series(self, key: str) -> List[TimeSeriesPoint]:
        async with self._lock:
            return list(self.cache.get(key, []))

    async def get_all(self) -> Dict[str, List[TimeSeriesPoint]]:
        async with self._lock:
            return {k: list(v) for k, v in self.cache.items()}


app_state = AppState()

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ..schemas import TimeSeriesPoint


def _read_csv_pairs(path: Path, date_fmt: str = None, skip_hash: bool = True) -> List[TimeSeriesPoint]:
    points: List[TimeSeriesPoint] = []
    if not path.exists():
        return points
    with path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            if skip_hash and row[0].startswith("#"):
                continue
            try:
                ds, vs = row[0].strip(), row[1].strip()
            except Exception:
                continue
            try:
                if date_fmt:
                    dt = datetime.strptime(ds, date_fmt)
                    t = dt.date().isoformat()
                else:
                    if len(ds) == 7:
                        t = f"{ds}-01"
                    else:
                        _ = datetime.fromisoformat(ds)
                        t = ds
                v = float(vs)
                points.append(TimeSeriesPoint(t=t, v=v))
            except Exception:
                continue
    points.sort(key=lambda p: p.t)
    return points


@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def _fetch_text(url: str, timeout: float = 15.0) -> str:
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.text


async def fetch_co2(data_dir: Path) -> List[TimeSeriesPoint]:
    url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_mlo.csv"
    try:
        text = await _fetch_text(url)
        rows: List[TimeSeriesPoint] = []
        for line in text.splitlines():
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 6:
                continue
            try:
                year = int(parts[0])
                month = int(parts[1])
                value = float(parts[4]) if parts[4] != "-99.99" else float(parts[5])
                t = f"{year:04d}-{month:02d}-01"
                rows.append(TimeSeriesPoint(t=t, v=value))
            except Exception:
                continue
        rows.sort(key=lambda p: p.t)
        if rows:
            return rows
        raise ValueError("Empty CO2 rows")
    except Exception:
        return _read_csv_pairs(data_dir / "co2.csv")


async def fetch_temp(data_dir: Path) -> List[TimeSeriesPoint]:
    url = "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts.csv"
    try:
        text = await _fetch_text(url)
        lines = text.splitlines()
        data: List[TimeSeriesPoint] = []
        start_idx = 0
        for i, line in enumerate(lines):
            if line.startswith("Year"):
                start_idx = i + 1
                break
        for line in lines[start_idx:]:
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) <= 1:
                parts = [p for p in line.strip().split() if p]
            if not parts:
                continue
            try:
                year = int(parts[0])
                for m in range(1, 13):
                    if m >= len(parts):
                        break
                    val_str = parts[m]
                    if val_str in {"***", "NA", "-"}:
                        continue
                    val = float(val_str) / 100.0 if abs(float(val_str)) > 50 else float(val_str) / 10.0
                    t = f"{year:04d}-{m:02d}-01"
                    data.append(TimeSeriesPoint(t=t, v=val))
            except Exception:
                continue
        data.sort(key=lambda p: p.t)
        if data:
            return data
        raise ValueError("Empty temp rows")
    except Exception:
        return _read_csv_pairs(data_dir / "temp.csv")


async def fetch_oni(data_dir: Path) -> List[TimeSeriesPoint]:
    fallback = data_dir / "oni.csv"
    if fallback.exists():
        return _read_csv_pairs(fallback)
    return []


# New indicators
async def fetch_ao(data_dir: Path) -> List[TimeSeriesPoint]:
    return _read_csv_pairs(data_dir / "ao.csv")


async def fetch_sea_ice(data_dir: Path) -> List[TimeSeriesPoint]:
    return _read_csv_pairs(data_dir / "sea_ice.csv")


async def fetch_glaciers(data_dir: Path) -> List[TimeSeriesPoint]:
    return _read_csv_pairs(data_dir / "glaciers.csv")


async def fetch_ghg(data_dir: Path) -> List[TimeSeriesPoint]:
    return _read_csv_pairs(data_dir / "ghg.csv")


async def fetch_nao(data_dir: Path) -> List[TimeSeriesPoint]:
    return _read_csv_pairs(data_dir / "nao.csv")


async def fetch_ocean_heat(data_dir: Path) -> List[TimeSeriesPoint]:
    return _read_csv_pairs(data_dir / "ocean_heat.csv")


async def fetch_all_indicators(data_dir: Path) -> Dict[str, List[TimeSeriesPoint]]:
    return {
        "ao": await fetch_ao(data_dir),
        "sea_ice": await fetch_sea_ice(data_dir),
        "co2": await fetch_co2(data_dir),
        "glaciers": await fetch_glaciers(data_dir),
        "ghg": await fetch_ghg(data_dir),
        "nao": await fetch_nao(data_dir),
        "ocean_heat": await fetch_ocean_heat(data_dir),
        "oni": await fetch_oni(data_dir),
        "temp": await fetch_temp(data_dir),
    }

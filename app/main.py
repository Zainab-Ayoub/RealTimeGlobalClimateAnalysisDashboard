import asyncio
import os
from pathlib import Path
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .state import app_state
from .routers.indicators import router as indicators_router

BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "3600"))

app = FastAPI(title="Real-Time Global Climate Analysis Dashboard")

# CORS (adjust if you deploy behind a different host)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend and static assets
if FRONTEND_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIR)), name="assets")
    static_dir = FRONTEND_DIR / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

app.include_router(indicators_router)


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/")
async def index() -> FileResponse:
    index_path = FRONTEND_DIR / "index.html"
    return FileResponse(str(index_path))


@app.get("/indicator")
async def indicator_page() -> FileResponse:
    page = FRONTEND_DIR / "indicator.html"
    return FileResponse(str(page))


# Simple WebSocket hub for live updates
connected_clients: Set[WebSocket] = set()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        await websocket.send_json({"type": "hello", "message": "Connected to Real-Time Climate Dashboard"})
        while True:
            _ = await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        connected_clients.discard(websocket)


async def broadcast(payload):
    if not connected_clients:
        return
    dead: Set[WebSocket] = set()
    for ws in connected_clients:
        try:
            await ws.send_json(payload)
        except Exception:
            dead.add(ws)
    for ws in dead:
        connected_clients.discard(ws)


async def periodic_updates():
    while True:
        await app_state.refresh()
        data = await app_state.get_all()
        payload = {"type": "update"}
        for key, series in data.items():
            payload[key] = [[p.t, p.v] for p in series[-360:]]
        await broadcast(payload)
        await asyncio.sleep(REFRESH_SECONDS)


@app.on_event("startup")
async def on_startup():
    await app_state.refresh()
    asyncio.create_task(periodic_updates())

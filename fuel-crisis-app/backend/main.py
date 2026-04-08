"""
Fuel Crisis Command Centre — FastAPI Backend
"""
import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

from backend.routers import metrics, infrastructure, fuelwatch, minerals, genie, briefing, forecast

app = FastAPI(
    title="Fuel Crisis Command Centre",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routers — these MUST be registered before the static file catch-all
app.include_router(metrics.router, prefix="/api", tags=["metrics"])
app.include_router(infrastructure.router, prefix="/api", tags=["infrastructure"])
app.include_router(fuelwatch.router, prefix="/api", tags=["fuelwatch"])
app.include_router(minerals.router, prefix="/api", tags=["minerals"])
app.include_router(genie.router, prefix="/api/genie", tags=["genie"])
app.include_router(briefing.router, prefix="/api/briefing", tags=["briefing"])
app.include_router(forecast.router, prefix="/api", tags=["forecast"])


@app.get("/api/health")
def health():
    from backend.database import get_databricks_db
    db_ok = False
    error_msg = ""
    try:
        db = get_databricks_db()
        db_ok = db.test_connection()
    except Exception as e:
        error_msg = str(e)[:100]
    return {
        "status": "healthy" if db_ok else "degraded",
        "databricks_sql": "connected" if db_ok else f"disconnected: {error_msg}",
        "warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID", "not set"),
    }


# --- Static frontend serving ---
# Mount built React app. The catch-all MUST exclude /api paths.
_candidates = [
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "frontend", "dist"),
    os.path.join(os.getcwd(), "frontend", "dist"),
]
static_dir = next((d for d in _candidates if os.path.isdir(d)), None)

if static_dir:
    print(f"Serving frontend from: {static_dir}")
    # Serve /assets directly
    assets_dir = os.path.join(static_dir, "assets")
    if os.path.isdir(assets_dir):
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

# SPA fallback — catch all non-API GET routes and serve index.html
@app.get("/{full_path:path}")
def serve_spa(full_path: str):
    # Never intercept /api routes
    if full_path.startswith("api"):
        return JSONResponse(status_code=404, content={"detail": f"Not found: /{full_path}"})

    if static_dir:
        # Try serving an actual file first
        file_path = os.path.join(static_dir, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        # SPA fallback — return index.html for client-side routing
        index = os.path.join(static_dir, "index.html")
        if os.path.isfile(index):
            return FileResponse(index)

    return JSONResponse(status_code=404, content={"detail": "Frontend not built"})

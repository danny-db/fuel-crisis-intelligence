"""
MVT Vector Tile router — serves Mapbox Vector Tiles from Lakebase/PostGIS.

Layers:
- refineries: Fuel infrastructure refineries
- terminals: Fuel infrastructure terminals
- depots: Fuel infrastructure depots
- fuelwatch: WA fuel station prices (with price coloring)
- minerals: Mineral deposits (~5000 points)
"""
import time
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from backend.database import fetch_tile, get_lakebase_pool

router = APIRouter()

# In-memory tile cache (layer, z, x, y) -> (bytes, timestamp)
_tile_cache: dict[tuple, tuple[bytes, float]] = {}
CACHE_TTL = 300  # 5 minutes
CACHE_MAX = 2000

SCHEMA = "fuel"

# Layer definitions — maps to PostGIS tables
LAYERS = {
    "refineries": {
        "table": f"{SCHEMA}.refineries",
        "columns": ["name", "operator", "state", "status", "capacity_bpd"],
        "geom_col": "geom",
        "min_zoom": 0,
        "max_zoom": 16,
    },
    "terminals": {
        "table": f"{SCHEMA}.terminals",
        "columns": ["name", "operator", "state", "type"],
        "geom_col": "geom",
        "min_zoom": 0,
        "max_zoom": 16,
    },
    "depots": {
        "table": f"{SCHEMA}.depots",
        "columns": ["name", "operator", "state", "type"],
        "geom_col": "geom",
        "min_zoom": 0,
        "max_zoom": 16,
    },
    "fuelwatch": {
        "table": f"{SCHEMA}.fuelwatch",
        "columns": ["trading_name", "brand", "address", "price"],
        "geom_col": "geom",
        "min_zoom": 4,
        "max_zoom": 16,
    },
    "minerals": {
        "table": f"{SCHEMA}.minerals",
        "columns": ["name", "commodity", "state"],
        "geom_col": "geom",
        "min_zoom": 0,
        "max_zoom": 16,
    },
}


def _evict_stale():
    """Evict stale cache entries."""
    now = time.time()
    stale = [k for k, (_, ts) in _tile_cache.items() if now - ts > CACHE_TTL]
    for k in stale:
        del _tile_cache[k]
    # Hard limit
    if len(_tile_cache) > CACHE_MAX:
        oldest = sorted(_tile_cache.items(), key=lambda x: x[1][1])
        for k, _ in oldest[:len(_tile_cache) - CACHE_MAX]:
            del _tile_cache[k]


@router.get("/{layer}/{z}/{x}/{y}.pbf")
async def get_tile(layer: str, z: int, x: int, y: int):
    """Serve MVT tile for a layer at z/x/y coordinates."""
    if layer not in LAYERS:
        raise HTTPException(404, f"Unknown layer: {layer}")

    pool = get_lakebase_pool()
    if pool is None:
        raise HTTPException(503, "Lakebase not configured")

    layer_def = LAYERS[layer]
    if z < layer_def["min_zoom"] or z > layer_def["max_zoom"]:
        return Response(content=b"", media_type="application/x-protobuf")

    # Check cache
    cache_key = (layer, z, x, y)
    if cache_key in _tile_cache:
        data, ts = _tile_cache[cache_key]
        if time.time() - ts < CACHE_TTL:
            return Response(
                content=data,
                media_type="application/x-protobuf",
                headers={"Cache-Control": "public, max-age=300", "Access-Control-Allow-Origin": "*"},
            )

    # Build MVT query
    columns = ", ".join(f'"{c}"' for c in layer_def["columns"])
    geom_col = layer_def["geom_col"]
    table = layer_def["table"]

    query = f"""
    SELECT ST_AsMVT(q, $1, 4096, 'geom') FROM (
        SELECT
            ST_AsMVTGeom(
                ST_Transform("{geom_col}", 3857),
                ST_TileEnvelope($2::integer, $3::integer, $4::integer),
                4096, 256, true
            ) AS geom,
            {columns}
        FROM {table}
        WHERE ST_Intersects(
            ST_Transform("{geom_col}", 3857),
            ST_TileEnvelope($2, $3, $4)
        )
    ) q
    """

    try:
        tile_data = await fetch_tile(query, layer, z, x, y)
    except Exception as e:
        # Fallback: try without ST_TileEnvelope (manual bbox)
        try:
            n = 2 ** z
            lon_min = x / n * 360 - 180
            lon_max = (x + 1) / n * 360 - 180
            import math
            lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
            lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))

            fallback_query = f"""
            SELECT ST_AsMVT(q, $1, 4096, 'geom') FROM (
                SELECT
                    ST_AsMVTGeom(
                        ST_Transform("{geom_col}", 3857),
                        ST_MakeEnvelope(
                            {lon_min * 20037508.34 / 180}, {math.log(math.tan((90 + lat_min) * math.pi / 360)) / math.pi * 20037508.34},
                            {lon_max * 20037508.34 / 180}, {math.log(math.tan((90 + lat_max) * math.pi / 360)) / math.pi * 20037508.34},
                            3857
                        ),
                        4096, 256, true
                    ) AS geom,
                    {columns}
                FROM {table}
                WHERE "{geom_col}" IS NOT NULL
            ) q
            """
            tile_data = await fetch_tile(fallback_query, layer)
        except Exception:
            raise HTTPException(500, f"Tile query failed: {e}")

    if tile_data is None:
        tile_data = b""

    # Cache
    _evict_stale()
    _tile_cache[cache_key] = (tile_data, time.time())

    return Response(
        content=tile_data,
        media_type="application/x-protobuf",
        headers={"Cache-Control": "public, max-age=300", "Access-Control-Allow-Origin": "*"},
    )

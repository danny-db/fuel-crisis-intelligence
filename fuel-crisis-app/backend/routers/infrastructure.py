"""Infrastructure router — GeoJSON from Lakebase PostGIS."""
import logging
from fastapi import APIRouter, HTTPException
from backend.database import get_lakebase_connection

logger = logging.getLogger("uvicorn.error")
router = APIRouter()

PG_SCHEMA = "fuel"


@router.get("/infrastructure/{infra_type}")
def get_infrastructure(infra_type: str):
    logger.info(f"Infrastructure request: {infra_type}")
    table_map = {
        "refineries": f"{PG_SCHEMA}.refineries",
        "terminals": f"{PG_SCHEMA}.terminals",
        "depots": f"{PG_SCHEMA}.depots",
    }
    if infra_type not in table_map:
        raise HTTPException(400, f"Unknown type: {infra_type}")

    conn = None
    try:
        conn = get_lakebase_connection()
        if conn is None:
            raise HTTPException(503, "Lakebase not configured")
        cur = conn.cursor()
        cur.execute(f"SELECT *, ST_X(geom) as lon, ST_Y(geom) as lat FROM {table_map[infra_type]}")
        cols = [desc[0] for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        logger.info(f"Infrastructure {infra_type}: {len(rows)} rows from Lakebase")

        features = []
        for row in rows:
            lat, lon = row.get("lat"), row.get("lon")
            if lat and lon:
                props = {k: v for k, v in row.items() if k not in ("lat", "lon", "geom", "id")}
                # Serialize non-JSON types
                for k, v in props.items():
                    if v is not None and not isinstance(v, (str, int, float, bool)):
                        props[k] = str(v)
                features.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]}, "properties": props})
        return {"type": "FeatureCollection", "features": features}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Infrastructure {infra_type} FAILED: {e}", exc_info=True)
        raise HTTPException(500, str(e))
    finally:
        if conn:
            conn.close()

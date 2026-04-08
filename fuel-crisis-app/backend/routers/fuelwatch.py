"""FuelWatch router — WA station prices from Lakebase PostGIS."""
import logging
from fastapi import APIRouter, HTTPException
from backend.database import get_lakebase_connection

logger = logging.getLogger("uvicorn.error")
router = APIRouter()

PG_SCHEMA = "fuel"


@router.get("/fuelwatch")
def get_fuelwatch():
    conn = None
    try:
        conn = get_lakebase_connection()
        if conn is None:
            raise HTTPException(503, "Lakebase not configured")
        cur = conn.cursor()
        cur.execute(f"SELECT trading_name, brand, address, price, ST_X(geom) as lon, ST_Y(geom) as lat FROM {PG_SCHEMA}.fuelwatch")
        cols = [desc[0] for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        logger.info(f"FuelWatch: {len(rows)} stations from Lakebase")

        features = []
        for row in rows:
            lat, lon = row.get("lat"), row.get("lon")
            if lat and lon:
                props = {k: v for k, v in row.items() if k not in ("lat", "lon")}
                features.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]}, "properties": props})
        return {"type": "FeatureCollection", "features": features}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"FuelWatch FAILED: {e}", exc_info=True)
        raise HTTPException(500, str(e))
    finally:
        if conn:
            conn.close()

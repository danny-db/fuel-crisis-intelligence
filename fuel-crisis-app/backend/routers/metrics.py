"""Metrics router — KPI data for the ticker bar."""
import logging
from fastapi import APIRouter, HTTPException
from backend.database import get_databricks_db

logger = logging.getLogger("uvicorn.error")
router = APIRouter()


@router.get("/metrics")
def get_metrics():
    db = get_databricks_db()
    T = db.full_schema
    metrics = {}

    # Use tables that actually exist on this workspace
    queries = {
        "brent_price": f"SELECT brent_usd_bbl as val FROM {T}.conflict_scenario_overlay WHERE day_number = 0",
        "aud_usd": f"SELECT aud_usd as val FROM {T}.conflict_scenario_overlay WHERE day_number = 0",
        "iea_days": f"SELECT COUNT(*) as val FROM {T}.petroleum_iea_days_coverage",
        "operational_refineries": f"SELECT COUNT(*) as val FROM {T}.fuel_infrastructure_refineries WHERE status = 'Operational'",
        "iran_opec_pct": f"""SELECT ROUND(
            (SELECT value_tbpd FROM {T}.opec_production_by_country WHERE country='Iran' AND period=(SELECT MAX(period) FROM {T}.opec_production_by_country))
            / NULLIF((SELECT SUM(value_tbpd) FROM {T}.opec_production_by_country WHERE period=(SELECT MAX(period) FROM {T}.opec_production_by_country)), 0)
            * 100, 1) as val""",
    }

    # Also try FRED tables if they exist (enrichment may have loaded them)
    fred_overrides = {
        "brent_price": f"SELECT ROUND(price_usd_per_barrel, 2) as val FROM {T}.brent_crude_oil_prices ORDER BY date DESC LIMIT 1",
        "aud_usd": f"SELECT ROUND(aud_usd_rate, 4) as val FROM {T}.aud_usd_exchange_rate ORDER BY date DESC LIMIT 1",
    }

    for key, sql_query in queries.items():
        try:
            metrics[key] = db.query_scalar(sql_query)
        except Exception as e:
            logger.warning(f"Metrics {key}: {e}")
            metrics[key] = None

    # Override with FRED data if available
    for key, sql_query in fred_overrides.items():
        try:
            val = db.query_scalar(sql_query)
            if val is not None:
                metrics[key] = val
        except Exception:
            pass  # FRED table doesn't exist, keep fallback

    return metrics


@router.get("/scenario")
def get_scenario():
    db = get_databricks_db()
    T = db.full_schema
    try:
        return {"data": db.query(f"SELECT scenario_date, day_number, phase, brent_usd_bbl, aud_usd, adelaide_ulp_cpl, iea_days_coverage, opec_supply_mbpd FROM {T}.conflict_scenario_overlay ORDER BY day_number")}
    except Exception as e:
        raise HTTPException(500, str(e))

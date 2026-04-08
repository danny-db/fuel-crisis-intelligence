"""Forecast router — ai_forecast() powered projections."""
import logging
from fastapi import APIRouter, HTTPException
from backend.database import get_databricks_db

logger = logging.getLogger("uvicorn.error")
router = APIRouter()


@router.get("/forecast/fuel-prices")
def get_fuel_price_forecast():
    """Historical quarterly + ai_forecast projected fuel prices."""
    db = get_databricks_db()
    T = db.full_schema

    try:
        # Historical quarterly data
        historical = db.query(f"""
            SELECT CONCAT(Year, '-', Quarter) as period,
                   MAKE_DATE(CAST(Year AS INT),
                     CASE Quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 4 WHEN 'Q3' THEN 7 WHEN 'Q4' THEN 10 END,
                     1) as ds,
                   CAST(Regular_unleaded_petrol_91_RON_cpl AS DOUBLE) AS ulp_price,
                   CAST(Automotive_diesel_cpl AS DOUBLE) AS diesel_price,
                   'actual' as series_type
            FROM {T}.petroleum_au_fuel_prices
            WHERE Year IS NOT NULL AND Quarter IS NOT NULL
            ORDER BY ds
        """)
        logger.info(f"Forecast: {len(historical)} historical quarterly rows")

        # ai_forecast for ULP + Diesel (multi-value)
        forecast = db.query(f"""
            WITH quarterly_prices AS (
                SELECT MAKE_DATE(CAST(Year AS INT),
                       CASE Quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 4 WHEN 'Q3' THEN 7 WHEN 'Q4' THEN 10 END,
                       1) AS ds,
                       CAST(Regular_unleaded_petrol_91_RON_cpl AS DOUBLE) AS ulp_price,
                       CAST(Automotive_diesel_cpl AS DOUBLE) AS diesel_price
                FROM {T}.petroleum_au_fuel_prices
                WHERE Year IS NOT NULL AND Quarter IS NOT NULL
            )
            SELECT ds,
                   ROUND(ulp_price_forecast, 1) as ulp_forecast,
                   ROUND(ulp_price_upper, 1) as ulp_upper,
                   ROUND(ulp_price_lower, 1) as ulp_lower,
                   ROUND(diesel_price_forecast, 1) as diesel_forecast,
                   ROUND(diesel_price_upper, 1) as diesel_upper,
                   ROUND(diesel_price_lower, 1) as diesel_lower,
                   'forecast' as series_type
            FROM AI_FORECAST(
                TABLE(quarterly_prices),
                horizon => '2029-01-01',
                time_col => 'ds',
                value_col => ARRAY('ulp_price', 'diesel_price')
            )
        """)
        logger.info(f"Forecast: {len(forecast)} forecast rows via ai_forecast()")

        # Brent crude monthly forecast (if table exists)
        brent_historical = []
        brent_forecast = []
        try:
            brent_historical = db.query(f"""
                SELECT DATE_TRUNC('MONTH', date) as ds,
                       ROUND(AVG(price_usd_per_barrel), 2) as brent_price,
                       'actual' as series_type
                FROM {T}.brent_crude_oil_prices
                GROUP BY 1 ORDER BY 1
            """)
            logger.info(f"Brent historical: {len(brent_historical)} months")

            brent_forecast = db.query(f"""
                WITH brent_monthly AS (
                    SELECT DATE_TRUNC('MONTH', date) AS ds,
                           AVG(price_usd_per_barrel) AS brent_price
                    FROM {T}.brent_crude_oil_prices GROUP BY 1
                )
                SELECT ds,
                       ROUND(brent_price_forecast, 2) as brent_forecast,
                       ROUND(brent_price_upper, 2) as brent_upper,
                       ROUND(brent_price_lower, 2) as brent_lower,
                       'forecast' as series_type
                FROM AI_FORECAST(
                    TABLE(brent_monthly),
                    horizon => '2028-01-01',
                    time_col => 'ds',
                    value_col => 'brent_price'
                )
            """)
            logger.info(f"Brent forecast: {len(brent_forecast)} months via ai_forecast()")
        except Exception as e:
            logger.warning(f"Brent forecast unavailable: {e}")

        return {
            "historical": [dict(r) for r in historical],
            "forecast": [dict(r) for r in forecast],
            "brent_historical": [dict(r) for r in brent_historical],
            "brent_forecast": [dict(r) for r in brent_forecast],
            "method": "AI_FORECAST()",
            "description": "Databricks AI_FORECAST — automated ML time series forecasting"
        }
    except Exception as e:
        logger.error(f"Forecast FAILED: {e}", exc_info=True)
        raise HTTPException(500, str(e))

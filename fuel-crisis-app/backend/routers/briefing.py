"""AI Briefing router — PM crisis briefing via FMAPI."""
import os
import logging
import requests as req
from fastapi import APIRouter, HTTPException
from backend.database import get_databricks_db

logger = logging.getLogger("uvicorn.error")
router = APIRouter()


@router.post("/generate")
def generate_briefing():
    model = os.getenv("FMAPI_MODEL", "databricks-claude-sonnet-4-6")
    logger.info(f"Briefing: starting with model={model}")

    db = get_databricks_db()
    sdk = db.sdk
    host = sdk.config.host.rstrip("/")
    headers = sdk.config.authenticate()
    headers["Content-Type"] = "application/json"
    T = db.full_schema

    context_parts = []
    queries = {
        "Brent Crude": f"SELECT ROUND(price_usd_per_barrel, 2) as price, date FROM {T}.brent_crude_oil_prices ORDER BY date DESC LIMIT 1",
        "AUD/USD": f"SELECT ROUND(aud_usd_rate, 4) as rate, date FROM {T}.aud_usd_exchange_rate ORDER BY date DESC LIMIT 1",
        "Iran OPEC": f"SELECT country, value_tbpd, period FROM {T}.opec_production_by_country WHERE country = 'Iran' ORDER BY period DESC LIMIT 1",
        "Refineries": f"SELECT name, operator, state, capacity_bpd FROM {T}.fuel_infrastructure_refineries WHERE status = 'Operational'",
        "Scenario Day 14": f"SELECT phase, brent_usd_bbl, aud_usd, adelaide_ulp_cpl, iea_days_coverage FROM {T}.conflict_scenario_overlay WHERE day_number = 14",
    }
    for label, sql in queries.items():
        try:
            result = db.query(sql)
            context_parts.append(f"**{label}:** {result}")
            logger.info(f"Briefing data: {label} OK ({len(result)} rows)")
        except Exception as e:
            context_parts.append(f"**{label}:** unavailable ({e})")
            logger.warning(f"Briefing data: {label} FAILED: {e}")

    prompt = f"""You are the Chief Energy Security Advisor to the Australian Prime Minister.

An Iran-US military conflict has erupted. The Strait of Hormuz is threatened. Australia imports ~90% of refined fuel.

Using the data below, write a concise classified briefing note using markdown formatting.

## FORMATTING RULES — follow these exactly:
- Use ## for section headings (not #, not plain text)
- Use bullet points (- ) for all lists, NOT tables
- Use **bold** for key numbers and country names
- Keep paragraphs short (2-3 sentences max)
- NO markdown tables — use bullet point lists instead
- NO pipe characters

## REQUIRED SECTIONS:

## 1. Situation Summary
2-3 sentences on the current threat.

## 2. Immediate Impact
- Brent crude price and trajectory
- AUD/USD impact
- Domestic pump price projection
- Supply chain disruption timeline

## 3. Critical Vulnerabilities
- Only **4 operational refineries** (list each with state and capacity as bullet points)
- IEA reserve days vs 90-day obligation
- Freight dependency on diesel (75% road)

## 4. 14-Day Scenario Projection
List day milestones as bullet points (Day 0, Day 3, Day 7, Day 14) with key metrics.

## 5. Recommended Actions
5 specific, numbered, actionable recommendations for the PM.

## DATA:
{chr(10).join(context_parts)}

Be specific with numbers. Keep it under 800 words."""

    logger.info(f"Briefing: calling FMAPI at {host}/serving-endpoints/{model}/invocations")
    try:
        resp = req.post(f"{host}/serving-endpoints/{model}/invocations", headers=headers,
                        json={"messages": [{"role": "user", "content": prompt}], "max_tokens": 2000, "temperature": 0.3}, timeout=90)
        logger.info(f"Briefing FMAPI response: {resp.status_code}")
        resp.raise_for_status()
        choices = resp.json().get("choices", [])
        if choices:
            content = choices[0].get("message", {}).get("content", "")
            logger.info(f"Briefing generated: {len(content)} chars")
            return {"briefing": content, "model": model}
        return {"briefing": "No response generated", "model": model}
    except Exception as e:
        logger.error(f"Briefing FMAPI failed: {e}")
        raise HTTPException(502, f"FMAPI call failed: {e}")

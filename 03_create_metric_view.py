#!/usr/bin/env python3
"""
Create the SA DEM Fuel Crisis Metric View via Databricks SQL.
"""
import subprocess
import json

PROFILE = "e2-demo-west"
WAREHOUSE_ID = "e9b34f7a2e4b0561"

metric_view_sql = """
CREATE OR REPLACE VIEW danny_catalog.dem_schema.fuel_crisis_metrics
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "SA Fuel Crisis KPIs for Department of Energy and Mining"
source: danny_catalog.dem_schema.aip_terminal_gate_prices
dimensions:
  - name: Year Period
    expr: year_period
  - name: Fuel Type
    expr: fuel_type
  - name: City
    expr: city
measures:
  - name: Average Price (c/L)
    expr: AVG(price_cpl)
  - name: Max Price (c/L)
    expr: MAX(price_cpl)
  - name: Min Price (c/L)
    expr: MIN(price_cpl)
  - name: Price Volatility
    expr: STDDEV(price_cpl)
  - name: Record Count
    expr: COUNT(*)
$$
"""

# Execute via Databricks SQL Statement API
payload = {
    "warehouse_id": WAREHOUSE_ID,
    "statement": metric_view_sql,
    "wait_timeout": "30s"
}

result = subprocess.run(
    [
        "databricks", "api", "post",
        "/api/2.0/sql/statements",
        "--json", json.dumps(payload),
        "--profile", PROFILE
    ],
    capture_output=True,
    text=True
)

if result.returncode == 0:
    response = json.loads(result.stdout)
    status = response.get("status", {}).get("state", "unknown")
    print(f"Metric view creation status: {status}")
    if status == "SUCCEEDED":
        print("Metric view created: danny_catalog.dem_schema.fuel_crisis_metrics")
    else:
        error = response.get("status", {}).get("error", {})
        print(f"Details: {json.dumps(response.get('status', {}), indent=2)}")
else:
    print(f"Error: {result.stderr}")
    print(f"stdout: {result.stdout}")

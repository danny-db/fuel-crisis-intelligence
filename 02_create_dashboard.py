#!/usr/bin/env python3
"""Create the SA DEM Fuel Crisis AI/BI Dashboard via Lakeview REST API."""
import sys
sys.path.insert(0, "/Users/danny.wong/.claude/plugins/cache/fe-vibe/fe-databricks-tools/1.1.1/skills/databricks-lakeview-dashboard/resources")
from lakeview_builder import LakeviewDashboard
import json, subprocess

PROFILE = "e2-demo-west"
WAREHOUSE_ID = "e9b34f7a2e4b0561"
PARENT_PATH = "/Users/danny.wong@databricks.com"
T = "danny_catalog.dem_schema"

dashboard = LakeviewDashboard("SA Fuel Crisis Intelligence")

# ---- DATASETS ----
dashboard.add_dataset("fuel_prices", "Fuel Prices",
    f"SELECT year_period, fuel_type, city, price_cpl FROM {T}.aip_terminal_gate_prices")

dashboard.add_dataset("adelaide_prices", "Adelaide Prices",
    f"SELECT year_period, fuel_type, price_cpl FROM {T}.aip_terminal_gate_prices WHERE city = 'Adelaide' ORDER BY year_period")

dashboard.add_dataset("latest_ulp", "Latest ULP",
    f"SELECT ROUND(AVG(price_cpl),1) as price_cpl FROM {T}.aip_terminal_gate_prices WHERE city='Adelaide' AND fuel_type='ULP' AND year_period=(SELECT MAX(year_period) FROM {T}.aip_terminal_gate_prices)")

dashboard.add_dataset("latest_diesel", "Latest Diesel",
    f"SELECT ROUND(AVG(price_cpl),1) as price_cpl FROM {T}.aip_terminal_gate_prices WHERE city='Adelaide' AND fuel_type='Diesel' AND year_period=(SELECT MAX(year_period) FROM {T}.aip_terminal_gate_prices)")

dashboard.add_dataset("petrol_sales", "Petroleum Sales",
    f"SELECT source_sheet, data_category, COUNT(*) as record_count FROM {T}.petroleum_sales_by_state GROUP BY source_sheet, data_category")

dashboard.add_dataset("petrol_sales_count", "Sales Count",
    f"SELECT COUNT(*) as cnt FROM {T}.petroleum_sales_by_state")

dashboard.add_dataset("nger_top", "Top Emitters",
    f"SELECT Organisation_name, CAST(REPLACE(`Total_scope_1_emissions_t_CO2-e`, ',', '') AS BIGINT) as scope1_tonnes, CAST(REPLACE(`Total_scope_2_emissions_t_CO2-e`, ',', '') AS BIGINT) as scope2_tonnes FROM {T}.nger_corporate_emissions WHERE `Total_scope_1_emissions_t_CO2-e` != '' ORDER BY scope1_tonnes DESC LIMIT 15")

dashboard.add_dataset("nger_count", "Emitter Count",
    f"SELECT COUNT(*) as cnt FROM {T}.nger_corporate_emissions")

dashboard.add_dataset("stocks", "Petroleum Stocks",
    f"SELECT source_sheet, data_category, COUNT(*) as record_count FROM {T}.petroleum_stocks GROUP BY source_sheet, data_category")

# ---- PAGE 1: Executive Overview ----
dashboard.pages = []
dashboard._current_page = None
dashboard.add_page("Executive Overview")

dashboard.add_counter("latest_ulp", "price_cpl", "AVG", title="Adelaide ULP (c/L)",
    position={"x": 0, "y": 0, "width": 1, "height": 2})
dashboard.add_counter("latest_diesel", "price_cpl", "AVG", title="Adelaide Diesel (c/L)",
    position={"x": 1, "y": 0, "width": 1, "height": 2})
dashboard.add_counter("petrol_sales_count", "cnt", "SUM", title="Petroleum Records",
    position={"x": 2, "y": 0, "width": 1, "height": 2})
dashboard.add_counter("nger_count", "cnt", "SUM", title="Reporting Corporations",
    position={"x": 3, "y": 0, "width": 1, "height": 2})

dashboard.add_filter_dropdown("fuel_prices", "city", title="City",
    position={"x": 4, "y": 0, "width": 2, "height": 2})

dashboard.add_line_chart("adelaide_prices", "year_period", "price_cpl", "AVG",
    title="Adelaide Fuel Price Trends (Annual Avg c/L)",
    position={"x": 0, "y": 2, "width": 6, "height": 5},
    color_field="fuel_type")

dashboard.add_bar_chart("petrol_sales", "source_sheet", "record_count", "SUM",
    title="Petroleum Data by Category",
    position={"x": 0, "y": 7, "width": 6, "height": 4})

# ---- PAGE 2: Fuel Supply & Stocks ----
dashboard.add_page("Fuel Supply & Stocks")

dashboard.add_table("stocks",
    columns=[
        {"field": "source_sheet", "title": "Data Sheet", "type": "string"},
        {"field": "data_category", "title": "Category", "type": "string"},
        {"field": "record_count", "title": "Records", "type": "integer"},
    ],
    title="Petroleum Stock Data Summary",
    position={"x": 0, "y": 0, "width": 6, "height": 5})

dashboard.add_table("petrol_sales",
    columns=[
        {"field": "source_sheet", "title": "Data Sheet", "type": "string"},
        {"field": "data_category", "title": "Category", "type": "string"},
        {"field": "record_count", "title": "Records", "type": "integer"},
    ],
    title="Petroleum Sales Data Summary",
    position={"x": 0, "y": 5, "width": 6, "height": 5})

# ---- PAGE 3: Industry Impact & Emissions ----
dashboard.add_page("Industry Impact & Emissions")

dashboard.add_bar_chart("nger_top", "Organisation_name", "scope1_tonnes", "SUM",
    title="Top 15 Corporate Emitters (Scope 1, t CO2-e)",
    position={"x": 0, "y": 0, "width": 6, "height": 6},
    sort_descending=True)

# ---- PAGE 4: All Fuel Prices ----
dashboard.add_page("Fuel Price Detail")

dashboard.add_bar_chart("fuel_prices", "city", "price_cpl", "AVG",
    title="Average Fuel Price by City",
    position={"x": 0, "y": 0, "width": 6, "height": 5},
    color_field="fuel_type")

dashboard.add_filter_dropdown("fuel_prices", "fuel_type", title="Fuel Type",
    position={"x": 0, "y": 5, "width": 2, "height": 2})

# ---- Create via API ----
payload = dashboard.get_api_payload(WAREHOUSE_ID, PARENT_PATH)
result = subprocess.run(
    ["databricks", "api", "post", "/api/2.0/lakeview/dashboards",
     "--json", json.dumps(payload), "--profile", PROFILE],
    capture_output=True, text=True)

if result.returncode == 0:
    response = json.loads(result.stdout)
    did = response.get("dashboard_id", "?")
    print(f"Dashboard created: https://e2-demo-field-eng.cloud.databricks.com/sql/dashboardsv3/{did}")

    # Publish
    subprocess.run(
        ["databricks", "api", "post", f"/api/2.0/lakeview/dashboards/{did}/published",
         "--json", json.dumps({"warehouse_id": WAREHOUSE_ID, "embed_credentials": True}),
         "--profile", PROFILE],
        capture_output=True, text=True)
    print("Dashboard published")
else:
    print(f"Error: {result.stderr}\n{result.stdout}")

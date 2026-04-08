# Databricks notebook source
# MAGIC %md
# MAGIC # Create 6-Page Fuel Crisis Story Dashboard
# MAGIC
# MAGIC Creates and publishes the AI/BI Dashboard via Lakeview REST API.
# MAGIC
# MAGIC **Pages:**
# MAGIC 1. The Trigger — Global Oil Shock
# MAGIC 2. Australia's Exposure
# MAGIC 3. Impact on Consumers
# MAGIC 4. State-by-State Vulnerability
# MAGIC 5. Industry & Energy at Risk
# MAGIC 6. Infrastructure Vulnerability

# COMMAND ----------

# DBTITLE v1,Configuration
# Read parameters from DAB base_parameters / widgets, with sensible fallbacks
dbutils.widgets.text("warehouse_id", "4c97f6c0eff73127")
dbutils.widgets.text("catalog", "danny_catalog")
dbutils.widgets.text("schema", "dem_schema")

WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
T = f"{CATALOG}.{SCHEMA}"

import json, requests

# Get workspace URL and token from notebook context
host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
api_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
base_url = f"https://{host}"

# Derive PARENT_PATH from the current user
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
PARENT_PATH = f"/Users/{current_user}"

print(f"Warehouse:   {WAREHOUSE_ID}")
print(f"Catalog:     {CATALOG}")
print(f"Schema:      {SCHEMA}")
print(f"Parent path: {PARENT_PATH}")

# COMMAND ----------

# DBTITLE v1,Import Dashboard Builder
# lakeview_builder.py is deployed alongside this notebook via DAB
import sys, os
# On Databricks, workspace files are in the same directory as the notebook
notebook_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
# Also try the bundle root (files are synced to workspace)
for p in ["/Workspace" + notebook_dir, "/Workspace/Users/danny.wong@databricks.com/.bundle/fuel-crisis-intelligence/dev/files", "."]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from lakeview_builder import LakeviewDashboard
    print("Loaded lakeview_builder from workspace")
except ImportError:
    # Fallback: inline minimal builder if import fails
    print("WARNING: Could not import lakeview_builder. Using %run fallback...")
    raise

# COMMAND ----------

# DBTITLE v1,Build Dashboard
dashboard = LakeviewDashboard("Australian Fuel Crisis Intelligence — Story")

# ============================================================
# DATASETS
# ============================================================

# Page 1: Global Oil Shock (using conflict_scenario_overlay which has brent_usd_bbl and aud_usd)
dashboard.add_dataset("brent_daily", "Brent Crude Daily",
    f"SELECT scenario_date as date, brent_usd_bbl as price_usd_per_barrel FROM {T}.conflict_scenario_overlay ORDER BY scenario_date")

dashboard.add_dataset("brent_latest", "Brent Latest",
    f"SELECT brent_usd_bbl as price_usd_per_barrel FROM {T}.conflict_scenario_overlay ORDER BY scenario_date DESC LIMIT 1")

dashboard.add_dataset("aud_latest", "AUD/USD Latest",
    f"SELECT aud_usd as aud_usd_rate FROM {T}.conflict_scenario_overlay ORDER BY scenario_date DESC LIMIT 1")

dashboard.add_dataset("opec_latest", "OPEC Production (Latest Month)",
    f"SELECT country, value_tbpd FROM {T}.opec_production_by_country WHERE period = (SELECT MAX(period) FROM {T}.opec_production_by_country) AND value_tbpd IS NOT NULL ORDER BY value_tbpd DESC")

dashboard.add_dataset("iran_share", "Iran OPEC Share",
    f"""SELECT ROUND(
        (SELECT value_tbpd FROM {T}.opec_production_by_country WHERE country='Iran' AND period=(SELECT MAX(period) FROM {T}.opec_production_by_country))
        / (SELECT SUM(value_tbpd) FROM {T}.opec_production_by_country WHERE period=(SELECT MAX(period) FROM {T}.opec_production_by_country))
        * 100, 1
    ) as iran_pct""")

# Page 2: Australia's Exposure
dashboard.add_dataset("imports_by_country", "Crude Imports by Country",
    f"""SELECT
        CASE WHEN COALESCE(TRIM(col), '') = '' THEN 'Unknown' ELSE TRIM(col) END as source_country,
        COUNT(*) as record_count
    FROM {T}.petroleum_imports_by_country
    WHERE source_sheet LIKE '%Imports%'
    GROUP BY 1
    ORDER BY record_count DESC
    LIMIT 15""")

dashboard.add_dataset("iea_days", "IEA Days of Coverage",
    f"SELECT * FROM {T}.petroleum_iea_days_coverage")

dashboard.add_dataset("iea_latest", "IEA Latest",
    f"SELECT COUNT(*) as cnt FROM {T}.petroleum_iea_days_coverage")

dashboard.add_dataset("oecd_prices", "OECD Fuel Prices",
    f"SELECT * FROM {T}.petroleum_fuel_prices_oecd")

# Page 3: Consumer Impact
dashboard.add_dataset("wholesale_by_city", "Wholesale Prices by City",
    f"""SELECT city, fuel_type, ROUND(AVG(price_cpl), 1) as avg_price_cpl
    FROM {T}.aip_terminal_gate_prices
    WHERE year_period = (SELECT MAX(year_period) FROM {T}.aip_terminal_gate_prices)
    GROUP BY city, fuel_type
    ORDER BY city""")

dashboard.add_dataset("cpi_fuel", "Scenario Pump Prices by Phase",
    f"""SELECT phase, day_number, adelaide_ulp_cpl, brent_usd_bbl, aud_usd
    FROM {T}.conflict_scenario_overlay
    ORDER BY day_number""")

dashboard.add_dataset("aud_trend", "AUD/USD Trend (Scenario)",
    f"SELECT scenario_date as date, aud_usd as aud_usd_rate FROM {T}.conflict_scenario_overlay ORDER BY scenario_date")

dashboard.add_dataset("adelaide_ulp", "Adelaide ULP Latest",
    f"""SELECT ROUND(AVG(price_cpl), 1) as price_cpl
    FROM {T}.aip_terminal_gate_prices
    WHERE city = 'Adelaide' AND fuel_type = 'ULP'
      AND year_period = (SELECT MAX(year_period) FROM {T}.aip_terminal_gate_prices)""")

# Page 4: State Vulnerability
dashboard.add_dataset("sales_by_state", "Sales by State",
    f"SELECT * FROM {T}.petroleum_sales_by_state_national")

dashboard.add_dataset("sales_state_agg", "Sales by State Aggregated",
    f"""SELECT source_sheet, COUNT(*) as record_count
    FROM {T}.petroleum_sales_by_state_national
    GROUP BY source_sheet
    ORDER BY record_count DESC""")

# Page 5: Industry & Energy
dashboard.add_dataset("nger_top15", "Top 15 Corporate Emitters",
    f"""SELECT Organisation_name,
        CAST(REPLACE(`Total_scope_1_emissions_t_CO2-e`, ',', '') AS BIGINT) as scope1_tonnes
    FROM {T}.nger_corporate_emissions
    WHERE `Total_scope_1_emissions_t_CO2-e` != ''
    ORDER BY scope1_tonnes DESC
    LIMIT 15""")

dashboard.add_dataset("power_by_fuel", "Power Stations by Fuel",
    f"""SELECT Primary_fuel, COUNT(*) as station_count
    FROM {T}.nger_electricity_sector
    WHERE Primary_fuel IS NOT NULL AND Primary_fuel != ''
    GROUP BY Primary_fuel
    ORDER BY station_count DESC""")

dashboard.add_dataset("power_count", "Total Power Stations",
    f"SELECT COUNT(*) as cnt FROM {T}.nger_electricity_sector")

# Page 6: Infrastructure
dashboard.add_dataset("refineries", "Refineries",
    f"SELECT * FROM {T}.fuel_infrastructure_refineries")

dashboard.add_dataset("refinery_count", "Operational Refineries",
    f"SELECT COUNT(*) as cnt FROM {T}.fuel_infrastructure_refineries WHERE status = 'Operational'")

dashboard.add_dataset("terminals", "Fuel Terminals",
    f"SELECT * FROM {T}.fuel_infrastructure_terminals")

# ============================================================
# PAGE 1: The Trigger — Global Oil Shock
# ============================================================
dashboard.pages = []
dashboard._current_page = None
dashboard.add_page("The Trigger — Global Oil Shock")

dashboard.add_counter("brent_latest", "price_usd_per_barrel", "AVG",
    title="Brent Crude (USD/bbl)",
    position={"x": 0, "y": 0, "width": 2, "height": 2})

dashboard.add_counter("aud_latest", "aud_usd_rate", "AVG",
    title="AUD/USD",
    position={"x": 2, "y": 0, "width": 2, "height": 2})

dashboard.add_counter("iran_share", "iran_pct", "AVG",
    title="Iran % of OPEC",
    position={"x": 4, "y": 0, "width": 2, "height": 2})

dashboard.add_line_chart("brent_daily", "date", "price_usd_per_barrel", "AVG",
    time_grain="DAY",
    title="Brent Crude Oil — Daily Price (USD/bbl)",
    position={"x": 0, "y": 2, "width": 6, "height": 5})

dashboard.add_bar_chart("opec_latest", "country", "value_tbpd", "SUM",
    title="OPEC Production by Country (Thousand bbl/day)",
    position={"x": 0, "y": 7, "width": 6, "height": 5},
    sort_descending=True)

# ============================================================
# PAGE 2: Australia's Exposure
# ============================================================
dashboard.add_page("Australia's Exposure")

dashboard.add_pie_chart("imports_by_country", "record_count", "source_country", "SUM",
    title="Crude Oil Imports — Source Countries",
    position={"x": 0, "y": 0, "width": 3, "height": 5})

dashboard.add_counter("iea_latest", "cnt", "SUM",
    title="IEA Coverage Records",
    position={"x": 3, "y": 0, "width": 1, "height": 2})

dashboard.add_table("iea_days",
    columns=[{"field": "source_sheet", "title": "Source", "type": "string"}],
    title="IEA Days of Net Import Coverage",
    position={"x": 3, "y": 2, "width": 3, "height": 4})

dashboard.add_table("oecd_prices",
    columns=[{"field": "source_sheet", "title": "Source", "type": "string"}],
    title="Australia vs OECD Fuel Prices",
    position={"x": 0, "y": 5, "width": 6, "height": 5})

# ============================================================
# PAGE 3: Impact on Consumers
# ============================================================
dashboard.add_page("Impact on Consumers")

dashboard.add_counter("adelaide_ulp", "price_cpl", "AVG",
    title="Adelaide ULP (c/L)",
    position={"x": 0, "y": 0, "width": 2, "height": 2})

dashboard.add_bar_chart("wholesale_by_city", "city", "avg_price_cpl", "AVG",
    title="Wholesale Fuel Prices by City (c/L, Latest Year)",
    position={"x": 0, "y": 2, "width": 6, "height": 4},
    color_field="fuel_type")

dashboard.add_table("cpi_fuel",
    columns=[
        {"field": "phase", "title": "Phase", "type": "string"},
        {"field": "day_number", "title": "Day", "type": "integer"},
        {"field": "adelaide_ulp_cpl", "title": "Adelaide ULP (c/L)", "type": "float"},
        {"field": "brent_usd_bbl", "title": "Brent (USD/bbl)", "type": "float"},
        {"field": "aud_usd", "title": "AUD/USD", "type": "float"},
    ],
    title="Scenario: Consumer Fuel Price Projections",
    position={"x": 0, "y": 6, "width": 3, "height": 4})

dashboard.add_line_chart("aud_trend", "date", "aud_usd_rate", "AVG",
    time_grain="DAY",
    title="AUD/USD Exchange Rate — Scenario Projection",
    position={"x": 3, "y": 6, "width": 3, "height": 4})

# ============================================================
# PAGE 4: State-by-State Vulnerability
# ============================================================
dashboard.add_page("State-by-State Vulnerability")

dashboard.add_filter_dropdown("sales_by_state", "source_sheet",
    title="Data Source",
    position={"x": 0, "y": 0, "width": 2, "height": 2})

dashboard.add_bar_chart("sales_state_agg", "source_sheet", "record_count", "SUM",
    title="Petroleum Data Records by State/Territory",
    position={"x": 0, "y": 2, "width": 6, "height": 5},
    sort_descending=True)

dashboard.add_table("sales_by_state",
    columns=[{"field": "source_sheet", "title": "Data Source", "type": "string"}],
    title="Petroleum Sales — State & Territory Detail",
    position={"x": 0, "y": 7, "width": 6, "height": 5})

# ============================================================
# PAGE 5: Industry & Energy at Risk
# ============================================================
dashboard.add_page("Industry & Energy at Risk")

dashboard.add_counter("power_count", "cnt", "SUM",
    title="Total Power Stations",
    position={"x": 0, "y": 0, "width": 2, "height": 2})

dashboard.add_bar_chart("nger_top15", "Organisation_name", "scope1_tonnes", "SUM",
    title="Top 15 Corporate Emitters (Scope 1, t CO2-e)",
    position={"x": 0, "y": 2, "width": 6, "height": 5},
    sort_descending=True)

dashboard.add_bar_chart("power_by_fuel", "Primary_fuel", "station_count", "SUM",
    title="Power Stations by Primary Fuel Type",
    position={"x": 0, "y": 7, "width": 6, "height": 4},
    sort_descending=True)

# ============================================================
# PAGE 6: Infrastructure Vulnerability
# ============================================================
dashboard.add_page("Infrastructure Vulnerability")

dashboard.add_counter("refinery_count", "cnt", "SUM",
    title="Operational Refineries",
    position={"x": 0, "y": 0, "width": 2, "height": 2})

dashboard.add_table("refineries",
    columns=[
        {"field": "name", "title": "Refinery", "type": "string"},
        {"field": "operator", "title": "Operator", "type": "string"},
        {"field": "state", "title": "State", "type": "string"},
        {"field": "status", "title": "Status", "type": "string"},
        {"field": "capacity_bpd", "title": "Capacity (bbl/day)", "type": "integer"},
    ],
    title="Australian Refineries",
    position={"x": 0, "y": 2, "width": 6, "height": 4})

dashboard.add_table("terminals",
    columns=[
        {"field": "name", "title": "Terminal", "type": "string"},
        {"field": "operator", "title": "Operator", "type": "string"},
        {"field": "state", "title": "State", "type": "string"},
        {"field": "type", "title": "Type", "type": "string"},
    ],
    title="Fuel Import Terminals",
    position={"x": 0, "y": 6, "width": 6, "height": 6})

print("Dashboard built: 6 pages, 21 datasets")

# COMMAND ----------

# DBTITLE v1,Create & Publish via REST API (idempotent)
DASHBOARD_TITLE = "Australian Fuel Crisis Intelligence — Story"

# ── Idempotent: delete existing dashboard with same name before creating ──
try:
    list_resp = requests.get(
        f"{base_url}/api/2.0/lakeview/dashboards",
        headers=api_headers,
        params={"page_size": 100},
    )
    if list_resp.status_code == 200:
        for d in list_resp.json().get("dashboards", []):
            if d.get("display_name") == DASHBOARD_TITLE:
                old_id = d.get("dashboard_id")
                del_resp = requests.delete(
                    f"{base_url}/api/2.0/lakeview/dashboards/{old_id}",
                    headers=api_headers,
                )
                print(f"Deleted existing dashboard {old_id}: {del_resp.status_code}")
except Exception as e:
    print(f"Warning — could not check for existing dashboard: {e}")

payload = dashboard.get_api_payload(WAREHOUSE_ID, PARENT_PATH)

# Create dashboard
resp = requests.post(
    f"{base_url}/api/2.0/lakeview/dashboards",
    headers=api_headers,
    json=payload,
)

if resp.status_code == 200:
    result = resp.json()
    dashboard_id = result.get("dashboard_id", "?")
    print(f"Dashboard created: {base_url}/sql/dashboardsv3/{dashboard_id}")

    # Publish
    pub_resp = requests.post(
        f"{base_url}/api/2.0/lakeview/dashboards/{dashboard_id}/published",
        headers=api_headers,
        json={"warehouse_id": WAREHOUSE_ID, "embed_credentials": True},
    )
    if pub_resp.status_code == 200:
        print("Dashboard published successfully")
    else:
        print(f"Publish warning: {pub_resp.status_code} {pub_resp.text}")

    # Store dashboard ID for downstream use
    dbutils.jobs.taskValues.set(key="dashboard_id", value=dashboard_id)
else:
    print(f"Error creating dashboard: {resp.status_code}")
    print(resp.text)

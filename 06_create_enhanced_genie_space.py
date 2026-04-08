# Databricks notebook source
# MAGIC %md
# MAGIC # Create Enhanced Genie Space — Australian Fuel Crisis Intelligence
# MAGIC
# MAGIC **Reproducible notebook** for creating the Genie space in any workspace.
# MAGIC
# MAGIC Creates a Genie space with:
# MAGIC - 10 crisis-framed sample questions
# MAGIC - 8 example SQL queries
# MAGIC - Comprehensive text instructions covering ~38 tables
# MAGIC
# MAGIC **How it works:** Tables are referenced in the text instructions and example SQLs.
# MAGIC Genie auto-discovers which tables to query based on these references.
# MAGIC The `data_sources` field is left empty (no table restrictions).
# MAGIC
# MAGIC **To recreate in a new workspace:** Update `WAREHOUSE_ID`, `PARENT_PATH`,
# MAGIC `CATALOG`, and `SCHEMA` in the Configuration cell, then run all cells.

# COMMAND ----------

# DBTITLE v1,Configuration
# Read parameters from DAB base_parameters / widgets, with sensible fallbacks
dbutils.widgets.text("warehouse_id", "4c97f6c0eff73127")
dbutils.widgets.text("catalog", "danny_catalog")
dbutils.widgets.text("schema", "dem_schema")

WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
CATALOG      = dbutils.widgets.get("catalog")
SCHEMA       = dbutils.widgets.get("schema")
T = f"{CATALOG}.{SCHEMA}"

import json, uuid, requests

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
api_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
base_url = f"https://{host}"

# Derive PARENT_PATH from the current user
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
PARENT_PATH = f"/Users/{current_user}"

def gen_id():
    return uuid.uuid4().hex

print(f"Workspace:   {base_url}")
print(f"Schema:      {T}")
print(f"Warehouse:   {WAREHOUSE_ID}")
print(f"Parent path: {PARENT_PATH}")

# COMMAND ----------

# DBTITLE v1,Verify Tables Exist
print("Checking tables in schema...")
tables_df = spark.sql(f"SHOW TABLES IN {T}")
existing_tables = [row.tableName for row in tables_df.collect()]
print(f"Found {len(existing_tables)} tables in {T}")
for t in sorted(existing_tables):
    print(f"  {t}")

# COMMAND ----------

# DBTITLE v1,Define Sample Questions, Example SQLs & Instructions
ALL_TABLES = sorted([
    f"{T}.aip_retail_diesel_prices",
    f"{T}.aip_retail_ulp_prices",
    f"{T}.aip_terminal_gate_prices",
    f"{T}.aud_usd_exchange_rate",
    f"{T}.australia_trade_balance",
    f"{T}.brent_crude_oil_prices",
    f"{T}.conflict_scenario_overlay",
    f"{T}.cpi_automotive_fuel_by_city",
    f"{T}.disaster_events",
    f"{T}.domestic_freight_by_mode",
    f"{T}.energy_consumption_by_state",
    f"{T}.fuel_infrastructure_depots",
    f"{T}.fuel_infrastructure_refineries",
    f"{T}.fuel_infrastructure_terminals",
    f"{T}.geoscience_au_mineral_deposits",
    f"{T}.nger_corporate_emissions",
    f"{T}.nger_electricity_sector",
    f"{T}.nger_state_emissions_by_industry",
    f"{T}.opec_production_by_country",
    f"{T}.petroleum_au_fuel_prices",
    f"{T}.petroleum_consumption_cover",
    f"{T}.petroleum_exports_by_country",
    f"{T}.petroleum_fuel_prices_oecd",
    f"{T}.petroleum_iea_days_coverage",
    f"{T}.petroleum_imports_by_country",
    f"{T}.petroleum_production",
    f"{T}.petroleum_production_by_basin",
    f"{T}.petroleum_refinery_production",
    f"{T}.petroleum_sales_by_state",
    f"{T}.petroleum_sales_by_state_national",
    f"{T}.petroleum_stock_by_product",
    f"{T}.petroleum_stocks",
    f"{T}.petroleum_supply",
    f"{T}.qld_fuel_prices",
    f"{T}.rba_exchange_rates",
    f"{T}.sa_diesel_generation_plants",
    f"{T}.sa_mineral_deposits",
    f"{T}.wa_fuelwatch_stations",
])

# NOTE: data_sources is intentionally empty. Genie auto-discovers tables
# from the text instructions and example SQL queries below.
# Using explicit table keys in data_sources causes protobuf serialization errors.
data_sources = {}

sample_questions = sorted([
    {"id": gen_id(), "question": ["How much of Australia's crude oil comes from the Middle East?"]},
    {"id": gen_id(), "question": ["If Iran goes offline, what percentage of OPEC production is lost?"]},
    {"id": gen_id(), "question": ["How many days of fuel reserves does Australia have vs the 90-day IEA obligation?"]},
    {"id": gen_id(), "question": ["What is the current Brent crude oil price and how has it trended?"]},
    {"id": gen_id(), "question": ["Which Australian states consume the most diesel?"]},
    {"id": gen_id(), "question": ["How many operational refineries does Australia have and where are they?"]},
    {"id": gen_id(), "question": ["Compare CPI automotive fuel for Adelaide vs Sydney since 2020"]},
    {"id": gen_id(), "question": ["Which SA power stations depend on gas or diesel?"]},
    {"id": gen_id(), "question": ["What are today's WA fuel station prices?"]},
    {"id": gen_id(), "question": ["What is the trade balance impact of rising oil imports?"]},
], key=lambda x: x["id"])

instructions_text = f"""This Genie space supports the **Australian Fuel Crisis Intelligence** platform.

**Scenario context:** An Iran-US military conflict has disrupted Middle East oil exports. The Strait of Hormuz is threatened. Australia imports ~90% of its refined fuel. The Prime Minister needs answers NOW.

## Key Tables & Columns

### Global Macro / Oil Markets
- **{T}.brent_crude_oil_prices**: Daily Brent crude benchmark. Columns: `date`, `price_usd_per_barrel`.
- **{T}.aud_usd_exchange_rate**: Daily AUD/USD rate. Columns: `date`, `aud_usd_rate`.
- **{T}.rba_exchange_rates**: RBA exchange rates incl. Middle East currencies (AED, SAR).
- **{T}.australia_trade_balance**: Monthly exports vs imports. Columns: `date`, `value`, `series`.
- **{T}.opec_production_by_country**: Monthly OPEC crude output. Columns: `country`, `period`, `value_tbpd`. **Iran = ~13.6% of OPEC.**

### Conflict Scenario
- **{T}.conflict_scenario_overlay**: 90-day projection if Iran goes offline. Columns: `scenario_date`, `day_number`, `phase`, `brent_usd_bbl`, `aud_usd`, `adelaide_ulp_cpl`, `iea_days_coverage`, `opec_supply_mbpd`.

### Australian Fuel Pricing
- **{T}.aip_terminal_gate_prices**: Annual wholesale prices. Columns: `year_period`, `fuel_type`, `city`, `price_cpl`.
- **{T}.aip_retail_ulp_prices** / **aip_retail_diesel_prices**: Weekly retail prices.
- **{T}.cpi_automotive_fuel_by_city**: ABS CPI index by capital city.
- **{T}.wa_fuelwatch_stations**: WA station-level daily prices with lat/long.
- **{T}.qld_fuel_prices**: QLD station-level prices.

### Supply & Reserves
- **{T}.petroleum_iea_days_coverage**: IEA days of net import coverage (target: 90 days).
- **{T}.petroleum_stock_by_product** / **petroleum_stocks**: Stock levels.

### Infrastructure
- **{T}.fuel_infrastructure_refineries**: Only 4 operational. Columns: `name`, `operator`, `state`, `status`, `capacity_bpd`.
- **{T}.fuel_infrastructure_terminals**: Bulk fuel terminals with lat/long.
- **{T}.fuel_infrastructure_depots**: Distribution depots.

### Energy & Industry
- **{T}.nger_corporate_emissions**: Use CAST(REPLACE(`Total_scope_1_emissions_t_CO2-e`, ',', '') AS BIGINT).
- **{T}.nger_electricity_sector**: Power stations. Key column: `Primary_fuel`.
- **{T}.domestic_freight_by_mode**: Freight by mode. 75% road (diesel).

### Resources
- **{T}.geoscience_au_mineral_deposits**: ~5000 deposits with lat/long. Mining = diesel-dependent.

## Notes
- Prices in `aip_terminal_gate_prices` are cents per litre (c/L).
- NGER emissions columns are strings with commas — use CAST(REPLACE(col, ',', '') AS BIGINT).
- IEA obligation is 90 days. Australia is typically below this.
"""

example_sqls = sorted([
    {"id": gen_id(), "question": ["What percentage of OPEC production does Iran represent?"],
     "sql": [f"SELECT country, value_tbpd, ROUND(value_tbpd / SUM(value_tbpd) OVER () * 100, 1) as pct_of_opec\n",
             f"FROM {T}.opec_production_by_country\n",
             f"WHERE period = (SELECT MAX(period) FROM {T}.opec_production_by_country) AND value_tbpd IS NOT NULL\n",
             "ORDER BY value_tbpd DESC;"]},
    {"id": gen_id(), "question": ["Current Brent crude oil price and 30-day trend"],
     "sql": [f"SELECT date, price_usd_per_barrel FROM {T}.brent_crude_oil_prices\n",
             f"WHERE date >= DATEADD(DAY, -30, (SELECT MAX(date) FROM {T}.brent_crude_oil_prices))\n",
             "ORDER BY date DESC;"]},
    {"id": gen_id(), "question": ["How many operational refineries does Australia have?"],
     "sql": [f"SELECT name, operator, state, status, capacity_bpd FROM {T}.fuel_infrastructure_refineries\n",
             "WHERE status = 'Operational' ORDER BY capacity_bpd DESC;"]},
    {"id": gen_id(), "question": ["Projected Brent price on day 14 of the Iran crisis"],
     "sql": [f"SELECT scenario_date, day_number, phase, brent_usd_bbl, aud_usd, adelaide_ulp_cpl, iea_days_coverage\n",
             f"FROM {T}.conflict_scenario_overlay WHERE day_number = 14;"]},
    {"id": gen_id(), "question": ["Which states consume the most diesel?"],
     "sql": [f"SELECT source_sheet, COUNT(*) as records FROM {T}.petroleum_sales_by_state_national\n",
             "GROUP BY source_sheet ORDER BY records DESC;"]},
    {"id": gen_id(), "question": ["AUD/USD exchange rate trend this year"],
     "sql": [f"SELECT date, aud_usd_rate FROM {T}.aud_usd_exchange_rate\n",
             "WHERE date >= '2026-01-01' ORDER BY date;"]},
    {"id": gen_id(), "question": ["Top 10 WA fuel station prices"],
     "sql": [f"SELECT `trading-name`, brand, address, price, latitude, longitude\n",
             f"FROM {T}.wa_fuelwatch_stations WHERE price IS NOT NULL\n",
             "ORDER BY price DESC LIMIT 10;"]},
    {"id": gen_id(), "question": ["Trade balance trend — exports vs imports"],
     "sql": [f"SELECT date, series, value FROM {T}.australia_trade_balance\n",
             "WHERE date >= '2020-01-01' ORDER BY date, series;"]},
], key=lambda x: x["id"])

# COMMAND ----------

# DBTITLE v1,Build Serialized Space Payload
serialized_space = {
    "version": 2,
    "config": {"sample_questions": sample_questions},
    "data_sources": data_sources,  # Empty — Genie auto-discovers from instructions
    "instructions": {
        "text_instructions": [{"id": gen_id(), "content": [instructions_text]}],
        "example_question_sqls": example_sqls
    }
}

# Build table_identifiers from tables that actually exist in the schema
existing_tables_df = spark.sql(f"SHOW TABLES IN {T}")
existing_table_names = [row.tableName for row in existing_tables_df.collect()]
table_identifiers = [f"{T}.{t}" for t in sorted(existing_table_names)]
print(f"  Tables to attach: {len(table_identifiers)}")

payload = {
    "title": "Australian Fuel Crisis Intelligence",
    "description": (
        "Ask questions about Australia's fuel security during a geopolitical crisis. "
        "Covers oil prices, exchange rates, OPEC production, fuel infrastructure, "
        "consumer prices, state vulnerability, and conflict scenario projections."
    ),
    "warehouse_id": WAREHOUSE_ID,
    "parent_path": PARENT_PATH,
    "table_identifiers": table_identifiers,
    "serialized_space": json.dumps(serialized_space)
}

print(f"Payload built:")
print(f"  Title: {payload['title']}")
print(f"  Tables referenced in instructions: {len(ALL_TABLES)}")
print(f"  Sample questions: {len(sample_questions)}")
print(f"  Example SQLs: {len(example_sqls)}")

# COMMAND ----------

# DBTITLE v1,Create or Skip Genie Space (idempotent)
SPACE_TITLE = "Australian Fuel Crisis Intelligence"

# Check if a space with this title already exists
existing_space_id = None
try:
    list_resp = requests.get(
        f"{base_url}/api/2.0/genie/spaces",
        headers=api_headers,
        params={"page_size": 100},
    )
    if list_resp.status_code == 200:
        spaces = list_resp.json().get("spaces", [])
        for s in spaces:
            if s.get("title", "") == SPACE_TITLE:
                existing_space_id = s.get("space_id", s.get("id"))
                break
except Exception as e:
    print(f"  Could not list existing spaces: {e}")

if existing_space_id:
    space_id = existing_space_id
    print(f"Genie space already exists — skipping creation")
    print(f"  Space ID:  {space_id}")
    print(f"  URL:       {base_url}/genie/rooms/{space_id}")
else:
    resp = requests.post(
        f"{base_url}/api/2.0/genie/spaces",
        headers=api_headers,
        json=payload,
    )

    if resp.status_code == 200:
        result = resp.json()
        space_id = result.get("space_id", result.get("id", "unknown"))
        print(f"Genie space created successfully!")
    else:
        print(f"Error creating Genie space: {resp.status_code}")
        print(resp.text)
        raise Exception(f"Genie API returned {resp.status_code}")

    print(f"  Space ID:  {space_id}")
    print(f"  URL:       {base_url}/genie/rooms/{space_id}")

print(f"  Warehouse: {WAREHOUSE_ID}")

# Attach tables via data-rooms API (the public genie/spaces API doesn't persist table_identifiers)
try:
    tables_payload = {
        "space_id": space_id,
        "display_name": SPACE_TITLE,
        "warehouse_id": WAREHOUSE_ID,
        "table_identifiers": table_identifiers,
        "run_as_type": "VIEWER",
    }
    attach_resp = requests.patch(
        f"{base_url}/api/2.0/data-rooms/{space_id}",
        headers=api_headers,
        json=tables_payload,
    )
    if attach_resp.status_code == 200:
        attached = attach_resp.json().get("table_identifiers", [])
        print(f"  Tables attached via data-rooms API: {len(attached)}")
    else:
        print(f"  Table attachment failed: {attach_resp.status_code} {attach_resp.text[:100]}")
except Exception as e:
    print(f"  Could not attach tables: {e}")

# Grant CAN_RUN to the Databricks App service principal (if app exists)
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    # Find the fuel-crisis-command-centre app SP
    apps = [a for a in w.apps.list() if a.name == "fuel-crisis-command-centre"]
    if apps:
        sp_client_id = apps[0].service_principal_client_id
        print(f"\n  Granting CAN_RUN to app SP: {sp_client_id}")
        resp = requests.put(
            f"{base_url}/api/2.0/permissions/genie/{space_id}",
            headers=api_headers,
            json={"access_control_list": [
                {"service_principal_name": sp_client_id, "permission_level": "CAN_RUN"}
            ]},
        )
        if resp.status_code == 200:
            print(f"  Permission granted")
        else:
            print(f"  Permission grant failed: {resp.status_code} {resp.text[:100]}")

        # Grant UC permissions to app SP so it can query the schema
        try:
            spark.sql(f"GRANT USE_CATALOG ON CATALOG {CATALOG} TO `{sp_client_id}`")
            spark.sql(f"GRANT USE_SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{sp_client_id}`")
            spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG}.{SCHEMA} TO `{sp_client_id}`")
            print(f"  UC grants applied for SP {sp_client_id}")
        except Exception as e:
            print(f"  UC grants failed: {e}")
    else:
        print(f"\n  App 'fuel-crisis-command-centre' not found — skip SP permission grant")
except Exception as e:
    print(f"  Could not grant app permissions: {e}")

print(f"\nSet GENIE_SPACE_ID={space_id} in the Command Centre app config")

# Store space ID for downstream DAB tasks
try:
    dbutils.jobs.taskValues.set(key="genie_space_id", value=space_id)
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Delete & Recreate
# MAGIC
# MAGIC To delete an existing Genie space and start fresh, uncomment and run:

# COMMAND ----------

# # DBTITLE v1,Delete Existing Space (uncomment to use)
# SPACE_ID_TO_DELETE = "01f12f487c4416d69f0ca99589d723ef"  # ← paste space ID here
#
# resp = requests.delete(
#     f"{base_url}/api/2.0/genie/spaces/{SPACE_ID_TO_DELETE}",
#     headers=api_headers,
# )
# print(f"Delete response: {resp.status_code}")
# if resp.status_code == 200:
#     print("Space deleted. Re-run the creation cell above.")
# else:
#     print(resp.text)

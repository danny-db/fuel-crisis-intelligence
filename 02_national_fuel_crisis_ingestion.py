# Databricks notebook source
# MAGIC %md
# MAGIC # Australian National Fuel Crisis Intelligence - Data Ingestion
# MAGIC
# MAGIC **Purpose:** Expand from SA-specific to a **nationwide** fuel crisis intelligence platform, ingesting all publicly available Australian datasets needed to answer executive-level questions during a fuel crisis.
# MAGIC
# MAGIC **Prime Minister's Questions This Data Answers:**
# MAGIC 1. What are current wholesale and retail fuel prices in every capital city?
# MAGIC 2. How many days of fuel reserves do we have? Are we meeting IEA obligations?
# MAGIC 3. Which states are most vulnerable to supply disruption?
# MAGIC 4. What's our import dependency — which countries supply our fuel?
# MAGIC 5. Which industries consume the most energy and would be hit hardest?
# MAGIC 6. Where are Australia's critical mining/resource assets that depend on diesel?
# MAGIC 7. How do our fuel prices compare to the OECD?
# MAGIC 8. Have recent natural disasters compounded fuel supply risks?
# MAGIC 9. What retail fuel prices are consumers paying at the pump?
# MAGIC
# MAGIC **New National Tables (this notebook):**
# MAGIC | Table | Source | Description |
# MAGIC |-------|--------|-------------|
# MAGIC | `aip_retail_ulp_prices` | AIP API | Weekly retail ULP prices by 170+ locations |
# MAGIC | `aip_retail_diesel_prices` | AIP API | Weekly retail diesel prices by 170+ locations |
# MAGIC | `petroleum_imports_by_country` | DCCEEW | Crude & product imports by source country |
# MAGIC | `petroleum_exports_by_country` | DCCEEW | Petroleum exports by destination |
# MAGIC | `petroleum_fuel_prices_oecd` | DCCEEW | Australian vs OECD fuel price comparison |
# MAGIC | `petroleum_stock_coverage` | DCCEEW | IEA days of net import coverage |
# MAGIC | `nger_electricity_sector` | CER | Power station emissions & generation data |
# MAGIC | `energy_consumption_by_state` | energy.gov.au | Energy consumption by state and fuel type (Table F) |
# MAGIC | `geoscience_au_mineral_deposits` | Geoscience Australia | National mineral deposits via WFS |
# MAGIC | `disaster_events` | AIDR / data.gov.au | Historical Australian disaster events |
# MAGIC | `qld_fuel_prices` | QLD Gov | Station-level fuel prices (Queensland) |
# MAGIC
# MAGIC **Target Schema:** `danny_catalog.dem_schema`

# COMMAND ----------

# MAGIC %pip install openpyxl requests --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE v1,Configuration & Helpers
try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "danny_catalog"
try:
    SCHEMA = dbutils.widgets.get("schema")
except Exception:
    SCHEMA = "dem_schema"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

import os, re, json, traceback
import requests
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *

TEMP_DIR = "/tmp/national_fuel_crisis"
os.makedirs(TEMP_DIR, exist_ok=True)

def download_file(url, filename, description="file"):
    filepath = f"{TEMP_DIR}/{filename}"
    print(f"  Downloading {description}...")
    try:
        resp = requests.get(url, timeout=120, headers={"User-Agent": "Mozilla/5.0 (Databricks)"}, allow_redirects=True)
        resp.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(resp.content)
        print(f"    OK: {len(resp.content)/1024:.0f} KB")
        return filepath
    except Exception as e:
        print(f"    FAILED: {e}")
        return None

def download_with_fallbacks(urls, filename, description="file"):
    for url in urls:
        result = download_file(url, filename, description)
        if result:
            return result
    return None

def sanitize_columns(pdf):
    cleaned = []
    for col in pdf.columns:
        clean = str(col).strip()
        clean = re.sub(r'[,;{}\(\)\n\t=]', '', clean)
        clean = re.sub(r'\s+', '_', clean)
        clean = re.sub(r'_+', '_', clean).strip('_')
        if not clean:
            clean = "col"
        cleaned.append(clean)
    seen = {}
    final = []
    for c in cleaned:
        if c in seen:
            seen[c] += 1
            final.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            final.append(c)
    pdf.columns = final
    return pdf

def safe_to_spark(pdf):
    pdf = sanitize_columns(pdf)
    for col in pdf.columns:
        if pdf[col].dtype == 'object':
            pdf[col] = pdf[col].astype(str).replace('nan', '')
    return spark.createDataFrame(pdf)

def save_as_delta(df, table_name, description="table"):
    full_name = f"{FULL_SCHEMA}.{table_name}"
    print(f"  Saving -> {full_name}")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    count = spark.table(full_name).count()
    print(f"    {count:,} rows saved")
    return count

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")
print(f"Schema {FULL_SCHEMA} ready\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: AIP Weekly Retail Fuel Prices (National)
# MAGIC
# MAGIC The AIP publishes **weekly average retail prices** for ULP and Diesel across **174 metropolitan and regional locations** in every state/territory. This is what consumers actually pay at the pump.
# MAGIC
# MAGIC > **PM Question:** *"What are Australians paying for fuel right now, and where are prices highest?"*

# COMMAND ----------

# DBTITLE v1,Ingest AIP Retail ULP & Diesel Prices
print("=" * 60)
print("SECTION 1: AIP Weekly Retail Fuel Prices")
print("=" * 60)

for fuel_type, url, table_name in [
    ("ULP", "http://api.aip.com.au/public/retailUlp", "aip_retail_ulp_prices"),
    ("Diesel", "http://api.aip.com.au/public/retailDiesel", "aip_retail_diesel_prices"),
]:
    print(f"\n  --- {fuel_type} ---")
    try:
        resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        print(f"    Status: {resp.status_code}, Content-Type: {resp.headers.get('content-type','?')[:50]}")

        content_type = resp.headers.get('content-type', '')
        if 'json' in content_type:
            data = resp.json()
            if isinstance(data, list):
                pdf = pd.DataFrame(data)
            elif isinstance(data, dict):
                pdf = pd.DataFrame(data.get('data', data.get('results', [data])))
            pdf['fuel_type'] = fuel_type
            save_as_delta(safe_to_spark(pdf), table_name, f"AIP Retail {fuel_type}")
        elif 'html' in content_type:
            # Parse HTML tables
            dfs = pd.read_html(resp.text)
            if dfs:
                pdf = pd.concat(dfs, ignore_index=True)
                pdf['fuel_type'] = fuel_type
                print(f"    Parsed {len(dfs)} HTML tables, {len(pdf)} total rows")
                print(f"    Columns: {list(pdf.columns)[:6]}")
                save_as_delta(safe_to_spark(pdf), table_name, f"AIP Retail {fuel_type}")
            else:
                print(f"    No tables found in HTML response")
        else:
            # Try saving raw and parsing
            fpath = f"{TEMP_DIR}/aip_retail_{fuel_type.lower()}.html"
            with open(fpath, "wb") as f:
                f.write(resp.content)
            try:
                dfs = pd.read_html(fpath)
                if dfs:
                    pdf = pd.concat(dfs, ignore_index=True)
                    pdf['fuel_type'] = fuel_type
                    save_as_delta(safe_to_spark(pdf), table_name, f"AIP Retail {fuel_type}")
            except Exception as e2:
                print(f"    Could not parse: {e2}")
    except Exception as e:
        print(f"    ERROR: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Australian Petroleum Statistics - National Detail
# MAGIC
# MAGIC We already ingested the petroleum statistics in the SA notebook. Here we extract specific **nationally critical sheets** into purpose-built tables for the PM's briefing.
# MAGIC
# MAGIC > **PM Questions:** *"How many days of fuel reserves do we have?" / "Where does our fuel come from?" / "How do our prices compare to the OECD?"*

# COMMAND ----------

# DBTITLE v1,Ingest Targeted Petroleum Statistics Sheets
print("=" * 60)
print("SECTION 2: Targeted Petroleum Statistics")
print("=" * 60)

petrol_file = download_with_fallbacks([
    "https://data.gov.au/data/dataset/d889484e-fb65-4190-a2e3-1739517cbf9b/resource/a5825135-47fa-4c59-a4e8-94e19511cb95/download/australian-petroleum-statistics-data-extract-january-2026.xlsx",
], "petrol_stats_national.xlsx", "Australian Petroleum Statistics")

if petrol_file:
    xl = pd.ExcelFile(petrol_file)

    # Map specific sheets to dedicated tables
    sheet_table_map = {
        "Imports volume by country": "petroleum_imports_by_country",
        "Exports volume by country": "petroleum_exports_by_country",
        "OECD fuel prices and taxes": "petroleum_fuel_prices_oecd",
        "Australian fuel prices": "petroleum_au_fuel_prices",
        "IEA days net import cover": "petroleum_iea_days_coverage",
        "Consumption cover": "petroleum_consumption_cover",
        "Stock volume by product": "petroleum_stock_by_product",
        "Sales by state and territory": "petroleum_sales_by_state_national",
        "Refinery production": "petroleum_refinery_production",
        "Petroleum production": "petroleum_production",
        "Petroleum production by basin": "petroleum_production_by_basin",
    }

    for sheet_name, table_name in sheet_table_map.items():
        if sheet_name not in xl.sheet_names:
            print(f"  Sheet '{sheet_name}' not found, skipping")
            continue
        try:
            df_raw = pd.read_excel(petrol_file, sheet_name=sheet_name, header=None)
            # Auto-detect header row
            header_row = None
            for i in range(min(20, len(df_raw))):
                row_vals = [str(v).strip() for v in df_raw.iloc[i].values if pd.notna(v) and str(v).strip()]
                non_numeric = [v for v in row_vals if not v.replace('.', '').replace('-', '').isdigit()]
                if len(non_numeric) >= 2:
                    header_row = i
                    break
            if header_row is not None:
                df = pd.read_excel(petrol_file, sheet_name=sheet_name, header=header_row)
                df.columns = [str(c).strip() for c in df.columns]
                df = df.dropna(how='all')
                df = df[df.iloc[:, 0:].notna().any(axis=1)]
                df['source_sheet'] = sheet_name
                save_as_delta(safe_to_spark(df), table_name, sheet_name)
            else:
                print(f"  {sheet_name}: No header found, skipping")
        except Exception as e:
            print(f"  {sheet_name}: ERROR - {e}")
else:
    print("  SKIPPED: Could not download petroleum statistics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: NGER Electricity Sector Emissions
# MAGIC
# MAGIC The CER publishes emissions data specifically for the **electricity sector** - power stations, generation facilities. This shows which power plants are most fuel-dependent.
# MAGIC
# MAGIC > **PM Question:** *"Which power stations would go offline in a fuel crisis?"*

# COMMAND ----------

# DBTITLE v1,Ingest NGER Electricity Sector Data
print("=" * 60)
print("SECTION 3: NGER Electricity Sector Emissions")
print("=" * 60)

nger_elec_file = download_with_fallbacks([
    "https://cer.gov.au/document/greenhouse-and-energy-information-designated-generation-facility-2024-25-0",
    "https://cer.gov.au/document/greenhouse-and-energy-information-designated-generation-facility-2024-25",
], "nger_electricity.csv", "NGER Electricity Sector 2024-25")

if nger_elec_file:
    for reader_fn, enc, name in [
        (lambda f: pd.read_csv(f, encoding='latin-1'), 'latin-1', 'CSV-latin1'),
        (lambda f: pd.read_csv(f), None, 'CSV-utf8'),
        (lambda f: pd.read_excel(f), None, 'Excel'),
    ]:
        try:
            pdf_elec = reader_fn(nger_elec_file)
            print(f"  Parsed as {name}: {pdf_elec.shape}")
            print(f"  Columns: {list(pdf_elec.columns)[:6]}")
            save_as_delta(safe_to_spark(pdf_elec), "nger_electricity_sector", "NGER Electricity Sector")
            break
        except Exception:
            continue
    else:
        print("  WARNING: Could not parse NGER electricity data")
else:
    print("  SKIPPED: Could not download NGER electricity data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Australian Energy Statistics - Table F (National)
# MAGIC
# MAGIC Energy consumption by **state, industry sector, and fuel type**. This shows which states and industries are most dependent on specific fuels.
# MAGIC
# MAGIC > **PM Question:** *"Which states and industries consume the most diesel and would be hardest hit?"*

# COMMAND ----------

# DBTITLE v1,Ingest Energy Statistics Table F
print("=" * 60)
print("SECTION 4: Energy Statistics Table F")
print("=" * 60)

energy_file = download_with_fallbacks([
    "https://www.energy.gov.au/sites/default/files/Australian%20Energy%20Statistics%202022%20Table%20F_0.xlsx",
    "https://www.energy.gov.au/sites/default/files/australian_energy_statistics_2025_table_f.xlsx",
    "https://www.energy.gov.au/sites/default/files/Australian%20Energy%20Statistics%202025%20Table%20F.xlsx",
], "energy_table_f.xlsx", "Energy Statistics Table F")

if energy_file:
    xl = pd.ExcelFile(energy_file)
    print(f"  Sheets: {xl.sheet_names}")

    all_dfs = []
    for sheet_name in xl.sheet_names:
        try:
            df_raw = pd.read_excel(energy_file, sheet_name=sheet_name, header=None)
            header_row = None
            for i in range(min(15, len(df_raw))):
                row_vals = [str(v).strip() for v in df_raw.iloc[i].values if pd.notna(v)]
                if any((len(v) >= 4 and v[:4].isdigit()) for v in row_vals) or any('-' in v and len(v) <= 7 for v in row_vals):
                    header_row = i
                    break
            if header_row is not None:
                df = pd.read_excel(energy_file, sheet_name=sheet_name, header=header_row)
                df.columns = [str(c).strip() for c in df.columns]
                df = df.dropna(how='all')
                df['source_sheet'] = sheet_name
                all_dfs.append(df)
                print(f"  Sheet '{sheet_name}': {len(df)} rows")
        except Exception as e:
            print(f"  Sheet '{sheet_name}': ERROR - {e}")

    if all_dfs:
        pdf_energy = pd.concat(all_dfs, ignore_index=True)
        save_as_delta(safe_to_spark(pdf_energy), "energy_consumption_by_state", "Energy Consumption by State")
else:
    print("  SKIPPED: Could not download Table F")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Geoscience Australia - National Mineral Deposits
# MAGIC
# MAGIC Geoscience Australia's OZMIN database provides data on mineral deposits, mines, and resource projects across the entire country via OGC WFS (Web Feature Service).
# MAGIC
# MAGIC > **PM Question:** *"Where are our critical mining operations and how dependent are they on diesel supply?"*

# COMMAND ----------

# DBTITLE v1,Ingest Geoscience Australia Mineral Deposits (WFS)
print("=" * 60)
print("SECTION 5: Geoscience Australia Mineral Deposits")
print("=" * 60)

# Geoscience Australia WFS endpoint for mineral occurrences
GA_WFS_BASE = "https://services.ga.gov.au/gis/earthresource/ows"
ga_params = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "GetFeature",
    "typeNames": "ama:MineralOccurrences",
    "count": "5000",
    "outputFormat": "application/json"
}

try:
    print("  Querying GA WFS for MineralOccurrences (up to 5000 records)...")
    resp = requests.get(GA_WFS_BASE, params=ga_params, timeout=120, headers={"User-Agent": "Mozilla/5.0"})
    print(f"    Status: {resp.status_code}, Size: {len(resp.content)/1024:.0f} KB")

    if resp.status_code == 200:
        data = resp.json()
        features = data.get("features", [])
        print(f"    Features returned: {len(features)}")

        if features:
            # Flatten GeoJSON features into a DataFrame
            records = []
            for f in features:
                props = f.get("properties", {})
                geom = f.get("geometry", {})
                if geom and geom.get("coordinates"):
                    coords = geom["coordinates"]
                    if isinstance(coords[0], list):
                        coords = coords[0]  # Handle multi-point
                    props["longitude"] = coords[0] if len(coords) > 0 else None
                    props["latitude"] = coords[1] if len(coords) > 1 else None
                records.append(props)

            pdf_ga = pd.DataFrame(records)
            print(f"    Columns: {list(pdf_ga.columns)[:10]}")
            save_as_delta(safe_to_spark(pdf_ga), "geoscience_au_mineral_deposits", "GA Mineral Deposits")
        else:
            print("    No features returned")
    else:
        print(f"    WFS request failed: {resp.status_code}")
except Exception as e:
    print(f"    ERROR: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Australian Disaster Events
# MAGIC
# MAGIC The **Australian Institute for Disaster Resilience (AIDR)** maintains a database of all Australian disasters (natural and man-made) from 1753 to present, with category, location, and impact data.
# MAGIC
# MAGIC > **PM Question:** *"Are recent natural disasters compounding our fuel supply problems? Which regions are double-hit?"*

# COMMAND ----------

# DBTITLE v1,Ingest AIDR Disaster Events
print("=" * 60)
print("SECTION 6: Australian Disaster Events")
print("=" * 60)

# AIDR disaster events from data.gov.au
disaster_file = download_with_fallbacks([
    "https://data.gov.au/data/dataset/disaster-events-with-category-impact-and-location/resource/disaster-events/download/disaster-events.csv",
], "disaster_events.csv", "Australian Disaster Events")

# If direct download fails, try the CKAN API
if disaster_file is None:
    try:
        # Search for the dataset via CKAN
        resp = requests.get(
            "https://data.gov.au/data/api/3/action/package_show?id=disaster-events-with-category-impact-and-location",
            timeout=30, headers={"User-Agent": "Mozilla/5.0"}
        )
        if resp.status_code == 200:
            pkg = resp.json()
            for resource in pkg.get("result", {}).get("resources", []):
                if resource.get("format", "").upper() == "CSV":
                    disaster_file = download_file(resource["url"], "disaster_events.csv", "Disaster Events (CKAN)")
                    if disaster_file:
                        break
    except Exception as e:
        print(f"    CKAN lookup failed: {e}")

if disaster_file:
    try:
        pdf_disaster = pd.read_csv(disaster_file, encoding='latin-1', low_memory=False)
        print(f"  Shape: {pdf_disaster.shape}")
        print(f"  Columns: {list(pdf_disaster.columns)[:8]}")
        save_as_delta(safe_to_spark(pdf_disaster), "disaster_events", "Australian Disaster Events")
    except Exception as e:
        print(f"  Parse error: {e}")
else:
    print("  SKIPPED: Could not download disaster events data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Queensland Fuel Prices (Station-Level)
# MAGIC
# MAGIC Queensland has the best publicly available station-level fuel price data in Australia, published monthly on data.qld.gov.au.
# MAGIC
# MAGIC > **PM Question:** *"What are the most expensive fuel locations and are remote communities being price-gouged?"*

# COMMAND ----------

# DBTITLE v1,Ingest QLD Station-Level Fuel Prices
print("=" * 60)
print("SECTION 7: Queensland Fuel Prices (Station-Level)")
print("=" * 60)

# QLD publishes monthly CSV files - try the most recent
qld_urls = [
    "https://www.data.qld.gov.au/dataset/fuel-price-reporting-2025/resource/download",
]

# Try CKAN API to find the latest resource
try:
    resp = requests.get(
        "https://www.data.qld.gov.au/api/3/action/package_show?id=fuel-price-reporting-2026",
        timeout=30, headers={"User-Agent": "Mozilla/5.0"}
    )
    if resp.status_code == 200:
        pkg = resp.json()
        resources = pkg.get("result", {}).get("resources", [])
        # Get the latest CSV resource
        csv_resources = [r for r in resources if r.get("format", "").upper() == "CSV"]
        if csv_resources:
            latest = csv_resources[-1]  # Most recent
            qld_urls.insert(0, latest["url"])
            print(f"  Found QLD resource: {latest.get('name', '?')}")
    else:
        # Try 2025
        resp = requests.get(
            "https://www.data.qld.gov.au/api/3/action/package_show?id=fuel-price-reporting-2025",
            timeout=30, headers={"User-Agent": "Mozilla/5.0"}
        )
        if resp.status_code == 200:
            pkg = resp.json()
            resources = pkg.get("result", {}).get("resources", [])
            csv_resources = [r for r in resources if r.get("format", "").upper() == "CSV"]
            if csv_resources:
                latest = csv_resources[-1]
                qld_urls.insert(0, latest["url"])
                print(f"  Found QLD resource: {latest.get('name', '?')}")
except Exception as e:
    print(f"  QLD CKAN lookup: {e}")

qld_file = download_with_fallbacks(qld_urls, "qld_fuel_prices.csv", "QLD Fuel Prices")

if qld_file:
    try:
        pdf_qld = pd.read_csv(qld_file, low_memory=False)
        print(f"  Shape: {pdf_qld.shape}")
        print(f"  Columns: {list(pdf_qld.columns)}")
        save_as_delta(safe_to_spark(pdf_qld), "qld_fuel_prices", "QLD Station-Level Fuel Prices")
    except Exception as e:
        print(f"  Parse error: {e}")
else:
    print("  SKIPPED: Could not download QLD fuel prices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: AI-Powered National Crisis Briefing

# COMMAND ----------

# DBTITLE v1,AI Briefing: National Fuel Security Assessment
try:
    result = spark.sql(f"""
        SELECT ai_query(
          'databricks-claude-sonnet-4-6',
          CONCAT(
            'You are the Chief Energy Advisor to the Australian Prime Minister during a national fuel crisis. ',
            'Based on the following national fuel data summary, provide a 2-paragraph briefing note on: ',
            '1) Current national fuel price situation across states, and ',
            '2) Key vulnerabilities and recommended immediate actions. ',
            'Wholesale fuel prices by city (annual avg, cents/litre): ',
            (SELECT CONCAT_WS('; ', COLLECT_LIST(
              CONCAT(city, ': ', fuel_type, ' ', ROUND(price_cpl, 1), 'c/L (', year_period, ')')
            ))
            FROM (
              SELECT city, fuel_type, price_cpl, year_period
              FROM {FULL_SCHEMA}.aip_terminal_gate_prices
              WHERE year_period = (SELECT MAX(year_period) FROM {FULL_SCHEMA}.aip_terminal_gate_prices)
              ORDER BY city, fuel_type
            )),
            '. Top 5 corporate emitters: ',
            (SELECT CONCAT_WS('; ', COLLECT_LIST(Organisation_name))
             FROM {FULL_SCHEMA}.nger_corporate_emissions
             LIMIT 5)
          )
        ) AS national_briefing
    """)
    print("\n--- NATIONAL FUEL CRISIS BRIEFING ---")
    result.show(truncate=False)
except Exception as e:
    print(f"  AI briefing failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification: All National Tables

# COMMAND ----------

# DBTITLE v1,Verify All Tables
print("=" * 60)
print("NATIONAL DATA VERIFICATION")
print("=" * 60)

national_tables = [
    # Original SA tables
    "aip_terminal_gate_prices",
    "petroleum_sales_by_state",
    "petroleum_stocks",
    "petroleum_supply",
    "nger_corporate_emissions",
    "nger_state_emissions_by_industry",
    # New national tables
    "aip_retail_ulp_prices",
    "aip_retail_diesel_prices",
    "petroleum_imports_by_country",
    "petroleum_exports_by_country",
    "petroleum_fuel_prices_oecd",
    "petroleum_au_fuel_prices",
    "petroleum_iea_days_coverage",
    "petroleum_consumption_cover",
    "petroleum_stock_by_product",
    "petroleum_sales_by_state_national",
    "petroleum_refinery_production",
    "petroleum_production",
    "petroleum_production_by_basin",
    "nger_electricity_sector",
    "energy_consumption_by_state",
    "geoscience_au_mineral_deposits",
    "disaster_events",
    "qld_fuel_prices",
]

print(f"\n{'Table':<50} {'Status':<10} {'Rows':>12}")
print("-" * 75)
ok = 0
for table in national_tables:
    try:
        count = spark.table(f"{FULL_SCHEMA}.{table}").count()
        status = "OK" if count > 0 else "EMPTY"
        print(f"{table:<50} {status:<10} {count:>12,}")
        if count > 0:
            ok += 1
    except Exception:
        print(f"{table:<50} {'MISSING':<10} {'N/A':>12}")

print(f"\n{ok}/{len(national_tables)} tables loaded")

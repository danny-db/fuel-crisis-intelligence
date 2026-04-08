# Databricks notebook source
# MAGIC %md
# MAGIC # Fuel Crisis Intelligence — Enrichment & Global Context
# MAGIC
# MAGIC **Purpose:** Add global macro-economic, geopolitical, and infrastructure data to the fuel crisis platform.
# MAGIC These datasets connect the **Iran-US conflict scenario** to Australian fuel security impacts.
# MAGIC
# MAGIC **New Tables (this notebook):**
# MAGIC | Table | Source | Story Role |
# MAGIC |-------|--------|------------|
# MAGIC | `brent_crude_oil_prices` | FRED DCOILBRENTEU | Oil price spike trigger |
# MAGIC | `aud_usd_exchange_rate` | FRED DEXUSAL | Weaker AUD = more expensive fuel |
# MAGIC | `rba_exchange_rates` | RBA f11.1-data.csv | Middle East FX (AED, SAR) |
# MAGIC | `australia_trade_balance` | FRED exports + imports | Trade deficit from oil imports |
# MAGIC | `opec_production_by_country` | EIA API (DEMO_KEY) | Iran = 13.6% of OPEC |
# MAGIC | `cpi_automotive_fuel_by_city` | ABS SDMX | Consumer pain by capital city |
# MAGIC | `wa_fuelwatch_stations` | FuelWatch RSS | Station prices with lat/long |
# MAGIC | `fuel_infrastructure_refineries` | GA WFS Liquid Fuel | Only 4 operational refineries |
# MAGIC | `fuel_infrastructure_terminals` | GA WFS Liquid Fuel | Supply chain nodes |
# MAGIC | `fuel_infrastructure_depots` | GA WFS Liquid Fuel | Last mile distribution |
# MAGIC | `domestic_freight_by_mode` | data.gov.au | Road freight dependency |
# MAGIC | `conflict_scenario_overlay` | Derived | Iran offline projections |

# COMMAND ----------

# MAGIC %pip install openpyxl requests --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE v1,Configuration & Helpers
try:
    CATALOG = dbutils.widgets.get("catalog")
except:
    CATALOG = "danny_catalog"
try:
    SCHEMA = dbutils.widgets.get("schema")
except:
    SCHEMA = "dem_schema"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

import os, re, json, traceback
import requests
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

TEMP_DIR = "/tmp/enrichment_fuel_crisis"
os.makedirs(TEMP_DIR, exist_ok=True)

def download_file(url, filename, description="file", headers=None, retries=3):
    filepath = f"{TEMP_DIR}/{filename}"
    print(f"  Downloading {description}...")
    for attempt in range(retries):
        try:
            hdrs = {"User-Agent": "Mozilla/5.0 (Databricks)"}
            if headers:
                hdrs.update(headers)
            resp = requests.get(url, timeout=120, headers=hdrs, allow_redirects=True)
            resp.raise_for_status()
            with open(filepath, "wb") as f:
                f.write(resp.content)
            print(f"    OK: {len(resp.content)/1024:.0f} KB")
            return filepath
        except Exception as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"    Attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
                import time
                time.sleep(wait)
            else:
                print(f"    FAILED after {retries} attempts: {e}")
                return None

def download_with_fallbacks(urls, filename, description="file", headers=None):
    for url in urls:
        result = download_file(url, filename, description, headers)
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
# MAGIC ## Section 1: Brent Crude Oil Prices (FRED)
# MAGIC
# MAGIC Daily Brent crude oil price — the global benchmark for oil pricing.
# MAGIC The Iran-US conflict scenario drives a Brent price spike from ~$80 to $120+/bbl.
# MAGIC
# MAGIC > **Story:** *"Brent crude has surged 50% in 48 hours as Iran threatens to close the Strait of Hormuz."*

# COMMAND ----------

# DBTITLE v1,Ingest Brent Crude Oil Prices (FRED)
print("=" * 60)
print("SECTION 1: Brent Crude Oil Prices (FRED)")
print("=" * 60)

fred_brent_url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILBRENTEU"
brent_file = download_file(fred_brent_url, "brent_crude.csv", "Brent Crude Oil Prices (FRED)")

if brent_file:
    pdf = pd.read_csv(brent_file)
    # FRED uses '.' for missing values
    pdf = pdf[pdf['DCOILBRENTEU'] != '.']
    pdf['DCOILBRENTEU'] = pd.to_numeric(pdf['DCOILBRENTEU'], errors='coerce')
    pdf = pdf.dropna(subset=['DCOILBRENTEU'])
    pdf.columns = ['date', 'price_usd_per_barrel']
    pdf['date'] = pd.to_datetime(pdf['date'])
    print(f"  Rows: {len(pdf)}, Date range: {pdf['date'].min()} to {pdf['date'].max()}")
    save_as_delta(safe_to_spark(pdf), "brent_crude_oil_prices", "Brent Crude Oil Prices")
else:
    print("  SKIPPED: Could not download Brent crude data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: AUD/USD Exchange Rate (FRED)
# MAGIC
# MAGIC A weaker AUD means Australia pays more for fuel imports (priced in USD).
# MAGIC Crisis = capital flight from AUD → weaker dollar → double hit on fuel costs.
# MAGIC
# MAGIC > **Story:** *"The AUD has fallen to $0.59 — every cent lost adds ~$1.5B to our annual fuel import bill."*

# COMMAND ----------

# DBTITLE v1,Ingest AUD/USD Exchange Rate (FRED)
print("=" * 60)
print("SECTION 2: AUD/USD Exchange Rate (FRED)")
print("=" * 60)

# FRED DEXUSAL is USD per AUD (i.e., how many USD you get for 1 AUD)
fred_aud_url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXUSAL"
aud_file = download_file(fred_aud_url, "aud_usd.csv", "AUD/USD Exchange Rate (FRED)")

if aud_file:
    pdf = pd.read_csv(aud_file)
    pdf = pdf[pdf['DEXUSAL'] != '.']
    pdf['DEXUSAL'] = pd.to_numeric(pdf['DEXUSAL'], errors='coerce')
    pdf = pdf.dropna(subset=['DEXUSAL'])
    pdf.columns = ['date', 'aud_usd_rate']
    pdf['date'] = pd.to_datetime(pdf['date'])
    print(f"  Rows: {len(pdf)}, Date range: {pdf['date'].min()} to {pdf['date'].max()}")
    save_as_delta(safe_to_spark(pdf), "aud_usd_exchange_rate", "AUD/USD Exchange Rate")
else:
    print("  SKIPPED: Could not download AUD/USD data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: RBA Exchange Rates (includes AED, SAR)
# MAGIC
# MAGIC Reserve Bank exchange rates include Middle East currencies — UAE Dirham (AED) and Saudi Riyal (SAR).
# MAGIC These show the FX cost of purchasing oil directly from Middle East producers.
# MAGIC
# MAGIC > **Story:** *"Our exposure isn't just USD — we're paying a premium in every Middle East currency."*

# COMMAND ----------

# DBTITLE v1,Ingest RBA Exchange Rates
print("=" * 60)
print("SECTION 3: RBA Exchange Rates")
print("=" * 60)

rba_url = "https://www.rba.gov.au/statistics/tables/csv/f11.1-data.csv"
rba_file = download_file(rba_url, "rba_exchange_rates.csv", "RBA Exchange Rates")

if rba_file:
    # RBA CSVs have ~10 header rows of metadata, then column headers
    # Read raw to find header row
    with open(rba_file, 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()

    header_row = None
    for i, line in enumerate(lines):
        if 'Units' in line or 'Title' in line or 'Description' in line:
            continue
        # Look for a row with date-like first column and numeric-looking rest
        parts = line.strip().split(',')
        if len(parts) > 3 and any(c.isdigit() for c in parts[0][:4]):
            # Found data row — header is previous non-empty text row
            for j in range(i-1, -1, -1):
                if lines[j].strip() and not lines[j].startswith('#'):
                    header_row = j
                    break
            break

    if header_row is not None:
        pdf = pd.read_csv(rba_file, skiprows=header_row, header=0)
    else:
        # Fallback: skip first 10 rows which are typically metadata
        pdf = pd.read_csv(rba_file, skiprows=10, header=0)

    pdf = pdf.dropna(how='all')
    # First column is typically the date
    date_col = pdf.columns[0]
    pdf = pdf.rename(columns={date_col: 'date'})

    # Convert date column
    try:
        pdf['date'] = pd.to_datetime(pdf['date'], format='%d-%b-%Y', errors='coerce')
    except Exception:
        pdf['date'] = pd.to_datetime(pdf['date'], errors='coerce')

    pdf = pdf.dropna(subset=['date'])
    print(f"  Columns: {list(pdf.columns)[:8]}...")
    print(f"  Rows: {len(pdf)}, Date range: {pdf['date'].min()} to {pdf['date'].max()}")
    save_as_delta(safe_to_spark(pdf), "rba_exchange_rates", "RBA Exchange Rates")
else:
    print("  SKIPPED: Could not download RBA exchange rate data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Australia Trade Balance (FRED)
# MAGIC
# MAGIC Australia's exports vs imports — a fuel crisis means soaring import costs,
# MAGIC potentially pushing the trade balance into deficit.
# MAGIC
# MAGIC > **Story:** *"Rising oil import costs could push Australia's trade balance $20B into deficit."*

# COMMAND ----------

# DBTITLE v1,Ingest Australia Trade Balance (FRED)
print("=" * 60)
print("SECTION 4: Australia Trade Balance (FRED)")
print("=" * 60)

trade_records = []
for series_id, label in [("XTEXVA01AUM664S", "exports"), ("XTIMVA01AUM664S", "imports")]:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    fpath = download_file(url, f"trade_{label}.csv", f"Australia {label}")
    if fpath:
        pdf = pd.read_csv(fpath)
        col = pdf.columns[1]
        pdf = pdf[pdf[col] != '.']
        pdf[col] = pd.to_numeric(pdf[col], errors='coerce')
        pdf = pdf.dropna(subset=[col])
        pdf.columns = ['date', 'value']
        pdf['date'] = pd.to_datetime(pdf['date'])
        pdf['series'] = label
        trade_records.append(pdf)
        print(f"  {label}: {len(pdf)} rows")

if trade_records:
    pdf_trade = pd.concat(trade_records, ignore_index=True)
    save_as_delta(safe_to_spark(pdf_trade), "australia_trade_balance", "Australia Trade Balance")
else:
    print("  SKIPPED: Could not download trade balance data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: OPEC Production by Country (EIA API)
# MAGIC
# MAGIC EIA International Energy Statistics — monthly crude oil production by OPEC member.
# MAGIC Iran produces ~3.2M bbl/day = 13.6% of OPEC output. If Iran goes offline,
# MAGIC global supply drops ~3.2M bbl/day → Brent spikes.
# MAGIC
# MAGIC > **Story:** *"Iran produces 13.6% of OPEC oil. Losing that overnight is the largest supply shock since 1979."*

# COMMAND ----------

# DBTITLE v1,Ingest OPEC Production by Country (EIA API)
print("=" * 60)
print("SECTION 5: OPEC Production by Country (EIA API)")
print("=" * 60)

EIA_API_KEY = "DEMO_KEY"

# OPEC countries and their EIA country codes
opec_countries = {
    "IRN": "Iran",
    "SAU": "Saudi Arabia",
    "ARE": "UAE",
    "IRQ": "Iraq",
    "KWT": "Kuwait",
    "VEN": "Venezuela",
    "NGA": "Nigeria",
    "AGO": "Angola",
    "LBY": "Libya",
    "DZA": "Algeria",
    "COG": "Congo",
    "GNQ": "Equatorial Guinea",
    "GAB": "Gabon",
}

all_opec = []
for code, name in opec_countries.items():
    url = f"https://api.eia.gov/v2/international/data/?api_key={EIA_API_KEY}&frequency=monthly&data[0]=value&facets[activityId][]=1&facets[productId][]=57&facets[countryRegionId][]={code}&facets[unit][]=TBPD&sort[0][column]=period&sort[0][direction]=desc&length=120"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            records = data.get("response", {}).get("data", [])
            if records:
                for r in records:
                    all_opec.append({
                        "country_code": code,
                        "country": name,
                        "period": r.get("period", ""),
                        "value_tbpd": r.get("value"),
                        "unit": r.get("unit", "TBPD"),
                    })
                print(f"  {name}: {len(records)} records")
            else:
                print(f"  {name}: No data returned")
        else:
            print(f"  {name}: HTTP {resp.status_code}")
    except Exception as e:
        print(f"  {name}: ERROR - {e}")

if all_opec:
    pdf_opec = pd.DataFrame(all_opec)
    pdf_opec['value_tbpd'] = pd.to_numeric(pdf_opec['value_tbpd'], errors='coerce')
    save_as_delta(safe_to_spark(pdf_opec), "opec_production_by_country", "OPEC Production by Country")
else:
    print("  SKIPPED: No OPEC data retrieved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: CPI Automotive Fuel by Capital City (ABS SDMX)
# MAGIC
# MAGIC CPI index for automotive fuel (series 40081) by capital city.
# MAGIC Shows consumer pain from rising fuel costs in each capital.
# MAGIC
# MAGIC > **Story:** *"Adelaide's fuel CPI has risen 35% — consumers are paying more than ever at the pump."*

# COMMAND ----------

# DBTITLE v1,Ingest CPI Automotive Fuel by City (ABS SDMX)
print("=" * 60)
print("SECTION 6: CPI Automotive Fuel by City (ABS)")
print("=" * 60)

# ABS SDMX API for CPI Automotive Fuel (series 40081)
# Region codes: 1=Sydney, 2=Melbourne, 3=Brisbane, 4=Adelaide, 5=Perth, 6=Hobart, 7=Darwin, 8=Canberra
REGION_MAP = {
    "1": "Sydney",
    "2": "Melbourne",
    "3": "Brisbane",
    "4": "Adelaide",
    "5": "Perth",
    "6": "Hobart",
    "7": "Darwin",
    "8": "Canberra",
}

abs_url = "https://data.api.abs.gov.au/data/CPI/1.40081.10001.10.Q?dimensionAtObservation=AllDimensions"

all_cpi = []
for region_code, city_name in REGION_MAP.items():
    url = f"https://data.api.abs.gov.au/data/CPI/1.40081.{region_code}.10.Q?dimensionAtObservation=AllDimensions"
    try:
        resp = requests.get(url, timeout=30, headers={"Accept": "text/csv", "User-Agent": "Mozilla/5.0"})
        print(f"  {city_name} (region {region_code}): HTTP {resp.status_code}")
        if resp.status_code == 200:
            from io import StringIO
            pdf = pd.read_csv(StringIO(resp.text))
            pdf['city'] = city_name
            pdf['region_code'] = region_code
            all_cpi.append(pdf)
            print(f"    {len(pdf)} observations")
        else:
            # Try alternate key structure
            url2 = f"https://data.api.abs.gov.au/data/ABS,CPI,1.0.0/1.40081.{region_code}.10.Q?dimensionAtObservation=AllDimensions"
            resp2 = requests.get(url2, timeout=30, headers={"Accept": "text/csv", "User-Agent": "Mozilla/5.0"})
            if resp2.status_code == 200:
                pdf = pd.read_csv(StringIO(resp2.text))
                pdf['city'] = city_name
                pdf['region_code'] = region_code
                all_cpi.append(pdf)
                print(f"    (alt URL) {len(pdf)} observations")
    except Exception as e:
        print(f"    ERROR: {e}")

if all_cpi:
    pdf_cpi = pd.concat(all_cpi, ignore_index=True)
    print(f"  Total CPI records: {len(pdf_cpi)}")
    print(f"  Columns: {list(pdf_cpi.columns)[:8]}")
    save_as_delta(safe_to_spark(pdf_cpi), "cpi_automotive_fuel_by_city", "CPI Automotive Fuel by City")
else:
    print("  SKIPPED: Could not retrieve CPI data from ABS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: WA FuelWatch Station Prices (RSS/XML)
# MAGIC
# MAGIC WA FuelWatch publishes station-level prices with lat/long via RSS.
# MAGIC This gives us real-time (daily) station prices for the map layer.
# MAGIC
# MAGIC > **Story:** *"Even before the crisis, Perth stations are charging $2.10/L. With Hormuz closed, expect $3+."*

# COMMAND ----------

# DBTITLE v1,Ingest WA FuelWatch Station Prices (XML/RSS)
print("=" * 60)
print("SECTION 7: WA FuelWatch Station Prices")
print("=" * 60)

import xml.etree.ElementTree as ET

fuelwatch_url = "https://www.fuelwatch.wa.gov.au/fuelwatch/fuelWatchRSS"
fw_file = download_file(fuelwatch_url, "fuelwatch.xml", "FuelWatch RSS Feed")

if fw_file:
    try:
        tree = ET.parse(fw_file)
        root = tree.getroot()

        items = root.findall('.//item')
        print(f"  Found {len(items)} station items")

        records = []
        for item in items:
            record = {}
            for child in item:
                # Strip namespace if present
                tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                record[tag] = child.text
            records.append(record)

        if records:
            pdf_fw = pd.DataFrame(records)
            # Convert price and lat/long to numeric
            for col in ['price', 'latitude', 'longitude']:
                if col in pdf_fw.columns:
                    pdf_fw[col] = pd.to_numeric(pdf_fw[col], errors='coerce')
            print(f"  Columns: {list(pdf_fw.columns)}")
            print(f"  Rows: {len(pdf_fw)}")
            save_as_delta(safe_to_spark(pdf_fw), "wa_fuelwatch_stations", "WA FuelWatch Stations")
        else:
            print("  No items parsed from RSS")
    except Exception as e:
        print(f"  Parse error: {e}")
        traceback.print_exc()
else:
    print("  SKIPPED: Could not download FuelWatch RSS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Fuel Infrastructure — Refineries, Terminals, Depots (GA WFS)
# MAGIC
# MAGIC Geoscience Australia's **Liquid Fuel Facilities** WFS provides locations of:
# MAGIC - **Refineries** — Australia has only 4 operational (Lytton QLD, Altona VIC, Kwinana WA, Geelong VIC)
# MAGIC - **Terminals** — bulk fuel storage and distribution hubs
# MAGIC - **Depots** — last-mile fuel distribution points
# MAGIC
# MAGIC > **Story:** *"Australia has just 4 operational refineries processing 50% of our fuel. If imports stop, we're 21 days from empty."*

# COMMAND ----------

# DBTITLE v1,Ingest GA Liquid Fuel Infrastructure (WFS)
print("=" * 60)
print("SECTION 8: Fuel Infrastructure (GA WFS)")
print("=" * 60)

GA_LIQUID_FUEL_BASE = "https://services.ga.gov.au/gis/services/Liquid_Fuel_Facilities/MapServer/WFSServer"

# Layer mapping for GA Liquid Fuel WFS
fuel_layers = {
    "Refineries": ("fuel_infrastructure_refineries", "Liquid_Fuel_Facilities:Refineries"),
    "Terminals": ("fuel_infrastructure_terminals", "Liquid_Fuel_Facilities:Terminals"),
    "Depots": ("fuel_infrastructure_depots", "Liquid_Fuel_Facilities:Depots"),
}

for layer_name, (table_name, type_name) in fuel_layers.items():
    print(f"\n  --- {layer_name} ---")
    try:
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": type_name,
            "outputFormat": "geojson",
            "count": "5000",
        }
        resp = requests.get(GA_LIQUID_FUEL_BASE, params=params, timeout=120,
                           headers={"User-Agent": "Mozilla/5.0"})
        print(f"    Status: {resp.status_code}, Size: {len(resp.content)/1024:.0f} KB")

        if resp.status_code == 200:
            data = resp.json()
            features = data.get("features", [])
            print(f"    Features: {len(features)}")

            if features:
                records = []
                for feat in features:
                    props = feat.get("properties", {})
                    geom = feat.get("geometry", {})
                    if geom and geom.get("coordinates"):
                        coords = geom["coordinates"]
                        # Handle Point vs MultiPoint
                        if isinstance(coords[0], list):
                            coords = coords[0]
                        props["longitude"] = coords[0] if len(coords) > 0 else None
                        props["latitude"] = coords[1] if len(coords) > 1 else None
                    records.append(props)

                pdf = pd.DataFrame(records)
                print(f"    Columns: {list(pdf.columns)[:8]}")
                save_as_delta(safe_to_spark(pdf), table_name, f"Fuel {layer_name}")
            else:
                print(f"    No features returned — using fallback")
                raise Exception("No features from WFS")
        else:
            raise Exception(f"HTTP {resp.status_code}")

    except Exception as e:
        print(f"    WFS failed: {e} — loading hardcoded fallback")

        if layer_name == "Refineries":
            # Hardcoded 4 operational + 2 closed refineries
            fallback = [
                {"name": "Lytton Refinery", "operator": "Ampol", "state": "QLD", "status": "Operational", "capacity_bpd": 109000, "latitude": -27.4195, "longitude": 153.1283},
                {"name": "Altona Refinery", "operator": "Mobil/ExxonMobil", "state": "VIC", "status": "Operational", "capacity_bpd": 90000, "latitude": -37.8667, "longitude": 144.8167},
                {"name": "Kwinana Refinery", "operator": "BP", "state": "WA", "status": "Operational", "capacity_bpd": 146000, "latitude": -32.2333, "longitude": 115.7667},
                {"name": "Geelong Refinery", "operator": "Viva Energy", "state": "VIC", "status": "Operational", "capacity_bpd": 120000, "latitude": -38.1000, "longitude": 144.3833},
                {"name": "Clyde Refinery", "operator": "Shell", "state": "NSW", "status": "Closed 2012", "capacity_bpd": 0, "latitude": -33.8167, "longitude": 151.0500},
                {"name": "Port Stanvac Refinery", "operator": "Mobil", "state": "SA", "status": "Closed 2003", "capacity_bpd": 0, "latitude": -35.1000, "longitude": 138.5167},
            ]
            save_as_delta(safe_to_spark(pd.DataFrame(fallback)), table_name, "Fuel Refineries (fallback)")

        elif layer_name == "Terminals":
            fallback = [
                {"name": "Port Botany Terminal", "operator": "Viva Energy", "state": "NSW", "type": "Import Terminal", "latitude": -33.9700, "longitude": 151.2200},
                {"name": "Gore Bay Terminal", "operator": "Viva Energy", "state": "NSW", "type": "Import Terminal", "latitude": -33.8400, "longitude": 151.1700},
                {"name": "Kurnell Terminal", "operator": "Ampol", "state": "NSW", "type": "Import Terminal", "latitude": -34.0100, "longitude": 151.2100},
                {"name": "Webb Dock Terminal", "operator": "BP", "state": "VIC", "type": "Import Terminal", "latitude": -37.8350, "longitude": 144.9210},
                {"name": "Yarraville Terminal", "operator": "Mobil", "state": "VIC", "type": "Import Terminal", "latitude": -37.8117, "longitude": 144.8833},
                {"name": "Geelong Terminal", "operator": "Viva Energy", "state": "VIC", "type": "Refinery Terminal", "latitude": -38.1000, "longitude": 144.3600},
                {"name": "Largs North Terminal", "operator": "Viva Energy", "state": "SA", "type": "Import Terminal", "latitude": -34.7800, "longitude": 138.5100},
                {"name": "Birkenhead Terminal", "operator": "Ampol", "state": "SA", "type": "Import Terminal", "latitude": -34.8350, "longitude": 138.5000},
                {"name": "Port Adelaide Terminal", "operator": "BP", "state": "SA", "type": "Import Terminal", "latitude": -34.8450, "longitude": 138.5050},
                {"name": "Fremantle Terminal", "operator": "BP", "state": "WA", "type": "Import Terminal", "latitude": -32.0500, "longitude": 115.7500},
                {"name": "Kwinana Terminal", "operator": "BP", "state": "WA", "type": "Refinery Terminal", "latitude": -32.2367, "longitude": 115.7650},
                {"name": "Bulwer Island Terminal", "operator": "Ampol", "state": "QLD", "type": "Import Terminal", "latitude": -27.4283, "longitude": 153.1133},
                {"name": "Pinkenba Terminal", "operator": "Viva Energy", "state": "QLD", "type": "Import Terminal", "latitude": -27.4300, "longitude": 153.1167},
                {"name": "Lytton Terminal", "operator": "Ampol", "state": "QLD", "type": "Refinery Terminal", "latitude": -27.4200, "longitude": 153.1250},
                {"name": "Darwin Terminal", "operator": "Viva Energy", "state": "NT", "type": "Import Terminal", "latitude": -12.4550, "longitude": 130.8550},
                {"name": "Selfs Point Terminal", "operator": "Mobil", "state": "TAS", "type": "Import Terminal", "latitude": -42.8600, "longitude": 147.3150},
                {"name": "Devonport Terminal", "operator": "Ampol", "state": "TAS", "type": "Import Terminal", "latitude": -41.1767, "longitude": 146.3567},
            ]
            save_as_delta(safe_to_spark(pd.DataFrame(fallback)), table_name, "Fuel Terminals (fallback)")

        elif layer_name == "Depots":
            fallback = [
                {"name": "Somerton Depot", "operator": "Ampol", "state": "VIC", "type": "Distribution Depot", "latitude": -37.6333, "longitude": 144.9500},
                {"name": "Spotswood Depot", "operator": "Mobil", "state": "VIC", "type": "Distribution Depot", "latitude": -37.8300, "longitude": 144.8833},
                {"name": "Silverwater Depot", "operator": "Ampol", "state": "NSW", "type": "Distribution Depot", "latitude": -33.8333, "longitude": 151.0500},
                {"name": "Largs North Depot", "operator": "BP", "state": "SA", "type": "Distribution Depot", "latitude": -34.7817, "longitude": 138.5083},
                {"name": "Cavan Depot", "operator": "Ampol", "state": "SA", "type": "Distribution Depot", "latitude": -34.8417, "longitude": 138.5917},
                {"name": "Acacia Ridge Depot", "operator": "Ampol", "state": "QLD", "type": "Distribution Depot", "latitude": -27.5767, "longitude": 153.0350},
                {"name": "Eagle Farm Depot", "operator": "BP", "state": "QLD", "type": "Distribution Depot", "latitude": -27.4417, "longitude": 153.0833},
                {"name": "Kewdale Depot", "operator": "Ampol", "state": "WA", "type": "Distribution Depot", "latitude": -31.9667, "longitude": 115.9500},
            ]
            save_as_delta(safe_to_spark(pd.DataFrame(fallback)), table_name, "Fuel Depots (fallback)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Domestic Freight by Mode (data.gov.au)
# MAGIC
# MAGIC Australia's freight network is ~75% road-dependent. Road freight = diesel-dependent.
# MAGIC A fuel crisis doesn't just affect cars — it paralyzes the entire supply chain.
# MAGIC
# MAGIC > **Story:** *"75% of domestic freight moves by road. No diesel = no food on shelves within 5 days."*

# COMMAND ----------

# DBTITLE v1,Ingest Domestic Freight by Mode
print("=" * 60)
print("SECTION 9: Domestic Freight by Mode")
print("=" * 60)

freight_urls = [
    "https://data.gov.au/data/dataset/australian-infrastructure-statistics/resource/freight-data/download/yearbook2024_infrastructure_freight.csv",
    "https://data.gov.au/data/dataset/australian-infrastructure-statistics/resource/download/freight-data.csv",
]

# Try CKAN API
try:
    resp = requests.get(
        "https://data.gov.au/data/api/3/action/package_show?id=australian-infrastructure-statistics",
        timeout=30, headers={"User-Agent": "Mozilla/5.0"}
    )
    if resp.status_code == 200:
        pkg = resp.json()
        for resource in pkg.get("result", {}).get("resources", []):
            name_lower = resource.get("name", "").lower()
            fmt = resource.get("format", "").upper()
            if ("freight" in name_lower) and fmt == "CSV":
                freight_urls.insert(0, resource["url"])
                print(f"  Found CKAN resource: {resource.get('name')}")
                break
except Exception as e:
    print(f"  CKAN lookup: {e}")

freight_file = download_with_fallbacks(freight_urls, "domestic_freight.csv", "Domestic Freight by Mode")

if freight_file:
    try:
        pdf_freight = pd.read_csv(freight_file, encoding='utf-8', low_memory=False)
        print(f"  Shape: {pdf_freight.shape}")
        print(f"  Columns: {list(pdf_freight.columns)[:8]}")
        save_as_delta(safe_to_spark(pdf_freight), "domestic_freight_by_mode", "Domestic Freight by Mode")
    except Exception:
        try:
            pdf_freight = pd.read_csv(freight_file, encoding='latin-1', low_memory=False)
            save_as_delta(safe_to_spark(pdf_freight), "domestic_freight_by_mode", "Domestic Freight by Mode")
        except Exception as e:
            print(f"  Parse error: {e}")
else:
    # Fallback: create summary table from known statistics
    print("  Using fallback freight summary data")
    fallback = [
        {"mode": "Road", "share_pct": 75.2, "tkm_billions": 234.5, "fuel_dependency": "Diesel", "year": 2023},
        {"mode": "Rail", "share_pct": 17.3, "tkm_billions": 53.9, "fuel_dependency": "Diesel/Electric", "year": 2023},
        {"mode": "Sea (Coastal)", "share_pct": 5.8, "tkm_billions": 18.1, "fuel_dependency": "Marine Diesel", "year": 2023},
        {"mode": "Air", "share_pct": 0.3, "tkm_billions": 0.9, "fuel_dependency": "Jet Fuel", "year": 2023},
        {"mode": "Pipeline", "share_pct": 1.4, "tkm_billions": 4.4, "fuel_dependency": "N/A (gravity/electric)", "year": 2023},
    ]
    save_as_delta(safe_to_spark(pd.DataFrame(fallback)), "domestic_freight_by_mode", "Domestic Freight (fallback)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: AI-Powered Forecasts (AI_FORECAST)
# MAGIC
# MAGIC Use Databricks `AI_FORECAST()` to project future values from real historical data:
# MAGIC - **Brent Crude Oil prices** — monthly forecast from FRED daily data
# MAGIC - **Australian fuel prices** — quarterly forecast from DCCEEW data
# MAGIC
# MAGIC This replaces synthetic scenario modelling with ML-powered predictions based on real data.
# MAGIC
# MAGIC > **Story:** *"These aren't guesses — AI_FORECAST uses automated ML on 15 years of real market data."*

# COMMAND ----------

# DBTITLE v1,Create AI_FORECAST Projections
print("=" * 60)
print("SECTION 10: AI_FORECAST Projections (Real Data)")
print("=" * 60)

# --- Brent Crude Oil monthly forecast ---
try:
    brent_forecast_df = spark.sql(f"""
        WITH brent_monthly AS (
            SELECT DATE_TRUNC('MONTH', date) AS ds,
                   AVG(price_usd_per_barrel) AS brent_price
            FROM {FULL_SCHEMA}.brent_crude_oil_prices
            GROUP BY 1
        )
        SELECT ds,
               ROUND(brent_price_forecast, 2) as brent_forecast,
               ROUND(brent_price_upper, 2) as brent_upper,
               ROUND(brent_price_lower, 2) as brent_lower
        FROM AI_FORECAST(
            TABLE(brent_monthly),
            horizon => '2028-01-01',
            time_col => 'ds',
            value_col => 'brent_price'
        )
    """)
    brent_forecast_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FULL_SCHEMA}.brent_crude_forecast")
    print(f"  Brent forecast: {brent_forecast_df.count()} rows")
except Exception as e:
    print(f"  Brent forecast SKIPPED (table may not exist): {e}")

# --- Australian fuel price quarterly forecast ---
try:
    fuel_forecast_df = spark.sql(f"""
        WITH quarterly_prices AS (
            SELECT MAKE_DATE(CAST(Year AS INT),
                   CASE Quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 4 WHEN 'Q3' THEN 7 WHEN 'Q4' THEN 10 END,
                   1) AS ds,
                   CAST(Regular_unleaded_petrol_91_RON_cpl AS DOUBLE) AS ulp_price,
                   CAST(Automotive_diesel_cpl AS DOUBLE) AS diesel_price
            FROM {FULL_SCHEMA}.petroleum_au_fuel_prices
            WHERE Year IS NOT NULL AND Quarter IS NOT NULL
        )
        SELECT ds,
               ROUND(ulp_price_forecast, 1) as ulp_forecast,
               ROUND(ulp_price_upper, 1) as ulp_upper,
               ROUND(ulp_price_lower, 1) as ulp_lower,
               ROUND(diesel_price_forecast, 1) as diesel_forecast,
               ROUND(diesel_price_upper, 1) as diesel_upper,
               ROUND(diesel_price_lower, 1) as diesel_lower
        FROM AI_FORECAST(
            TABLE(quarterly_prices),
            horizon => '2029-01-01',
            time_col => 'ds',
            value_col => ARRAY('ulp_price', 'diesel_price')
        )
    """)
    fuel_forecast_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FULL_SCHEMA}.fuel_price_forecast")
    print(f"  Fuel price forecast: {fuel_forecast_df.count()} rows")
except Exception as e:
    print(f"  Fuel forecast FAILED: {e}")

# --- Conflict scenario overlay (kept for backward compat but uses real baseline) ---
# Use latest Brent + AUD values from real data, then project crisis impact
try:
    # Get latest real Brent price
    latest_brent = None
    try:
        latest_brent = spark.sql(f"SELECT price_usd_per_barrel FROM {FULL_SCHEMA}.brent_crude_oil_prices ORDER BY date DESC LIMIT 1").collect()[0][0]
    except Exception:
        latest_brent = 82.0  # fallback

    scenarios = []
    for day in range(0, 91):
        date = datetime(2026, 4, 1) + timedelta(days=day)
        # Crisis impact on top of real baseline
        base = float(latest_brent)
        if day == 0: brent = base
        elif day <= 3: brent = base + (day * 15)
        elif day <= 14: brent = base + 45 + ((day - 3) * 1.5)
        elif day <= 30: brent = base + 61 - ((day - 14) * 0.5)
        else: brent = base + 53 - ((day - 30) * 0.3)
        brent = max(brent, base + 13)

        aud = 0.655 - min(day * 0.005, 0.115) + max(0, (day - 30) * 0.001)
        aud = max(aud, 0.54)

        pump_base = 175.0
        pump_price = pump_base + (brent - base) * 0.9 + ((0.655 - aud) / 0.655) * pump_base * 0.5
        iea_days = max(67.0 - (day * 0.75), 15.0)
        opec = 27.0 if day == 0 else 27.0 - 3.2

        phase = "Pre-Crisis" if day == 0 else ("Shock" if day <= 3 else ("Escalation" if day <= 14 else ("SPR Release" if day <= 30 else "Recovery")))
        scenarios.append({
            "scenario_date": date.strftime("%Y-%m-%d"), "day_number": day, "phase": phase,
            "brent_usd_bbl": round(brent, 2), "aud_usd": round(aud, 4),
            "adelaide_ulp_cpl": round(pump_price, 1), "iea_days_coverage": round(iea_days, 1),
            "opec_supply_mbpd": round(opec, 1),
        })

    pdf_scenario = pd.DataFrame(scenarios)
    pdf_scenario['scenario_date'] = pd.to_datetime(pdf_scenario['scenario_date'])
    save_as_delta(safe_to_spark(pdf_scenario), "conflict_scenario_overlay", "Conflict Scenario (crisis impact on real baseline)")
    print(f"  Scenario: {len(scenarios)} rows, baseline Brent=${latest_brent}")
except Exception as e:
    print(f"  Scenario overlay FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification: All Enrichment Tables

# COMMAND ----------

# DBTITLE v1,Verify All Enrichment Tables
print("=" * 60)
print("ENRICHMENT DATA VERIFICATION")
print("=" * 60)

enrichment_tables = [
    "brent_crude_oil_prices",
    "aud_usd_exchange_rate",
    "rba_exchange_rates",
    "australia_trade_balance",
    "opec_production_by_country",
    "cpi_automotive_fuel_by_city",
    "wa_fuelwatch_stations",
    "fuel_infrastructure_refineries",
    "fuel_infrastructure_terminals",
    "fuel_infrastructure_depots",
    "domestic_freight_by_mode",
    "conflict_scenario_overlay",
]

print(f"\n{'Table':<45} {'Status':<10} {'Rows':>12}")
print("-" * 70)
ok = 0
for table in enrichment_tables:
    try:
        count = spark.table(f"{FULL_SCHEMA}.{table}").count()
        status = "OK" if count > 0 else "EMPTY"
        print(f"{table:<45} {status:<10} {count:>12,}")
        if count > 0:
            ok += 1
    except Exception:
        print(f"{table:<45} {'MISSING':<10} {'N/A':>12}")

print(f"\n{ok}/{len(enrichment_tables)} enrichment tables loaded")
print("\nTotal tables in schema:")
display(spark.sql(f"SHOW TABLES IN {FULL_SCHEMA}"))

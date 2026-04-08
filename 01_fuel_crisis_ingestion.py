# Databricks notebook source
# MAGIC %md
# MAGIC # SA DEM Fuel Crisis Intelligence - Data Ingestion & Analysis
# MAGIC
# MAGIC **Purpose:** Demonstrate how to ingest publicly available Australian fuel/energy datasets into Databricks using PySpark, then leverage AI (`ai_query`) for intelligent analysis.
# MAGIC
# MAGIC **Data Sources:**
# MAGIC 1. AIP Terminal Gate Prices (wholesale fuel prices for Adelaide)
# MAGIC 2. Australian Petroleum Statistics (state-level fuel sales, stocks, imports)
# MAGIC 3. NGER Corporate Emissions Data (greenhouse gas reporting)
# MAGIC 4. SA Diesel Generation Plants (diesel power infrastructure)
# MAGIC 5. SARIG Mineral Deposits (SA mining/resource locations)
# MAGIC
# MAGIC **Approach:** Download public Excel/CSV files -> Parse with pandas/openpyxl -> Convert to PySpark -> Save as Delta tables in Unity Catalog
# MAGIC
# MAGIC **Target Schema:** `danny_catalog.dem_schema`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup & Configuration

# COMMAND ----------

# DBTITLE v1,Install Required Libraries
# MAGIC %pip install openpyxl requests --quiet

# COMMAND ----------

# DBTITLE v1,Restart Python after pip install
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

import os, requests, traceback
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *

TEMP_DIR = "/tmp/dem_fuel_crisis"
os.makedirs(TEMP_DIR, exist_ok=True)

def download_file(url, filename, description="file"):
    """Download a file from a URL. Returns filepath on success, None on failure."""
    filepath = f"{TEMP_DIR}/{filename}"
    print(f"  Downloading {description}...")
    try:
        resp = requests.get(url, timeout=120, headers={"User-Agent": "Mozilla/5.0 (Databricks)"}, allow_redirects=True)
        resp.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(resp.content)
        print(f"    OK: {len(resp.content)/1024:.0f} KB, content-type: {resp.headers.get('content-type','?')[:50]}")
        return filepath
    except Exception as e:
        print(f"    FAILED: {e}")
        return None

def download_with_fallbacks(urls, filename, description="file"):
    """Try multiple URLs until one succeeds."""
    for url in urls:
        result = download_file(url, filename, description)
        if result:
            return result
    return None

def sanitize_columns(pdf):
    """Clean column names to be Delta-safe: remove special chars like (),;{}\\n\\t="""
    import re
    cleaned = []
    for col in pdf.columns:
        clean = str(col).strip()
        clean = re.sub(r'[,;{}\(\)\n\t=]', '', clean)
        clean = re.sub(r'\s+', '_', clean)
        clean = re.sub(r'_+', '_', clean).strip('_')
        if not clean:
            clean = "col"
        cleaned.append(clean)
    # Handle duplicates by appending _2, _3, etc.
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
    """Convert pandas DataFrame to Spark, handling mixed types and column names."""
    pdf = sanitize_columns(pdf)
    for col in pdf.columns:
        if pdf[col].dtype == 'object':
            pdf[col] = pdf[col].astype(str).replace('nan', '')
    return spark.createDataFrame(pdf)

def save_as_delta(df, table_name, description="table"):
    """Save a PySpark DataFrame as a managed Delta table."""
    full_name = f"{FULL_SCHEMA}.{table_name}"
    print(f"  Saving -> {full_name}")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    count = spark.table(full_name).count()
    print(f"    {count:,} rows saved")
    return count

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA} COMMENT 'SA DEM - Fuel Crisis Intelligence'")
print(f"Schema {FULL_SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: AIP Terminal Gate Prices
# MAGIC
# MAGIC The **Australian Institute of Petroleum (AIP)** publishes annual average wholesale fuel prices at terminal gates across Australian cities. Terminal Gate Prices (TGPs) are the baseline wholesale price before retail margins and transport costs.
# MAGIC
# MAGIC > **Teaching point:** The AIP Excel file has a `NOTES` sheet plus data sheets for Petrol and Diesel. Row 0 = column headers (cities), row 3 onward = annual data. We skip the notes sheet and melt the city columns into long format.

# COMMAND ----------

# DBTITLE v1,Ingest AIP Terminal Gate Prices
print("=" * 60)
print("SECTION 2: AIP Terminal Gate Prices")
print("=" * 60)

aip_file = download_with_fallbacks([
    "https://www.aip.com.au/sites/default/files/download-files/2026-01/AIP_Annual_TGP_Data.xlsx",
    "https://www.aip.com.au/sites/default/files/download-files/2025-12/AIP_Annual_TGP_Data.xlsx",
], "aip_tgp.xlsx", "AIP Terminal Gate Prices")

if aip_file:
    xl = pd.ExcelFile(aip_file)
    print(f"  Sheets: {xl.sheet_names}")

    # The data sheets are "Average Petrol TGP" and "Average Diesel TGP"
    # Structure: Row 0 = [label, city1, city2, ...], Row 1 = notes, Row 2 = "Calendar Year", Row 3+ = data
    all_prices = []
    for sheet_name in xl.sheet_names:
        if 'NOTES' in sheet_name.upper():
            continue

        df_raw = pd.read_excel(aip_file, sheet_name=sheet_name, header=None)
        print(f"\n  Sheet '{sheet_name}': {df_raw.shape}")

        # Row 0 has headers: first col is label (e.g. "AVERAGE PETROL TGP"), rest are city names
        cities = [str(v).strip() for v in df_raw.iloc[0, 1:].values if pd.notna(v) and str(v).strip()]
        print(f"    Cities: {cities}")

        # Data starts at row 3 (after label, notes, "Calendar Year" rows)
        for i in range(3, len(df_raw)):
            year_val = df_raw.iloc[i, 0]
            if pd.isna(year_val):
                continue
            year_str = str(year_val).strip()

            for j, city in enumerate(cities):
                price_val = df_raw.iloc[i, j + 1]
                if pd.notna(price_val):
                    try:
                        price_float = float(price_val)
                        # Determine fuel type from sheet name
                        fuel_type = "ULP" if "petrol" in sheet_name.lower() else "Diesel"
                        all_prices.append({
                            "year_period": year_str,
                            "city": city,
                            "price_cpl": round(price_float, 2),
                            "fuel_type": fuel_type,
                        })
                    except (ValueError, TypeError):
                        pass

    if all_prices:
        pdf_prices = pd.DataFrame(all_prices)
        print(f"\n  Total records: {len(pdf_prices)}")
        print(f"  Adelaide records: {(pdf_prices['city'] == 'Adelaide').sum()}")
        print(f"  Year range: {pdf_prices['year_period'].min()} to {pdf_prices['year_period'].max()}")

        spark_prices = spark.createDataFrame(pdf_prices)
        spark_prices.filter(F.col("city") == "Adelaide").orderBy("year_period").show(10, truncate=False)
        save_as_delta(spark_prices, "aip_terminal_gate_prices", "AIP Terminal Gate Prices")
    else:
        print("  WARNING: No price data parsed")
else:
    print("  SKIPPED: Could not download AIP data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Australian Petroleum Statistics
# MAGIC
# MAGIC The **DCCEEW** publishes monthly petroleum statistics with sheets for:
# MAGIC - Sales by state and territory
# MAGIC - Stock volumes
# MAGIC - Imports/exports
# MAGIC - Refinery production
# MAGIC
# MAGIC > **Teaching point:** This workbook has 25+ sheets. We target the key data sheets and auto-detect headers by looking for rows containing "Date" or month patterns.

# COMMAND ----------

# DBTITLE v1,Ingest Australian Petroleum Statistics
print("=" * 60)
print("SECTION 3: Australian Petroleum Statistics")
print("=" * 60)

petrol_file = download_with_fallbacks([
    "https://data.gov.au/data/dataset/d889484e-fb65-4190-a2e3-1739517cbf9b/resource/a5825135-47fa-4c59-a4e8-94e19511cb95/download/australian-petroleum-statistics-data-extract-january-2026.xlsx",
], "petrol_stats.xlsx", "Australian Petroleum Statistics")

if petrol_file:
    xl = pd.ExcelFile(petrol_file)
    print(f"  Total sheets: {len(xl.sheet_names)}")
    print(f"  Sheets: {xl.sheet_names}")

    # Target the most useful data sheets
    target_sheets_sales = ['Sales of products', 'Sales by state and territory', 'Sales of lubricants', 'Australian fuel prices']
    target_sheets_stocks = ['Stock volume by product', 'Stock mass by product', 'Consumption cover', 'IEA days net import cover']
    target_sheets_supply = ['Petroleum production', 'Refinery production', 'Imports volume', 'Exports volume']

    def parse_petrol_sheet(filepath, sheet_name):
        """Parse a petroleum statistics sheet with auto-detected header."""
        df_raw = pd.read_excel(filepath, sheet_name=sheet_name, header=None)

        # Find header row: look for a row with "Date" or multiple non-null string values
        header_row = None
        for i in range(min(20, len(df_raw))):
            row_vals = [str(v).strip() for v in df_raw.iloc[i].values if pd.notna(v) and str(v).strip()]
            # Header row typically has 3+ non-empty cells that aren't just numbers
            non_numeric = [v for v in row_vals if not v.replace('.', '').replace('-', '').isdigit()]
            if len(non_numeric) >= 3:
                header_row = i
                break

        if header_row is None:
            return None

        df = pd.read_excel(filepath, sheet_name=sheet_name, header=header_row)
        df.columns = [str(c).strip() for c in df.columns]
        df = df.dropna(how='all')
        # Remove rows that are all NaN except first col
        df = df[df.iloc[:, 1:].notna().any(axis=1)]
        return df

    all_sales = []
    all_stocks = []
    all_supply = []

    for sheet_name in xl.sheet_names:
        if sheet_name in ['Index', 'Copyright ', 'Data sources and notes']:
            continue
        try:
            df = parse_petrol_sheet(petrol_file, sheet_name)
            if df is not None and len(df) > 0:
                df['source_sheet'] = sheet_name

                if sheet_name in target_sheets_stocks:
                    df['data_category'] = 'stocks'
                    all_stocks.append(df)
                    print(f"  {sheet_name}: {len(df)} rows -> STOCKS")
                elif sheet_name in target_sheets_supply:
                    df['data_category'] = 'supply'
                    all_supply.append(df)
                    print(f"  {sheet_name}: {len(df)} rows -> SUPPLY")
                else:
                    df['data_category'] = 'sales'
                    all_sales.append(df)
                    print(f"  {sheet_name}: {len(df)} rows -> SALES")
        except Exception as e:
            print(f"  {sheet_name}: ERROR - {e}")

    # Save tables
    if all_sales:
        pdf_all_sales = pd.concat(all_sales, ignore_index=True)
        save_as_delta(safe_to_spark(pdf_all_sales), "petroleum_sales_by_state", "Petroleum Sales")

    if all_stocks:
        pdf_all_stocks = pd.concat(all_stocks, ignore_index=True)
        save_as_delta(safe_to_spark(pdf_all_stocks), "petroleum_stocks", "Petroleum Stocks")

    if all_supply:
        pdf_all_supply = pd.concat(all_supply, ignore_index=True)
        save_as_delta(safe_to_spark(pdf_all_supply), "petroleum_supply", "Petroleum Supply")

    if not (all_sales or all_stocks or all_supply):
        print("  WARNING: No petroleum data parsed")
else:
    print("  SKIPPED: Could not download petroleum statistics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: NGER Corporate Emissions Data
# MAGIC
# MAGIC The **Clean Energy Regulator** publishes NGER data showing greenhouse gas emissions by corporation. This is a clean CSV with columns for organisation name, ABN, scope 1 & 2 emissions, and net energy consumed.
# MAGIC
# MAGIC > **Teaching point:** This CSV is straightforward - `pd.read_csv()` handles it directly. We filter for SA-relevant corporations in downstream analysis.

# COMMAND ----------

# DBTITLE v1,Ingest NGER Corporate Emissions
print("=" * 60)
print("SECTION 4: NGER Corporate Emissions")
print("=" * 60)

nger_file = download_with_fallbacks([
    "https://cer.gov.au/document/greenhouse-and-energy-information-registered-corporation-2024-25-0",
], "nger_corporate.csv", "NGER Corporate Emissions 2024-25")

if nger_file:
    # It's CSV but uses Windows-1252 encoding (contains 0x96 em-dash bytes)
    pdf_nger = pd.read_csv(nger_file, encoding='latin-1')
    print(f"  Columns: {list(pdf_nger.columns)}")
    print(f"  Shape: {pdf_nger.shape}")
    print(f"  Sample:")
    print(pdf_nger.head(3).to_string())

    spark_nger = safe_to_spark(pdf_nger)
    save_as_delta(spark_nger, "nger_corporate_emissions", "NGER Corporate Emissions")
else:
    print("  SKIPPED: Could not download NGER data")

# Also get the XLSX version (has more detail)
nger_xlsx_file = download_with_fallbacks([
    "https://cer.gov.au/document/greenhouse-and-energy-information-registered-corporation-2024-25",
], "nger_register.xlsx", "NGER Register 2024-25")

if nger_xlsx_file:
    try:
        pdf_reg = pd.read_excel(nger_xlsx_file)
        print(f"\n  NGER Register: {pdf_reg.shape}")
        print(f"  Columns: {list(pdf_reg.columns)[:8]}")
        pdf_reg.columns = [str(c).strip().replace('\n', ' ') for c in pdf_reg.columns]
        save_as_delta(safe_to_spark(pdf_reg), "nger_state_emissions_by_industry", "NGER Register Detail")
    except Exception:
        try:
            pdf_reg = pd.read_csv(nger_xlsx_file, encoding='latin-1')
            pdf_reg.columns = [str(c).strip().replace('\n', ' ') for c in pdf_reg.columns]
            save_as_delta(safe_to_spark(pdf_reg), "nger_state_emissions_by_industry", "NGER Register Detail")
        except Exception as e:
            print(f"  NGER Register parse failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: SA Diesel Generation Plants
# MAGIC
# MAGIC The SA Government publishes a database of diesel generation plants including location, capacity, and ownership information.
# MAGIC
# MAGIC > **Teaching point:** CSV from government open data portals. Straightforward `pd.read_csv()` ingestion.

# COMMAND ----------

# DBTITLE v1,Ingest SA Diesel Generation Plants
print("=" * 60)
print("SECTION 5: SA Diesel Generation Plants")
print("=" * 60)

# Use the CKAN DataStore API (resource_id: 79b776e1-cd4b-4479-b937-398d729b1b40)
# This returns JSON records which we can convert to a DataFrame
diesel_api_url = "https://data.sa.gov.au/data/api/3/action/datastore_search?resource_id=79b776e1-cd4b-4479-b937-398d729b1b40&limit=10000"
print(f"  Using CKAN DataStore API...")

try:
    resp = requests.get(diesel_api_url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    data = resp.json()
    if data.get("success"):
        records = data["result"]["records"]
        print(f"  Retrieved {len(records)} records via CKAN API")
        if records:
            pdf_diesel = pd.DataFrame(records)
            # Remove CKAN internal columns
            drop_cols = [c for c in pdf_diesel.columns if c.startswith('_')]
            pdf_diesel = pdf_diesel.drop(columns=drop_cols, errors='ignore')
            print(f"  Columns: {list(pdf_diesel.columns)}")
            save_as_delta(safe_to_spark(pdf_diesel), "sa_diesel_generation_plants", "SA Diesel Generation Plants")
        else:
            print("  WARNING: CKAN API returned 0 records")
    else:
        print(f"  CKAN API error: {data.get('error', 'unknown')}")
except Exception as e:
    print(f"  CKAN API failed: {e}")
    print("  Manual: https://data.sa.gov.au/data/dataset/diesel-generation-plants")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: SARIG Mineral Deposits
# MAGIC
# MAGIC The **South Australian Resources Information Gateway (SARIG)** provides data on 6,500+ mineral deposits and mines across SA with geographic coordinates.
# MAGIC
# MAGIC > **Teaching point:** SARIG data is hosted on a CKAN catalog. We download the CSV directly from the catalog resource URL.

# COMMAND ----------

# DBTITLE v1,Ingest SARIG Mineral Deposits
print("=" * 60)
print("SECTION 6: SARIG Mineral Deposits")
print("=" * 60)

# SARIG catalog returns 403 for direct download. Use the CKAN API instead.
sarig_api_url = "https://catalog.sarig.sa.gov.au/api/3/action/datastore_search?resource_id=29e3a993-3c0c-44fa-9a22-0ab68053e612&limit=10000"
sarig_file = None
try:
    print(f"  Trying SARIG CKAN API...")
    resp = requests.get(sarig_api_url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    if resp.status_code == 200:
        data = resp.json()
        if data.get("success"):
            records = data["result"]["records"]
            print(f"  Got {len(records)} records via CKAN API")
            if records:
                pdf_sarig = pd.DataFrame(records)
                drop_cols = [c for c in pdf_sarig.columns if c.startswith('_')]
                pdf_sarig = pdf_sarig.drop(columns=drop_cols, errors='ignore')
                sarig_file = "API"  # flag that we have data
        else:
            print(f"  CKAN API not available, trying direct download...")
    else:
        print(f"  SARIG API returned {resp.status_code}, trying direct download...")
except Exception as e:
    print(f"  SARIG API error: {e}")

if sarig_file is None:
    sarig_file = download_with_fallbacks([
        "https://catalog.sarig.sa.gov.au/dataset/1bf02be3-ce1b-4c22-a529-8d8773caa46a/resource/29e3a993-3c0c-44fa-9a22-0ab68053e612/download/sa-all-mines-and-mineral-deposits.csv",
    ], "sa_minerals.csv", "SA Mineral Deposits")

if sarig_file == "API":
    # Data already loaded via CKAN API into pdf_sarig
    print(f"  Shape: {pdf_sarig.shape}")
    print(f"  Columns: {list(pdf_sarig.columns)[:10]}")
    save_as_delta(safe_to_spark(pdf_sarig), "sa_mineral_deposits", "SA Mineral Deposits")
elif sarig_file:
    try:
        pdf_sarig = pd.read_csv(sarig_file, low_memory=False)
        print(f"  Shape: {pdf_sarig.shape}")
        print(f"  Columns: {list(pdf_sarig.columns)[:10]}")
        save_as_delta(safe_to_spark(pdf_sarig), "sa_mineral_deposits", "SA Mineral Deposits")
    except Exception as e:
        print(f"  Parse failed: {e}")
else:
    print("  SKIPPED: Could not download SARIG data")
    print("  Manual: https://catalog.sarig.sa.gov.au/dataset/mesac743")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: AI-Powered Analysis with `ai_query()`
# MAGIC
# MAGIC **`ai_query()`** calls foundation models directly from SQL, letting us generate natural language insights from our structured data.
# MAGIC
# MAGIC We use `databricks-claude-sonnet-4-6` to:
# MAGIC 1. Summarize fuel pricing trends
# MAGIC 2. Assess supply risk
# MAGIC 3. Identify industry impacts
# MAGIC
# MAGIC > **Teaching point:** `ai_query()` bridges structured data and natural language - powerful for executive summaries.

# COMMAND ----------

# DBTITLE v1,AI Analysis 1: Fuel Pricing Trend Summary
print("=" * 60)
print("SECTION 7: AI-Powered Analysis")
print("=" * 60)

try:
    result = spark.sql(f"""
        SELECT ai_query(
          'databricks-claude-sonnet-4-6',
          CONCAT(
            'You are an energy analyst for the South Australian Department of Energy and Mining. ',
            'Analyze the following Adelaide fuel pricing data (annual averages in cents per litre) ',
            'and provide a concise executive summary of trends. Highlight any concerning patterns ',
            'for fuel crisis preparedness. Data: ',
            (SELECT CONCAT_WS('; ', COLLECT_LIST(
              CONCAT(year_period, ': ', fuel_type, ' = ', ROUND(price_cpl, 1), ' c/L')
            ))
            FROM (
              SELECT year_period, fuel_type, AVG(price_cpl) as price_cpl
              FROM {FULL_SCHEMA}.aip_terminal_gate_prices
              WHERE city = 'Adelaide'
              GROUP BY year_period, fuel_type
              ORDER BY year_period DESC
              LIMIT 40
            ))
          )
        ) AS fuel_price_summary
    """)
    print("\n--- AI: Fuel Pricing Trends ---")
    result.show(truncate=False)
except Exception as e:
    print(f"  ai_query fuel pricing failed (table may not exist): {e}")

# COMMAND ----------

# DBTITLE v1,AI Analysis 2: Supply Risk Assessment
try:
    result = spark.sql(f"""
        SELECT ai_query(
          'databricks-claude-sonnet-4-6',
          CONCAT(
            'You are a fuel supply chain risk analyst for South Australia. ',
            'Based on the following petroleum statistics summary, assess the fuel supply risk. ',
            'Provide a risk rating (LOW/MEDIUM/HIGH/CRITICAL) with brief justification. ',
            'Data sheets available: ',
            (SELECT CONCAT_WS('; ', COLLECT_LIST(summary))
             FROM (
               SELECT CONCAT(source_sheet, ' (', data_category, '): ', COUNT(*), ' rows') as summary
               FROM {FULL_SCHEMA}.petroleum_sales_by_state
               GROUP BY source_sheet, data_category
               LIMIT 20
             ))
          )
        ) AS supply_risk_assessment
    """)
    print("\n--- AI: Supply Risk Assessment ---")
    result.show(truncate=False)
except Exception as e:
    print(f"  ai_query supply risk failed: {e}")

# COMMAND ----------

# DBTITLE v1,AI Analysis 3: Emissions & Industry Impact
try:
    result = spark.sql(f"""
        SELECT ai_query(
          'databricks-claude-sonnet-4-6',
          CONCAT(
            'You are an energy policy analyst for the SA Department of Energy and Mining. ',
            'These are the top greenhouse gas emitters in Australia from the NGER scheme. ',
            'Identify which are likely to have significant SA operations and explain ',
            'how a fuel crisis would impact them. Provide recommendations. Data: ',
            (SELECT CONCAT_WS('; ', COLLECT_LIST(info))
             FROM (
               SELECT * FROM {FULL_SCHEMA}.nger_corporate_emissions
               LIMIT 20
             ) t
             LATERAL VIEW INLINE(ARRAY(STRUCT(CONCAT_WS(', ', *) as info))) e
            )
          )
        ) AS industry_impact_narrative
    """)
    print("\n--- AI: Industry Impact ---")
    result.show(truncate=False)
except Exception as e:
    # Simpler approach if LATERAL VIEW fails
    try:
        result = spark.sql(f"""
            SELECT ai_query(
              'databricks-claude-sonnet-4-6',
              CONCAT(
                'You are an energy policy analyst. The top 5 Australian corporate emitters are: ',
                (SELECT CONCAT_WS('; ', COLLECT_LIST(`Organisation name`))
                 FROM {FULL_SCHEMA}.nger_corporate_emissions
                 LIMIT 5),
                '. Explain how a fuel crisis would impact these industries in South Australia.'
              )
            ) AS industry_impact
        """)
        print("\n--- AI: Industry Impact ---")
        result.show(truncate=False)
    except Exception as e2:
        print(f"  ai_query industry impact failed: {e2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# DBTITLE v1,Verify All Tables
print("=" * 60)
print("VERIFICATION")
print("=" * 60)

expected_tables = [
    "aip_terminal_gate_prices",
    "petroleum_sales_by_state",
    "petroleum_stocks",
    "petroleum_supply",
    "nger_corporate_emissions",
    "nger_state_emissions_by_industry",
    "sa_diesel_generation_plants",
    "sa_mineral_deposits",
]

print(f"\n{'Table':<45} {'Status':<10} {'Rows':>12}")
print("-" * 70)
ok_count = 0
for table in expected_tables:
    try:
        count = spark.table(f"{FULL_SCHEMA}.{table}").count()
        status = "OK" if count > 0 else "EMPTY"
        print(f"{table:<45} {status:<10} {count:>12,}")
        if count > 0:
            ok_count += 1
    except Exception:
        print(f"{table:<45} {'MISSING':<10} {'N/A':>12}")

print(f"\n{ok_count}/{len(expected_tables)} tables loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **AI/BI Dashboard** - Interactive visualizations of fuel pricing, supply, and industry impact
# MAGIC 2. **Genie Space** - Natural language Q&A over the fuel crisis data
# MAGIC 3. **Metric Views** - Standardized KPIs for ongoing monitoring
# MAGIC 4. **Scheduled Jobs** - Automated data refresh via Databricks Workflows

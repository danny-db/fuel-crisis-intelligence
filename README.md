# Australian Fuel Crisis Intelligence

An end-to-end Databricks Lakehouse demo covering **national and state-level** Australian fuel crisis analysis — from public data ingestion to AI-powered insights, interactive dashboards, and natural language Q&A via Genie.

Built for the **South Australian Department of Energy and Mining (DEM)** and expandable to any Australian government agency.

## What This Demo Covers

1. **Data Ingestion** — Download public Excel/CSV/API datasets from Australian government sources using PySpark + pandas
2. **AI Analysis** — Use `ai_query()` with foundation models to generate executive briefings and risk assessments
3. **AI/BI Dashboards** — Lakeview dashboards for SA-specific and national fuel intelligence
4. **Genie Spaces** — Natural language Q&A over fuel crisis data (SA + National)
5. **Metric Views** — Standardized KPIs for fuel pricing, IEA reserve coverage

## Data Sources

### SA-Focused (Notebook 01)

| Dataset | Source | Description |
|---------|--------|-------------|
| AIP Terminal Gate Prices | [aip.com.au](https://www.aip.com.au/historical-ulp-and-diesel-tgp-data) | Annual average wholesale fuel prices (ULP, Diesel) by Australian city |
| Australian Petroleum Statistics | [data.gov.au](https://data.gov.au/data/dataset/australian-petroleum-statistics) | Monthly state-level fuel sales, stocks, imports, exports |
| NGER Corporate Emissions | [cer.gov.au](https://cer.gov.au/markets/reports-and-data/nger-reporting-data-and-registers) | Greenhouse gas emissions by corporation (NGER scheme) |
| SA Diesel Generation Plants | [data.sa.gov.au](https://data.sa.gov.au/data/dataset/diesel-generation-plants) | Diesel power generation infrastructure in SA |
| SARIG Mineral Deposits | [catalog.sarig.sa.gov.au](https://catalog.sarig.sa.gov.au/dataset/mesac743) | SA mineral deposits/mines with geographic coordinates |

### National Expansion (Notebook 02)

| Dataset | Source | PM Question Answered |
|---------|--------|---------------------|
| Petroleum Imports by Country | [data.gov.au](https://data.gov.au/data/dataset/australian-petroleum-statistics) | Where does our fuel come from? |
| Petroleum Exports by Country | data.gov.au | What are we exporting? |
| OECD Fuel Price Comparison | data.gov.au | How do we compare internationally? |
| IEA Days of Import Coverage | data.gov.au | How many days of reserves do we have? |
| Petroleum Stock by Product | data.gov.au | Which fuel types are running low? |
| Sales by State & Territory | data.gov.au | Which states consume the most fuel? |
| Domestic Petroleum Production | data.gov.au | Are we producing enough domestically? |
| Refinery Production | data.gov.au | What's our refinery output? |
| NGER Electricity Sector | [cer.gov.au](https://cer.gov.au/markets/reports-and-data/nger-reporting-data-and-registers) | Which power stations depend on fuel? |
| Geoscience AU Mineral Deposits | [Geoscience Australia WFS](https://services.ga.gov.au/gis/earthresource/ows) | Where are critical mining operations? |
| Energy Statistics Table F | [energy.gov.au](https://www.energy.gov.au/energy-data/australian-energy-statistics) | Fuel consumption by state & industry |

## Tables Created (~20 tables)

### Core Tables (Notebook 01)

| Table | Rows | Description |
|-------|------|-------------|
| `aip_terminal_gate_prices` | ~688 | Annual avg wholesale fuel prices by city and fuel type |
| `petroleum_sales_by_state` | ~39,000 | Monthly petroleum sales (all sheets combined) |
| `petroleum_stocks` | ~750 | Petroleum stock volumes |
| `petroleum_supply` | ~750 | Production, refinery output |
| `nger_corporate_emissions` | ~400 | Corporate scope 1/2 emissions |
| `nger_state_emissions_by_industry` | ~400 | NGER register detail |

### National Tables (Notebook 02)

| Table | Rows | Description |
|-------|------|-------------|
| `petroleum_imports_by_country` | ~8,800 | Monthly imports by source country |
| `petroleum_exports_by_country` | ~8,900 | Monthly exports by destination |
| `petroleum_fuel_prices_oecd` | ~395 | Australia vs OECD price comparison |
| `petroleum_au_fuel_prices` | ~64 | Quarterly domestic retail prices |
| `petroleum_iea_days_coverage` | ~187 | IEA days of net import coverage |
| `petroleum_stock_by_product` | ~187 | Stock levels by fuel product type |
| `petroleum_sales_by_state_national` | ~1,300 | Sales by state (dedicated table) |
| `petroleum_production` | ~187 | Domestic petroleum production |
| `petroleum_refinery_production` | ~187 | Refinery output |
| `nger_electricity_sector` | ~809 | Power station emissions + fuel types |
| `geoscience_au_mineral_deposits` | ~5,000 | National mineral deposits (WFS) |

## Quick Start

### Prerequisites
- A Databricks workspace with Unity Catalog enabled
- A catalog and schema you have write access to
- Serverless compute (recommended) or a SQL warehouse

### 1. Run the SA Notebook

```bash
databricks workspace import /Users/<you>/sa-dem-fuel-crisis/01_fuel_crisis_ingestion \
  --file 01_fuel_crisis_ingestion.py --format SOURCE --language PYTHON --overwrite \
  --profile <your-profile>
```

Update the configuration cell with your catalog and schema, then run all cells.

### 2. Run the National Notebook

```bash
databricks workspace import /Users/<you>/sa-dem-fuel-crisis/02_national_fuel_crisis_ingestion \
  --file 02_national_fuel_crisis_ingestion.py --format SOURCE --language PYTHON --overwrite \
  --profile <your-profile>
```

### 3. Create Dashboards

Update config variables in `02_create_dashboard.py`, then:

```bash
python3 02_create_dashboard.py
```

Or import dashboard configs directly:

```bash
# SA Dashboard
databricks api post /api/2.0/lakeview/dashboards \
  --json "$(python3 -c "
import json
with open('dashboard_config.json') as f: d = json.load(f)
d['warehouse_id'] = 'YOUR_WAREHOUSE_ID'
d['parent_path'] = '/Users/you@company.com'
print(json.dumps(d))
")" --profile <your-profile>

# National Dashboard
databricks api post /api/2.0/lakeview/dashboards \
  --json "$(python3 -c "
import json
with open('national_dashboard_config.json') as f: d = json.load(f)
d['warehouse_id'] = 'YOUR_WAREHOUSE_ID'
d['parent_path'] = '/Users/you@company.com'
print(json.dumps(d))
")" --profile <your-profile>
```

### 4. Create Metric Views & Genie Spaces

```bash
python3 03_create_metric_view.py
python3 04_create_genie_space.py
```

## File Structure

```
├── 01_fuel_crisis_ingestion.py            # Notebook: SA data ingestion + AI analysis
├── 02_national_fuel_crisis_ingestion.py   # Notebook: National data ingestion + AI briefing
├── 02_create_dashboard.py                 # Script: create SA Lakeview dashboard
├── 03_create_metric_view.py               # Script: create metric views
├── 04_create_genie_space.py               # Script: create SA Genie space
├── dashboard_config.json                  # Exported SA dashboard definition
├── national_dashboard_config.json         # Exported National dashboard definition
└── README.md
```

## Configuration

The helper scripts use these variables — update for your environment:

| Variable | Description | Example |
|----------|-------------|---------|
| `PROFILE` | Databricks CLI profile | `my-workspace` |
| `WAREHOUSE_ID` | SQL warehouse ID | `abcdef1234567890` |
| `PARENT_PATH` | Workspace path for dashboard/Genie | `/Users/you@company.com` |
| `CATALOG` | Unity Catalog name | `main` |
| `SCHEMA` | Schema name | `fuel_crisis` |

## Architecture

```
Public Data Sources              Databricks Lakehouse                 Consumer Layer
┌──────────────────────┐    ┌─────────────────────────────┐    ┌─────────────────────┐
│ AIP (wholesale/retail│───▶│                             │───▶│ SA Dashboard        │
│   fuel prices)       │    │  Unity Catalog              │    │ (4 pages)           │
│                      │    │  ~20 Delta Tables           │    │                     │
│ DCCEEW Petroleum     │───▶│                             │───▶│ National Dashboard  │
│   Statistics (sales, │    │  ┌─────────────────────┐    │    │ (4 pages)           │
│   stocks, imports,   │    │  │ Petroleum Stats     │    │    │                     │
│   OECD prices)       │    │  │ NGER Emissions      │    │───▶│ SA Genie Space      │
│                      │    │  │ Energy Statistics    │    │    │ (NL Q&A)            │
│ CER NGER (corporate  │───▶│  │ Geoscience Deposits │    │    │                     │
│   + electricity      │    │  └─────────────────────┘    │───▶│ National Genie Space│
│   sector emissions)  │    │                             │    │ (14 tables, NL Q&A) │
│                      │    │  ai_query() Analysis        │    │                     │
│ Geoscience Australia │───▶│  (Claude Sonnet 4.6)        │───▶│ Metric Views        │
│   (WFS mineral data) │    │                             │    │ (fuel pricing KPIs) │
│                      │    │  Metric Views               │    │                     │
│ SA Gov (diesel       │───▶│  (national + SA)            │    │                     │
│   plants, SARIG)     │    └─────────────────────────────┘    └─────────────────────┘
└──────────────────────┘
```

## Notes

- **Serverless compute** is recommended (handles UC credential management automatically)
- Government data source URLs may change — both notebooks handle download failures gracefully
- Column names from Excel files are automatically sanitized for Delta Lake compatibility
- NGER CSV files use Windows-1252 encoding (handled with `encoding='latin-1'`)
- NGER numeric columns are stored as strings with commas — use `CAST(REPLACE(col, ',', '') AS BIGINT)` in queries
- Geoscience Australia data is fetched via OGC WFS (Web Feature Service) returning GeoJSON
- The IEA obligation for fuel reserves is 90 days of net import coverage

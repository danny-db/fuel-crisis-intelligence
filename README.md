# SA Fuel Crisis Intelligence

An end-to-end Databricks demo for the **South Australian Department of Energy and Mining (DEM)** showing how to ingest publicly available Australian fuel and energy datasets, perform AI-powered analysis, and build interactive dashboards — all within the Databricks Lakehouse platform.

## What This Demo Covers

1. **Data Ingestion** — Download public Excel/CSV datasets from Australian government sources using PySpark + pandas
2. **AI Analysis** — Use `ai_query()` with foundation models to generate executive summaries and risk assessments from structured data
3. **AI/BI Dashboard** — 4-page Lakeview dashboard visualizing fuel prices, supply chains, emissions, and industry impact
4. **Genie Space** — Natural language Q&A over the fuel crisis data
5. **Metric Views** — Standardized fuel pricing KPIs

## Data Sources

| Dataset | Source | Description |
|---------|--------|-------------|
| AIP Terminal Gate Prices | [aip.com.au](https://www.aip.com.au/historical-ulp-and-diesel-tgp-data) | Annual average wholesale fuel prices (ULP, Diesel) by Australian city |
| Australian Petroleum Statistics | [data.gov.au](https://data.gov.au/data/dataset/australian-petroleum-statistics) | Monthly state-level fuel sales, stocks, imports, exports |
| NGER Corporate Emissions | [cer.gov.au](https://cer.gov.au/markets/reports-and-data/nger-reporting-data-and-registers) | Greenhouse gas emissions by corporation under the NGER scheme |
| SA Diesel Generation Plants | [data.sa.gov.au](https://data.sa.gov.au/data/dataset/diesel-generation-plants) | Diesel power generation infrastructure in SA |
| SARIG Mineral Deposits | [catalog.sarig.sa.gov.au](https://catalog.sarig.sa.gov.au/dataset/mesac743) | 6,500+ mineral deposits/mines with geographic coordinates |

## Tables Created

All tables are written to Unity Catalog under your configured `CATALOG.SCHEMA`:

| Table | Rows | Description |
|-------|------|-------------|
| `aip_terminal_gate_prices` | ~688 | Annual avg wholesale fuel prices by city and fuel type |
| `petroleum_sales_by_state` | ~39,000 | Monthly petroleum sales by state, product type |
| `petroleum_stocks` | ~750 | Petroleum stock volumes by product |
| `petroleum_supply` | ~750 | Production, refinery output, imports |
| `nger_corporate_emissions` | ~400 | Corporate scope 1/2 emissions and energy consumed |
| `nger_state_emissions_by_industry` | ~400 | NGER register detail |

## Quick Start

### Prerequisites
- A Databricks workspace with Unity Catalog enabled
- A catalog and schema you have write access to
- A SQL warehouse or serverless compute

### 1. Run the Notebook

Upload `01_fuel_crisis_ingestion.py` to your Databricks workspace:

```bash
databricks workspace import /Users/<you>/sa-dem-fuel-crisis/01_fuel_crisis_ingestion \
  --file 01_fuel_crisis_ingestion.py --format SOURCE --language PYTHON --overwrite \
  --profile <your-profile>
```

Open the notebook and **update the configuration cell** with your catalog and schema:

```python
CATALOG = "your_catalog"
SCHEMA = "your_schema"
```

Run all cells. The notebook will:
- Install `openpyxl` and `requests`
- Download datasets from public URLs
- Parse Excel/CSV files with pandas
- Save as Delta tables in Unity Catalog
- Run `ai_query()` analysis with Claude

### 2. Create the Dashboard

Update the config variables in `02_create_dashboard.py`, then run:

```bash
python3 02_create_dashboard.py
```

Or import `dashboard_config.json` directly via the Lakeview API:

```bash
databricks api post /api/2.0/lakeview/dashboards \
  --json "$(python3 -c "
import json
with open('dashboard_config.json') as f: d = json.load(f)
d['warehouse_id'] = 'YOUR_WAREHOUSE_ID'
d['parent_path'] = '/Users/you@company.com'
print(json.dumps(d))
")" --profile <your-profile>
```

### 3. Create the Metric View

Update the catalog/schema in `03_create_metric_view.py`, then run:

```bash
python3 03_create_metric_view.py
```

### 4. Create the Genie Space

Update the config in `04_create_genie_space.py`, then run:

```bash
python3 04_create_genie_space.py
```

## File Structure

```
├── 01_fuel_crisis_ingestion.py    # Databricks notebook: data ingestion + AI analysis
├── 02_create_dashboard.py         # Script: create Lakeview dashboard via REST API
├── 03_create_metric_view.py       # Script: create metric view via SQL statement API
├── 04_create_genie_space.py       # Script: create Genie space via REST API
├── dashboard_config.json          # Exported Lakeview dashboard definition (importable)
└── README.md
```

## Configuration

The helper scripts (`02`, `03`, `04`) use these variables at the top of each file — update them for your environment:

| Variable | Description | Example |
|----------|-------------|---------|
| `PROFILE` | Databricks CLI profile | `my-workspace` |
| `WAREHOUSE_ID` | SQL warehouse ID | `abcdef1234567890` |
| `PARENT_PATH` | Workspace path for dashboard/Genie | `/Users/you@company.com` |
| `CATALOG` | Unity Catalog name | `main` |
| `SCHEMA` | Schema name | `fuel_crisis` |

## Architecture

```
Public Data Sources          Databricks Lakehouse              Consumer Layer
┌─────────────────┐    ┌───────────────────────────┐    ┌──────────────────┐
│ AIP (Excel)     │───▶│                           │───▶│ AI/BI Dashboard  │
│ data.gov.au     │───▶│  Unity Catalog            │───▶│ (4 pages)        │
│ cer.gov.au      │───▶│  Delta Tables             │    │                  │
│ data.sa.gov.au  │───▶│                           │───▶│ Genie Space      │
│ SARIG           │───▶│  ai_query() Analysis      │───▶│ (Natural Lang)   │
└─────────────────┘    │                           │    │                  │
                       │  Metric Views             │───▶│ Metric Views     │
                       └───────────────────────────┘    └──────────────────┘
```

## Notes

- **Serverless compute** is recommended for running the notebook (handles UC credential management automatically)
- Some government data source URLs may change over time — the notebook handles download failures gracefully
- The SA Diesel Generation Plants and SARIG Mineral Deposits datasets may require manual download if their APIs are unavailable
- Column names from government Excel files are automatically sanitized for Delta Lake compatibility
- NGER CSV files use Windows-1252 encoding (handled with `encoding='latin-1'`)

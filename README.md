# Australian Fuel Crisis Intelligence

An end-to-end Databricks demo that builds a **national fuel crisis intelligence platform** — from public data ingestion to AI-powered forecasting, interactive dashboards, natural language Q&A, and a geospatial command centre app.

Demonstrates: **Unity Catalog, Lakeflow Jobs, AI/BI Dashboards, Genie Spaces, AI_FORECAST, FMAPI, Lakebase (PostGIS), Databricks Apps, and DABs.**

## Scenario

An Iran-US military conflict disrupts Middle East oil exports. The Strait of Hormuz is threatened. Australia imports ~90% of its refined fuel. The Prime Minister needs answers NOW.

This platform answers: How exposed are we? What happens to prices? Which states go dark first? How many days of reserves do we have?

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATABRICKS PLATFORM                            │
│                                                                        │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────────────────────┐ │
│  │ Lakeflow Jobs│    │ Unity Catalog│    │    Lakebase (PostGIS)     │ │
│  │ (DAB-managed)│───▶│ 30+ Delta    │───▶│    Reverse ETL            │ │
│  │              │    │ Tables       │    │    MVT Vector Tiles       │ │
│  └──────────────┘    └──────┬───────┘    └───────────┬───────────────┘ │
│                             │                        │                 │
│         ┌───────────────────┼────────────────────────┤                 │
│         ▼                   ▼                        ▼                 │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────────────┐│
│  │ AI/BI        │  │ Genie Space  │  │ Databricks App               ││
│  │ Dashboard    │  │ (NL Q&A)     │  │ "Fuel Crisis Command Centre" ││
│  │ (8 pages)    │  │ (21 tables)  │  │                              ││
│  │              │  │              │  │ Map | Dashboard | Genie | AI ││
│  └──────────────┘  └──────────────┘  └───────────────────────────────┘│
│                                                                        │
│  All deployed via Databricks Asset Bundles (DABs)                      │
└─────────────────────────────────────────────────────────────────────────┘
```

## Data Sources (All Free, No Auth Required)

| Source | Data | Tables |
|--------|------|--------|
| FRED (Federal Reserve) | Brent crude oil prices, AUD/USD exchange rate | 2 |
| EIA (US Energy Info) | OPEC production by country | 1 |
| AIP (Australian Institute of Petroleum) | Wholesale + retail fuel prices | 3 |
| DCCEEW (Petroleum Statistics) | Imports, exports, IEA reserves, sales, production | 11 |
| CER (Clean Energy Regulator) | Corporate + electricity sector emissions | 3 |
| Geoscience Australia (WFS) | Mineral deposits, fuel infrastructure | 4 |
| ABS (SDMX) | CPI automotive fuel by city | 1 |
| WA FuelWatch (RSS) | Station-level daily fuel prices | 1 |
| RBA | Exchange rates (incl. Middle East currencies) | 1 |
| AI_FORECAST() | ML-projected fuel prices + Brent crude | 2 |

## Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog
- Databricks CLI configured (`databricks auth login`)
- Node.js 18+ (for the app frontend build)

### Deploy

```bash
# Clone
git clone https://github.com/danny-db/fuel-crisis-intelligence.git
cd fuel-crisis-intelligence

# Configure your profile and variables
export PROFILE=your-profile

# Deploy the bundle (creates jobs + app)
databricks bundle deploy -t demo --profile $PROFILE

# Run the full E2E pipeline (ingest → dashboard → genie → lakebase)
databricks bundle run full_pipeline -t demo --profile $PROFILE

# Build and deploy the app frontend
cd fuel-crisis-app/frontend && npm install && npm run build && cd ../..
databricks apps deploy fuel-crisis-command-centre \
  --source-code-path $(databricks bundle summary -t demo --profile $PROFILE | grep Path | awk '{print $2}')/files/fuel-crisis-app \
  --profile $PROFILE
```

### Configuration

All notebooks read parameters from DAB `base_parameters`. Override in `databricks.yml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog` | `danny_catalog` | Unity Catalog name |
| `schema` | `dem_schema` | Schema for all tables |
| `warehouse_id` | `4c97f6c0eff73127` | SQL warehouse ID |
| `serving_endpoint` | `databricks-claude-sonnet-4-6` | FMAPI model endpoint |

## File Structure

```
├── databricks.yml                     # DAB bundle (5 jobs + 1 app)
├── 01_fuel_crisis_ingestion.py        # SA data ingestion (AIP, petroleum, NGER, minerals)
├── 02_national_fuel_crisis_ingestion.py # National expansion (imports, OECD, IEA, QLD prices)
├── 03_enrichment_ingestion.py         # Global enrichment (FRED, EIA, ABS, GA WFS) + AI_FORECAST
├── 05_create_story_dashboard.py       # 8-page AI/BI Lakeview dashboard
├── 06_create_enhanced_genie_space.py  # Genie space with 21 tables + data-rooms API
├── 07_setup_lakebase_tiles.py         # Lakebase PostGIS reverse ETL for MVT tiles
├── lakeview_builder.py                # Helper: programmatic Lakeview dashboard builder
├── dashboard_story.lvdash.json        # Exported dashboard JSON (8 pages)
├── DEMO_SCRIPT.md                     # 15-20 min meetup demo talking points
└── fuel-crisis-app/                   # Databricks App (React + FastAPI)
    ├── app.yaml                       # App config (command + env vars)
    ├── requirements.txt               # Python deps
    ├── backend/                        # FastAPI (metrics, infrastructure, genie, briefing, forecast)
    └── frontend/                       # React 18 + TypeScript + MapLibre GL + Recharts + Tailwind
```

## Databricks App — Fuel Crisis Command Centre

| Tab | Description | Data Source |
|-----|-------------|-------------|
| **Map** | Interactive map with refineries, terminals, WA fuel stations, mineral deposits, Strait of Hormuz | Lakebase PostGIS (GeoJSON) |
| **Dashboard** | KPI cards + AI_FORECAST charts (Brent crude + fuel prices) | Databricks SQL + AI_FORECAST() |
| **Genie** | Natural language Q&A with follow-up support + SQL display | Genie Conversation API |
| **AI Briefing** | PM crisis briefing generated via FMAPI (Claude Sonnet) | FMAPI + Databricks SQL |

## E2E Pipeline (Lakeflow Jobs)

```
sa_fuel_data → national_fuel_data → enrichment_data ─┬─ create_dashboard
                                                      ├─ create_genie_space
                                                      └─ setup_lakebase
```

All tasks use serverless compute with `base_parameters` for cross-workspace portability.

## Notes

- **Serverless compute**: Default for this workspace. For workspaces with egress restrictions, switch to classic clusters in `databricks.yml`
- **Lakebase**: Uses INSERT-based reverse ETL. For production, use Sync Tables if you have `CREATE CATALOG` permissions
- **FRED/EIA downloads**: Include retry logic (3 attempts with backoff) for network resilience
- **Genie tables**: Attached via `PATCH /api/2.0/data-rooms/{space_id}` (not the public genie/spaces API)
- **App credentials**: The app's service principal auto-authenticates via Databricks SDK. UC grants are applied by the pipeline.
- **AI_FORECAST**: Uses Databricks built-in ML forecasting on real historical data (no synthetic projections)

## License

Internal demo — not for external distribution.

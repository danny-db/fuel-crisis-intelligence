#!/usr/bin/env python3
"""
Create the SA DEM Fuel Crisis Genie Space via REST API.
"""
import subprocess
import json
import uuid

PROFILE = "e2-demo-west"
WAREHOUSE_ID = "e9b34f7a2e4b0561"
PARENT_PATH = "/Users/danny.wong@databricks.com"
CATALOG = "danny_catalog"
SCHEMA = "dem_schema"
T = f"{CATALOG}.{SCHEMA}"

def gen_id():
    return uuid.uuid4().hex

# Build the serialized_space JSON
serialized_space = {
    "version": 2,
    "config": {
        "sample_questions": sorted([
            {
                "id": gen_id(),
                "question": ["What is the latest Adelaide diesel terminal gate price?"]
            },
            {
                "id": gen_id(),
                "question": ["How have ULP and diesel prices trended over the past 12 months in Adelaide?"]
            },
            {
                "id": gen_id(),
                "question": ["Which fuel type has the highest price volatility?"]
            },
            {
                "id": gen_id(),
                "question": ["How many diesel generation plants are in South Australia?"]
            },
            {
                "id": gen_id(),
                "question": ["What are the top 10 mineral deposits in SA by commodity?"]
            }
        ], key=lambda x: x["id"])
    },
    "data_sources": {},
    "instructions": {
        "text_instructions": [
            {
                "id": gen_id(),
                "content": [
                    "This Genie space supports fuel crisis intelligence analysis for the South Australian Department of Energy and Mining.\n",
                    "\n",
                    "Key tables:\n",
                    f"- {T}.aip_terminal_gate_prices: Annual average wholesale fuel prices (ULP, Diesel) by Australian city. Key columns: year_period, fuel_type, city, price_cpl (cents per litre).\n",
                    f"- {T}.petroleum_sales_by_state: Australian petroleum sales data by state and product type.\n",
                    f"- {T}.petroleum_stocks: Petroleum stock/inventory levels.\n",
                    f"- {T}.nger_corporate_emissions: NGER scheme corporate greenhouse gas emissions data.\n",
                    f"- {T}.nger_state_emissions_by_industry: Emissions breakdown by state and ANZSIC industry.\n",
                    f"- {T}.energy_consumption_by_industry: Energy consumption by industry sector and fuel type.\n",
                    f"- {T}.sa_diesel_generation_plants: SA diesel power generation infrastructure.\n",
                    f"- {T}.sa_mineral_deposits: SA mineral deposits and mines with geographic coordinates.\n",
                    "\n",
                    "Focus area: South Australia (SA). When users ask about 'fuel prices', default to Adelaide terminal gate prices.\n",
                    "When filtering for SA, look for 'South Australia', 'SA', or 'Adelaide' in relevant columns.\n",
                    "Price data is in cents per litre (c/L). To convert to dollars per litre, divide by 100.\n",
                    "The year_period column contains calendar year (e.g. '2024') or financial year (e.g. '2023-24') strings.\n",
                    f"Note: {T}.sa_diesel_generation_plants and {T}.sa_mineral_deposits may not exist if the source data was unavailable.\n"
                ]
            }
        ],
        "example_question_sqls": sorted([
            {
                "id": gen_id(),
                "question": ["Latest Adelaide fuel prices"],
                "sql": [
                    f"SELECT fuel_type, ROUND(AVG(price_cpl), 1) as avg_price_cpl, year_period\n",
                    f"FROM {T}.aip_terminal_gate_prices\n",
                    "WHERE city = 'Adelaide'\n",
                    f"  AND year_period = (SELECT MAX(year_period) FROM {T}.aip_terminal_gate_prices)\n",
                    "GROUP BY fuel_type, year_period\n",
                    "ORDER BY fuel_type;"
                ]
            },
            {
                "id": gen_id(),
                "question": ["Monthly fuel price trend for Adelaide"],
                "sql": [
                    "SELECT year_period, fuel_type,\n",
                    "  ROUND(AVG(price_cpl), 1) as avg_price_cpl,\n",
                    "  ROUND(MAX(price_cpl), 1) as max_price_cpl,\n",
                    "  ROUND(MIN(price_cpl), 1) as min_price_cpl\n",
                    f"FROM {T}.aip_terminal_gate_prices\n",
                    "WHERE city = 'Adelaide'\n",
                    "GROUP BY year_period, fuel_type\n",
                    "ORDER BY year_period DESC, fuel_type;"
                ]
            },
            {
                "id": gen_id(),
                "question": ["Count of SA diesel generation plants"],
                "sql": [
                    f"SELECT COUNT(*) as total_plants FROM {T}.sa_diesel_generation_plants;"
                ]
            }
        ], key=lambda x: x["id"])
    }
}

payload = {
    "title": "SA Fuel Crisis Intelligence",
    "description": (
        "Ask questions about South Australia fuel pricing, supply chain, "
        "heavy fuel users, and crisis preparedness. "
        "Covers AIP pricing, petroleum statistics, NGER emissions, and SA resources data."
    ),
    "warehouse_id": WAREHOUSE_ID,
    "parent_path": PARENT_PATH,
    "serialized_space": json.dumps(serialized_space)
}

result = subprocess.run(
    [
        "databricks", "api", "post",
        "/api/2.0/genie/spaces",
        "--json", json.dumps(payload),
        "--profile", PROFILE
    ],
    capture_output=True,
    text=True
)

if result.returncode == 0:
    response = json.loads(result.stdout)
    space_id = response.get("space_id", response.get("id", "unknown"))
    print(f"Genie space created successfully!")
    print(f"Space ID: {space_id}")
    print(f"URL: https://e2-demo-field-eng.cloud.databricks.com/genie/rooms/{space_id}")
else:
    print(f"Error creating Genie space: {result.stderr}")
    print(f"stdout: {result.stdout}")

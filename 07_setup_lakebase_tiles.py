# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Setup — Reverse ETL to PostGIS for MVT Vector Tiles
# MAGIC
# MAGIC **Data flow:** Unity Catalog Delta tables → Lakebase Autoscaling (PostGIS) → MVT vector tiles
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Creates a Lakebase Autoscaling project (idempotent — skips if exists)
# MAGIC 2. Enables PostGIS, creates spatial tables with GIST indexes
# MAGIC 3. Loads geospatial data from Delta tables into Lakebase
# MAGIC 4. Verifies MVT tile generation works
# MAGIC
# MAGIC Every step is idempotent — safe to re-run.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Note on Sync Table vs INSERT-based loading:**
# MAGIC
# MAGIC This notebook uses direct `INSERT` statements via psycopg2 to load data from Delta into Lakebase.
# MAGIC This is because the shared `fevm` demo workspace does not grant permissions to register new UC catalogs
# MAGIC for Lakebase, which is required by the Sync Table approach (`CREATE SYNC TABLE`).
# MAGIC
# MAGIC **In a real production scenario, Sync Table is the preferred approach:**
# MAGIC ```sql
# MAGIC CREATE SYNC TABLE lakebase_catalog.fuel.refineries
# MAGIC FROM danny_catalog.dem_schema.fuel_infrastructure_refineries;
# MAGIC ```
# MAGIC Sync Table provides automatic CDC (change data capture), zero-ETL replication from Delta to Lakebase,
# MAGIC and is managed entirely by the platform — no custom INSERT logic needed. Use it when you have
# MAGIC `CREATE CATALOG` permissions on the workspace.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary "databricks-sdk>=0.90.0" --upgrade --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE v1,Configuration
try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "danny_catalog"
try:
    SCHEMA = dbutils.widgets.get("schema")
except Exception:
    SCHEMA = "dem_schema"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# Lakebase Autoscaling
LAKEBASE_PROJECT = "fuel-crisis-tiles"
LAKEBASE_BRANCH = "production"
LAKEBASE_DATABASE = "databricks_postgres"
PG_SCHEMA = "fuel"
APP_ROLE = "fuel_crisis_app"

import time, traceback, secrets

APP_PASSWORD = secrets.token_urlsafe(24)

# COMMAND ----------

# DBTITLE v1,Step 1: Create Lakebase Autoscaling Project (idempotent)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Project, ProjectSpec, ProjectDefaultEndpointSettings,
    Branch, Endpoint, EndpointSpec, EndpointType,
)

w = WorkspaceClient()

project_path = f"projects/{LAKEBASE_PROJECT}"
branch_path = f"{project_path}/branches/{LAKEBASE_BRANCH}"
endpoint_path = f"{branch_path}/endpoints/primary"

# Create project (skip if exists)
try:
    project = w.postgres.get_project(name=project_path)
    print(f"Project '{LAKEBASE_PROJECT}' already exists (PG{project.status.pg_version})")
except Exception as e:
    if "NOT_FOUND" in str(e) or "not found" in str(e).lower():
        print(f"Creating project '{LAKEBASE_PROJECT}'...")
        w.postgres.create_project(
            project=Project(
                spec=ProjectSpec(
                    display_name="Fuel Crisis Intelligence — Vector Tiles",
                    pg_version=17,
                    enable_pg_native_login=True,
                    default_endpoint_settings=ProjectDefaultEndpointSettings(
                        autoscaling_limit_min_cu=0.5,
                        autoscaling_limit_max_cu=2,
                    ),
                )
            ),
            project_id=LAKEBASE_PROJECT,
        )
        for attempt in range(12):
            try:
                p = w.postgres.get_project(name=project_path)
                print(f"  Project ready (PG{p.status.pg_version})")
                break
            except Exception:
                pass
            time.sleep(5)
        print(f"Project '{LAKEBASE_PROJECT}' created!")
    else:
        raise

# Verify branch (auto-created with project)
try:
    branch = w.postgres.get_branch(name=branch_path)
    print(f"Branch '{LAKEBASE_BRANCH}': {branch.status.current_state}")
except Exception as e:
    if "NOT_FOUND" in str(e) or "not found" in str(e).lower():
        print(f"Creating branch '{LAKEBASE_BRANCH}'...")
        w.postgres.create_branch(parent=project_path, branch=Branch(), branch_id=LAKEBASE_BRANCH)
        time.sleep(5)
        print(f"Branch created!")
    else:
        raise

# Verify endpoint (auto-created with project)
try:
    ep = w.postgres.get_endpoint(name=endpoint_path)
    print(f"Endpoint: {ep.status.hosts.host} ({ep.status.current_state})")
except Exception as e:
    if "NOT_FOUND" in str(e) or "not found" in str(e).lower():
        print("Creating primary endpoint...")
        w.postgres.create_endpoint(
            parent=branch_path,
            endpoint=Endpoint(spec=EndpointSpec(
                endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=0.5, autoscaling_limit_max_cu=2,
            )),
            endpoint_id="primary",
        )
        time.sleep(10)
        ep = w.postgres.get_endpoint(name=endpoint_path)
        print(f"Endpoint created: {ep.status.hosts.host}")
    else:
        raise

print("Step 1 DONE")

# COMMAND ----------

# DBTITLE v1,Step 2: Get Credentials & Connect
endpoint = w.postgres.get_endpoint(name=endpoint_path)
pg_host = endpoint.status.hosts.host
print(f"Host: {pg_host}")

# Generate credential
cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
pg_token = cred.token
print(f"Token obtained (length: {len(pg_token)})")

# Get current user
me = w.current_user.me()
pg_user = me.user_name
print(f"User: {pg_user}")
print("Step 2 DONE")

# COMMAND ----------

# DBTITLE v1,Step 3: Create Native App Role + PostGIS + Schema
import psycopg2

conn = psycopg2.connect(
    host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
    user=pg_user, password=pg_token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

# Native Postgres role (idempotent)
cur.execute(f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{APP_ROLE}') THEN
            CREATE ROLE {APP_ROLE} WITH LOGIN PASSWORD '{APP_PASSWORD}';
        ELSE
            ALTER ROLE {APP_ROLE} WITH PASSWORD '{APP_PASSWORD}';
        END IF;
    END
    $$;
""")
print(f"Role '{APP_ROLE}' ready")

# PostGIS
cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
print("PostGIS enabled")

# Schema
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};")
print(f"Schema '{PG_SCHEMA}' ready")

# Grants
cur.execute(f"GRANT USAGE ON SCHEMA {PG_SCHEMA} TO {APP_ROLE};")
cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {PG_SCHEMA} TO {APP_ROLE};")
cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {PG_SCHEMA} GRANT SELECT ON TABLES TO {APP_ROLE};")
print("Permissions granted")

cur.close()
conn.close()
print("Step 3 DONE")

# COMMAND ----------

# DBTITLE v1,Step 4: Create PostGIS Tables (idempotent)
conn = psycopg2.connect(
    host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
    user=pg_user, password=pg_token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

tables = {
    "refineries": f"""CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.refineries (
        id SERIAL PRIMARY KEY, name VARCHAR(200), operator VARCHAR(200),
        state VARCHAR(10), status VARCHAR(50), capacity_bpd INTEGER,
        geom geometry(Point, 4326))""",
    "terminals": f"""CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.terminals (
        id SERIAL PRIMARY KEY, name VARCHAR(200), operator VARCHAR(200),
        state VARCHAR(10), type VARCHAR(100), geom geometry(Point, 4326))""",
    "depots": f"""CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.depots (
        id SERIAL PRIMARY KEY, name VARCHAR(200), operator VARCHAR(200),
        state VARCHAR(10), type VARCHAR(100), geom geometry(Point, 4326))""",
    "fuelwatch": f"""CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.fuelwatch (
        id SERIAL PRIMARY KEY, trading_name VARCHAR(200), brand VARCHAR(100),
        address VARCHAR(300), price DOUBLE PRECISION, geom geometry(Point, 4326))""",
    "minerals": f"""CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.minerals (
        id SERIAL PRIMARY KEY, name VARCHAR(300), commodity VARCHAR(200),
        state VARCHAR(50), geom geometry(Point, 4326))""",
}

for name, ddl in tables.items():
    cur.execute(ddl)
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{name}_geom ON {PG_SCHEMA}.{name} USING GIST (geom);")
    print(f"  {name}: OK")

cur.close()
conn.close()
print("Step 4 DONE — All PostGIS tables with GIST indexes")

# COMMAND ----------

# DBTITLE v1,Step 5: Reverse ETL — Delta → Lakebase
conn = psycopg2.connect(
    host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
    user=pg_user, password=pg_token, sslmode="require",
)
conn.autocommit = False
cur = conn.cursor()

def reverse_etl(delta_table, pg_table, col_map):
    """Load point data from Delta table to Lakebase PostGIS."""
    try:
        pdf = spark.table(f"{FULL_SCHEMA}.{delta_table}").toPandas()
        print(f"\n  {delta_table} → {PG_SCHEMA}.{pg_table}: {len(pdf)} source rows")
        cur.execute(f"TRUNCATE {PG_SCHEMA}.{pg_table} RESTART IDENTITY;")
        count = 0
        for _, row in pdf.iterrows():
            lat, lon = row.get("latitude"), row.get("longitude")
            if lat is None or lon is None: continue
            try: lat_f, lon_f = float(lat), float(lon)
            except: continue
            vals = {pg: (str(row.get(dt, ""))[:300] if row.get(dt) is not None else None)
                    for pg, dt in col_map.items()}
            cols = list(vals.keys())
            ph = ", ".join(["%s"] * (len(cols) + 1))
            cur.execute(
                f"INSERT INTO {PG_SCHEMA}.{pg_table} ({', '.join(cols)}, geom) VALUES ({ph})",
                [vals[c] for c in cols] + [f"SRID=4326;POINT({lon_f} {lat_f})"])
            count += 1
        conn.commit()
        print(f"    Loaded {count} points")
        return count
    except Exception as e:
        conn.rollback()
        print(f"    ERROR: {e}")
        return 0

total = 0
total += reverse_etl("fuel_infrastructure_refineries", "refineries",
    {"name":"name","operator":"operator","state":"state","status":"status","capacity_bpd":"capacity_bpd"})
total += reverse_etl("fuel_infrastructure_terminals", "terminals",
    {"name":"name","operator":"operator","state":"state","type":"type"})
total += reverse_etl("fuel_infrastructure_depots", "depots",
    {"name":"name","operator":"operator","state":"state","type":"type"})

# FuelWatch (custom column mapping)
try:
    fw = spark.table(f"{FULL_SCHEMA}.wa_fuelwatch_stations").toPandas()
    print(f"\n  wa_fuelwatch_stations → {PG_SCHEMA}.fuelwatch: {len(fw)} source rows")
    cur.execute(f"TRUNCATE {PG_SCHEMA}.fuelwatch RESTART IDENTITY;")
    count = 0
    for _, row in fw.iterrows():
        lat, lon = row.get("latitude"), row.get("longitude")
        if lat is None or lon is None: continue
        try: lat_f, lon_f = float(lat), float(lon)
        except: continue
        price = None
        try: price = float(row.get("price"))
        except: pass
        cur.execute(
            f"INSERT INTO {PG_SCHEMA}.fuelwatch (trading_name, brand, address, price, geom) "
            f"VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))",
            (str(row.get("trading-name", row.get("trading_name", "")))[:200],
             str(row.get("brand", ""))[:100], str(row.get("address", ""))[:300],
             price, lon_f, lat_f))
        count += 1
    conn.commit()
    total += count
    print(f"    Loaded {count} stations")
except Exception as e:
    conn.rollback()
    print(f"    FuelWatch: {e}")

# Minerals (batch commits for ~5000 points)
try:
    mins = spark.table(f"{FULL_SCHEMA}.geoscience_au_mineral_deposits").toPandas()
    print(f"\n  mineral_deposits → {PG_SCHEMA}.minerals: {len(mins)} source rows")
    cur.execute(f"TRUNCATE {PG_SCHEMA}.minerals RESTART IDENTITY;")
    count = 0
    for _, row in mins.iterrows():
        lat, lon = row.get("latitude"), row.get("longitude")
        if lat is None or lon is None: continue
        try: lat_f, lon_f = float(lat), float(lon)
        except: continue
        name = str(row.get("name", row.get("Name", row.get("DEPOSIT_NAME", ""))))[:300]
        commodity = str(row.get("commodity", row.get("Commodity", row.get("COMMODITY", ""))))[:200]
        state = str(row.get("state", row.get("State", row.get("STATE", ""))))[:50]
        cur.execute(
            f"INSERT INTO {PG_SCHEMA}.minerals (name, commodity, state, geom) "
            f"VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))",
            (name, commodity, state, lon_f, lat_f))
        count += 1
        if count % 500 == 0: conn.commit()
    conn.commit()
    total += count
    print(f"    Loaded {count} minerals")
except Exception as e:
    conn.rollback()
    print(f"    Minerals: {e}")

cur.close()
conn.close()
print(f"\nReverse ETL complete: {total} total points loaded to Lakebase")

# COMMAND ----------

# DBTITLE v1,Step 6: Verify & Test MVT Generation
conn = psycopg2.connect(
    host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
    user=pg_user, password=pg_token, sslmode="require",
)
cur = conn.cursor()

print("=== LAKEBASE VERIFICATION ===")
for table in ["refineries", "terminals", "depots", "fuelwatch", "minerals"]:
    cur.execute(f"SELECT COUNT(*) FROM {PG_SCHEMA}.{table}")
    print(f"  {PG_SCHEMA}.{table}: {cur.fetchone()[0]} rows")

# Test MVT tile generation
cur.execute(f"""
SELECT LENGTH(ST_AsMVT(q, 'refineries', 4096, 'geom')) FROM (
    SELECT ST_AsMVTGeom(
        ST_Transform(geom, 3857),
        ST_MakeEnvelope(12000000, -5000000, 17000000, -1000000, 3857),
        4096, 256, true
    ) AS geom, name
    FROM {PG_SCHEMA}.refineries
) q
""")
mvt_size = cur.fetchone()[0]
print(f"\n  MVT test tile: {mvt_size} bytes (refineries)")

cur.close()
conn.close()

print(f"\n=== APP CONFIGURATION ===")
print(f"LAKEBASE_HOST={pg_host}")
print(f"LAKEBASE_USER={APP_ROLE}")
print(f"LAKEBASE_PASSWORD={APP_PASSWORD}")
print(f"LAKEBASE_SCHEMA={PG_SCHEMA}")

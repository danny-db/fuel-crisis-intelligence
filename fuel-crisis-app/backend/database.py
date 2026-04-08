"""
Database connectivity for Databricks Apps.
Uses databricks-sdk for auto-authentication + databricks-sql-connector for SQL.
"""
import os
import logging
from typing import Optional
from contextlib import contextmanager

logger = logging.getLogger("uvicorn.error")


class DatabricksDB:
    def __init__(self):
        from databricks.sdk import WorkspaceClient

        logger.info("DatabricksDB: initializing WorkspaceClient...")
        self._sdk = WorkspaceClient()
        self._config = self._sdk.config

        self.warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
        self.catalog = os.getenv("DATABRICKS_CATALOG", "danny_catalog")
        self.schema = os.getenv("DATABRICKS_SCHEMA", "dem_schema")

        self._hostname = self._config.host.replace("https://", "").rstrip("/")
        self._http_path = f"/sql/1.0/warehouses/{self.warehouse_id}"

        logger.info(f"DatabricksDB: host={self._hostname} warehouse={self.warehouse_id} schema={self.full_schema}")

    @property
    def full_schema(self) -> str:
        return f"{self.catalog}.{self.schema}"

    @property
    def sdk(self):
        return self._sdk

    def _get_token(self) -> str:
        header = self._config.authenticate()
        auth = header.get("Authorization", "")
        return auth.replace("Bearer ", "") if auth else ""

    @contextmanager
    def get_connection(self):
        from databricks import sql
        token = self._get_token()
        logger.info(f"DatabricksDB: connecting to {self._hostname}{self._http_path} token_len={len(token)}")
        connection = sql.connect(
            server_hostname=self._hostname,
            http_path=self._http_path,
            access_token=token,
            catalog=self.catalog,
            schema=self.schema,
        )
        try:
            yield connection
        finally:
            connection.close()

    def query(self, sql_query: str) -> list[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            return [dict(zip(columns, row)) for row in rows]

    def query_scalar(self, sql_query: str):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else None

    def test_connection(self) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                return True
        except Exception as e:
            logger.error(f"DatabricksDB: connection test failed: {e}")
            return False


_db: Optional[DatabricksDB] = None


def get_databricks_db() -> DatabricksDB:
    global _db
    if _db is None:
        try:
            _db = DatabricksDB()
        except Exception as e:
            logger.error(f"DatabricksDB: INIT FAILED: {e}")
            raise
    return _db


# ============================================================
# Lakebase/PostGIS connection (native Postgres role)
# ============================================================

def get_lakebase_connection():
    """Get a psycopg2 connection to Lakebase using native Postgres credentials.

    Uses env vars: LAKEBASE_HOST, LAKEBASE_PORT, LAKEBASE_DATABASE,
    LAKEBASE_USER, LAKEBASE_PASSWORD, LAKEBASE_SCHEMA.
    """
    import psycopg2

    host = os.getenv("LAKEBASE_HOST", "")
    if not host:
        logger.error("LAKEBASE_HOST not set")
        return None

    port = int(os.getenv("LAKEBASE_PORT", "5432"))
    database = os.getenv("LAKEBASE_DATABASE", "databricks_postgres")
    user = os.getenv("LAKEBASE_USER", "")
    password = os.getenv("LAKEBASE_PASSWORD", "")
    schema = os.getenv("LAKEBASE_SCHEMA", "fuel")

    if not user or not password:
        logger.error("LAKEBASE_USER / LAKEBASE_PASSWORD not set")
        return None

    logger.info(f"Lakebase connecting as {user} to {host}:{port}/{database}")

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            sslmode="require",
            options=f"-c search_path={schema},public",
        )
        return conn
    except Exception as e:
        logger.error(f"Lakebase connection failed: {e}")
        return None

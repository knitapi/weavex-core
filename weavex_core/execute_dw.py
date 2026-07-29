# weavex_core/execute_dw.py
#
# Unified thin HTTP client for all data warehouse operations.
# Forwards to weavex-bridge-dw Cloud Run service.
# No driver code here — all BigQuery/Redshift/Snowflake logic lives in the bridge.
#
# Usage:
#   from weavex_core import (
#       execute_dw_query, execute_dw_write,
#       list_dw_datasets, list_dw_tables, describe_dw_table,
#       create_dw_table
#   )

import os
import httpx
from dataclasses import dataclass, field
from typing import Optional


# ── Result types ───────────────────────────────────────────────────────────────

@dataclass
class DWQueryResult:
    rows:          list[dict]
    row_count:     int
    columns:       list[str]
    bytes_scanned: int = 0
    job_id:        str = ""
    provider:      str = ""
    duration_ms:   int = 0


@dataclass
class DWWriteResult:
    rows_written: int
    rows_failed:  int
    job_id:       str = ""
    provider:     str = ""
    duration_ms:  int = 0


@dataclass
class DatasetInfo:
    name:     str
    location: str = ""


@dataclass
class TableInfo:
    name:      str
    dataset:   str
    full_name: str
    row_count: int = 0


@dataclass
class ColumnInfo:
    name:        str
    type:        str
    mode:        str
    description: str = ""


@dataclass
class TableSchema:
    table:    str
    provider: str
    columns:  list[ColumnInfo] = field(default_factory=list)


@dataclass
class ColumnDefinition:
    name:        str
    type:        str              # generic: string, integer, bigint, float,
    #          boolean, date, timestamp, json
    nullable:    bool = True
    description: str  = ""
    primary_key: bool = False


@dataclass
class CreateTableResult:
    table:       str
    provider:    str
    created:     bool   # False if already existed and if_not_exists=True
    ddl:         str
    duration_ms: int = 0


# ── Query ──────────────────────────────────────────────────────────────────────

def execute_dw_query(
        context:        dict,
        integration_id: str,
        query:          str,
        params:         Optional[dict] = None,
        max_results:    int = 50_000,
        timeout:        int = 120
) -> DWQueryResult:
    """
    Execute a SELECT query against a data warehouse.

    Args:
        context:        Pass as-is from activity params.
        integration_id: integration_ids.get("bigquery") etc.
        query:          SQL with provider-appropriate params.
                          BigQuery:           @param_name
                          Redshift/Snowflake: %(param_name)s
        params:         Query parameters dict.
        max_results:    Max rows to return (default 50k).
        timeout:        Query timeout in seconds (default 120).

    Returns:
        DWQueryResult with rows, row_count, columns, bytes_scanned.
    """
    response = _call_bridge("/query", {
        "context":        context,
        "integration_id": integration_id,
        "query":          query,
        "params":         params,
        "max_results":    max_results,
        "timeout":        timeout
    }, http_timeout=timeout + 30)

    return DWQueryResult(
        rows          = response["rows"],
        row_count     = response["row_count"],
        columns       = response["columns"],
        bytes_scanned = response.get("bytes_scanned", 0),
        job_id        = response.get("job_id", ""),
        provider      = response.get("provider", ""),
        duration_ms   = response.get("duration_ms", 0)
    )


# ── Write ──────────────────────────────────────────────────────────────────────

def execute_dw_write(
        context:           dict,
        integration_id:    str,
        table:             str,
        rows:              list[dict],
        write_mode:        str = "append",
        upsert_keys:       Optional[list[str]] = None,
        s3_integration_id: Optional[str] = None,
        batch_size:        int = 500,
        timeout:           int = 120
) -> DWWriteResult:
    """
    Write rows to a data warehouse table.

    Args:
        context:           Pass as-is from activity params.
        integration_id:    integration_ids.get("bigquery") etc.
        table:             Fully qualified table name.
                             BigQuery:  "project.dataset.table"  or "dataset.table"
                             Redshift:  "schema.table"
                             Snowflake: "database.schema.table"
        rows:              List of dicts — keys are column names.
        write_mode:        "append" | "upsert" | "replace"
        upsert_keys:       Required for write_mode="upsert".
        s3_integration_id: Required for Redshift bulk writes (>=500 rows).
        batch_size:        Rows per batch for inline writes (default 500).
        timeout:           Operation timeout in seconds (default 120).

    Returns:
        DWWriteResult with rows_written, rows_failed, job_id.
    """
    if not rows:
        return DWWriteResult(rows_written=0, rows_failed=0)

    response = _call_bridge("/write", {
        "context":           context,
        "integration_id":    integration_id,
        "table":             table,
        "rows":              rows,
        "write_mode":        write_mode,
        "upsert_keys":       upsert_keys,
        "s3_integration_id": s3_integration_id,
        "batch_size":        batch_size,
        "timeout":           timeout
    }, http_timeout=timeout + 30)

    return DWWriteResult(
        rows_written = response["rows_written"],
        rows_failed  = response["rows_failed"],
        job_id       = response.get("job_id", ""),
        provider     = response.get("provider", ""),
        duration_ms  = response.get("duration_ms", 0)
    )


# ── Discovery ──────────────────────────────────────────────────────────────────

def list_dw_datasets(
        context:        dict,
        integration_id: str
) -> list[DatasetInfo]:
    """
    List all datasets/schemas available for this integration.
    BigQuery  → datasets in the project
    Redshift  → schemas in the database
    Snowflake → databases
    """
    response = _call_bridge("/discovery/datasets", {
        "context":        context,
        "integration_id": integration_id
    })
    return [
        DatasetInfo(name=d["name"], location=d.get("location", ""))
        for d in response["datasets"]
    ]


def list_dw_tables(
        context:        dict,
        integration_id: str,
        dataset:        str
) -> list[TableInfo]:
    """
    List all tables in a dataset/schema.

    Args:
        dataset: dataset name (BQ), schema name (Redshift), or database.schema (Snowflake)
    """
    response = _call_bridge("/discovery/tables", {
        "context":        context,
        "integration_id": integration_id,
        "dataset":        dataset
    })
    return [
        TableInfo(
            name      = t["name"],
            dataset   = t["dataset"],
            full_name = t["full_name"],
            row_count = t.get("row_count", 0)
        )
        for t in response["tables"]
    ]


def describe_dw_table(
        context:        dict,
        integration_id: str,
        table:          str
) -> TableSchema:
    """
    Get full column schema for a specific table.

    Args:
        table: dataset.table (BQ/Redshift) or database.schema.table (Snowflake)
    """
    response = _call_bridge("/discovery/describe", {
        "context":        context,
        "integration_id": integration_id,
        "table":          table
    })
    return TableSchema(
        table    = response["table"],
        provider = response["provider"],
        columns  = [
            ColumnInfo(
                name        = c["name"],
                type        = c["type"],
                mode        = c["mode"],
                description = c.get("description", "")
            )
            for c in response["columns"]
        ]
    )


# ── DDL ────────────────────────────────────────────────────────────────────────

def create_dw_table(
        context:        dict,
        integration_id: str,
        table:          str,
        columns:        list[ColumnDefinition],
        partition_by:   Optional[str]       = None,
        cluster_by:     Optional[list[str]] = None,
        if_not_exists:  bool                = True
) -> CreateTableResult:
    """
    Create a table in the data warehouse.

    Args:
        context:        Pass as-is from activity params.
        integration_id: integration_ids.get("bigquery") etc.
        table:          Fully qualified table name (dataset.table).
        columns:        List of ColumnDefinition objects.
        partition_by:   Column name to partition by (BQ/Redshift DISTKEY).
        cluster_by:     Column names to cluster by (BQ/Snowflake).
        if_not_exists:  Skip if table already exists (default True).

    Column types (generic — mapped per provider):
        string, integer, bigint, float, double, boolean,
        date, datetime, timestamp, json, bytes

    Returns:
        CreateTableResult with table name, DDL executed, and created flag.
    """
    response = _call_bridge("/create-table", {
        "context":        context,
        "integration_id": integration_id,
        "table":          table,
        "columns":        [
            {
                "name":        c.name,
                "type":        c.type,
                "nullable":    c.nullable,
                "description": c.description,
                "primary_key": c.primary_key
            }
            for c in columns
        ],
        "partition_by":  partition_by,
        "cluster_by":    cluster_by,
        "if_not_exists": if_not_exists
    })

    return CreateTableResult(
        table       = response["table"],
        provider    = response["provider"],
        created     = response["created"],
        ddl         = response["ddl"],
        duration_ms = response.get("duration_ms", 0)
    )


# ── HTTP client ────────────────────────────────────────────────────────────────

def _bridge_url() -> str:
    url = os.environ.get("WEAVEX_BRIDGE_DW_URL")
    if not url:
        raise RuntimeError(
            "WEAVEX_BRIDGE_DW_URL not set — "
            "set to weavex-bridge-dw Cloud Run internal URL"
        )
    return url.rstrip("/")


def _call_bridge(endpoint: str, payload: dict, http_timeout: int = 30) -> dict:
    url = f"{_bridge_url()}{endpoint}"
    try:
        with httpx.Client(timeout=http_timeout) as client:
            response = client.post(url, json=payload)
    except httpx.TimeoutException:
        raise RuntimeError(f"Bridge server timed out on {endpoint} after {http_timeout}s")
    except httpx.RequestError as e:
        raise RuntimeError(f"Could not reach bridge server at {url}: {e}")

    if response.status_code == 400:
        raise ValueError(f"DW operation failed: {_detail(response)}")
    if response.status_code >= 500:
        raise RuntimeError(f"Bridge server error {response.status_code}: {_detail(response)}")
    if response.status_code != 200:
        raise RuntimeError(f"Unexpected bridge response {response.status_code}")

    return response.json()


def _detail(response: httpx.Response) -> str:
    try:
        return response.json().get("detail", response.text[:200])
    except Exception:
        return response.text[:200]
#!/usr/bin/env python3
"""
test_dw_customer_snowflake.py
-------------------------------
Manual test for execute_dw_write / execute_dw_query against a customer
table on Snowflake, using weavex_core directly (not raw HTTP) — this
exercises the exact code path a generated workflow uses via execute_dw.py.

Requires:
    WEAVEX_BRIDGE_DW_URL env var set (see execute_dw.py's _bridge_url())
    A real integration_id for a connected Snowflake account with write
    access to the target table.

Usage:
    python test_dw_customer_snowflake.py
"""

import os
import uuid

from weavex_core import execute_dw_query, execute_dw_write

# ── CONFIG — edit before running ────────────────────────────────────────────

INTEGRATION_ID = "wvx_sk_b18wMDJmMGVjOWJhMjI0NzE4Om9fMDAyZjBlYzliYTIyNDcxODpiaWdxdWVyeQ"
TABLE          = "test.customers"   # database.schema.table


def main():
    os.environ["WEAVEX_BRIDGE_DW_URL"] = "http://localhost:9090"

    context = {"execution_id": "manual-test-customer-snowflake"}

    # Unique marker so the query step confirms exactly this row, not just
    # "does the table have any rows at all".
    marker_email = f"test-{uuid.uuid4().hex[:8]}@example.com"

    # ── Write ────────────────────────────────────────────────────────────
    # Single row, well under the 500-row bulk threshold — goes through the
    # inline executemany INSERT path, not internal-stage COPY. `id` is
    # AUTOINCREMENT, so we don't pass it.
    print("--- execute_dw_write ---")
    write_result = execute_dw_write(
        context        = context,
        integration_id = INTEGRATION_ID,
        table          = TABLE,
        rows           = [
            {
                "name":       "Jane Doe3",
                "email":      marker_email,
                "created_at": "2026-01-01T00:00:00Z"
            }
        ],
        write_mode = "append"
    )
    print(f"rows_written={write_result.rows_written} rows_failed={write_result.rows_failed} "
          f"provider={write_result.provider} duration_ms={write_result.duration_ms}")
    assert write_result.rows_written == 1, f"Expected 1 row written, got {write_result.rows_written}"
    assert write_result.rows_failed == 0, f"Expected 0 rows failed, got {write_result.rows_failed}"

    # ── Query ────────────────────────────────────────────────────────────
    # Snowflake INSERTs are synchronous and immediately queryable — no
    # propagation delay to account for (unlike BigQuery's streaming insert).
    print("\n--- execute_dw_query ---")
    query_result = execute_dw_query(
        context        = context,
        integration_id = INTEGRATION_ID,
        query          = f"""
            SELECT COUNT(*) AS record_count
            FROM `test.customers`
            WHERE CONTAINS_SUBSTR(email, @term)
        """,
        params      = {"term": "jan"},
        max_results = 10
    )
    print(f"row_count={query_result.row_count} columns={query_result.columns} "
          f"provider={query_result.provider} duration_ms={query_result.duration_ms}")
    for row in query_result.rows:
        # Snowflake returns column names UPPERCASE by default — access
        # results with .get("ID"), .get("EMAIL"), etc., not lowercase keys.
        print(f"  {row}")

    assert query_result.row_count == 1, (
        f"Expected 1 row for email={marker_email}, got {query_result.row_count}"
    )

    print("\n✓ write + query round-trip verified for Snowflake customer table")


if __name__ == "__main__":
    os.environ["WEAVEX_DW_BRIDGE_API_KEY"] = "e04UUhMQvuu26qmhEaoD1jhtma8DOPwbEmBTPT8lUszyl8XI2iJQOM03Kw8d7g8e"
    main()
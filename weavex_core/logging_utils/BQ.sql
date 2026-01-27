CREATE OR REPLACE TABLE `weavex-475116.logs.api_gateway_logs` (
  log_type STRING,
  method STRING,
  url STRING,
  status_code INTEGER,
  duration_ms INTEGER,
  request_payload JSON,
  response_payload JSON,
  vendor_name STRING,
  api_data JSON,
  context JSON,
  timestamp FLOAT64,
  project_id STRING,
  account_id STRING,
  event_id STRING,
  -- This column now calculates itself
  event_date DATE
)
PARTITION BY event_date
CLUSTER BY project_id, account_id;
OPTIONS (
  partition_expiration_days = 90,
  description = "API traffic logs with 90-day retention"
);

CREATE OR REPLACE TABLE `logs.sync_execution_logs` (
  sync_id STRING,
  log_type STRING,      -- "RECORD_STATUS", "VENDOR_HTTP"
  duration_ms INT64,
  record_id STRING,
  external_id STRING,
  entity_type STRING,
  action STRING,
  status STRING,
  error_message STRING,
  vendor_url STRING,
  vendor_method STRING,
  vendor_request_id STRING,
  sync_data JSON,       -- From 'metadata' in SDK
  context JSON,         -- From 'context' in SDK
  -- Enriched Fields from base.py
  timestamp FLOAT64,
  project_id STRING,
  account_id STRING,
  event_id STRING,
  -- Virtual Column for Partitioning
  event_date DATE
)
PARTITION BY event_date
CLUSTER BY account_id, project_id, sync_id
OPTIONS (
  partition_expiration_days = 90,
  description = "Sync engine internal execution logs"
);

CREATE OR REPLACE TABLE `logs.billing_ledger` (
  source STRING,        -- "API_TRIGGER", "SYNC_WORKFLOW"
  resource_id STRING,
  quantity INT64,
  duration_ms INT64,
  status STRING,
  bill_data JSON,       -- From 'metadata' in SDK
  context JSON,         -- From 'context' in SDK
  -- Enriched Fields from base.py
  timestamp FLOAT64,
  project_id STRING,
  account_id STRING,
  event_id STRING,
  -- Virtual Column for Partitioning
  event_date DATE
)
PARTITION BY event_date
CLUSTER BY account_id, project_id
OPTIONS (
  description = "Immutable billing records with no expiry"
);

CREATE OR REPLACE TABLE `logs.service_logs` (
  severity STRING,
  message STRING,
  sync_id STRING,       -- Optional sync identifier
  context JSON,         -- Mandatory context object
  details JSON,         -- Additional metadata/kwargs
  -- Enriched fields from BaseLogger
  timestamp FLOAT64,
  project_id STRING,
  account_id STRING,
  event_id STRING,
  -- Virtual partition column
  event_date DATE
)
PARTITION BY event_date
CLUSTER BY account_id, project_id, severity
OPTIONS (
  partition_expiration_days = 90,
  description = "Application service logs captured from stdout/stderr"
);

CREATE OR REPLACE TABLE `logs.unknown_logs` (
  raw_payload JSON,      -- Captures the entire unrouted message
  -- Enriched Fields (from base.py)
  timestamp FLOAT64,
  project_id STRING,
  account_id STRING,
  event_id STRING,
  -- Virtual Partition Column
  event_date DATE
)
PARTITION BY event_date
CLUSTER BY account_id, project_id
OPTIONS (
  partition_expiration_days = 90,
  description = "Catch-all for logs that do not match specific routing filters"
);


--------------------
CREATE OR REPLACE VIEW `your_project.logs.unified_audit_log` AS
-- 1. API Traffic Logs
SELECT
    timestamp,
    event_id,
    account_id,
    project_id,
    'API' AS source_table,
    log_type AS event_type,
    sync_id, -- NULL for API logs
    CONCAT(method, ' ', url, ' (', status_code, ')') AS summary,
    JSON_SET(context, '$.api_data', api_data) AS metadata,
    event_date
FROM `your_project.logs.api_gateway_logs`

UNION ALL

-- 2. Sync Execution Logs
SELECT
    timestamp,
    event_id,
    account_id,
    project_id,
    'SYNC' AS source_table,
    log_type AS event_type,
    sync_id,
    CONCAT(entity_type, ' ', action, ': ', status) AS summary,
    JSON_SET(context, '$.sync_data', sync_data) AS metadata,
    event_date
FROM `your_project.logs.sync_execution_logs`

UNION ALL

-- 3. Service & Stdout Logs
SELECT
    timestamp,
    event_id,
    account_id,
    project_id,
    'SERVICE' AS source_table,
    severity AS event_type,
    sync_id,
    message AS summary,
    JSON_SET(context, '$.details', details) AS metadata,
    event_date
FROM `your_project.logs.service_logs`

UNION ALL

-- 4. Billing Ledger
SELECT
    timestamp,
    event_id,
    account_id,
    project_id,
    'BILLING' AS source_table,
    source AS event_type,
    resource_id AS sync_id, -- resource_id often correlates to sync_id
    CONCAT('Billed: ', quantity, ' units (', status, ')') AS summary,
    JSON_SET(context, '$.bill_data', bill_data) AS metadata,
    event_date
FROM `your_project.logs.billing_ledger`

UNION ALL

-- 5. Unknown/Unrouted Logs
SELECT
    timestamp,
    event_id,
    account_id,
    project_id,
    'UNKNOWN' AS source_table,
    'UNROUTED' AS event_type,
    NULL AS sync_id,
    'Raw payload captured' AS summary,
    raw_payload AS metadata,
    event_date
FROM `your_project.logs.unknown_logs`;
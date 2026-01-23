import sys
import json
import time
import queue
import threading
import os  # Added missing os import
from typing import List, Dict  # Added Dict to typing imports

# Google Cloud Imports
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import pubsub_v1

from .base import BaseLogger

# -------------------------------------------------------------------------
# STDOUT LOGGER (Local Dev)
# -------------------------------------------------------------------------
class StdoutLogger(BaseLogger):
    def log(self, payload: dict, blocking: bool = False):
        data = self._enrich(payload)
        # We print JSON so it's parsable by tools like Datadog/CloudWatch if needed
        print(json.dumps(data, default=str), flush=True)

    def flush(self):
        sys.stdout.flush()

    def shutdown(self):
        self.flush()

    def _print_std(self, severity: str, message: str, details: Dict = None):
        """
        Helper to print JSON-structured logs to stdout.
        Cloud Run/Logging picks up the 'severity' field automatically.
        """
        payload = {
            "severity": severity,
            "message": message,
            "timestamp": time.time(),
            "component": "weavex-core",
            "project_id": self.project_id
        }
        if details:
            payload.update(details)

        # Write to stderr for errors so they flag immediately in consoles
        stream = sys.stderr if severity in ["ERROR", "CRITICAL"] else sys.stdout
        print(json.dumps(payload, default=str), file=stream, flush=True)

    def info(self, message: str, **kwargs):
        """Log simple info messages."""
        self._print_std("INFO", message, kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning messages."""
        self._print_std("WARNING", message, kwargs)

    def error(self, message: str, **kwargs):
        """Log error messages."""
        self._print_std("ERROR", message, kwargs)

    def debug(self, message: str, **kwargs):
        """Log debug messages (useful for dev)."""
        self._print_std("DEBUG", message, kwargs)

# -------------------------------------------------------------------------
# ASYNC BIGQUERY LOGGER (Production)
# -------------------------------------------------------------------------
class PubSubLogger(BaseLogger):
    def __init__(self, topic_id: str, project_id: str = None):
        super().__init__(project_id)
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, topic_id)

    def log(self, payload: dict, blocking: bool = True):
        # 1. Identify the table for the filter (API, SYNC, or BILLING)
        log_table = payload.get("log_table", "UNKNOWN")

        # 2. Standard enrichment
        data = self._enrich(payload)
        message_bytes = json.dumps(data, default=str).encode("utf-8")

        # 3. Publish with 'log_table' as a metadata attribute
        future = self.publisher.publish(
            self.topic_path,
            message_bytes,
            log_table=log_table  # <--- Crucial for filtering
        )

        if blocking:
            future.result()

    def flush(self):
        pass # PubSub client handles its own batching/flushing

    def shutdown(self):
        pass

    # Updated transports.py snippet for PubSubLogger
    def _print_std(self, severity: str, message: str, details: Dict = None):
        """
        Captures stdout logs, enriches them with mandatory context/sync_id,
        and routes them to the 'SERVICE' BigQuery table via Pub/Sub.
        """
        details = details or {}

        # 1. Extract specifically required fields
        sync_id = details.pop("sync_id", None)
        context = details.pop("context", {}) # Mandatory: default to empty dict if missing

        # 2. Build the 'SERVICE' table payload
        payload = {
            "log_table": "SERVICE",
            "severity": severity,
            "message": message,
            "sync_id": sync_id,
            "context": context,
            "details": details  # Remaining kwargs
        }

        # 3. Ingest via Pub/Sub
        self.log(payload, blocking=False)

        # 4. Local console print (enriched with event_id/timestamp from self.log)
        stream = sys.stderr if severity in ["ERROR", "CRITICAL"] else sys.stdout
        print(json.dumps(payload, default=str), file=stream, flush=True)

    def info(self, message: str, **kwargs):
        """Log simple info messages."""
        self._print_std("INFO", message, kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning messages."""
        self._print_std("WARNING", message, kwargs)

    def error(self, message: str, **kwargs):
        """Log error messages."""
        self._print_std("ERROR", message, kwargs)

    def debug(self, message: str, **kwargs):
        """Log debug messages (useful for dev)."""
        self._print_std("DEBUG", message, kwargs)
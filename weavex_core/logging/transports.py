import sys
import json
import time
import queue
import threading
import os  # Added missing os import
from typing import List, Dict  # Added Dict to typing imports

# Google Cloud Imports
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError

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
class AsyncBigQueryLogger(BaseLogger):
    def __init__(self, table_path=None, project_id=None):
        super().__init__(project_id)

        self.table_path = table_path or os.getenv("WEAVEX_BQ_TABLE")
        if not self.table_path:
            raise ValueError("Table path is required for BigQuery Logger")

        # Initialize Client
        # Note: We use the standard Client for insert_rows_json as it handles
        # dictionary->JSON conversion natively.
        self.client = bigquery.Client(project=self.project_id)

        # Async Buffer
        self.queue = queue.Queue(maxsize=10000)
        self.batch_size = 100
        self.linger_ms = 0.5  # 500ms max wait before writing a partial batch
        self.running = True

        # Start Background Thread
        self.worker = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker.start()

    def log(self, payload: dict, blocking: bool = False):
        data = self._enrich(payload)

        # Case A: High Priority (Billing/Audit) - Write Immediately & Wait
        if blocking:
            self._write_batch_sync([data])
            return

        # Case B: Standard Logs - buffer them
        try:
            self.queue.put(data, block=False)
        except queue.Full:
            # Fallback to stderr if queue is full to avoid crashing the app
            print(f"⚠️ Log Buffer Full! Dropping log: {data.get('log_type')}", file=sys.stderr)

    def flush(self):
        """Injects a sentinel to force the worker to drain the queue."""
        if not self.running: return
        flush_event = threading.Event()
        self.queue.put(("__FLUSH__", flush_event))
        flush_event.wait(timeout=10.0)

    def shutdown(self):
        if not self.running: return
        self.running = False

        # Drain remaining items
        final_batch = []
        while not self.queue.empty():
            try:
                item = self.queue.get_nowait()
                if isinstance(item, dict): final_batch.append(item)
            except queue.Empty:
                break

        if final_batch:
            print(f"⚠️ Flushing {len(final_batch)} logs before exit...", file=sys.stderr)
            self._write_batch_sync(final_batch)

    def _worker_loop(self):
        batch = []
        last_flush = time.time()

        while self.running:
            try:
                # Wait for items
                item = self.queue.get(timeout=self.linger_ms)

                # Check for Flush Sentinel
                if isinstance(item, tuple) and item[0] == "__FLUSH__":
                    if batch: self._write_batch_sync(batch)
                    batch = []
                    last_flush = time.time()
                    item[1].set() # Signal main thread
                    continue

                batch.append(item)
            except queue.Empty:
                pass

            # Auto-Flush Logic (Batch Size or Time limit)
            time_since = time.time() - last_flush
            if len(batch) >= self.batch_size or (batch and time_since >= self.linger_ms):
                self._write_batch_sync(batch)
                batch = []
                last_flush = time.time()

    def _write_batch_sync(self, batch: List[dict]):
        """
        Writes a batch of logs to BigQuery using streaming inserts.
        """
        if not batch: return

        try:
            # table_path format: "project.dataset.table"
            errors = self.client.insert_rows_json(self.table_path, batch)

            if errors:
                print(f"❌ BigQuery Insert Errors in {self.table_path}: {errors}", file=sys.stderr)
                # In a real system, you might write these to a dead-letter file
        except GoogleAPICallError as e:
            print(f"❌ BigQuery Network Failed: {e}", file=sys.stderr)
        except Exception as e:
            print(f"❌ BigQuery Unknown Error: {e}", file=sys.stderr)

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
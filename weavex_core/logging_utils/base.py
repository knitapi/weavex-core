import os
import time
import uuid
import signal
import sys
import atexit
from abc import ABC, abstractmethod
from typing import Dict, Any
from datetime import datetime, timezone

class BaseLogger(ABC):
    def __init__(self, project_id: str = None, account_id: str = None, gcp_project_id: str = None):
        # Fallback to environment variables if not explicitly provided
        self.project_id = project_id or os.getenv("WEAVEX_PROJECT_ID")
        self.account_id = account_id or os.getenv("WEAVEX_ACCOUNT_ID")

        if not self.project_id:
            print("⚠️ Warning: WEAVEX_PROJECT_ID not set. Logging may fail.", file=sys.stderr)

        # Actual GCP Infrastructure Identity
        self.gcp_project_id = gcp_project_id or os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")

        if not self.gcp_project_id:
            print("⚠️ Warning: GCP_PROJECT_ID not set. Pub/Sub calls will fail.", file=sys.stderr)

        # Register lifecycle handlers for clean shutdown
        atexit.register(self.shutdown)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    @abstractmethod
    def log(self, payload: Dict[str, Any], blocking: bool = False):
        """
        blocking=False: Async publish (Pub/Sub default).
        blocking=True: Wait for server acknowledgment (Critical for Billing).
        """
        pass

    @abstractmethod
    def shutdown(self):
        """Cleanup Pub/Sub clients and flush internal buffers."""
        pass

    def _enrich(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Standardizes every log entry with critical metadata.
        Ensures the payload matches the BigQuery schema expectations.
        """
        # 1. Timing: Use Unix float for precision (matches BQ FLOAT64)
        now_utc = datetime.now(timezone.utc)

        if "timestamp" not in payload:
            payload["timestamp"] = now_utc.timestamp()

        if "event_date" not in payload:
            # Format: 'YYYY-MM-DD'
            payload["event_date"] = datetime.fromtimestamp(now_utc.timestamp()).strftime('%Y-%m-%d')

        # 2. Identity: Ensure project and account IDs are present
        if "project_id" not in payload:
            payload["project_id"] = self.project_id
        if "account_id" not in payload:
            payload["account_id"] = self.account_id

        # 3. Traceability: Unique event ID for deduplication in BigQuery
        if "event_id" not in payload:
            payload["event_id"] = str(uuid.uuid4())

        return payload

    def _handle_sigterm(self, signum, frame):
        """Ensures logs are flushed when the container/process is killed."""
        print("🛑 Received SIGTERM. Shutting down logger...", file=sys.stderr)
        self.shutdown()
        sys.exit(0)
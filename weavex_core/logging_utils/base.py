import os
import time
import uuid
import signal
import sys
import atexit
from abc import ABC, abstractmethod

class BaseLogger(ABC):
    def __init__(self, project_id=None):
        self.project_id = project_id or os.getenv("WEAVEX_PROJECT_ID")
        self.host_name = os.getenv("HOSTNAME", "unknown-host")

        # Auto-shutdown on exit to flush logs
        atexit.register(self.shutdown)
        # Catch termination signals (critical for Cloud Run / K8s)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    @abstractmethod
    def log(self, payload: dict, blocking: bool = False):
        """
        blocking=False: Fire & Forget (Async). For Debug/Audit.
        blocking=True: Wait for confirm (Sync). For Billing/Checkpoints.
        """
        pass

    @abstractmethod
    def flush(self):
        """Force writes all pending logs."""
        pass

    @abstractmethod
    def shutdown(self):
        """Cleanup resources and final flush."""
        pass

    def _enrich(self, payload):
        """Adds standard metadata to every log."""
        if "timestamp" not in payload:
            # BigQuery likes timestamps in seconds (float) or ISO strings
            payload["timestamp"] = time.time()
        if "project_id" not in payload and self.project_id:
            payload["project_id"] = self.project_id
        if "event_id" not in payload:
            payload["event_id"] = str(uuid.uuid4())

        # Add runtime context
        payload["_host"] = self.host_name
        return payload

    def _handle_sigterm(self, signum, frame):
        print("🛑 Received SIGTERM. Flushing logs...", file=sys.stderr)
        self.shutdown()
        sys.exit(0)
import time
from typing import Optional, Dict, Any
import os, json

from .transports import PubSubLogger, StdoutLogger

class WeavexServicesLogger:
    """
    The main logging entry point for Weavex Services.
    Routes logs to the correct BigQuery tables based on the method called.
    """

    def __init__(self, project_id: str = None, logger_type: str = "STDOUT", gcp_project_id: str = None):
        self.project_id = project_id or os.getenv("WEAVEX_PROJECT_ID")
        self.gcp_project_id = gcp_project_id or os.getenv("GCP_PROJECT_ID")
        self.logger_type = logger_type

        if self.logger_type == "PUBSUB":
            topic_id = os.getenv("WEAVEX_LOG_TOPIC", "weavex-logs")
            self.logger = PubSubLogger(
                topic_id=topic_id,
                project_id=self.project_id,
                gcp_project_id=self.gcp_project_id
            )
        else:
            # Fallback to standard output for local development
            self.logger = StdoutLogger(self.project_id)

    def log_api_traffic(self,
                        log_type: str,
                        method: str,
                        url: str,
                        status_code: int,
                        duration_ms: int,
                        req_payload: Dict[str, Any],
                        resp_payload: Dict[str, Any],
                        vendor_name: Optional[str] = None,
                        metadata: Optional[Dict[str, Any]] = None,
                        context: Optional[Dict[str, Any]] = None):
        """
        Logs ingress (Gateway) or egress (Vendor) API traffic.
        """
        payload = {
            "log_table": "API",
            "log_type": log_type,
            "method": method,
            "url": url,
            "status_code": int(status_code),
            "duration_ms": int(duration_ms),
            # Stringify JSON fields for BQ ingestion compatibility
            "request_payload": json.dumps(req_payload or {}),
            "response_payload": json.dumps(resp_payload or {}),
            "vendor_name": vendor_name,
            "api_data": json.dumps(metadata or {}),
            "context": json.dumps(context or {})
        }
        self.logger.log(payload, blocking=True)

    def log_sync_event(self,
                       sync_id: str,
                       log_type: str,
                       duration_ms: int,
                       record_id: Optional[str] = None,
                       external_id: Optional[str] = None,
                       entity_type: Optional[str] = None,
                       action: Optional[str] = None,
                       status: Optional[str] = None,
                       error_message: Optional[str] = None,
                       vendor_url: Optional[str] = None,
                       vendor_method: Optional[str] = None,
                       vendor_req_id: Optional[str] = None,
                       metadata: Optional[Dict[str, Any]] = None,
                       context: Optional[Dict[str, Any]] = None):
        """
        Logs internal sync logic (Record Status) or external calls (Vendor HTTP).
        """
        payload = {
            "log_table": "SYNC",
            "sync_id": sync_id,
            "log_type": log_type,
            "duration_ms": int(duration_ms),
            "record_id": record_id,
            "external_id": external_id,
            "entity_type": entity_type,
            "action": action,
            "status": status,
            "error_message": error_message,
            "vendor_url": vendor_url,
            "vendor_method": vendor_method,
            "vendor_request_id": vendor_req_id,
            # Stringify JSON fields
            "sync_data": json.dumps(metadata or {}),
            "context": json.dumps(context or {})
        }
        self.logger.log(payload, blocking=True)

    def log_billable_event(self,
                           source: str,
                           resource_id: str,
                           quantity: int,
                           duration_ms: int,
                           metadata: Optional[Dict[str, Any]] = None,
                           context: Optional[Dict[str, Any]] = None,
                           status: str = "SUCCESS"):
        """
        CRITICAL: Logs billable events. Uses blocking=True to ensure durability.
        """
        payload = {
            "log_table": "BILLING",
            "source": source,
            "resource_id": resource_id,
            "quantity": int(quantity),
            "duration_ms": int(duration_ms),
            "status": status,
            # Stringify JSON fields
            "bill_data": json.dumps(metadata or {}),
            "context": json.dumps(context or {})
        }
        self.logger.log(payload, blocking=True)

    def info(self, message: str, **kwargs):
        """Logs info message to 'service_logs' table."""
        self.logger.info(message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Logs warning message to 'service_logs' table."""
        self.logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs):
        """Logs error message to 'service_logs' table."""
        self.logger.error(message, **kwargs)

    def debug(self, message: str, **kwargs):
        """Logs debug message to 'service_logs' table."""
        self.logger.debug(message, **kwargs)

    def shutdown(self):
        time.sleep(5)
        self.logger.shutdown()
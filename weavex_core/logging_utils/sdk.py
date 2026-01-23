from typing import Optional, Dict, Any
import os

from .transports import PubSubLogger, StdoutLogger

class WeavexServicesLogger:
    """
    The main logging entry point for Weavex Services.
    Routes logs to the correct BigQuery tables based on the method called.
    """

    def __init__(self, project_id: str, logger_type: str = "STDOUT"):
        self.project_id = project_id or os.getenv("WEAVEX_PROJECT_ID")
        self.logger_type = logger_type

        # Initialize Transport
        if self.logger_type == "PUB_SUB":
            # Using a single ingestion topic for all log types.
            # The consumer will route them to the right table based on the payload.
            topic_id = os.getenv("WEAVEX_LOG_TOPIC", "weavex-logs")

            # We use one logger instance; it handles everything via the same topic
            self.logger = PubSubLogger(topic_id=topic_id, project_id=self.project_id)
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
            "log_type": log_type,  # "GATEWAY_ENTRY", "VENDOR_CALL"
            "method": method,
            "url": url,
            "status_code": status_code,
            "duration_ms": duration_ms,
            "request_payload": req_payload,
            "response_payload": resp_payload,
            "vendor_name": vendor_name,
            "api_data": metadata or {},
            "context": context or {}
        }
        self.logger.log(payload, blocking=False)

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
            "log_type": log_type, # "RECORD_STATUS", "VENDOR_HTTP"
            "duration_ms": duration_ms,
            "record_id": record_id,
            "external_id": external_id,
            "entity_type": entity_type,
            "action": action,
            "status": status,
            "error_message": error_message,
            "vendor_url": vendor_url,
            "vendor_method": vendor_method,
            "vendor_request_id": vendor_req_id,
            "sync_data": metadata or {},
            "context": context or {}
        }
        self.logger.log(payload, blocking=False)

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
            "source": source,        # "API_TRIGGER", "SYNC_WORKFLOW"
            "resource_id": resource_id,
            "quantity": quantity,
            "duration_ms": duration_ms,
            "status": status,
            "bill_data": metadata or {},
            "context": context or {}
        }
        self.logger.log(payload, blocking=True)

    def shutdown(self):
        self.logger.shutdown()
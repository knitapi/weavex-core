from typing import Optional, Dict, Any
import os

from .transports import AsyncBigQueryLogger, StdoutLogger

class WeavexServicesLogger:
    """
    The main logging entry point for Weavex Services.
    Routes logs to the correct BigQuery tables based on the method called.
    """

    def __init__(self, project_id: str, logger_type: str = "STDOUT"):
        self.project_id = project_id
        self.logger_type = logger_type

        # Initialize Transport
        # In DEV/LOCAL, we often just want to see logs in the terminal
        if self.logger_type == "BQ":
            self.api_logger = AsyncBigQueryLogger(table_path="logs.api_gateway_logs", project_id=project_id)
            self.sync_logger = AsyncBigQueryLogger(table_path="logs.sync_execution_logs", project_id=project_id)
            self.billing_logger = AsyncBigQueryLogger(table_path="logs.billing_ledger", project_id=project_id)
        else:
            self.api_logger = StdoutLogger(project_id)
            self.sync_logger = StdoutLogger(project_id)
            self.billing_logger = StdoutLogger(project_id)

    def log_api_traffic(self,
                        execution_id: str,
                        log_type: str,
                        account_id: str,
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
            "execution_id": execution_id,
            "log_type": log_type,  # "GATEWAY_ENTRY", "VENDOR_CALL"
            "account_id": account_id,
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
        self.api_logger.log(payload, blocking=False)

    def log_sync_event(self,
                       sync_id: str,
                       account_id: str,
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
            "sync_id": sync_id,
            "account_id": account_id,
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
        self.sync_logger.log(payload, blocking=False)

    def log_billable_event(self,
                           account_id: str,
                           project_id: str,
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
            "account_id": account_id,
            "project_id": project_id,
            "source": source,        # "API_TRIGGER", "SYNC_WORKFLOW"
            "resource_id": resource_id,
            "quantity": quantity,
            "duration_ms": duration_ms,
            "status": status,
            "bill_data": metadata or {},
            "context": context or {}
        }
        self.billing_logger.log(payload, blocking=True)

    def shutdown(self):
        self.api_logger.shutdown()
        self.sync_logger.shutdown()
        self.billing_logger.shutdown()
import json
import sys
import time
import os

class Logger:
    def __init__(self, account_id, execution_id, project_id):
        self.account_id = account_id
        self.execution_id = execution_id
        self.project_id = project_id
        self.sequence_id = 0

        if not self.project_id:
            # You can decide to raise error here or warn
            print(json.dumps({"severity": "WARNING", "message": "`project_id` not set"}))

    def log(self, message, severity="INFO", step="general", data=None):
        self.sequence_id += 1
        entry = {
            "account_id": self.account_id,
            "project_id": self.project_id,
            "severity": severity,
            "message": message,
            "execution_id": self.execution_id,
            "sequence_id": self.sequence_id,
            "component": "workflow", # You might make this dynamic
            "step": step,
            "timestamp": int(time.time() * 1000)
        }
        if data:
            entry["data"] = data

        print(json.dumps(entry))
        sys.stdout.flush()

    def log_api_call(self, url, method, request_body, response_status, response_body):
        # Special helper for full API tracing
        self.log(
            message=f"API Call: {method} {url} [{response_status}]",
            step="api_call",
            data={
                "url": url,
                "method": method,
                "request": request_body,
                "response": response_body,
                "status_code": response_status
            }
        )

    def log_response(self, status_code, body):
        self.log(
            message=f"Response: {status_code}",
            severity="ERROR" if status_code >= 400 else "INFO",
            step="response",
            data={"status_code": status_code, "body": body}
        )
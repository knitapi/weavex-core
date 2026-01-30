import json
import requests
from dataclasses import dataclass
from typing import Optional, Any, Dict

@dataclass
class VendorResponse:
    actual_resp: str
    status_code: int
    body: Any  # Automatically parsed JSON if possible, else raw string
    headers: Dict[str, str]

def make_passthrough_call(
        context: dict,
        integration_id: str,
        method: str,
        path: str,
        body: Optional[dict] = None,
        content_type: str = None,
        headers: Optional[dict] = None
) -> VendorResponse:
    """
    Makes an authenticated call via the Knit API Proxy.
    Validates that the context contains required auth and tracing details.
    """

    # 1. Strict Validation of Context
    if not isinstance(context, dict):
        raise ValueError("The 'context' parameter must be a dictionary.")

    api_key = context.get("knit_api_key")
    execution_id = context.get("execution_id")
    # Retrieve environment, defaulting to production
    knit_env = str(context.get("knit_env", "production")).lower()

    if not api_key:
        raise ValueError("Missing 'knit_api_key' in context. Authentication is required.")

    if not execution_id:
        raise ValueError("Missing 'execution_id' in context. Tracing is required.")

    # 2. Setup Request
    # Dynamic URL selection based on environment
    if knit_env == "sandbox":
        base_url = "https://api.sandbox.getknit.dev/v1.0/passthrough"
    else:
        base_url = "https://api.getknit.dev/v1.0/passthrough"

    final_headers = {
        "Authorization": f"Bearer {api_key}",
        "X-Knit-Integration-Id": integration_id,
        "X-Knit-Execution-Id": execution_id, # Propagation for tracing
        "Content-Type": "application/json"
    }

    # TODO: send 'context' as well
    payload = {
        "method": method.upper(),
        "path": path,
        "body": json.dumps(body) if body else None,
        "contentType": content_type if content_type else "application/json",
        "headers": headers if headers else {"Accept": "application/json"}
    }

    # 3. Network Call
    try:
        resp = requests.post(base_url, json=payload, headers=final_headers)
    except Exception as e:
        raise RuntimeError(f"Proxy Network Connection Error: {e}")

    # 4. Extract Status & Unwrap Body
    final_status = resp.status_code
    final_body = None
    final_headers = {}

    try:
        if resp.content:
            proxy_data = resp.json()
            response_wrapper = proxy_data.get("data", {}).get("response", {})

            final_headers = response_wrapper.get("headers", {})
            raw_body_str = response_wrapper.get("body", "{}")

            try:
                final_body = json.loads(raw_body_str)
            except (json.JSONDecodeError, TypeError):
                final_body = raw_body_str
    except Exception:
        final_body = resp.text

    return VendorResponse(actual_resp=resp.content, status_code=final_status, body=final_body, headers=final_headers)
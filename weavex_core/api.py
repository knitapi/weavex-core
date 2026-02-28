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
    Handles nested JSON string decoding for backward compatibility.
    """

    # 1. Strict Validation of Context
    if not isinstance(context, dict):
        raise ValueError("The 'context' parameter must be a dictionary.")

    api_key = context.get("knit_api_key")
    execution_id = context.get("execution_id")
    knit_env = str(context.get("knit_env", "production")).lower()
    region = str(context.get("region", "")).lower()

    if not api_key or not execution_id:
        raise ValueError("Missing 'knit_api_key' or 'execution_id' in context.")

    if not execution_id:
        raise ValueError("Missing 'execution_id' in context. Tracing is required.")

    # 2. Setup Request
    if knit_env == "sandbox":
        base_url = "https://api.sandbox.getknit.dev/v1.0/weavex.passthrough"
    elif region == "eu":
        base_url = "https://api.eu.getknit.dev/v1.0/weavex.passthrough"
    else:
        base_url = "https://api.getknit.dev/v1.0/weavex.passthrough"

    final_headers = {
        "Authorization": f"Bearer {api_key}",
        "X-Knit-Integration-Id": integration_id,
        "X-Knit-Execution-Id": execution_id, # Propagation for tracing
        "Content-Type": "application/json"
    }

    payload = {
        "context": context,
        "method": method.upper(),
        "path": path,
        "body": json.dumps(body) if body else None,
        "contentType": content_type if content_type else "application/json",
        "headers": headers if headers else {"Accept": "application/json"}
    }

    # 3. Network Call
    try:
        resp = requests.post(base_url, json=payload, headers=final_headers)
        resp_content = resp.content
    except Exception as e:
        raise RuntimeError(f"Proxy Network Connection Error: {e}")

    # 4. Extract Status & Unwrap Body
    final_status = resp.status_code
    final_body = None
    final_headers = {}

    try:
        proxy_data = resp.json()

        # Check for proxy-level success
        if not proxy_data.get("success", False):
            error_info = proxy_data.get("error", {})
            final_body = error_info.get("msg", "Unknown error from proxy")
            return VendorResponse(actual_resp=resp_content, status_code=final_status, body=final_body, headers={})

        # Navigate to the inner response
        response_wrapper = proxy_data.get("data", {}).get("response", {})
        final_headers = response_wrapper.get("headers", {})
        raw_body = response_wrapper.get("body", "{}")

        # --- RECURSIVE DECODING LOGIC ---
        # This handles the "Double-Encoding" shown in your logs
        if isinstance(raw_body, str):
            try:
                # First pass: Converts escaped string to clean string or dict
                decoded = json.loads(raw_body)
                # Second pass: If it's still a string, decode it again into a dict
                if isinstance(decoded, str):
                    try:
                        final_body = json.loads(decoded)
                    except (json.JSONDecodeError, TypeError):
                        final_body = decoded
                else:
                    final_body = decoded
            except (json.JSONDecodeError, TypeError):
                final_body = raw_body
        else:
            final_body = raw_body

    except Exception:
        # Fallback to raw text if JSON parsing fails entirely
        final_body = resp.text

    return VendorResponse(actual_resp=resp.content, status_code=final_status, body=final_body, headers=final_headers)
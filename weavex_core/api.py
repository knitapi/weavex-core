import os
import json
import requests
from dataclasses import dataclass
from typing import Optional, Any, Dict

@dataclass
class VendorResponse:
    status_code: int
    body: Any  # Automatically parsed JSON if possible, else raw string
    headers: Dict[str, str]

def make_passthrough_call(
        integration_id: str,
        method: str,
        path: str,
        params: Optional[dict] = None,
        body: Optional[dict] = None
) -> VendorResponse:

    api_key = os.environ.get("KNIT_API_KEY")
    base_url = "https://api.getknit.dev/v1.0/passthrough"

    if not api_key:
        raise ValueError("Environment variable 'KNIT_API_KEY' is required.")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "X-Knit-Integration-Id": integration_id,
        "Content-Type": "application/json"
    }

    payload = {
        "method": method.upper(),
        "path": path,
        "params": params,
        "body": json.dumps(body) if body else None
    }

    # 1. Network Call
    # We do NOT raise_for_status() here because we want to return 4xx/5xx to the caller
    # so they can decide logic (e.g., 404 might be valid "Not Found").
    try:
        resp = requests.post(base_url, json=payload, headers=headers)
    except Exception as e:
        raise RuntimeError(f"Proxy Network Connection Error: {e}")

    # 2. Extract Status (Directly from Proxy Response)
    final_status = resp.status_code

    # 3. Unwrap Body
    final_body = None
    final_headers = {}

    try:
        if resp.content:
            proxy_data = resp.json()
            # If the proxy itself failed (e.g. auth error), success might be False
            # But usually, if status_code reflects vendor, we just want the body.

            response_wrapper = proxy_data.get("data", {}).get("response", {})

            # Extract Headers
            final_headers = response_wrapper.get("headers", {})

            # Extract & Parse Body
            raw_body_str = response_wrapper.get("body", "{}")
            try:
                final_body = json.loads(raw_body_str)
            except (json.JSONDecodeError, TypeError):
                final_body = raw_body_str
    except Exception:
        # Fallback for fatal proxy errors (e.g. 502 Bad Gateway HTML)
        final_body = resp.text

    return VendorResponse(status_code=final_status, body=final_body, headers=final_headers)
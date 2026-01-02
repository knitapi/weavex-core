import os
import json
import requests
from abc import ABC, abstractmethod
from typing import Optional, Any

class APIProxy(ABC):
    @abstractmethod
    def make_passthrough_call(
            self,
            integration_id: str,
            method: str,
            path: str,
            params: Optional[dict] = None,
            body: Optional[dict] = None,
            headers: Optional[dict] = None
    ) -> requests.Response:
        pass

class KnitAPIProxy(APIProxy):
    """Implementation for Knit.com Passthrough API."""

    def __init__(self):
        self.api_key = os.environ.get("KNIT_API_KEY")
        self.base_url = "https://api.getknit.dev/v1.0/passthrough"

    def make_passthrough_call(
            self,
            integration_id: str,
            method: str,
            path: str,
            params: Optional[dict] = None,
            body: Optional[dict] = None,
            headers: Optional[dict] = None
    ) -> requests.Response:

        if not self.api_key:
            raise ValueError("Environment variable 'KNIT_API_KEY' is required for KnitAPIProxy.")

        req_headers = {
            "Authorization": f"Bearer {self.api_key}",
            "X-Knit-Integration-Id": integration_id,
            "Content-Type": "application/json"
        }
        if headers:
            req_headers.update(headers)

        payload = {
            "method": method.upper(),
            "path": path
        }

        if params:
            payload["params"] = params

        if body:
            payload["body"] = json.dumps(body) if not isinstance(body, str) else body

        return requests.post(self.base_url, headers=req_headers, json=payload)

def get_api_proxy() -> APIProxy:
    """Factory to get the configured APIProxy implementation."""
    backend = os.environ.get("API_BACKEND", "knit").lower()

    if backend == "knit":
        return KnitAPIProxy()
    else:
        raise ValueError(f"Unsupported API_BACKEND: {backend}")

def make_passthrough_call(
        integration_id: str,
        method: str,
        path: str,
        params: Optional[dict] = None,
        body: Optional[dict] = None,
        headers: Optional[dict] = None
) -> requests.Response:
    """Helper function to make a passthrough API call using the configured backend."""
    proxy = get_api_proxy()
    return proxy.make_passthrough_call(integration_id, method, path, params, body, headers)
# weavex_core/execute_api.py
#
# Inline skill for executing API steps.
#
# Usage:
#   from weavex_core.execute_api import execute_api
#
#   result = execute_api(
#       context        = context,
#       integration_id = integration_ids.get("bamboohr"),
#       method         = "GET",
#       path           = "/v1/employees/all?fields=id,firstName&status=Active",
#       headers        = {"Accept": "application/json"}
#   )
#
#   rows = result.body.get("employees") or []

import os
import time
import hmac
import hashlib
import base64
import json
import threading
from dataclasses import dataclass, field
from typing import Optional, Any

import httpx


# ── Response ──────────────────────────────────────────────────────────────────

@dataclass
class ApiResponse:
    status_code: int
    body:        Any               # dict if JSON, str if plain text
    headers:     dict[str, str]

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300


# ── Retry config ──────────────────────────────────────────────────────────────

@dataclass
class RetryConfig:
    # HTTP status codes that trigger a retry
    retry_on:        list[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])
    max_retries:     int       = 3
    backoff_seconds: float     = 2.0     # base backoff, doubles each retry
    respect_retry_after: bool  = True    # honour Retry-After header on 429


DEFAULT_RETRY = RetryConfig()


# ── Credential cache ──────────────────────────────────────────────────────────

@dataclass
class _CachedCredentials:
    credentials:  dict
    fetched_at:   float          # epoch seconds


class _CredentialCache:
    """
    In-memory cache of credentials keyed by integration_id.
    Lives for the lifetime of the Temporal worker process.
    Invalidated on auth errors or explicit eviction.
    """

    def __init__(self, ttl_seconds: int = 300):
        self._cache:  dict[str, _CachedCredentials] = {}
        self._lock:   threading.Lock                 = threading.Lock()
        self._ttl:    int                            = ttl_seconds

    def get(self, integration_id: str) -> Optional[dict]:
        with self._lock:
            entry = self._cache.get(integration_id)
            if not entry:
                return None
            if time.time() - entry.fetched_at > self._ttl:
                del self._cache[integration_id]
                return None
            return entry.credentials

    def set(self, integration_id: str, credentials: dict) -> None:
        with self._lock:
            self._cache[integration_id] = _CachedCredentials(
                credentials = credentials,
                fetched_at  = time.time()
            )

    def evict(self, integration_id: str) -> None:
        with self._lock:
            self._cache.pop(integration_id, None)


# singleton cache — shared across all execute_api calls in this process
_cache = _CredentialCache(ttl_seconds=300)


# ── Connect server client ─────────────────────────────────────────────────────

def _get_connect_server_url() -> str:
    url = os.environ.get("WEAVEX_CONNECT_SERVER_URL")
    if not url:
        raise RuntimeError(
            "WEAVEX_CONNECT_SERVER_URL env var not set"
        )
    return url.rstrip("/")


def _fetch_credentials(integration_id: str) -> dict:
    """
    Fetches credentials from the connect server vault.
    Error specs and rate limit config are bundled into the response
    and cached alongside credentials — no extra HTTP call needed.
    """
    url = f"{_get_connect_server_url()}/api/vault/{integration_id}"
    with httpx.Client(timeout=10) as client:
        response = client.get(url)
    if response.status_code == 404:
        raise RuntimeError(
            f"No credentials found for integration '{integration_id}' — "
            f"has the skill been connected?"
        )
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch credentials for '{integration_id}': "
            f"{response.status_code}"
        )
    data = response.json()
    # error specs bundled into vault response — cached with credentials
    # connect server reads from error_handling/data Firestore sub-collection
    credentials = data.get("credentials", data)
    credentials["__errorSpecs"]    = data.get("errorSpecs", [])
    credentials["__rateLimitSpec"] = data.get("rateLimitSpec", {})
    return credentials


def _update_credentials(integration_id: str, credentials: dict) -> None:
    """Updates credentials in the connect server vault (e.g. after token refresh)."""
    url = f"{_get_connect_server_url()}/api/vault/{integration_id}"
    with httpx.Client(timeout=10) as client:
        response = client.put(url, json=credentials)
    if response.status_code not in (200, 204):
        # non-fatal — log but don't fail the request
        print(
            f"WARN: failed to update credentials for '{integration_id}': "
            f"{response.status_code}"
        )



def _build_retry_config(error_specs: dict) -> RetryConfig:
    """
    Builds a RetryConfig from skill-level error specs.
    Falls back to DEFAULT_RETRY if no specs available.
    """
    specs        = error_specs.get("errorSpecs", [])
    rate_limit   = error_specs.get("rateLimitSpec", {})

    if not specs:
        return DEFAULT_RETRY

    retryable_codes = [s["statusCode"] for s in specs if s.get("retryable", False)]
    backoff_ms      = next(
        (s.get("backoffMs") for s in specs if s.get("statusCode") == 429 and s.get("backoffMs")),
        None
    )
    retry_after_header = rate_limit.get("retryAfterHeader")

    return RetryConfig(
        retry_on             = retryable_codes or [429, 500, 502, 503, 504],
        max_retries          = 3,
        backoff_seconds      = (backoff_ms / 1000.0) if backoff_ms else 2.0,
        respect_retry_after  = bool(retry_after_header)
    )

def _get_credentials(integration_id: str, force_refresh: bool = False) -> dict:
    """
    Returns credentials for an integration.
    Uses cache unless force_refresh=True.
    """
    if not force_refresh:
        cached = _cache.get(integration_id)
        if cached:
            return cached

    credentials = _fetch_credentials(integration_id)
    _cache.set(integration_id, credentials)
    return credentials


# ── Auth header builders ──────────────────────────────────────────────────────

def _build_auth_headers(credentials: dict) -> dict:
    auth_type = credentials.get("authType", "bearer").lower()

    if auth_type == "basic":
        username = credentials.get("username", "")
        password = credentials.get("password", "")
        encoded  = base64.b64encode(f"{username}:{password}".encode()).decode()
        return {"Authorization": f"Basic {encoded}"}

    elif auth_type in ("bearer", "oauth2"):
        token = credentials.get("accessToken") or credentials.get("token", "")
        return {"Authorization": f"Bearer {token}"}

    elif auth_type == "apikey":
        header_name = credentials.get("headerName", "X-API-Key")
        param_name  = credentials.get("paramName")
        # support both "apiKey" (AI providers) and "token" (SaaS)
        key         = credentials.get("apiKey") or credentials.get("token", "")
        if param_name:
            return {}   # param-based — handled in _build_auth_params
        return {header_name: key}

    elif auth_type == "custom":
        # validate_auth.py stores pre-built headers in credentials["headers"]
        return credentials.get("headers", {})

    return {}


def _build_auth_params(credentials: dict) -> dict:
    auth_type = credentials.get("authType", "bearer").lower()
    if auth_type == "apikey" and credentials.get("paramName"):
        key = credentials.get("apiKey") or credentials.get("token", "")
        return {credentials["paramName"]: key}
    return {}


# ── OAuth2 token refresh ──────────────────────────────────────────────────────

def _refresh_oauth_token(integration_id: str, credentials: dict) -> dict:
    """
    Refreshes an expired OAuth2 access token.
    Updates the vault and cache with new tokens.
    Returns updated credentials.
    """
    refresh_token = credentials.get("refreshToken")
    token_url     = credentials.get("tokenUrl")
    client_id     = credentials.get("clientId")
    client_secret = credentials.get("clientSecret")

    if not all([refresh_token, token_url, client_id, client_secret]):
        raise RuntimeError(
            f"Cannot refresh OAuth token for '{integration_id}' — "
            f"missing refreshToken, tokenUrl, clientId, or clientSecret"
        )

    print(f"[execute_api] refreshing OAuth token for {integration_id}")

    with httpx.Client(timeout=15) as client:
        response = client.post(
            token_url,
            data = {
                "grant_type":    "refresh_token",
                "refresh_token": refresh_token,
                "client_id":     client_id,
                "client_secret": client_secret
            }
        )

    if response.status_code != 200:
        raise RuntimeError(
            f"OAuth token refresh failed for '{integration_id}': "
            f"{response.status_code} — {response.text[:200]}"
        )

    tokens = response.json()

    # update credentials with new tokens
    updated = {
        **credentials,
        "accessToken":  tokens["access_token"],
        "refreshToken": tokens.get("refresh_token", refresh_token),  # some providers don't rotate
    }
    if "expires_in" in tokens:
        updated["expiresAt"] = int(time.time() * 1000) + tokens["expires_in"] * 1000

    # persist to vault and update cache
    _update_credentials(integration_id, updated)
    _cache.set(integration_id, updated)

    return updated


# ── URL builder ───────────────────────────────────────────────────────────────

def _build_url(credentials: dict, path: str) -> str:
    base_url = credentials.get("baseUrl", "").rstrip("/")
    if not base_url:
        raise RuntimeError(
            "Credentials missing baseUrl — check validate_auth.py stores it correctly"
        )
    # resolve {param} placeholders from credentials.params
    params = credentials.get("params", {})
    for key, value in params.items():
        path = path.replace(f"{{{key}}}", str(value))
    return base_url + "/" + path.lstrip("/")


# ── Core execution ────────────────────────────────────────────────────────────

def execute_api(
        context:        Any,
        integration_id: str,
        method:         str,
        path:           str,
        headers:        Optional[dict] = None,
        body:           Optional[Any]  = None,
        content_type:   str            = "application/json",
        timeout:        int            = 30,
        retry:          RetryConfig    = DEFAULT_RETRY
) -> ApiResponse:
    """
    Executes an API call for a connected integration.

    Args:
        context:        Temporal activity context — passed as-is, not used internally
        integration_id: Integration identifier from the connect flow
        method:         HTTP method (GET, POST, PUT, PATCH, DELETE)
        path:           Relative path including query string
                        Supports {param} placeholders resolved from credentials.params
        headers:        Additional headers (merged with auth headers)
        body:           Request body — dict (serialised to JSON) or string
        content_type:   Content-Type header (default: application/json)
        timeout:        Request timeout in seconds (default: 30)
        retry:          Retry configuration (default: RetryConfig())

    Returns:
        ApiResponse with status_code, body, headers

    Raises:
        RuntimeError: on unrecoverable errors (missing credentials, auth failure)
    """
    credentials    = _get_credentials(integration_id)
    error_specs    = credentials.get("__errorSpecs", [])
    rate_limit     = credentials.get("__rateLimitSpec", {})
    skill_specs    = {"errorSpecs": error_specs, "rateLimitSpec": rate_limit}
    retry          = _build_retry_config(skill_specs) if error_specs else retry

    attempt     = 0
    last_error  = None

    while attempt <= retry.max_retries:
        try:
            response = _execute_once(
                integration_id = integration_id,
                credentials    = credentials,
                method         = method,
                path           = path,
                extra_headers  = headers or {},
                body           = body,
                content_type   = content_type,
                timeout        = timeout
            )
        except httpx.TimeoutException:
            last_error = f"Request timed out after {timeout}s"
            attempt   += 1
            if attempt <= retry.max_retries:
                _backoff(attempt, retry.backoff_seconds)
            continue
        except httpx.RequestError as e:
            last_error = f"Network error: {e}"
            attempt   += 1
            if attempt <= retry.max_retries:
                _backoff(attempt, retry.backoff_seconds)
            continue

        # ── handle auth errors ────────────────────────────────────────────────

        if response.status_code == 401:
            if attempt == 0 and credentials.get("authType") == "oauth2":
                # try token refresh once
                print(f"[execute_api] 401 on {integration_id} — attempting token refresh")
                try:
                    credentials = _refresh_oauth_token(integration_id, credentials)
                    attempt    += 1
                    continue
                except RuntimeError as e:
                    raise RuntimeError(
                        f"Auth failed for '{integration_id}' and token refresh failed: {e}"
                    )
            elif attempt == 0:
                # non-OAuth 401 — evict cache and refetch credentials once
                print(f"[execute_api] 401 on {integration_id} — refetching credentials")
                _cache.evict(integration_id)
                try:
                    credentials = _get_credentials(integration_id, force_refresh=True)
                    attempt    += 1
                    continue
                except RuntimeError:
                    pass
            raise RuntimeError(
                f"Authentication failed for '{integration_id}' "
                f"(status 401) — check credentials in connect"
            )

        # ── handle retryable errors ───────────────────────────────────────────

        if response.status_code in retry.retry_on and attempt < retry.max_retries:
            wait = _get_wait_time(response, attempt, retry)
            print(
                f"[execute_api] {response.status_code} on attempt {attempt + 1} "
                f"for {integration_id} — retrying in {wait}s"
            )
            time.sleep(wait)
            attempt += 1
            continue

        # ── non-retryable error — use error spec meaning if available ─────────
        if not response.ok:
            specs = error_specs
            spec  = next((s for s in specs if s["statusCode"] == response.status_code), None)
            if spec and not spec.get("retryable", True):
                error_field = spec.get("errorField")
                if error_field and isinstance(response.body, dict):
                    # extract error message from response body using dot-notation path
                    msg = response.body
                    for key in error_field.split("."):
                        msg = msg.get(key, msg) if isinstance(msg, dict) else msg
                    print(f"[execute_api] non-retryable {response.status_code} for {integration_id}: {msg}")
                else:
                    print(f"[execute_api] non-retryable {response.status_code} for {integration_id}: {spec['meaning']}")

        # ── success or exhausted retries ──────────────────────────────────────
        return response

    raise RuntimeError(
        f"execute_api failed for '{integration_id}' after "
        f"{retry.max_retries} retries: {last_error}"
    )


def _execute_once(
        integration_id: str,
        credentials:    dict,
        method:         str,
        path:           str,
        extra_headers:  dict,
        body:           Any,
        content_type:   str,
        timeout:        int
) -> ApiResponse:
    """Single HTTP request — no retry logic."""
    url         = _build_url(credentials, path)
    auth_headers = _build_auth_headers(credentials)
    auth_params  = _build_auth_params(credentials)

    # merge headers — auth headers first, caller can override
    all_headers = {**auth_headers, **extra_headers}
    if body is not None:
        all_headers["Content-Type"] = content_type

    # build request body
    request_body = None
    if body is not None:
        if isinstance(body, dict) and content_type == "application/json":
            request_body = json.dumps(body)
        else:
            request_body = body

    with httpx.Client(timeout=timeout) as client:
        response = client.request(
            method  = method.upper(),
            url     = url,
            headers = all_headers,
            params  = auth_params or None,
            content = request_body
        )

    # parse body
    parsed_body = _parse_body(response)

    return ApiResponse(
        status_code = response.status_code,
        body        = parsed_body,
        headers     = dict(response.headers)
    )


def _parse_body(response: httpx.Response) -> Any:
    content_type = response.headers.get("content-type", "")
    if not response.content:
        return None
    if "application/json" in content_type:
        try:
            return response.json()
        except Exception:
            return response.text
    return response.text


def _get_wait_time(response: ApiResponse, attempt: int, retry: RetryConfig) -> float:
    if retry.respect_retry_after:
        retry_after = (response.headers.get("retry-after")
                       or response.headers.get("Retry-After"))
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass
    # exponential backoff: 2s, 4s, 8s...
    return retry.backoff_seconds * (2 ** attempt)


def _backoff(attempt: int, base: float) -> None:
    wait = base * (2 ** (attempt - 1))
    time.sleep(wait)
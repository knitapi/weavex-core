import json
import traceback


class WeavexError(Exception):
    """
    Structured error that propagates back to the Kotlin fix workflow.
    Serialises to JSON so Kotlin can parse it cleanly from the response body.
    Does NOT extend temporalio.ApplicationError — safe for non-Temporal runtimes.
    """

    FIXABLE_ERRORS = {
        "field_missing",
        "endpoint_wrong",
        "payload_rejected",
        "schema_mismatch",
        "field_type_mismatch",
        "connector_server_error"
    }
    RETRYABLE_ERRORS = {"rate_limited", "timeout", "connector_server_error"}
    NON_FIXABLE_ERRORS = {"auth_failure", "permission_denied"}

    def __init__(self, error_type: str, connector: str, step: str,
                 detail: dict = None, raw_error: str = None):
        self.error_type = error_type
        self.connector = connector
        self.step = step
        self.detail = detail or {}
        self.raw_error = raw_error
        self.fixable = error_type in self.FIXABLE_ERRORS
        self.retryable = error_type in self.RETRYABLE_ERRORS
        super().__init__(json.dumps(self.to_dict()))

    def to_dict(self) -> dict:
        return {
            "error_type": self.error_type,
            "connector": self.connector,
            "step": self.step,
            "detail": self.detail,
            "raw_error": self.raw_error,
            "fixable": self.fixable,
            "retryable": self.retryable,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_exception(cls, e: Exception, connector: str, step: str,
                       current_operation: str = None) -> "WeavexError":
        import requests

        op = current_operation or step

        if isinstance(e, WeavexError):
            return e

        if isinstance(e, requests.HTTPError):
            status = e.response.status_code
            try:
                api_body = e.response.json()
            except Exception:
                api_body = {"raw": e.response.text}
            return cls(
                error_type=cls._classify_http(status),
                connector=connector,
                step=op,
                detail={"http_status": status, "api_response": api_body},
                raw_error=str(e),
            )

        if isinstance(e, KeyError):
            return cls(
                error_type="field_missing",
                connector=connector,
                step=op,
                detail={"missing_field": str(e)},
                raw_error=str(e),
            )

        if isinstance(e, TypeError):
            return cls(
                error_type="field_type_mismatch",
                connector=connector,
                step=op,
                detail={"type_error": str(e)},
                raw_error=str(e),
            )

        if isinstance(e, TimeoutError):
            return cls(
                error_type="timeout",
                connector=connector,
                step=op,
                detail={},
                raw_error=str(e),
            )

        return cls(
            error_type="unknown",
            connector=connector,
            step=op,
            detail={"traceback": traceback.format_exc()},
            raw_error=str(e),
        )

    @staticmethod
    def _classify_http(status: int) -> str:
        return {
            401: "auth_failure",
            403: "permission_denied",
            404: "endpoint_wrong",
            429: "rate_limited",
            400: "payload_rejected",
            422: "schema_mismatch",
            500: "connector_server_error",
            502: "connector_server_error",
            503: "connector_server_error",
        }.get(status, "unknown_http_error")

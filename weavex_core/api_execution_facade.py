# weavex_core/api_execution_facade.py
#
# Unified API execution facade.
# Routes to skill executor (execute_api) or Knit passthrough (api)
# based on the integration_id prefix — no kb_origin needed by the coder.
#
# Skill integration IDs are prefixed with "wvx_sk_".
# Everything else routes to Knit passthrough.
#
# Usage:
#   from weavex_core.api_execution_facade import ApiExecutionFacade
#
#   result = ApiExecutionFacade.execute(
#       context        = context,
#       app_id         = "bamboohr",
#       integration_id = integration_id,   # prefix determines routing automatically
#       method         = "GET",
#       path           = "/v1/employees/all",
#   )
#   employees = result.body.get("employees", [])

from typing import Optional, Any

from .execute_api import execute_api, ApiResponse, RetryConfig, DEFAULT_RETRY
from .api import make_passthrough_call_normalised

SKILL_INTEGRATION_PREFIX = "wvx_sk_"


class ApiExecutionFacade:

    @staticmethod
    def _is_skill(integration_id: str) -> bool:
        return integration_id.startswith(SKILL_INTEGRATION_PREFIX)

    @staticmethod
    def execute(
            context:        Any,
            integration_id: str,
            method:         str,
            path:           str,
            body:           Optional[dict] = None,
            headers:        Optional[dict] = None,
            content_type:   str            = "application/json",
            app_base_url:   Optional[str]  = None,
            timeout:        int            = 30,
            retry:          RetryConfig    = DEFAULT_RETRY,
    ) -> ApiResponse:
        """
        Unified entry point for all API execution.
        Routes automatically based on integration_id prefix:
          - "wvx_sk_*" → skill executor (execute_api)
          - anything else → Knit passthrough (make_passthrough_call_normalised)

        Args:
            context:        Temporal activity context
            integration_id: Connected integration identifier — prefix determines routing
            method:         HTTP method (GET, POST, PUT, PATCH, DELETE)
            path:           Relative path including query string
            body:           Optional request body
            headers:        Additional headers
            content_type:   Content-Type (default: application/json)
            app_base_url:   Optional base URL override (Knit passthrough only)
            timeout:        Request timeout in seconds
            retry:          Retry configuration

        Returns:
            ApiResponse with status_code, body, headers
        """
        if ApiExecutionFacade._is_skill(integration_id):
            return ApiExecutionFacade._execute_skill(
                context        = context,
                integration_id = integration_id,
                method         = method,
                path           = path,
                body           = body,
                headers        = headers,
                content_type   = content_type,
                timeout        = timeout,
                retry          = retry,
            )
        else:
            return ApiExecutionFacade._execute_knit(
                context        = context,
                integration_id = integration_id,
                method         = method,
                path           = path,
                body           = body,
                headers        = headers,
                content_type   = content_type,
                app_base_url   = app_base_url,
            )

    @staticmethod
    def _execute_skill(
            context:        Any,
            integration_id: str,
            method:         str,
            path:           str,
            body:           Optional[dict],
            headers:        Optional[dict],
            content_type:   str,
            timeout:        int,
            retry:          RetryConfig,
    ) -> ApiResponse:
        return execute_api(
            context        = context,
            integration_id = integration_id,
            method         = method,
            path           = path,
            body           = body,
            headers        = headers,
            content_type   = content_type,
            timeout        = timeout,
            retry          = retry,
        )

    @staticmethod
    def _execute_knit(
            context:        Any,
            integration_id: str,
            method:         str,
            path:           str,
            body:           Optional[dict],
            headers:        Optional[dict],
            content_type:   str,
            app_base_url:   Optional[str],
    ) -> ApiResponse:
        return make_passthrough_call_normalised(
            context        = context,
            integration_id = integration_id,
            method         = method,
            path           = path,
            body           = body,
            content_type   = content_type,
            headers        = headers,
            app_base_url   = app_base_url,
        )
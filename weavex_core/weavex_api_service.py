import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)


class WeavexAPIService:
    def __init__(self, context: dict):
        self._base_url, self._headers = self.get_weavex_config(context)
        # Kept for log correlation across services (matches execution_id used
        # in the runner's own logs, e.g. "CALLING execute_workflow").
        self._execution_id = context.get("execution_id")

    def get_weavex_config(self, context: dict):
        """Resolves URL and Headers based on context"""
        api_key = context.get("knit_api_key")
        base_url = "https://weavex-cerebro-280006377455.europe-west1.run.app"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        return base_url, headers

    def _post(self, path: str, payload: dict) -> requests.Response:
        """
        Shared POST helper with explicit timeouts and before/after timing logs.
        timeout=(connect_timeout, read_timeout) — fails fast instead of hanging
        indefinitely if weavex-cerebro is slow or unreachable.
        """
        url = f"{self._base_url}{path}"
        t0 = time.time()
        print(f"[{self._execution_id}] SENDING POST {url} at {t0}", flush=True)
        try:
            resp = requests.post(url, headers=self._headers, json=payload, timeout=(10, 60))
        except requests.exceptions.Timeout:
            t1 = time.time()
            print(f"[{self._execution_id}] TIMEOUT calling {url} | elapsed={int((t1 - t0) * 1000)}ms", flush=True)
            raise
        except requests.exceptions.RequestException as e:
            t1 = time.time()
            print(f"[{self._execution_id}] ERROR calling {url} | elapsed={int((t1 - t0) * 1000)}ms | error={e}", flush=True)
            raise
        t1 = time.time()
        print(
            f"[{self._execution_id}] RECEIVED response from {url} | status={resp.status_code} | duration={int((t1 - t0) * 1000)}ms",
            flush=True,
        )
        return resp

    def init_checkpoint(
        self,
        project_id: str,
        execution_id: str,
        integration_ids: list,
        user_input: dict,
        context: dict,
    ) -> dict:
        payload = {
            "context": context,
            "projectId": project_id,
            "executionId": execution_id,
            "integrationIds": integration_ids,
            "userInput": user_input,
        }
        resp = self._post("/checkpoint.init", payload)
        resp.raise_for_status()
        return resp.json().get("data", {})

    def set_result_checkpoint(
        self,
        project_id: str,
        execution_id: str,
        step_id: str,
        checkpoint: dict,
        step_context: dict = None,
    ) -> None:
        payload = {
            "projectId": project_id,
            "stepId": step_id,
            "executionId": execution_id,
            "checkpoint": checkpoint,
        }
        if step_context is not None:
            payload["stepContext"] = step_context
        resp = self._post("/checkpoint.set", payload)
        resp.raise_for_status()

    def get_result_checkpoint(
        self, project_id: str, execution_id: str, step_id: str
    ) -> Optional[dict]:
        payload = {
            "projectId": project_id,
            "executionId": execution_id,
            "stepId": step_id,
        }
        resp = self._post("/checkpoint.get", payload)
        resp.raise_for_status()
        return resp.json().get("data")

    def clear_checkpoint(self, project_id: str, execution_id: str) -> None:
        payload = {
            "projectId": project_id,
            "executionId": execution_id,
        }
        resp = self._post("/checkpoint.clear", payload)
        resp.raise_for_status()

    # ── Deprecated ────────────────────────────────────────────────────────────

    # Deprecated: use set_result_checkpoint() instead.
    def set_checkpoint(
        self,
        project_id: str,
        execution_id: str,
        step_id: str,
        checkpoint: dict,
        step_context: dict = None,
    ) -> None:
        """Deprecated: use set_result_checkpoint() instead."""
        payload = {
            "projectId": project_id,
            "executionId": execution_id,
            "stepId": step_id,
            "checkpoint": checkpoint,
        }
        if step_context:
            payload["stepContext"] = step_context
        resp = self._post("/checkpoint.set", payload)
        resp.raise_for_status()

    # Deprecated: use get_result_checkpoint() instead.
    def get_checkpoint(
        self, project_id: str, execution_id: str, step_id: str
    ) -> Optional[dict]:
        """Deprecated: use get_result_checkpoint() instead."""
        return self.get_result_checkpoint(project_id, execution_id, step_id)

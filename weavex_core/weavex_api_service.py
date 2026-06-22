import requests
from typing import Optional


class WeavexAPIService:
    def __init__(self, context: dict):
        self._base_url, self._headers = self.get_weavex_config(context)

    def get_weavex_config(self, context: dict):
        """Resolves URL and Headers based on context"""
        api_key = context.get("knit_api_key")

        base_url = "https://weavex-cerebro-280006377455.europe-west1.run.app"

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        return base_url, headers

    def init_checkpoint(
        self,
        project_id: str,
        execution_id: str,
        integration_ids: list,
        user_input: dict,
        context: dict,
    ) -> dict:
        url = f"{self._base_url}/checkpoint.init"

        payload = {
            "context": context,
            "projectId": project_id,
            "executionId": execution_id,
            "integrationIds": integration_ids,
            "userInput": user_input,
        }

        resp = requests.post(url, headers=self._headers, json=payload)
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
        url = f"{self._base_url}/checkpoint.set"

        payload = {
            "projectId": project_id,
            "stepId": step_id,
            "executionId": execution_id,
            "checkpoint": checkpoint,
        }

        if step_context is not None:
            payload["stepContext"] = step_context

        resp = requests.post(url, headers=self._headers, json=payload)
        resp.raise_for_status()

    def get_result_checkpoint(
        self, project_id: str, execution_id: str, step_id: str
    ) -> Optional[dict]:
        url = f"{self._base_url}/checkpoint.get"

        payload = {
            "projectId": project_id,
            "executionId": execution_id,
            "stepId": step_id,
        }

        resp = requests.post(url, headers=self._headers, json=payload)
        resp.raise_for_status()

        return resp.json().get("data")

    def clear_checkpoint(self, project_id: str, execution_id: str) -> None:
        url = f"{self._base_url}/checkpoint.clear"

        payload = {
            "projectId": project_id,
            "executionId": execution_id,
        }

        resp = requests.post(url, headers=self._headers, json=payload)

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
        url = f"{self._base_url}/checkpoint.set"
        payload = {
            "projectId": project_id,
            "executionId": execution_id,
            "stepId": step_id,
            "checkpoint": checkpoint,
        }
        if step_context:
            payload["stepContext"] = step_context
        resp = requests.post(url, headers=self._headers, json=payload)
        resp.raise_for_status()

    # Deprecated: use get_result_checkpoint() instead.
    def get_checkpoint(
        self, project_id: str, execution_id: str, step_id: str
    ) -> Optional[dict]:
        """Deprecated: use get_result_checkpoint() instead."""
        return self.get_result_checkpoint(project_id, execution_id, step_id)

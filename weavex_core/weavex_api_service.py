from multiprocessing import context

import requests
from typing import Optional

class WeavexAPIService:
    def __init__(self, context: dict):
        self._context = context
        self._base_url, self._headers = self.get_weavex_config(context)

    def get_weavex_config(self, context: dict):
        """Resolves URL and Headers based on context"""
        api_key = context.get("knit_api_key")

        # base_url = "https://weavex-cerebro-280006377455.europe-west1.run.app"
        base_url = "https://afd2-2401-4900-8f5c-3f7d-99c-9d1c-72f6-6cb6.ngrok-free.app"

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        
        return base_url, headers

    def set_checkpoint(
        self,
        project_id: str,
        execution_id: str,
        step_id: str,
        checkpoint: dict,
        step_context: dict = None,
    ) -> None:
        url = f"{self._base_url}/checkpoint.set"
        payload = {
            "context": self._context,
            "projectId": project_id,
            "executionId": execution_id,
            "stepId": step_id,
            "checkpoint": checkpoint,
        }
        if step_context:
            payload["stepContext"] = step_context
        resp = requests.post(url, headers=self._headers, json=payload)
        resp.raise_for_status()

    def get_checkpoint(
        self, project_id: str, execution_id: str, step_id: str
    ) -> Optional[dict]:
        url = f"{self._base_url}/checkpoint.get"
        payload = {
            "context": self._context,
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
            "context": self._context,
            "projectId": project_id,
            "executionId": execution_id,
        }
        resp = requests.post(url, headers=self._headers, json=payload)
        resp.raise_for_status()

import time
import logging

logger = logging.getLogger(__name__)

class WeavexAPIService:
    def __init__(self, context: dict):
        self._base_url, self._headers = self.get_weavex_config(context)
        self._execution_id = context.get("execution_id")  # for log correlation

    def get_weavex_config(self, context: dict):
        api_key = context.get("knit_api_key")
        base_url = "https://weavex-cerebro-280006377455.europe-west1.run.app"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        return base_url, headers

    def init_checkpoint(self, project_id, execution_id, integration_ids, user_input, context):
        url = f"{self._base_url}/checkpoint.init"
        payload = {
            "context": context,
            "projectId": project_id,
            "executionId": execution_id,
            "integrationIds": integration_ids,
            "userInput": user_input,
        }
        t0 = time.time()
        print(f"[{self._execution_id}] SENDING POST {url} at {t0}", flush=True)
        resp = requests.post(url, headers=self._headers, json=payload, timeout=(10, 60))
        t1 = time.time()
        print(f"[{self._execution_id}] RECEIVED response from {url} | status={resp.status_code} | duration={int((t1-t0)*1000)}ms", flush=True)
        resp.raise_for_status()
        return resp.json().get("data", {})

    def get_result_checkpoint(self, project_id, execution_id, step_id):
        url = f"{self._base_url}/checkpoint.get"
        payload = {"projectId": project_id, "executionId": execution_id, "stepId": step_id}
        t0 = time.time()
        print(f"[{self._execution_id}] SENDING POST {url} at {t0}", flush=True)
        resp = requests.post(url, headers=self._headers, json=payload, timeout=(10, 60))
        t1 = time.time()
        print(f"[{self._execution_id}] RECEIVED response from {url} | status={resp.status_code} | duration={int((t1-t0)*1000)}ms", flush=True)
        resp.raise_for_status()
        return resp.json().get("data")

    # apply the same pattern to set_result_checkpoint and clear_checkpoint

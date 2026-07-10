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

    def _post(self, path: str, payload: dict, max_attempts: int = 3) -> requests.Response:
        """
        Shared POST helper with a shorter per-attempt timeout, multiple retries,
        and a small backoff between attempts.

        Root cause context: weavex-cerebro occasionally never logs receiving a
        request at all (confirmed via server-side instrumentation), while this
        client cleanly hits its read timeout. Every AppDBFactory call on the
        server side is confirmed non-blocking (uses .await(), not .get()), so
        this isn't application-level blocking — it's most consistent with
        transient network flakiness between Cloud Run services (e.g. a dropped
        packet or brief routing hiccup at the GFE layer). A retry with a fresh
        connection has reliably resolved it in practice.

        Using a shorter read timeout (20s) with more attempts (3) instead of a
        single long timeout (60s) with fewer attempts caps the worst case at
        roughly the same total wait, but resolves a transient stall in ~20s
        instead of ~60s whenever the very next attempt succeeds.
        """
        url = f"{self._base_url}{path}"

        last_exception: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            t0 = time.time()
            print(f"[{self._execution_id}] SENDING POST {url} at {t0} | attempt={attempt}/{max_attempts}", flush=True)
            try:
                resp = requests.post(url, headers=self._headers, json=payload, timeout=(5, 20))
                t1 = time.time()
                print(
                    f"[{self._execution_id}] RECEIVED response from {url} | status={resp.status_code} "
                    f"| duration={int((t1 - t0) * 1000)}ms | attempt={attempt}/{max_attempts}",
                    flush=True,
                )
                return resp

            except requests.exceptions.Timeout as e:
                last_exception = e
                t1 = time.time()
                will_retry = attempt < max_attempts
                print(
                    f"[{self._execution_id}] TIMEOUT calling {url} | elapsed={int((t1 - t0) * 1000)}ms "
                    f"| attempt={attempt}/{max_attempts} | will_retry={will_retry}",
                    flush=True,
                )
                if not will_retry:
                    raise
                time.sleep(0.5 * attempt)  # 0.5s, then 1s backoff before next attempt

            except requests.exceptions.RequestException as e:
                # Non-timeout errors (connection reset, DNS failure, etc.) are not retried
                # here — they usually indicate a real problem rather than transient flakiness.
                t1 = time.time()
                print(
                    f"[{self._execution_id}] ERROR calling {url} | elapsed={int((t1 - t0) * 1000)}ms | error={e}",
                    flush=True,
                )
                raise

        # Should be unreachable, but keeps type checkers happy and fails loudly if hit.
        if last_exception:
            raise last_exception
        raise RuntimeError(f"_post exhausted retries without a response or exception for {url}")

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

import json
from dataclasses import dataclass, asdict
from typing import Optional
from .weavex_api_service import WeavexAPIService


@dataclass
class StepCheckpoint:
    step_id: str  # snake_case step identifier e.g. "fetch_employees"
    status: str  # "success" | "failed"
    error: Optional[dict]  # WeavexError.to_dict() on failure

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "StepCheckpoint":
        return cls(**d)


class WorkflowCheckpointer:
    """
    Step-level checkpoint backed by the Weavex backend API.
    Enables resume-from-failure without re-executing successful steps.
    """

    def __init__(self, project_id: str, context: dict):
        self.project_id = project_id
        self.execution_id = context.get("execution_id")
        self._api = WeavexAPIService(context)

    def is_complete(self, step_id: str) -> bool:
        data = self._api.get_result_checkpoint(
            self.project_id, self.execution_id, step_id
        )
        if not data:
            return False
        return StepCheckpoint.from_dict(data).status == "success"

    def init(
        self,
        context: dict,
        integration_ids: list,
        user_input: dict,
    ) -> dict:
        return self._api.init_checkpoint(
            self.project_id, self.execution_id, integration_ids, user_input, context
        )

    def success(
        self,
        step_id: str,
        step_context: dict,
    ) -> None:
        cp = StepCheckpoint(step_id=step_id, status="success", error=None)

        self._api.set_result_checkpoint(
            self.project_id, self.execution_id, step_id, cp.to_dict(), step_context
        )

    def fail(
        self,
        step_id: str,
        error_json: str,
    ) -> None:
        try:
            error_dict = json.loads(error_json)
        except (json.JSONDecodeError, TypeError):
            error_dict = {"error_type": "unknown", "raw_error": error_json}
        cp = StepCheckpoint(step_id=step_id, status="failed", error=error_dict)

        self._api.set_result_checkpoint(
            self.project_id, self.execution_id, step_id, cp.to_dict(), None
        )

    def clear(self) -> None:
        """Call after full workflow success so the next fresh run starts clean."""
        self._api.clear_checkpoint(self.project_id, self.execution_id)

    # ── Deprecated ────────────────────────────────────────────────────────────

    # Deprecated: use success() instead.
    def mark_success(
        self,
        step_id: str,
        step_name: str,
        connector: str,
        operation_type: str,
        attempt: int = 0,
        step_context: dict = None,
    ) -> None:
        """Deprecated: use success() instead."""
        cp = StepCheckpoint(step_id=step_id, status="success", error=None)
        self._api.set_checkpoint(
            self.project_id, self.execution_id, step_id, cp.to_dict(), step_context
        )

    # Deprecated: use fail() instead.
    def mark_failed(
        self,
        step_id: str,
        step_name: str,
        connector: str,
        operation_type: str,
        error_json: str,
        attempt: int = 0,
    ) -> None:
        """Deprecated: use fail() instead."""
        try:
            error_dict = json.loads(error_json)
        except (json.JSONDecodeError, TypeError):
            error_dict = {"error_type": "unknown", "raw_error": error_json}
        cp = StepCheckpoint(step_id=step_id, status="failed", error=error_dict)
        self._api.set_checkpoint(
            self.project_id, self.execution_id, step_id, cp.to_dict()
        )

    # Deprecated: will be removed in a future version.
    def get_checkpoint(self, step_id: str) -> Optional[StepCheckpoint]:
        """Deprecated: will be removed in a future version."""
        data = self._api.get_result_checkpoint(
            self.project_id, self.execution_id, step_id
        )
        if not data:
            return None
        return StepCheckpoint.from_dict(data)

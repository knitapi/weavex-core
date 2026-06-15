import json
from dataclasses import dataclass, asdict
from typing import Optional
from .weavex_api_service import WeavexAPIService


@dataclass
class StepCheckpoint:
    step_id: str           # snake_case step identifier e.g. "fetch_employees"
    step_name: str         # friendly name e.g. "Fetch Employees from BambooHR"
    connector: str         # integration key e.g. "bamboohr"
    operation_type: str    # "read" | "write"
    status: str            # "success" | "failed"
    error: Optional[dict]  # WeavexError.to_dict() on failure
    attempt: int           # fix attempt number (0 = first run)

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

    def mark_success(
        self,
        step_id: str,
        step_name: str,
        connector: str,
        operation_type: str,
        attempt: int = 0,
        step_context: dict = None,
    ) -> None:
        cp = StepCheckpoint(
            step_id=step_id,
            step_name=step_name,
            connector=connector,
            operation_type=operation_type,
            status="success",
            error=None,
            attempt=attempt,
        )
        self._api.set_checkpoint(self.project_id, self.execution_id, step_id, cp.to_dict(), step_context)

    def mark_failed(
        self,
        step_id: str,
        step_name: str,
        connector: str,
        operation_type: str,
        error_json: str,
        attempt: int = 0,
    ) -> None:
        try:
            error_dict = json.loads(error_json)
        except (json.JSONDecodeError, TypeError):
            error_dict = {"error_type": "unknown", "raw_error": error_json}
        cp = StepCheckpoint(
            step_id=step_id,
            step_name=step_name,
            connector=connector,
            operation_type=operation_type,
            status="failed",
            error=error_dict,
            attempt=attempt,
        )
        self._api.set_checkpoint(self.project_id, self.execution_id, step_id, cp.to_dict())

    def get_checkpoint(self, step_id: str) -> Optional[StepCheckpoint]:
        data = self._api.get_checkpoint(self.project_id, self.execution_id, step_id)
        if not data:
            return None
        return StepCheckpoint.from_dict(data)

    def is_complete(self, step_id: str) -> bool:
        cp = self.get_checkpoint(step_id)
        return cp is not None and cp.status == "success"

    def clear(self) -> None:
        """Call after full workflow success so the next fresh run starts clean."""
        self._api.clear_checkpoint(self.project_id, self.execution_id)

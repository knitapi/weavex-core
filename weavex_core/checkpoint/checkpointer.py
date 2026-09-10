import json
import sys
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from ..dao import get_dao
from .events import get_event_publisher

# Bumped whenever the published event body changes shape. The subscriber reads
# it off both the body ("version") and the message attributes ("schemaVersion").
_EVENT_SCHEMA_VERSION = 1


def _compact_json(value: Any) -> str:
    """
    Compact JSON string, matching the Kotlin server's JsonElement.toString(),
    which is what previously produced these field values. Key order is preserved
    (not sorted) and non-ASCII is left literal, both to match kotlinx.
    """
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


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
    Step-level checkpoint for a workflow execution.
    Enables resume-from-failure without re-executing successful steps.

    No HTTP calls. Reads (`init`, `is_complete`) go straight to the app database
    through the DAO. Writes (`success`, `fail`, `clear`) are published as events
    to Pub/Sub, because the server-side work they trigger — mutating the project
    document and starting the fix workflow — does not belong in a client library.
    The weavex backend subscribes and performs it.

    Checkpoints are recorded for every project unconditionally — there is no
    TESTING-only gate.
    """

    def __init__(self, project_id: str, context: dict):
        self.project_id = project_id
        self.execution_id = context.get("execution_id")

        self._dao = None
        try:
            self._dao = get_dao()
        except Exception as e:
            self._log_error("__init__ (dao)", e)

        self._events = None
        try:
            self._events = get_event_publisher()
        except Exception as e:
            self._log_error("__init__ (event publisher)", e)

    def _log_error(self, where: str, exc: Exception) -> None:
        """
        Checkpointing must never fail an otherwise-healthy workflow step. Every
        public entry point catches everything and logs here instead of raising.
        """
        print(
            f"[weavex-core] checkpointer error in {where} | project={self.project_id} "
            f"execution={self.execution_id} | {type(exc).__name__}: {exc}",
            flush=True,
        )

    def is_complete(self, step_id: str) -> bool:
        """
        True only when a previous attempt recorded this step as successful.

        A step with no recorded entry is "pending", and any status other than
        "success" (e.g. "failed", or the "fixing" marker written by
        TestAndFixFlow) means not complete. Any error is logged and treated as
        "not complete" rather than raised.

        Reads `status` off the parsed object rather than building a StepCheckpoint:
        several Kotlin writers emit entries with no `error` key (BuildTestFlow's
        per-step failure marker, TestAndFixFlow.markCheckpointFixing), which the
        strict StepCheckpoint.from_dict cannot parse.
        """
        if not step_id:
            return False

        try:
            doc = self._dao.get_checkpoint(
                self.project_id, self.execution_id, fields=[step_id]
            )

            raw = doc.get(step_id)
            if not isinstance(raw, str):
                # Absent, or stored as a non-string. Kotlin's `doc[stepId] as? String`
                # yields null for both, which the route reports as "pending".
                return False

            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return False

            return isinstance(parsed, dict) and parsed.get("status") == "success"
        except Exception as e:
            self._log_error("is_complete", e)
            return False

    def init(
        self,
        context: dict,
        integration_ids: list,
        user_input: dict,
    ) -> dict:
        try:
            fields = {
                "context": _compact_json(context),
                "integrationIds": _compact_json(integration_ids),
                "userInput": _compact_json(user_input),
            }

            was_new, existing = self._dao.init_checkpoint(
                self.project_id, self.execution_id, fields
            )

            step_context_str = existing.get("step_context")
            step_context: Dict[str, Any] = {}
            if isinstance(step_context_str, str):
                try:
                    step_context = json.loads(step_context_str)
                except json.JSONDecodeError:
                    step_context = {}

            return {"init": was_new, "step_context": step_context}
        except Exception as e:
            self._log_error("init", e)
            return {}

    def _emit(self, event_type: str, body: Dict[str, Any]) -> None:
        """
        Wire contract with the weavex backend subscriber.

        `checkpoint` and `stepContext` are nested JSON objects, NOT strings: the
        subscriber deserialises this body straight into the existing Kotlin
        SetCheckpointRequest / ClearCheckpointRequest DTOs and lets kotlinx's
        JsonElement.toString() produce the stored value, exactly as
        POST /checkpoint.set does today. Pre-stringifying here would make the
        persisted bytes depend on Python's json.dumps matching kotlinx.

        Casing is mixed on purpose and mirrors the HTTP payload it replaces:
        camelCase envelope (the Kotlin DTO's field names), snake_case inside
        `checkpoint` (StepCheckpoint.to_dict(), which is what is_complete parses
        back out of Firestore). Do not "fix" it.

        Never raises. Publishing is fire-and-forget; a broker problem must not
        fail an otherwise-healthy workflow step.
        """
        try:
            event_id = str(uuid.uuid4())
            payload = {
                "eventId": event_id,
                "eventType": event_type,
                "version": _EVENT_SCHEMA_VERSION,
                "publishedAt": datetime.now(timezone.utc).isoformat(),
                "source": "weavex-core",
                "projectId": self.project_id,
                "executionId": self.execution_id,
            }
            payload.update(body)

            # Attributes are readable in a console pull without decoding the body
            # and usable in subscription filters. Strings only.
            attributes = {
                "eventType": event_type,
                "schemaVersion": str(_EVENT_SCHEMA_VERSION),
                "eventId": event_id,
                "projectId": self.project_id,
                "executionId": self.execution_id or "",
            }
            if body.get("stepId"):
                attributes["stepId"] = body["stepId"]

            # Same composite id as the Firestore checkpoint document, so the two
            # views line up when debugging. Gives per-execution FIFO delivery.
            self._events.publish(
                payload,
                ordering_key=f"{self.project_id}:{self.execution_id}",
                attributes=attributes,
            )
        except Exception as e:
            self._log_error(f"_emit ({event_type})", e)

    def success(
        self,
        step_id: str,
        step_context: dict,
    ) -> None:
        try:
            cp = StepCheckpoint(step_id=step_id, status="success", error=None)

            self._emit(
                "checkpoint.set",
                {
                    "stepId": step_id,
                    "checkpoint": cp.to_dict(),
                    "stepContext": step_context,
                },
            )
        except Exception as e:
            self._log_error("success", e)

    def fail(
        self,
        step_id: str,
        error_json: str,
    ) -> None:
        """
        Called from inside `except` blocks, both in the runner's activities and in
        every generated step. Nothing here may raise: an exception thrown here
        would replace the real step failure the caller is about to re-raise, and
        the fix agent would be handed the wrong error.
        """
        try:
            try:
                error_dict = json.loads(error_json)
            except (json.JSONDecodeError, TypeError):
                error_dict = {"error_type": "unknown", "raw_error": error_json}
            cp = StepCheckpoint(step_id=step_id, status="failed", error=error_dict)

            # stepContext omitted entirely on failure, matching the previous HTTP
            # payload and the Kotlin DTO's `stepContext: JsonElement? = null`.
            self._emit(
                "checkpoint.set", {"stepId": step_id, "checkpoint": cp.to_dict()}
            )
        except Exception as e:
            self._log_error("fail", e)

    def clear(self) -> None:
        """Call after full workflow success so the next fresh run starts clean."""
        try:
            self._emit("checkpoint.clear", {})
        except Exception as e:
            self._log_error("clear", e)

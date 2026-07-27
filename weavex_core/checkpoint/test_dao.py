"""
Offline contract test for the DAO-backed WorkflowCheckpointer methods.

Needs no GCP credentials and makes no network calls: it substitutes a
FakeDao for the real Firestore implementation and asserts the orchestration
logic in checkpoint.py (TESTING gate, JSON stringification, step_context
parsing, response shape, and every is_complete branch).

Run:  python -m weavex_core.checkpoint.test_dao
"""

import json
from typing import Any, Dict, Iterable, Optional, Tuple

from weavex_core.checkpoint import EventPublisher, WorkflowCheckpointer
from weavex_core.checkpoint import checkpointer as checkpoint_module
from weavex_core.dao import WeavexDao
from weavex_core.errors import ProjectNotFoundError


class FakeDao(WeavexDao):
    """Canned WeavexDao that records what the checkpointer asked it to do."""

    def __init__(
        self,
        status: Optional[str],
        was_new: bool = True,
        existing: Dict[str, Any] = None,
        checkpoint_doc: Dict[str, Any] = None,
    ):
        self.status = status
        self._was_new = was_new
        self._existing = existing if existing is not None else {}
        self._checkpoint_doc = checkpoint_doc if checkpoint_doc is not None else {}

        # Recorded for assertions
        self.init_calls = 0
        self.get_checkpoint_calls = 0
        self.last_fields: Optional[Dict[str, str]] = None
        self.last_get_fields: Optional[Iterable[str]] = None
        self.last_project_id: Optional[str] = None
        self.last_execution_id: Optional[str] = None
        self.last_org_id: Optional[str] = None

    def get_project_status(self, project_id: str, org_id: Optional[str] = None) -> Optional[str]:
        self.last_org_id = org_id
        return self.status

    def get_checkpoint(
        self, project_id: str, execution_id: str, fields: Optional[Iterable[str]] = None
    ) -> Dict[str, Any]:
        self.get_checkpoint_calls += 1
        self.last_get_fields = fields
        if not fields:
            return dict(self._checkpoint_doc)
        return {k: v for k, v in self._checkpoint_doc.items() if k in fields}

    def init_checkpoint(
        self, project_id: str, execution_id: str, fields: Dict[str, str]
    ) -> Tuple[bool, Dict[str, Any]]:
        self.init_calls += 1
        self.last_project_id = project_id
        self.last_execution_id = execution_id
        self.last_fields = fields
        return self._was_new, self._existing


class _NullEventPublisher(EventPublisher):
    """
    Stands in for the Pub/Sub publisher the checkpointer builds in __init__, so
    these DAO checks stay credential-free and offline. The publish path itself is
    covered by test_events.py.
    """

    def publish(self, payload, ordering_key="", attributes=None):
        pass

    def flush(self, timeout: float = 5.0) -> bool:
        return True


def _make_checkpointer(dao: FakeDao) -> WorkflowCheckpointer:
    """Builds a checkpointer wired to `dao` instead of the real Firestore DAO."""
    original_dao = checkpoint_module.get_dao
    original_pub = checkpoint_module.get_event_publisher
    checkpoint_module.get_dao = lambda: dao
    checkpoint_module.get_event_publisher = _NullEventPublisher
    try:
        return WorkflowCheckpointer(
            "proj_test",
            {
                "execution_id": "exec_test",
                "org_id": "org_test",
                "knit_api_key": "fake-key",
            },
        )
    finally:
        checkpoint_module.get_dao = original_dao
        checkpoint_module.get_event_publisher = original_pub


def _assert(condition, message="assertion failed"):
    """Lets the single-expression checks below use a lambda."""
    assert condition, message


CONTEXT = {"execution_id": "exec_test", "org_id": "org_test", "knit_api_key": "fake-key"}
INTEGRATION_IDS = {"hris": "int_123"}
USER_INPUT = {"limit": 50, "name": "café"}


def run_test():
    print("--- WorkflowCheckpointer.init contract test (offline) ---")
    failures = []

    def check(label, fn):
        try:
            fn()
            print(f"    PASS: {label}")
        except AssertionError as e:
            print(f"    FAIL: {label} -> {e}")
            failures.append(label)
        except Exception as e:
            print(f"    ERROR: {label} -> {type(e).__name__}: {e}")
            failures.append(label)

    # 1. Fresh init on a TESTING project
    print("\n[1] TESTING + new document")

    def case_1():
        dao = FakeDao("TESTING", was_new=True, existing={})
        cp = _make_checkpointer(dao)
        result = cp.init(CONTEXT, INTEGRATION_IDS, USER_INPUT)
        assert result == {"init": True, "step_context": {}}, result
        assert dao.last_project_id == "proj_test", dao.last_project_id
        assert dao.last_execution_id == "exec_test", dao.last_execution_id
        assert dao.last_org_id == "org_test", dao.last_org_id

    check("fresh init returns init=True and empty step_context", case_1)

    # 2. Resume: existing step_context is parsed back out
    print("\n[2] TESTING + existing document with step_context")

    def case_2():
        dao = FakeDao(
            "TESTING",
            was_new=False,
            existing={"step_context": '{"fetch_employees":{"processed_count":3}}'},
        )
        cp = _make_checkpointer(dao)
        result = cp.init(CONTEXT, INTEGRATION_IDS, USER_INPUT)
        assert result == {
            "init": False,
            "step_context": {"fetch_employees": {"processed_count": 3}},
        }, result

    check("resume returns init=False and parsed step_context", case_2)

    # 3. Non-TESTING project short-circuits without touching the checkpoint doc
    print("\n[3] Project not in TESTING")

    def case_3():
        dao = FakeDao("LIVE")
        cp = _make_checkpointer(dao)
        result = cp.init(CONTEXT, INTEGRATION_IDS, USER_INPUT)
        assert result == {}, result
        assert dao.init_calls == 0, f"init_checkpoint called {dao.init_calls} times"

    check("non-TESTING returns {} and never writes", case_3)

    # 4. Missing project raises
    print("\n[4] Project not found")

    def case_4():
        dao = FakeDao(None)
        cp = _make_checkpointer(dao)
        try:
            cp.init(CONTEXT, INTEGRATION_IDS, USER_INPUT)
        except ProjectNotFoundError:
            return
        raise AssertionError("expected ProjectNotFoundError")

    check("missing project raises ProjectNotFoundError", case_4)

    # 5. Corrupt step_context degrades to {} rather than raising
    print("\n[5] Malformed step_context")

    def case_5():
        dao = FakeDao("TESTING", was_new=False, existing={"step_context": "not-json"})
        cp = _make_checkpointer(dao)
        result = cp.init(CONTEXT, INTEGRATION_IDS, USER_INPUT)
        assert result == {"init": False, "step_context": {}}, result

    check("malformed step_context yields {} without raising", case_5)

    # 6. Stored fields are JSON strings that round-trip
    print("\n[6] Field stringification")

    def case_6():
        dao = FakeDao("TESTING", was_new=True, existing={})
        cp = _make_checkpointer(dao)
        cp.init(CONTEXT, INTEGRATION_IDS, USER_INPUT)

        fields = dao.last_fields
        assert set(fields.keys()) == {"context", "integrationIds", "userInput"}, fields.keys()
        for key, value in fields.items():
            assert isinstance(value, str), f"{key} is {type(value).__name__}, expected str"

        assert json.loads(fields["context"]) == CONTEXT
        assert json.loads(fields["integrationIds"]) == INTEGRATION_IDS
        assert json.loads(fields["userInput"]) == USER_INPUT

        # Compact separators, matching kotlinx JsonElement.toString()
        assert ", " not in fields["userInput"], fields["userInput"]
        # Non-ASCII left literal, also matching kotlinx
        assert "café" in fields["userInput"], fields["userInput"]

    check("fields are compact JSON strings that round-trip", case_6)

    # ------------------------------------------------------------------
    # is_complete
    # ------------------------------------------------------------------
    print("\n--- WorkflowCheckpointer.is_complete ---")

    SUCCESS = '{"step_id":"s","status":"success","error":null}'
    FAILED = '{"step_id":"s","status":"failed","error":{"error_type":"timeout"}}'

    def is_complete_case(status, doc, step_id="s"):
        """Returns (result_or_exception, dao) for one is_complete invocation."""
        dao = FakeDao(status, checkpoint_doc=doc)
        cp = _make_checkpointer(dao)
        return cp.is_complete(step_id), dao

    print("\n[7] Recorded success")
    check(
        "stored success returns True",
        lambda: _assert(is_complete_case("TESTING", {"s": SUCCESS})[0] is True),
    )

    print("\n[8] Recorded failure")
    check(
        "stored failure returns False",
        lambda: _assert(is_complete_case("TESTING", {"s": FAILED})[0] is False),
    )

    print("\n[9] Step absent from an existing document (pending)")
    check(
        "pending step returns False",
        lambda: _assert(is_complete_case("TESTING", {"other": SUCCESS})[0] is False),
    )

    print("\n[10] Checkpoint document missing entirely")
    check(
        "missing document returns False",
        lambda: _assert(is_complete_case("TESTING", {})[0] is False),
    )

    print("\n[11] Non-TESTING project with a success entry present")

    def case_11():
        result, dao = is_complete_case("LIVE", {"s": SUCCESS})
        assert result is False, result
        assert dao.get_checkpoint_calls == 0, (
            f"TESTING gate did not short-circuit: {dao.get_checkpoint_calls} read(s)"
        )

    check("non-TESTING returns False without reading the checkpoint", case_11)

    print("\n[12] Project not found")

    def case_12():
        dao = FakeDao(None, checkpoint_doc={"s": SUCCESS})
        cp = _make_checkpointer(dao)
        try:
            cp.is_complete("s")
        except ProjectNotFoundError:
            assert dao.get_checkpoint_calls == 0, "read the checkpoint before raising"
            return
        raise AssertionError("expected ProjectNotFoundError")

    check("missing project raises before reading the checkpoint", case_12)

    print("\n[13] Stored value is not valid JSON")
    check(
        "malformed stored JSON returns False without raising",
        lambda: _assert(is_complete_case("TESTING", {"s": "not-json"})[0] is False),
    )

    print("\n[14] Stored value is a map rather than a JSON string")
    check(
        "non-string entry returns False (mirrors Kotlin `as? String`)",
        lambda: _assert(is_complete_case("TESTING", {"s": {"status": "success"}})[0] is False),
    )

    print("\n[15] TestAndFixFlow 'fixing' marker")
    check(
        "status=fixing returns False",
        lambda: _assert(
            is_complete_case("TESTING", {"s": '{"step_id":"s","status":"fixing"}'})[0] is False
        ),
    )

    print("\n[16] Legacy entry carrying extra keys")
    check(
        "extra keys do not break parsing (regression: StepCheckpoint.from_dict)",
        lambda: _assert(
            is_complete_case(
                "TESTING",
                {
                    "s": '{"step_id":"s","status":"success","error":null,'
                         '"step_name":"x","connector":"y","operation_type":"z","attempt":1}'
                },
            )[0]
            is True
        ),
    )

    print("\n[17] Entry with no 'error' key (BuildTestFlow / markCheckpointFixing shape)")
    check(
        "missing error key does not raise TypeError",
        lambda: _assert(
            is_complete_case("TESTING", {"s": '{"step_id":"s","status":"failed"}'})[0] is False
        ),
    )

    print("\n[18] Read is projected to the single step field")

    def case_18():
        _, dao = is_complete_case("TESTING", {"s": SUCCESS})
        assert dao.last_get_fields == ["s"], (
            f"expected a projection of ['s'], got {dao.last_get_fields!r} — "
            "a full-document read re-downloads the accumulating step_context"
        )

    check("get_checkpoint is called with fields=[step_id]", case_18)

    print("\n[19] Empty step id")

    def case_19():
        result, dao = is_complete_case("TESTING", {"s": SUCCESS}, step_id="")
        assert result is False, result
        assert dao.get_checkpoint_calls == 0, "empty step_id should short-circuit"

    check("empty step_id returns False without a read", case_19)

    print("\n[20] Step id containing a dot")
    check(
        "dotted step id is treated as a literal key",
        lambda: _assert(
            is_complete_case(
                "TESTING", {"weird.step": '{"status":"success"}'}, step_id="weird.step"
            )[0]
            is True
        ),
    )

    print("\n--- Summary ---")
    if failures:
        print(f"{len(failures)} check(s) failed: {failures}")
    else:
        print("All checks passed.")


if __name__ == "__main__":
    run_test()

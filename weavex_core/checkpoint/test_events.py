"""
Offline contract test for the Pub/Sub-backed WorkflowCheckpointer writes.

Needs no GCP credentials and makes no network calls. Two halves:

  * WorkflowCheckpointer with a FakeDao and a FakeEventPublisher — asserts the
    TESTING gate, event envelope, message schema, ordering key and the
    never-raise guarantees of success / fail / clear.
  * PubSubEventPublisher with an injected FakeClient — asserts topic resolution,
    encoding, and the ordering-key pause/resume handling.

Run:  python -m weavex_core.checkpoint.test_events
"""

import json
import os
import threading
from typing import Optional

from weavex_core.checkpoint import (
    EventPublisher,
    PubSubEventPublisher,
    WorkflowCheckpointer,
)
from weavex_core.checkpoint import checkpointer as checkpoint_module
from weavex_core.checkpoint.test_dao import FakeDao
from weavex_core.errors import ProjectNotFoundError


class FakeEventPublisher(EventPublisher):
    """Records what checkpoint.py asked to publish. Optionally explodes."""

    def __init__(self, raises: Optional[Exception] = None):
        self.messages = []  # list[(payload, ordering_key, attributes)]
        self._raises = raises

    def publish(self, payload, ordering_key="", attributes=None):
        if self._raises:
            raise self._raises
        self.messages.append((payload, ordering_key, dict(attributes or {})))

    def flush(self, timeout: float = 5.0) -> bool:
        return True


class FakeFuture:
    """Minimal stand-in for a Pub/Sub publish future."""

    def __init__(self, exception: Optional[Exception] = None):
        self._exception = exception
        self._callbacks = []

    def add_done_callback(self, fn):
        # Real futures fire immediately when already resolved; so do we.
        self._callbacks.append(fn)
        fn(self)

    def result(self, timeout: Optional[float] = None):
        if self._exception:
            raise self._exception
        return "message-id"


class FakeClient:
    """Stand-in for pubsub_v1.PublisherClient via the `client=` injection point."""

    def __init__(self, publish_exception=None, future_exception=None):
        self.published = []  # list[(topic, data, ordering_key, attrs)]
        self.resumed = []  # list[(topic, ordering_key)]
        self._publish_exception = publish_exception
        self._future_exception = future_exception
        self.resumed_event = threading.Event()

    def topic_path(self, project, topic):
        return f"projects/{project}/topics/{topic}"

    def publish(self, topic, data, ordering_key="", **attrs):
        if self._publish_exception:
            raise self._publish_exception
        self.published.append((topic, data, ordering_key, attrs))
        return FakeFuture(self._future_exception)

    def resume_publish(self, topic, ordering_key):
        self.resumed.append((topic, ordering_key))
        self.resumed_event.set()


def _make_checkpointer(dao: FakeDao, publisher: EventPublisher) -> WorkflowCheckpointer:
    """Builds a checkpointer wired to the fakes instead of Firestore / Pub/Sub."""
    original_dao = checkpoint_module.get_dao
    original_pub = checkpoint_module.get_event_publisher
    checkpoint_module.get_dao = lambda: dao
    checkpoint_module.get_event_publisher = lambda: publisher
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


def _make_publisher(client: FakeClient, **kwargs) -> PubSubEventPublisher:
    return PubSubEventPublisher(project="proj-gcp", client=client, **kwargs)


def _assert(condition, message="assertion failed"):
    """Lets the single-expression checks below use a lambda."""
    assert condition, message


STEP_CONTEXT = {"fetch_employees": {"processed_count": 42, "name": "café"}}
ERROR_JSON = '{"error_type":"timeout","message":"upstream took too long"}'


def run_test():
    print("--- WorkflowCheckpointer event contract test (offline) ---")
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

    # ------------------------------------------------------------------
    # success
    # ------------------------------------------------------------------
    print("\n[1] success on a TESTING project")

    def case_1():
        pub = FakeEventPublisher()
        cp = _make_checkpointer(FakeDao("TESTING"), pub)
        cp.success("fetch_employees", STEP_CONTEXT)

        assert len(pub.messages) == 1, f"{len(pub.messages)} message(s)"
        payload, _, attrs = pub.messages[0]
        assert payload["eventType"] == "checkpoint.set", payload["eventType"]
        assert attrs["eventType"] == "checkpoint.set", attrs["eventType"]
        assert payload["projectId"] == "proj_test", payload["projectId"]
        assert payload["executionId"] == "exec_test", payload["executionId"]
        assert payload["stepId"] == "fetch_employees", payload["stepId"]
        assert payload["source"] == "weavex-core", payload["source"]
        assert payload["checkpoint"] == {
            "step_id": "fetch_employees",
            "status": "success",
            "error": None,
        }, payload["checkpoint"]
        assert payload["stepContext"] == STEP_CONTEXT, payload["stepContext"]

    check("success emits one checkpoint.set with checkpoint + stepContext", case_1)

    print("\n[2] success on a non-TESTING project")

    def case_2():
        pub = FakeEventPublisher()
        cp = _make_checkpointer(FakeDao("LIVE"), pub)
        cp.success("fetch_employees", STEP_CONTEXT)
        assert pub.messages == [], pub.messages

    check("non-TESTING publishes nothing", case_2)

    print("\n[3] success on a missing project")

    def case_3():
        pub = FakeEventPublisher()
        cp = _make_checkpointer(FakeDao(None), pub)
        try:
            cp.success("fetch_employees", STEP_CONTEXT)
        except ProjectNotFoundError:
            assert pub.messages == [], pub.messages
            return
        raise AssertionError("expected ProjectNotFoundError")

    check("missing project raises and publishes nothing (parity: set 404s)", case_3)

    # ------------------------------------------------------------------
    # fail
    # ------------------------------------------------------------------
    print("\n[4] fail with a valid error_json")

    def case_4():
        pub = FakeEventPublisher()
        cp = _make_checkpointer(FakeDao("TESTING"), pub)
        cp.fail("fetch_employees", ERROR_JSON)

        assert len(pub.messages) == 1, f"{len(pub.messages)} message(s)"
        payload, _, _ = pub.messages[0]
        assert payload["eventType"] == "checkpoint.set", payload["eventType"]
        assert payload["checkpoint"]["status"] == "failed", payload["checkpoint"]
        error = payload["checkpoint"]["error"]
        assert isinstance(error, dict), f"error is {type(error).__name__}, expected dict"
        assert error["error_type"] == "timeout", error
        assert "stepContext" not in payload, (
            "stepContext must be omitted entirely on failure, matching the "
            "Kotlin DTO's nullable field"
        )

    check("fail emits status=failed, error as a dict, no stepContext key", case_4)

    print("\n[5] fail with a malformed error_json")

    def case_5():
        pub = FakeEventPublisher()
        cp = _make_checkpointer(FakeDao("TESTING"), pub)
        cp.fail("fetch_employees", "not-json")
        payload, _, _ = pub.messages[0]
        assert payload["checkpoint"]["error"] == {
            "error_type": "unknown",
            "raw_error": "not-json",
        }, payload["checkpoint"]["error"]

    check("malformed error_json degrades to an unknown-error dict", case_5)

    print("\n[6] fail on a missing project")

    def case_6():
        pub = FakeEventPublisher()
        cp = _make_checkpointer(FakeDao(None), pub)
        cp.fail("fetch_employees", ERROR_JSON)  # must not raise
        assert pub.messages == [], pub.messages

    check("missing project is swallowed, publishes nothing", case_6)

    print("\n[7] fail when the publisher itself explodes")

    def case_7():
        pub = FakeEventPublisher(raises=RuntimeError("broker on fire"))
        cp = _make_checkpointer(FakeDao("TESTING"), pub)
        cp.fail("fetch_employees", ERROR_JSON)  # must not raise

    check(
        "publisher failure does not escape fail() (would mask the real step error)",
        case_7,
    )

    # ------------------------------------------------------------------
    # clear
    # ------------------------------------------------------------------
    print("\n[8] clear on a TESTING project")

    def case_8():
        pub = FakeEventPublisher()
        cp = _make_checkpointer(FakeDao("TESTING"), pub)
        cp.clear()

        assert len(pub.messages) == 1, f"{len(pub.messages)} message(s)"
        payload, _, attrs = pub.messages[0]
        assert payload["eventType"] == "checkpoint.clear", payload["eventType"]
        assert "stepId" not in payload, payload
        assert "stepId" not in attrs, attrs
        assert "checkpoint" not in payload, payload

    check("clear emits an envelope-only checkpoint.clear with no stepId", case_8)

    print("\n[9] clear on a missing project")

    def case_9():
        pub = FakeEventPublisher()
        cp = _make_checkpointer(FakeDao(None), pub)
        cp.clear()  # must not raise: /checkpoint.clear returns 200 for this
        assert pub.messages == [], pub.messages

    check("missing project returns silently (parity: clear 200s)", case_9)

    print("\n[10] clear on a non-TESTING project")

    def case_10():
        pub = FakeEventPublisher()
        cp = _make_checkpointer(FakeDao("LIVE"), pub)
        cp.clear()
        assert pub.messages == [], pub.messages

    check("non-TESTING clear publishes nothing", case_10)

    # ------------------------------------------------------------------
    # envelope invariants
    # ------------------------------------------------------------------
    print("\n[11] Ordering key")

    def case_11():
        pub = FakeEventPublisher()
        cp = _make_checkpointer(FakeDao("TESTING"), pub)
        cp.success("s", {})
        cp.fail("s", ERROR_JSON)
        cp.clear()
        keys = {key for _, key, _ in pub.messages}
        assert keys == {"proj_test:exec_test"}, keys

    check("all three events share the {project}:{execution} ordering key", case_11)

    print("\n[12] Attribute types and envelope/attribute agreement")

    def case_12():
        pub = FakeEventPublisher()
        cp = _make_checkpointer(FakeDao("TESTING"), pub)
        cp.success("fetch_employees", STEP_CONTEXT)
        payload, _, attrs = pub.messages[0]

        for key, value in attrs.items():
            assert isinstance(value, str), f"{key} is {type(value).__name__}, expected str"
        assert attrs["eventId"] == payload["eventId"], (attrs["eventId"], payload["eventId"])
        assert payload["version"] == 1, payload["version"]
        assert attrs["schemaVersion"] == "1", attrs["schemaVersion"]
        assert attrs["stepId"] == "fetch_employees", attrs["stepId"]

    check("attributes are all str and agree with the envelope", case_12)

    print("\n[13] eventId uniqueness")

    def case_13():
        pub = FakeEventPublisher()
        cp = _make_checkpointer(FakeDao("TESTING"), pub)
        cp.success("a", {})
        cp.success("b", {})
        first, second = pub.messages[0][0]["eventId"], pub.messages[1][0]["eventId"]
        assert first != second, f"eventId repeated: {first}"

    check("each event gets a fresh eventId", case_13)

    print("\n[14] Payload is JSON-serialisable")

    def case_14():
        pub = FakeEventPublisher()
        cp = _make_checkpointer(FakeDao("TESTING"), pub)
        cp.success("fetch_employees", STEP_CONTEXT)
        payload, _, _ = pub.messages[0]
        assert json.loads(json.dumps(payload)) == payload

    check("payload round-trips through json.dumps/loads unchanged", case_14)

    # ------------------------------------------------------------------
    # PubSubEventPublisher
    # ------------------------------------------------------------------
    print("\n--- PubSubEventPublisher ---")

    print("\n[15] Topic resolution")

    def case_15():
        original = os.environ.pop("WEAVEX_SERVICE_REGION", None)
        try:
            eu = _make_publisher(FakeClient())
            assert eu.topic_path == "projects/proj-gcp/topics/weavex-checkpoints-eu", (
                eu.topic_path
            )

            os.environ["WEAVEX_SERVICE_REGION"] = "us"
            us = _make_publisher(FakeClient())
            assert us.topic_path == "projects/proj-gcp/topics/weavex-checkpoints", (
                us.topic_path
            )
        finally:
            os.environ.pop("WEAVEX_SERVICE_REGION", None)
            if original is not None:
                os.environ["WEAVEX_SERVICE_REGION"] = original

    check("the -eu suffix is applied only outside a non-eu region", case_15)

    print("\n[16] Message encoding")

    def case_16():
        client = FakeClient()
        pub = _make_publisher(client)
        payload = {"a": 1, "b": {"name": "café"}}
        pub.publish(payload)

        _, data, _, _ = client.published[0]
        assert isinstance(data, bytes), type(data).__name__
        assert json.loads(data.decode("utf-8")) == payload
        # Compact separators, matching kotlinx JsonElement.toString()
        assert b", " not in data, data
        # Non-ASCII stays literal UTF-8 rather than \uXXXX, also matching kotlinx
        assert "café".encode("utf-8") in data, data

    check("data is compact UTF-8 bytes with literal non-ASCII", case_16)

    print("\n[17] Ordering key and attributes forwarding")

    def case_17():
        client = FakeClient()
        pub = _make_publisher(client)
        pub.publish({"a": 1}, ordering_key="k1", attributes={"eventId": "e1", "n": 7})

        _, _, ordering_key, attrs = client.published[0]
        assert ordering_key == "k1", ordering_key
        assert attrs == {"eventId": "e1", "n": "7"}, attrs

    check("ordering_key is forwarded and attributes are coerced to str kwargs", case_17)

    print("\n[18] Synchronous publish rejection")

    def case_18():
        client = FakeClient(publish_exception=RuntimeError("message too large"))
        pub = _make_publisher(client)
        pub.publish({"a": 1}, ordering_key="k1")  # must not raise
        assert pub._pending == set(), pub._pending

    check("a synchronous rejection is contained and leaves nothing pending", case_18)

    print("\n[19] Failed delivery resumes the ordering key")

    def case_19():
        client = FakeClient(future_exception=RuntimeError("NotFound: topic"))
        pub = _make_publisher(client)
        pub.publish({"a": 1}, ordering_key="k1")  # must not raise

        assert client.resumed_event.wait(timeout=5), "resume_publish was never called"
        assert client.resumed == [(pub.topic_path, "k1")], client.resumed
        assert pub._pending == set(), (
            f"future not discarded from _pending: {pub._pending}"
        )

    check("a failed future triggers resume_publish and clears _pending", case_19)

    print("\n--- Summary ---")
    if failures:
        print(f"{len(failures)} check(s) failed: {failures}")
    else:
        print("All checks passed.")


if __name__ == "__main__":
    run_test()

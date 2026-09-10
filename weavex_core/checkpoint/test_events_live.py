"""
Live Pub/Sub integration test for the checkpoint event publisher.

Requires ADC with:
  * roles/pubsub.publisher on topic weavex-checkpoints-eu
  * roles/pubsub.editor (or subscriber + subscription create/delete) so the test
    can stand up and tear down its own throwaway subscription
  * roles/datastore.user on Firestore database "weavex" (for check [6])

No subscriber exists yet, so this test creates one BEFORE publishing — messages
published to a topic with zero subscriptions are discarded and unrecoverable.

Configure the ids below, then run:

    python -m weavex_core.checkpoint.test_events_live

TESTING_PROJECT_ID must be a project currently in TESTING status.
"""

import json
import os
import time

from google.api_core import exceptions as gcp_exceptions
from google.cloud import pubsub_v1

from weavex_core.checkpoint import (
    PubSubEventPublisher,
    WorkflowCheckpointer,
    get_event_publisher,
)
from weavex_core.dao import CHECKPOINTS_COLLECTION, get_dao

# --- configure these -------------------------------------------------------
TESTING_PROJECT_ID = ""
TESTING_ORG_ID = ""

# Leave blank to resolve from WEAVEX_GCP_PROJECT / GCP_PROJECT_ID / ADC.
GCP_PROJECT = ""
# ---------------------------------------------------------------------------

STEP_ID = "events_verify_step"
STEP_CONTEXT = {STEP_ID: {"processed_count": 7, "label": "café"}}
ERROR_JSON = '{"error_type":"timeout","message":"upstream took too long"}'

# Bounded so a stuck pull cannot hang the run.
PULL_DEADLINE = 60.0


def _resolve_project() -> str:
    """Same resolution order as PubSubEventPublisher, so both agree."""
    if GCP_PROJECT:
        return GCP_PROJECT

    import google.auth

    return (
        os.environ.get("WEAVEX_GCP_PROJECT")
        or os.environ.get("GCP_PROJECT_ID")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or google.auth.default()[1]
    )


def _pull_all(subscriber, subscription_path, expected: int):
    """
    Pulls until `expected` messages arrive or PULL_DEADLINE elapses. Returns them
    in receive order, which under an ordered subscription is publish order.
    """
    received = []
    deadline = time.monotonic() + PULL_DEADLINE
    while len(received) < expected and time.monotonic() < deadline:
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": expected - len(received),
            },
            timeout=10,
        )
        for msg in response.received_messages:
            received.append(msg)
        if response.received_messages:
            subscriber.acknowledge(
                request={
                    "subscription": subscription_path,
                    "ack_ids": [m.ack_id for m in response.received_messages],
                }
            )
    return received


def run_test():
    print("--- Checkpoint event publisher live Pub/Sub test ---")

    if not TESTING_PROJECT_ID:
        print("ERROR: set TESTING_PROJECT_ID (and TESTING_ORG_ID) at the top of this file.")
        return

    gcp_project = _resolve_project()
    execution_id = f"events-verify-{int(time.time())}"
    ordering_key = f"{TESTING_PROJECT_ID}:{execution_id}"
    context = {
        "execution_id": execution_id,
        "org_id": TESTING_ORG_ID,
        "knit_api_key": "verify-key",
    }

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

    # Resolve the topic the same way production will.
    topic_path = get_event_publisher().topic_path
    print(f"    topic={topic_path}")
    print(f"    execution_id={execution_id}")

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        gcp_project, f"events-verify-{int(time.time())}"
    )
    messages = []

    try:
        # 1. Subscription first — a topic with no subscription drops everything.
        print(f"\n[1] Create throwaway ordered subscription | {subscription_path}")

        def case_1():
            subscriber.create_subscription(
                request={
                    "name": subscription_path,
                    "topic": topic_path,
                    "enable_message_ordering": True,
                    "ack_deadline_seconds": 60,
                }
            )

        check("subscription created with message ordering enabled", case_1)

        # 2. Publish the three write events through the real checkpointer
        print("\n[2] Publish success / fail / clear")

        def case_2():
            cp = WorkflowCheckpointer(TESTING_PROJECT_ID, context)
            cp.success(STEP_ID, STEP_CONTEXT)
            cp.fail(STEP_ID, ERROR_JSON)
            cp.clear()

            # 3 folded in here: the checkpointer owns its publisher, so flush
            # through it rather than building a second client.
            assert cp._events.flush(timeout=15) is True, "publisher did not drain in 15s"

        check("all three events published and flushed", case_2)

        # 4. Pull them back in order
        print("\n[4] Pull")

        def case_4():
            messages.extend(_pull_all(subscriber, subscription_path, expected=3))
            assert len(messages) == 3, f"expected 3 messages, got {len(messages)}"

            types = [m.message.attributes.get("eventType") for m in messages]
            assert types == ["checkpoint.set", "checkpoint.set", "checkpoint.clear"], types

            keys = {m.message.ordering_key for m in messages}
            assert keys == {ordering_key}, keys

        check("three messages arrive in publish order under one ordering key", case_4)

        # 5. Schema
        print("\n[5] Attributes and body")

        def case_5():
            success_msg, fail_msg, clear_msg = (m.message for m in messages)

            for msg in (success_msg, fail_msg, clear_msg):
                attrs = msg.attributes
                assert attrs["schemaVersion"] == "1", attrs["schemaVersion"]
                assert attrs["projectId"] == TESTING_PROJECT_ID, attrs["projectId"]
                assert attrs["executionId"] == execution_id, attrs["executionId"]
                body = json.loads(msg.data.decode("utf-8"))
                assert body["eventId"] == attrs["eventId"], (body["eventId"], attrs["eventId"])
                assert body["version"] == 1, body["version"]
                assert body["source"] == "weavex-core", body["source"]

            success = json.loads(success_msg.data.decode("utf-8"))
            assert success_msg.attributes["stepId"] == STEP_ID, success_msg.attributes
            assert isinstance(success["checkpoint"], dict), (
                "checkpoint must arrive as a nested object, not a string — the "
                "Kotlin subscriber decodes it into SetCheckpointRequest"
            )
            assert success["checkpoint"]["status"] == "success", success["checkpoint"]
            assert success["checkpoint"]["error"] is None, success["checkpoint"]
            assert success["stepContext"] == STEP_CONTEXT, success["stepContext"]

            fail = json.loads(fail_msg.data.decode("utf-8"))
            assert fail["checkpoint"]["status"] == "failed", fail["checkpoint"]
            assert isinstance(fail["checkpoint"]["error"], dict), fail["checkpoint"]
            assert "stepContext" not in fail, (
                "stepContext must be absent on failure, matching the nullable "
                "Kotlin DTO field"
            )

            clear = json.loads(clear_msg.data.decode("utf-8"))
            assert "stepId" not in clear, clear
            assert "stepId" not in clear_msg.attributes, dict(clear_msg.attributes)

        check("every message matches the documented schema", case_5)

        # 6. Nothing consumed it yet
        print("\n[6] No Firestore checkpoint document was written")

        def case_6():
            db = get_dao().db
            snap = (
                db.collection(CHECKPOINTS_COLLECTION)
                .document(f"{TESTING_PROJECT_ID}:{execution_id}")
                .get()
            )
            assert not snap.exists, (
                "a checkpoint document exists — either a subscriber is already "
                "live, or something still writes Firestore directly"
            )

        check("publishing alone does not persist a checkpoint", case_6)

        # 8. The pause hazard — the only real test of _on_done / resume_publish
        print("\n[8] Publish failure pauses and resumes the ordering key")

        def case_8():
            bad = PubSubEventPublisher(
                topic="does-not-exist-verify", project=gcp_project
            )
            # Two on one key: the second is the one that would be silently
            # dropped forever if the key were left paused.
            bad.publish({"n": 1}, ordering_key="pause-verify")
            bad.publish({"n": 2}, ordering_key="pause-verify")
            bad.flush(timeout=15)

            # Give the resume thread a moment; it is dispatched from the callback.
            time.sleep(2)

            # A third publish must still be accepted rather than rejected by a
            # wedged sequencer. It will also fail — that is expected; what matters
            # is that nothing raises into the caller.
            bad.publish({"n": 3}, ordering_key="pause-verify")
            bad.flush(timeout=15)
            print("    (expect NotFound + resume lines on stderr above)")

        check("a bad topic never raises into the caller and resumes the key", case_8)

    finally:
        # 7. Teardown
        print("\n[7] Teardown")
        try:
            subscriber.delete_subscription(request={"subscription": subscription_path})
            print(f"    deleted {subscription_path}")
        except gcp_exceptions.NotFound:
            print("    subscription already gone")
        except Exception as e:
            print(f"    WARNING could not delete subscription: {type(e).__name__}: {e}")
        subscriber.close()

    print("\n--- Summary ---")
    if failures:
        print(f"{len(failures)} check(s) failed: {failures}")
    else:
        print("All checks passed.")


if __name__ == "__main__":
    run_test()

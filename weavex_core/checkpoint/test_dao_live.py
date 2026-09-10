"""
Live Firestore integration test for the checkpoint DAO.

Requires ADC with read/write access to Firestore database "weavex"
(roles/datastore.user on the GCP project). It reads real project documents,
creates a throwaway checkpoint document, and deletes it again.

Configure the two project ids below, then run:

    python -m weavex_core.checkpoint.test_dao_live

TESTING_PROJECT_ID must be a project currently in TESTING status (any status
works for checkpointing itself, which is unconditional, but [1] asserts the
DAO's status lookup specifically finds TESTING).
"""

import json
import time

from google.cloud.firestore_v1.field_path import FieldPath

from weavex_core.checkpoint import WorkflowCheckpointer
from weavex_core.dao import CHECKPOINTS_COLLECTION, PROJECTS_COLLECTION, get_dao

# --- configure these -------------------------------------------------------
TESTING_PROJECT_ID = ""
TESTING_ORG_ID = ""
# ---------------------------------------------------------------------------

CONTEXT_PAYLOAD = {"knit_api_key": "verify-key", "region": "eu", "nested": {"a": [1, 2]}}
INTEGRATION_IDS = {"hris": "int_verify"}
USER_INPUT = {"limit": 25, "label": "café"}

# One entry per is_complete branch. Values are JSON strings, matching how
# /checkpoint.set stores them (request.checkpoint.toString()).
SEEDED_STEPS = {
    "verify_success": '{"step_id":"verify_success","status":"success","error":null}',
    "verify_failed": '{"step_id":"verify_failed","status":"failed","error":{"error_type":"timeout"}}',
    # TestAndFixFlow.markCheckpointFixing — note: no "error" key
    "verify_fixing": '{"step_id":"verify_fixing","status":"fixing"}',
    # Written by the deprecated mark_success; extra keys must not break parsing
    "verify_legacy": '{"step_id":"verify_legacy","status":"success","error":null,'
                     '"step_name":"x","connector":"y","operation_type":"z","attempt":1}',
    # BuildTestFlow's per-step failure marker — no "error" key
    "verify_no_error": '{"step_id":"verify_no_error","status":"failed"}',
    "verify_malformed": "not-json",
}


def _context(execution_id: str) -> dict:
    return {
        "execution_id": execution_id,
        "org_id": TESTING_ORG_ID,
        "knit_api_key": "verify-key",
    }


def run_test():
    print("--- Checkpoint DAO live Firestore test ---")

    if not TESTING_PROJECT_ID:
        print("ERROR: set TESTING_PROJECT_ID (and TESTING_ORG_ID) at the top of this file.")
        return

    dao = get_dao()
    db = dao.db
    execution_id = f"dao-verify-{int(time.time())}"
    doc_id = f"{TESTING_PROJECT_ID}:{execution_id}"
    doc_ref = db.collection(CHECKPOINTS_COLLECTION).document(doc_id)

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

    try:
        # 1. Both status lookup paths agree
        print("\n[1] get_project_status: point read vs query fallback")

        def case_1():
            by_doc = dao.get_project_status(TESTING_PROJECT_ID, TESTING_ORG_ID)
            by_query = dao.get_project_status(TESTING_PROJECT_ID, None)
            print(f"    org_id path={by_doc!r} | query path={by_query!r}")
            assert by_doc == "TESTING", f"expected TESTING, got {by_doc!r}"
            assert by_doc == by_query, f"paths disagree: {by_doc!r} vs {by_query!r}"

        check("both status paths return TESTING and agree", case_1)

        # 2. Unknown project
        print("\n[2] get_project_status for a nonexistent project")

        def case_2():
            status = dao.get_project_status("does-not-exist-dao-verify", None)
            assert status is None, f"expected None, got {status!r}"

        check("unknown project returns None", case_2)

        # 3. First init writes the document
        print(f"\n[3] First init | doc={doc_id}")

        def case_3():
            cp = WorkflowCheckpointer(TESTING_PROJECT_ID, _context(execution_id))
            result = cp.init(CONTEXT_PAYLOAD, INTEGRATION_IDS, USER_INPUT)
            assert result == {"init": True, "step_context": {}}, result

            snap = doc_ref.get()
            assert snap.exists, f"document {doc_id} was not created"
            data = snap.to_dict()
            assert set(data.keys()) == {"context", "integrationIds", "userInput"}, data.keys()
            for key, value in data.items():
                assert isinstance(value, str), f"{key} stored as {type(value).__name__}, expected str"
            assert json.loads(data["context"]) == CONTEXT_PAYLOAD
            assert json.loads(data["integrationIds"]) == INTEGRATION_IDS
            assert json.loads(data["userInput"]) == USER_INPUT

        check("first init creates the doc with three JSON-string fields", case_3)

        first_write = doc_ref.get().to_dict() or {}

        # 4. Second init must not overwrite the seeded fields
        print("\n[4] Second init with different payloads (idempotency)")

        def case_4():
            cp = WorkflowCheckpointer(TESTING_PROJECT_ID, _context(execution_id))
            result = cp.init({"changed": True}, {"other": "int_999"}, {"limit": 999})
            assert result.get("init") is False, result

            after = doc_ref.get().to_dict() or {}
            assert after == first_write, f"fields were overwritten:\n  before={first_write}\n  after={after}"

        check("re-init returns init=False and leaves fields untouched", case_4)

        # 5. step_context is read back and parsed
        print("\n[5] step_context round-trip")

        def case_5():
            doc_ref.set({"step_context": '{"fetch_employees":{"processed_count":7}}'}, merge=True)
            cp = WorkflowCheckpointer(TESTING_PROJECT_ID, _context(execution_id))
            result = cp.init(CONTEXT_PAYLOAD, INTEGRATION_IDS, USER_INPUT)
            assert result == {
                "init": False,
                "step_context": {"fetch_employees": {"processed_count": 7}},
            }, result

        check("existing step_context is returned parsed", case_5)

        # ------------------------------------------------------------------
        # is_complete
        # ------------------------------------------------------------------

        # 5a. Every is_complete branch against one seeded document
        print("\n[5a] is_complete branch matrix")

        def case_5a():
            doc_ref.set(SEEDED_STEPS, merge=True)
            # A real map, not a JSON string — exercises Kotlin's `as? String` path.
            doc_ref.set({"verify_not_a_string": {"status": "success"}}, merge=True)

            cp = WorkflowCheckpointer(TESTING_PROJECT_ID, _context(execution_id))
            expected = {
                "verify_success": True,
                "verify_failed": False,
                "verify_fixing": False,
                "verify_legacy": True,       # extra keys must not break parsing
                "verify_no_error": False,    # BuildTestFlow's {step_id,status} shape
                "verify_malformed": False,   # server would 500 here; we return False
                "verify_not_a_string": False,
                "verify_never_written": False,  # pending
            }
            actual = {step: cp.is_complete(step) for step in expected}
            assert actual == expected, f"\n  expected={expected}\n  actual  ={actual}"

        check("all is_complete branches match expectations", case_5a)

        # 5b. No checkpoint document at all
        print("\n[5b] is_complete with no checkpoint document")

        def case_5b():
            fresh_exec = f"dao-verify-none-{int(time.time())}"
            cp = WorkflowCheckpointer(TESTING_PROJECT_ID, _context(fresh_exec))
            assert cp.is_complete("anything") is False

            fresh_ref = db.collection(CHECKPOINTS_COLLECTION).document(
                f"{TESTING_PROJECT_ID}:{fresh_exec}"
            )
            assert not fresh_ref.get().exists, "is_complete created a document as a side effect"

        check("missing document returns False and writes nothing", case_5b)

        # 5c. A step id containing a dot must be read as a literal key
        print("\n[5c] Dotted step id (field-path escaping)")

        def case_5c():
            # set(merge=True) with a dotted string key would create a NESTED map;
            # an explicit FieldPath key writes the literal top-level field.
            doc_ref.update({FieldPath("weird.step"): '{"step_id":"weird.step","status":"success"}'})

            raw = doc_ref.get().to_dict() or {}
            assert "weird.step" in raw, f"expected a literal 'weird.step' key, got {sorted(raw)}"
            assert not isinstance(raw.get("weird"), dict), "wrote a nested map instead of a literal key"

            cp = WorkflowCheckpointer(TESTING_PROJECT_ID, _context(execution_id))
            assert cp.is_complete("weird.step") is True, (
                "dotted step id read as a nested path — FieldPath escaping is missing in the DAO"
            )

        check("dotted step id resolves to the literal field", case_5c)

        # 5d. Masked-read semantics, asserted against Firestore directly
        print("\n[5d] Masked-read semantics")

        def case_5d():
            snap = doc_ref.get(field_paths=["definitely_absent_field"])
            assert snap.exists is True, "existing doc reported as missing under a mask"
            assert snap.to_dict() == {}, snap.to_dict()

            missing_ref = db.collection(CHECKPOINTS_COLLECTION).document("no-such-doc-dao-verify")
            snap2 = missing_ref.get(field_paths=["anything"])
            assert snap2.exists is False, "nonexistent doc reported as existing"
            assert snap2.to_dict() is None, snap2.to_dict()

            assert dao.get_checkpoint(TESTING_PROJECT_ID, "no-such-exec-dao-verify") == {}
            projected = dao.get_checkpoint(
                TESTING_PROJECT_ID, execution_id, fields=["verify_success"]
            )
            assert set(projected) == {"verify_success"}, sorted(projected)

        check("masked reads behave as the DAO assumes", case_5d)

        # 6. Every project document carries a top-level status field
        print("\n[6] Top-level status field present on project documents")

        def case_6():
            docs = list(db.collection(PROJECTS_COLLECTION).select(["status"]).limit(20).stream())
            assert docs, "no project documents found"
            missing = [d.id for d in docs if not (d.to_dict() or {}).get("status")]
            assert not missing, f"{len(missing)} project doc(s) lack a top-level status: {missing[:5]}"
            print(f"    checked {len(docs)} project documents")

        check("all sampled project docs expose a top-level status", case_6)

    finally:
        # 7. Clean up the throwaway document
        print("\n[7] Cleanup")
        try:
            doc_ref.delete()
            print(f"    deleted {doc_id}")
        except Exception as e:
            print(f"    ERROR deleting {doc_id}: {e}")

    print("\n--- Summary ---")
    if failures:
        print(f"{len(failures)} check(s) failed: {failures}")
    else:
        print("All checks passed.")


if __name__ == "__main__":
    run_test()

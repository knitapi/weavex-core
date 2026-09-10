import hashlib
import json

from weavex_core.state import get_sync_state


def main():
    project_id = "test-project"
    record_id = "jane.doe@company.com"

    record_v1 = {
        "full_name": "Jane Doe",
        "employee_id": "EMP001",
        "work_email": "jane.doe@company.com",
    }

    # Same data, different key order — should hash identically
    record_v1_reordered = {
        "employee_id": "EMP001",
        "work_email": "jane.doe@company.com",
        "full_name": "Jane Doe",
    }

    # Actually changed data — should hash differently
    record_v2_changed = {
        "full_name": "Jane Smith",  # name changed
        "employee_id": "EMP001",
        "work_email": "jane.doe@company.com",
    }

    state = get_sync_state()
    hash_v1 = state.create_hash(project_id, record_id, record_v1)
    hash_v1_reordered = state.create_hash(project_id, record_id, record_v1_reordered)
    hash_v2 = state.create_hash(project_id, record_id, record_v2_changed)

    print(f"record_v1:            {hash_v1}")
    print(f"record_v1_reordered:  {hash_v1_reordered}")
    print(f"record_v2_changed:    {hash_v2}")
    print()

    assert hash_v1 == hash_v1_reordered, "FAIL: key order should not affect hash"
    print("PASS: key order does not affect hash")

    assert hash_v1 != hash_v2, "FAIL: changed data should produce a different hash"
    print("PASS: changed data produces a different hash")


if __name__ == "__main__":
    main()
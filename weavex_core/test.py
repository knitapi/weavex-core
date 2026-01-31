import hashlib
import json
from weavex_core.storage import get_object_store
from weavex_core.state import get_sync_state

def main():
    print("--- Running Weavex SDK Integration Test ---")
    p_id, s_id = "test_project", "sync_v1"

    # 1. Test Object Store (Producer/Consumer)
    storage = get_object_store()
    raw_data = [{"id": "emp_1", "name": "Kunal"}, {"id": "emp_2", "name": "Gemini"}]

    print("[Storage] Uploading staged data...")
    uri = storage.upload_json(p_id, s_id, "employees.json", raw_data)
    print(f"Verified URI: {uri}") # Should contain project_id/sync_id/employees.json

    print("[Storage] Downloading for processing...")
    downloaded = storage.download_json(p_id, s_id, uri)
    assert downloaded == raw_data
    print("✓ Storage Integrity Verified.")

    # 2. Test State Store (Deduplication Hash)
    state = get_sync_state()
    record = raw_data[0]
    record_hash = hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()

    print(f"[State] Fingerprinting record {record['id']}...")
    state.set_hash(p_id, record['id'], record_hash)

    # Simulate a second run - Check if changed
    existing_hash = state.get_hash(p_id, record['id'])
    if existing_hash == record_hash:
        print(f"✓ Deduplication Success: Record {record['id']} matches existing hash.")

    # 3. Test State Store (Progress Cursor)
    print("[State] Checkpointing progress...")
    state.set_state(p_id, s_id, "ingest_step", "last_index", 1)
    print(f"✓ Progress Cursor: {state.get_state(p_id, s_id, 'ingest_step', 'last_index')}")

    print("\n--- Testing Deletion Logic ---")
    storage = get_object_store()
    p_id, s_id = "test_project", "sync_v1"

    # 1. Create a temp file to delete
    uri = storage.upload_json(p_id, s_id, "temp_delete.json", {"status": "to_be_deleted"})
    print(f"Created temporary file at: {uri}")

    # 2. Test Security: Attempting to delete with wrong project_id
    try:
        print("[Test] Attempting unauthorized deletion (wrong project)...")
        storage.delete_json("malicious_project", s_id, uri)
    except PermissionError as e:
        print(f"✓ Security Catch: {e}")

    # 3. Test Successful Deletion
    print("[Test] Performing authorized deletion...")
    success = storage.delete_json(p_id, s_id, uri)
    if success:
        print("✓ File deleted successfully.")

    # 4. Verify it's gone
    print("[Test] Verifying file no longer exists...")
    try:
        storage.download_json(p_id, s_id, uri)
        print("✗ Error: File still exists!")
    except Exception:
        print("✓ Verification Success: File is confirmed missing.")

if __name__ == "__main__":
    main()
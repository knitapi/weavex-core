import os
import hashlib
import json
from abc import ABC, abstractmethod
from typing import Any, Optional
from google.cloud import firestore

class StateStore(ABC):
    """Abstract interface for Sync State Management."""

    @abstractmethod
    def get_state(self, project_id: str, sync_id: str, step_id: str, key: str) -> Any:
        pass

    @abstractmethod
    def set_state(self, project_id: str, sync_id: str, step_id: str, key: str, value: Any) -> None:
        pass

    @abstractmethod
    def delete_state(self, project_id: str, sync_id: str, step_id: str, key: str) -> None:
        pass

    @abstractmethod
    def get_hash(self, project_id: str, record_id: str) -> Optional[str]:
        pass

    @abstractmethod
    def set_hash(self, project_id: str, record_id: str, hash_value: str) -> None:
        pass

    @abstractmethod
    def delete_hash(self, project_id: str, record_id: str) -> None:
        pass

    @abstractmethod
    def create_hash(self, project_id: str, record_id: str, obj: dict) -> str:
        pass

class FirestoreStateStore(StateStore):
    """Google Cloud Firestore implementation."""

    def __init__(self):
        # Get the base database name
        base_db = os.environ.get("FIRESTORE_DATABASE", "weavex-state")

        # Get the region setting
        region = os.getenv("WEAVEX_SERVICE_REGION", "eu").lower()

        # Apply suffix logic
        if region == "eu":
            db_name = f"{base_db}-eu"
        else:
            db_name = base_db

        # Initialize client with the specific database name
        self.db = firestore.Client(database=db_name)

    def _get_state_doc(self, project_id, sync_id, step_id):
        # Structure: projects/{pid}/syncs/{sid}/steps/{stepid}
        return self.db.collection('projects').document(project_id) \
            .collection('syncs').document(sync_id) \
            .collection('steps').document(step_id)

    def _get_hash_doc(self, project_id, record_id):
        # Structure: projects/{pid}/hashes/{record_id}
        return self.db.collection('projects').document(project_id) \
            .collection('hashes').document(record_id)

    def get_state(self, project_id: str, sync_id: str, step_id: str, key: str) -> Any:
        doc = self._get_state_doc(project_id, sync_id, step_id).get()
        if doc.exists:
            return doc.to_dict().get(key)
        return None

    def set_state(self, project_id: str, sync_id: str, step_id: str, key: str, value: Any) -> None:
        doc_ref = self._get_state_doc(project_id, sync_id, step_id)
        doc_ref.set({key: value}, merge=True)

    def delete_state(self, project_id: str, sync_id: str, step_id: str, key: str) -> None:
        doc_ref = self._get_state_doc(project_id, sync_id, step_id)

        try:
            # We use update because we only want to remove a specific field
            doc_ref.update({key: firestore.DELETE_FIELD})
        except Exception as e:
            # Firestore throws a 404 (NotFound) if the document doesn't exist.
            # We catch it gracefully because deleting a field from a
            # non-existent document is effectively a "success".
            if "404" in str(e) or "NOT_FOUND" in str(e).upper():
                return
            raise e # Re-raise if it's a different error (like permission denied)

    def create_hash(self, project_id: str, record_id: str, obj: dict) -> str:
        """
        Computes a deterministic hash of `obj` for change-detection comparison.

        Pure function — does NOT read or write any stored state. Compare its
        output against a previously stored hash via get_hash(), and persist it
        via set_hash() only after successfully processing the record (see the
        CREATE -> CHECK -> ACT -> WRITE sequence this pairs with).

        `project_id` and `record_id` are accepted for signature parity with
        get_hash()/set_hash() but do NOT factor into the digest itself — the
        hash reflects only `obj`'s content.

        Args:
            project_id: Project/tenant identifier (unused in the digest itself).
            record_id: The record's unique identifier (unused in the digest
                itself — e.g. the field you're keying on, such as work_email).
            obj: The record data to hash. Only include the fields relevant to
                change detection — the full record for FULL_RECORD, or a
                pre-filtered sub-dict for SPECIFIC_FIELDS. This function does
                not filter; build `obj` accordingly before calling.

        Returns:
            A hex-encoded SHA-256 digest string.
        """
        serialized = json.dumps(
            obj,
            sort_keys=True,          # dict key order never affects the output
            separators=(",", ":"),   # no incidental whitespace differences
            default=str,             # gracefully stringify non-JSON-native types
            ensure_ascii=True,
        )
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def get_hash(self, project_id: str, record_id: str) -> Optional[str]:
        doc = self._get_hash_doc(project_id, record_id).get()
        if doc.exists:
            return doc.to_dict().get('hash')
        return None

    def set_hash(self, project_id: str, record_id: str, hash_value: str) -> None:
        doc_ref = self._get_hash_doc(project_id, record_id)
        doc_ref.set({'hash': hash_value}, merge=True)

    def delete_hash(self, project_id: str, record_id: str) -> None:
        self._get_hash_doc(project_id, record_id).delete()

def get_sync_state() -> StateStore:
    """Factory to get the configured StateStore implementation. Defaults to Firestore."""
    # Graceful default: defaults to 'firestore' if STATE_STORE_TYPE is not set
    backend = os.environ.get("STATE_STORE_TYPE", "firestore").lower()

    if backend == "firestore":
        return FirestoreStateStore()
    else:
        raise ValueError(f"Unsupported STATE_STORE_TYPE: {backend}")
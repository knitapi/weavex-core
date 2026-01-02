import os
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

class FirestoreStateStore(StateStore):
    """Google Cloud Firestore implementation."""

    def __init__(self):
        # Initialize client (picks up credentials from env GOOGLE_APPLICATION_CREDENTIALS)
        self.db = firestore.Client()

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
        doc_ref.update({key: firestore.DELETE_FIELD})

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
    """Factory to get the configured StateStore implementation."""
    backend = os.environ.get("STATE_BACKEND", "firestore").lower()

    if backend == "firestore":
        return FirestoreStateStore()
    else:
        raise ValueError(f"Unsupported STATE_BACKEND: {backend}")
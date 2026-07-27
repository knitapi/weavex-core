import os
from typing import Any, Dict, Iterable, Optional, Tuple

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1.field_path import FieldPath

from .base import WeavexDao

# Collections in the Weavex app database (mirrors FirestoreAppDB.kt).
PROJECTS_COLLECTION = "knit-weavex-projects"
CHECKPOINTS_COLLECTION = "knit-weavex-checkpoints"

# NOTE: unlike state.py / storage.py there is NO "-eu" region suffix here. The app
# database is a single global "weavex" database (Kotlin:
# FirestoreOptions.newBuilder().setDatabaseId("weavex")). Do not add
# WEAVEX_SERVICE_REGION logic to this module.
_DEFAULT_DATABASE = "weavex"

# Keep each RPC well inside the 30s Temporal start_to_close on
# checkpoint_init_activity so a stalled call fails fast and lets Temporal retry.
_RPC_TIMEOUT = float(os.environ.get("WEAVEX_FIRESTORE_TIMEOUT", "10"))


class FirestoreDb(WeavexDao):
    """Google Cloud Firestore implementation of WeavexDao."""

    def __init__(self, database: Optional[str] = None, project: Optional[str] = None):
        db_name = database or os.environ.get("WEAVEX_FIRESTORE_DATABASE", _DEFAULT_DATABASE)

        # None => infer the GCP project from ADC, same as the Kotlin client and as
        # workflow-runner's own firestore.Client(database=...) calls.
        gcp_project = project or os.environ.get("WEAVEX_GCP_PROJECT") or None

        # One client per instance. Do not turn this into a module-level shared
        # client: workflow-runner forks via ProcessPoolExecutor and gRPC channels
        # are not fork-safe. Per-instance construction means no channel is ever
        # inherited across a fork.
        self.db = firestore.Client(project=gcp_project, database=db_name)

    def get_project_status(self, project_id: str, org_id: Optional[str] = None) -> Optional[str]:
        collection = self.db.collection(PROJECTS_COLLECTION)

        if org_id:
            snap = collection.document(f"{org_id}:{project_id}").get(
                field_paths=["status"], timeout=_RPC_TIMEOUT
            )
            if snap.exists:
                return (snap.to_dict() or {}).get("status")

        docs = (
            collection
            .where(filter=FieldFilter("projectId", "==", project_id))
            .select(["status"])  # never pull compressedState
            .limit(1)
            .get(timeout=_RPC_TIMEOUT)
        )
        if not docs:
            return None
        return (docs[0].to_dict() or {}).get("status")

    def get_checkpoint(
        self, project_id: str, execution_id: str, fields: Optional[Iterable[str]] = None
    ) -> Dict[str, Any]:
        doc_ref = self.db.collection(CHECKPOINTS_COLLECTION).document(
            f"{project_id}:{execution_id}"
        )

        # Field names, not paths: a stepId containing "." must be read as that
        # literal key, not as a nested lookup that would silently return nothing.
        # An empty iterable folds to None on purpose — an empty DocumentMask is
        # indistinguishable from an unset one and would return the whole document.
        field_paths = [FieldPath(name).to_api_repr() for name in fields] if fields else None

        snap = doc_ref.get(field_paths=field_paths, timeout=_RPC_TIMEOUT)
        return snap.to_dict() or {}

    def init_checkpoint(
        self, project_id: str, execution_id: str, fields: Dict[str, str]
    ) -> Tuple[bool, Dict[str, Any]]:
        doc_ref = self.db.collection(CHECKPOINTS_COLLECTION).document(
            f"{project_id}:{execution_id}"
        )
        snap = doc_ref.get(timeout=_RPC_TIMEOUT)

        was_new = not snap.exists
        existing: Dict[str, Any] = {} if was_new else (snap.to_dict() or {})

        to_save = {k: v for k, v in fields.items() if k not in existing}
        if to_save:
            doc_ref.set(to_save, merge=True, timeout=_RPC_TIMEOUT)

        return was_new, existing

import os
import json
from abc import ABC, abstractmethod
from typing import Any
from google.cloud import storage

class ObjectStore(ABC):
    """Abstract interface for Object Storage."""

    @abstractmethod
    def upload_json(self, project_id: str, sync_id: str, key: str, data: Any) -> str:
        """Uploads data and returns a URI string."""
        pass

    @abstractmethod
    def download_json(self, project_id: str, sync_id: str, uri: str) -> Any:
        """Downloads data from a URI string."""
        pass

    @abstractmethod
    def delete_json(self, project_id: str, sync_id: str, uri: str) -> bool:
        """Deletes a JSON object from storage. Returns True if successful."""
        pass

class GCSObjectStore(ObjectStore):
    """Google Cloud Storage implementation."""

    def __init__(self):
        # Graceful default: If BUCKET_NAME is missing, use fallback
        self.bucket_name = os.environ.get("BUCKET_NAME", "weavex-flow-storage")
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)

    def upload_json(self, project_id: str, sync_id: str, key: str, data: Any) -> str:
        # Construct path using project_id and sync_id
        full_path = f"{project_id}/{sync_id}/{key}"
        blob = self.bucket.blob(full_path)

        blob.upload_from_string(
            data=json.dumps(data),
            content_type='application/json'
        )

        return f"gs://{self.bucket_name}/{full_path}"

    def download_json(self, project_id: str, sync_id: str, uri: str) -> Any:
        if not uri.startswith("gs://"):
            raise ValueError(f"Invalid GCS URI: {uri}. Must start with gs://")

        # Extract bucket and blob path
        try:
            path_parts = uri.replace("gs://", "").split("/", 1)
            bucket_name, blob_path = path_parts[0], path_parts[1]
        except IndexError:
            raise ValueError(f"Malformed GCS URI: {uri}")

        # Validation: Check if path starts with project_id/sync_id
        expected_prefix = f"{project_id}/{sync_id}/"
        if not blob_path.startswith(expected_prefix):
            raise ValueError(
                f"Security mismatch: URI {uri} does not belong to Project: {project_id}, Sync: {sync_id}"
            )

        # Download logic
        target_bucket = self.bucket if bucket_name == self.bucket_name else self.client.bucket(bucket_name)
        blob = target_bucket.blob(blob_path)

        return json.loads(blob.download_as_text())

    def delete_json(self, project_id: str, sync_id: str, uri: str) -> bool:
        if not uri.startswith("gs://"):
            raise ValueError(f"Invalid GCS URI: {uri}. Must start with gs://")

        # Extract bucket and path
        path_parts = uri.replace("gs://", "").split("/", 1)
        bucket_name, blob_path = path_parts[0], path_parts[1]

        # Security check: Ensure the path belongs to this project/sync
        expected_prefix = f"{project_id}/{sync_id}/"
        if not blob_path.startswith(expected_prefix):
            raise PermissionError(
                f"Unauthorized deletion: {uri} does not belong to Project: {project_id}, Sync: {sync_id}"
            )

        # Execute deletion
        target_bucket = self.bucket if bucket_name == self.bucket_name else self.client.bucket(bucket_name)
        blob = target_bucket.blob(blob_path)

        if blob.exists():
            blob.delete()
            return True
        return False

def get_object_store() -> ObjectStore:
    """Factory to get the configured ObjectStore implementation. Defaults to GCS."""
    backend = os.environ.get("OBJECT_STORAGE_TYPE", "gcs").lower()

    if backend == "gcs":
        return GCSObjectStore()
    raise ValueError(f"Unsupported OBJECT_STORAGE_TYPE: {backend}")
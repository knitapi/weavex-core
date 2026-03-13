import os
import json
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
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

    @abstractmethod
    def upload_report(self, project_id: str, sync_job_id: str, sync_run_id: str,
                      file_content: bytes, extension: str) -> str:
        """Uploads a report file. Returns the GCS URI."""
        pass

    @abstractmethod
    def download_report(self, project_id: str, sync_job_id: str, uri: str) -> bytes:
        """Downloads a report file. Returns raw bytes."""
        pass

    @abstractmethod
    def get_report_presigned_url(self, project_id: str, sync_job_id: str, uri: str,
                                  expiration_seconds: int = 3600) -> str:
        """Returns a presigned public URL for a report file."""
        pass

class GCSObjectStore(ObjectStore):
    """Google Cloud Storage implementation."""

    def __init__(self):
        # Get the base bucket name
        base_bucket = os.environ.get("BUCKET_NAME", "weavex-flow-storage")

        # Get the region setting
        region = os.getenv("WEAVEX_SERVICE_REGION", "eu").lower()

        # Apply suffix logic
        if region == "eu":
            self.bucket_name = f"{base_bucket}-eu"
        else:
            self.bucket_name = base_bucket

        # Initialize GCS client and bucket reference
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(self.bucket_name)

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

    _REPORT_CONTENT_TYPES = {
        "csv": "text/csv",
        "txt": "text/plain",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }

    def upload_report(self, project_id: str, sync_job_id: str, sync_run_id: str,
                      file_content: bytes, extension: str) -> str:
        if extension not in self._REPORT_CONTENT_TYPES:
            raise ValueError(f"Unsupported report extension: {extension}. Must be one of {list(self._REPORT_CONTENT_TYPES)}")

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{sync_run_id}_{timestamp}_report.{extension}"
        full_path = f"{project_id}/{sync_job_id}/reports/{filename}"
        blob = self.bucket.blob(full_path)

        blob.upload_from_string(file_content, content_type=self._REPORT_CONTENT_TYPES[extension])

        return f"gs://{self.bucket_name}/{full_path}"

    def _resolve_report_blob(self, project_id: str, sync_job_id: str, uri: str):
        if not uri.startswith("gs://"):
            raise ValueError(f"Invalid GCS URI: {uri}. Must start with gs://")

        try:
            path_parts = uri.replace("gs://", "").split("/", 1)
            bucket_name, blob_path = path_parts[0], path_parts[1]
        except IndexError:
            raise ValueError(f"Malformed GCS URI: {uri}")

        expected_prefix = f"{project_id}/{sync_job_id}/"
        if not blob_path.startswith(expected_prefix):
            raise PermissionError(
                f"Security mismatch: URI {uri} does not belong to Project: {project_id}, Sync Job: {sync_job_id}"
            )

        target_bucket = self.bucket if bucket_name == self.bucket_name else self.storage_client.bucket(bucket_name)
        return target_bucket.blob(blob_path)

    def download_report(self, project_id: str, sync_job_id: str, uri: str) -> bytes:
        blob = self._resolve_report_blob(project_id, sync_job_id, uri)
        return blob.download_as_bytes()

    def get_report_presigned_url(self, project_id: str, sync_job_id: str, uri: str,
                                  expiration_seconds: int = 3600) -> str:
        blob = self._resolve_report_blob(project_id, sync_job_id, uri)
        return blob.generate_signed_url(
            expiration=timedelta(seconds=expiration_seconds),
            method="GET",
            version="v4",
        )

def get_object_store() -> ObjectStore:
    """Factory to get the configured ObjectStore implementation. Defaults to GCS."""
    backend = os.environ.get("OBJECT_STORAGE_TYPE", "gcs").lower()

    if backend == "gcs":
        return GCSObjectStore()
    raise ValueError(f"Unsupported OBJECT_STORAGE_TYPE: {backend}")
import os
import json
from abc import ABC, abstractmethod
from typing import Any, Union
from google.cloud import storage

class ObjectStore(ABC):
    """Abstract interface for Object Storage."""

    @abstractmethod
    def upload_json(self, key: str, data: Any) -> str:
        """Uploads data and returns a URI string."""
        pass

    @abstractmethod
    def download_json(self, uri: str) -> Any:
        """Downloads data from a URI string."""
        pass

class GCSObjectStore(ObjectStore):
    """Google Cloud Storage implementation."""

    def __init__(self):
        self.bucket_name = os.environ.get("GCS_BUCKET_NAME")
        if not self.bucket_name:
            raise ValueError("Environment variable 'GCS_BUCKET_NAME' is required for GCSObjectStore.")

        # Initialize client (picks up credentials from env GOOGLE_APPLICATION_CREDENTIALS)
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)

    def upload_json(self, key: str, data: Any) -> str:
        blob = self.bucket.blob(key)
        blob.upload_from_string(
            data=json.dumps(data),
            content_type='application/json'
        )
        return f"gs://{self.bucket_name}/{key}"

    def download_json(self, uri: str) -> Any:
        if not uri.startswith("gs://"):
            raise ValueError(f"Invalid GCS URI: {uri}. Must start with gs://")

        # Parse URI: gs://bucket/path/to/blob
        try:
            path_parts = uri.replace("gs://", "").split("/", 1)
            bucket_name, blob_name = path_parts[0], path_parts[1]
        except IndexError:
            raise ValueError(f"Malformed GCS URI: {uri}")

        # Use efficient client usage
        if bucket_name == self.bucket_name:
            blob = self.bucket.blob(blob_name)
        else:
            blob = self.client.bucket(bucket_name).blob(blob_name)

        data_str = blob.download_as_text()
        return json.loads(data_str)

def get_object_store() -> ObjectStore:
    """Factory to get the configured ObjectStore implementation."""
    backend = os.environ.get("STORAGE_BACKEND", "gcs").lower()

    if backend == "gcs":
        return GCSObjectStore()
    # Add 's3' or 'azure' here in the future
    else:
        raise ValueError(f"Unsupported STORAGE_BACKEND: {backend}")
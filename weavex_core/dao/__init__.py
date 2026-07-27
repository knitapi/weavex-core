"""
Data access for the Weavex application database — the store behind the Kotlin
AppDB / weavex-cerebro.

Every read or write of that database goes through this package. Nothing else in
weavex_core should construct a Firestore client against it.

    base.py          WeavexDao — the abstract interface.
    firestore_db.py  FirestoreDb — the Google Cloud Firestore implementation.

Select an implementation with the factory:

    from weavex_core import get_dao
    dao = get_dao()
"""

import os

from .base import WeavexDao
from .firestore_db import (
    CHECKPOINTS_COLLECTION,
    PROJECTS_COLLECTION,
    FirestoreDb,
)


def get_dao() -> WeavexDao:
    """Factory to get the configured WeavexDao implementation. Defaults to Firestore."""
    backend = os.environ.get("WEAVEX_DAO_TYPE", "firestore").lower()

    if backend == "firestore":
        return FirestoreDb()
    else:
        raise ValueError(f"Unsupported WEAVEX_DAO_TYPE: {backend}")


__all__ = [
    "WeavexDao",
    "FirestoreDb",
    "get_dao",
    "PROJECTS_COLLECTION",
    "CHECKPOINTS_COLLECTION",
]

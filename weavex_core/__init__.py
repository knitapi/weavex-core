# weavex_core/__init__.py

# 1. Expose Logging (Existing)
from .logging_utils import get_logger

# 2. Expose Storage Factory
from .storage import get_object_store

# 3. Expose State Factory
from .state import get_sync_state

# 4. Expose API Proxy Helper
from .api import make_passthrough_call

# 5. Expose Structured Error
from .errors import WeavexError, ProjectNotFoundError

# 6. Expose App DB DAO
from .dao import get_dao, WeavexDao, FirestoreDb

# 7. Expose Checkpointing (checkpointer + its event publisher)
from .checkpoint import (
    StepCheckpoint,
    WorkflowCheckpointer,
    get_event_publisher,
    EventPublisher,
    PubSubEventPublisher,
)

# 8. Expose Weavex API Service
from .weavex_api_service import WeavexAPIService

# Expose Knit SDKs
from . import knit_consumer
from . import knit_mail
from . import knit_sync

# Optional: Define what is exported when someone uses `from weavex_core import *`
__all__ = [
    "get_logger",
    "get_object_store",
    "get_sync_state",
    "make_passthrough_call",
    "WeavexError",
    "ProjectNotFoundError",
    "StepCheckpoint",
    "WorkflowCheckpointer",
    "WeavexAPIService",
    "get_dao",
    "WeavexDao",
    "FirestoreDb",
    "get_event_publisher",
    "EventPublisher",
    "PubSubEventPublisher",
    "knit_consumer",
    "knit_mail",
    "knit_sync",
]
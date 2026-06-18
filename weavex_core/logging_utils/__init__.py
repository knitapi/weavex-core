import os
from .sdk import WeavexServicesLogger

# This allows you to do:
# from weavex_core.logging import get_logger
# logger = get_logger()

_GLOBAL_LOGGER = None

# def get_logger(project_id=None, env=None):
#     global _GLOBAL_LOGGER
#     if _GLOBAL_LOGGER is None:
#         # Default to environment variables if arguments not passed
#         pid = project_id or os.getenv("WEAVEX_PROJECT_ID", "weavex-475116")
#         print(project_id, pid)
#         type = env or os.getenv("WEAVEX_LOGGER_TYPE", "PUBSUB")
#         _GLOBAL_LOGGER = WeavexServicesLogger(project_id=pid, logger_type=type)
#     return _GLOBAL_LOGGER


# trying with the _GLOBAL_LOGGER to see how much overhead it adds
def get_logger(project_id=None, env=None):
    pid = project_id or os.getenv("WEAVEX_PROJECT_ID", "weavex-475116")
    logger_type = env or os.getenv("WEAVEX_LOGGER_TYPE", "PUBSUB")
    print(project_id, pid)
    return WeavexServicesLogger(project_id=pid, logger_type=logger_type)
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Optional, Tuple


class WeavexDao(ABC):
    """
    Abstract data-access interface for the Weavex application database
    (the store behind the Kotlin AppDB / weavex-cerebro).

    Implementations are thin: no business rules, no JSON shaping, no response
    assembly. Callers own that.
    """

    @abstractmethod
    def get_project_status(self, project_id: str, org_id: Optional[str] = None) -> Optional[str]:
        """
        Returns the project's lifecycle status as the raw stored string
        (e.g. "TESTING", "LIVE"), or None if no project with this projectId exists.

        org_id is an optimisation: when supplied the document is fetched directly
        by its "{org_id}:{project_id}" id (one point read); otherwise, or if that
        document is absent, it falls back to a projectId equality query, matching
        Kotlin getProject(null, projectId).

        Only the status field is read — project documents also carry a
        gzip-compressed full-state blob (generated step code) that must not be
        pulled here.
        """
        pass

    @abstractmethod
    def get_checkpoint(
        self,
        project_id: str,
        execution_id: str,
        fields: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        """
        Returns the checkpoint document for one workflow execution, or {} when no
        such document exists (port of Kotlin AppDB.getCheckpoint).

        The document is a flat map: one entry per stepId whose value is a JSON
        *string*, plus the reserved entries "step_context", "context",
        "integrationIds" and "userInput". Values are returned exactly as stored —
        no parsing, no shaping. Callers own that.

        `fields` is an optional server-side projection: an iterable of top-level
        field NAMES, not "."-delimited field paths. Names are escaped for you, so
        a stepId containing a dot is read as that literal key rather than as a
        nested lookup. Pass None (or an empty iterable) to read the whole document.

        With a projection, an absent field is ambiguous between "document does
        not exist" and "document exists but has no such field". Read without a
        projection if you need to tell those apart.
        """
        pass

    @abstractmethod
    def init_checkpoint(
        self, project_id: str, execution_id: str, fields: Dict[str, str]
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Creates or resumes the checkpoint document for one workflow execution.

        Writes only the keys of `fields` not already present (merge write, never
        an overwrite), so a re-run never clobbers what a previous attempt recorded.

        Returns (was_new, existing_data), where existing_data is the document
        contents as they were BEFORE this call ({} when was_new). Values are
        stored as JSON strings.

        Read-then-conditional-write, not transactional — identical to the Kotlin
        implementation it replaces.
        """
        pass

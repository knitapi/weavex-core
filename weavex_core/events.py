import json
import os
import sys
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import PublisherOptions

# Base topic name. The "-eu" suffix is applied exactly as in state.py and
# storage.py, so the effective default is "weavex-checkpoints-eu".
_DEFAULT_TOPIC = "weavex-checkpoints"

# Bound for flush(). With enable_message_ordering=True the client rewrites the
# publish retry deadline to 2**32 seconds, so an unbounded wait can hang forever.
_FLUSH_TIMEOUT = float(os.environ.get("WEAVEX_PUBSUB_FLUSH_TIMEOUT", "5"))


def _log(message: str) -> None:
    # stderr, matching weavex_api_service.py and logging_utils/transports.py.
    # Deliberately NOT get_logger(): BaseLogger.__init__ installs a signal
    # handler, which raises ValueError off the main thread, and
    # WorkflowCheckpointer is constructed inside asyncio.to_thread and inside a
    # ProcessPoolExecutor child.
    print(f"[weavex-core.events] {message}", file=sys.stderr, flush=True)


class EventPublisher(ABC):
    """
    Abstract transport for outbound domain events.

    Implementations are thin: they take an already-shaped payload and hand it to
    a broker. No envelope construction, no schema knowledge, no domain rules —
    callers own that (same division of labour as WeavexDao).
    """

    @abstractmethod
    def publish(
        self,
        payload: Dict[str, Any],
        ordering_key: str = "",
        attributes: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Fire and forget. Returns once the message is queued locally; never waits
        for broker acknowledgement and never raises on a delivery failure —
        failures are reported out of band. Publishing must not be the reason an
        otherwise-healthy workflow step fails.

        `ordering_key` requests FIFO delivery across messages sharing that key.
        `attributes` are broker metadata for routing/filtering; values are
        coerced to str.
        """
        pass

    @abstractmethod
    def flush(self, timeout: float = _FLUSH_TIMEOUT) -> bool:
        """Block up to `timeout` seconds for queued messages. True if drained."""
        pass


class PubSubEventPublisher(EventPublisher):
    """Google Cloud Pub/Sub implementation of EventPublisher."""

    def __init__(
        self,
        topic: Optional[str] = None,
        project: Optional[str] = None,
        client: Optional[Any] = None,
    ):
        base_topic = topic or os.environ.get("WEAVEX_CHECKPOINT_TOPIC", _DEFAULT_TOPIC)

        region = os.getenv("WEAVEX_SERVICE_REGION", "eu").lower()
        if region == "eu":
            topic_id = f"{base_topic}-eu"
        else:
            topic_id = base_topic

        # Unlike firestore.Client, topic_path REQUIRES an explicit project id —
        # there is no "pass None and let ADC decide". WEAVEX_GCP_PROJECT first
        # (same as dao.py), then the two names logging_utils already uses.
        gcp_project = (
            project
            or os.environ.get("WEAVEX_GCP_PROJECT")
            or os.environ.get("GCP_PROJECT_ID")
            or os.environ.get("GOOGLE_CLOUD_PROJECT")
        )
        if not gcp_project:
            try:
                import google.auth

                gcp_project = google.auth.default()[1]
            except Exception as e:
                raise ValueError(
                    "Cannot resolve a GCP project for Pub/Sub. Set WEAVEX_GCP_PROJECT."
                ) from e
        if not gcp_project:
            raise ValueError(
                "Cannot resolve a GCP project for Pub/Sub. Set WEAVEX_GCP_PROJECT."
            )

        # One client per instance, mirroring FirestoreDb. This is fork-safe by
        # construction: the checkpointer is built inside the forked child, so no
        # gRPC channel is ever inherited. Do not turn this into a module global
        # without adding an os.register_at_fork reset.
        self._client = client or pubsub_v1.PublisherClient(
            publisher_options=PublisherOptions(enable_message_ordering=True)
        )
        self.topic_path = self._client.topic_path(gcp_project, topic_id)

        self._pending = set()
        self._pending_lock = threading.Lock()

        # Logged once so a project/topic misconfiguration is visible in the logs
        # rather than only as a NotFound on every publish.
        _log(f"publishing checkpoint events to {self.topic_path}")

    def publish(
        self,
        payload: Dict[str, Any],
        ordering_key: str = "",
        attributes: Optional[Dict[str, str]] = None,
    ) -> None:
        # Compact separators + ensure_ascii=False, matching checkpoint._compact_json
        # and kotlinx's JsonElement.toString() on the other side. No default=str:
        # this payload becomes the persisted checkpoint, so a non-serializable
        # value must fail loudly here rather than be silently coerced.
        data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        attrs = {k: str(v) for k, v in (attributes or {}).items() if v is not None}
        event_id = attrs.get("eventId", "")

        try:
            future = self._client.publish(
                self.topic_path, data, ordering_key=ordering_key, **attrs
            )
        except Exception as e:
            # Synchronous rejections: MessageTooLargeError (>10MB), TypeError on a
            # bad attribute, RuntimeError("Cannot publish on a stopped sequencer.")
            # after stop(). A PAUSED ordering key does NOT land here — see _on_done.
            _log(
                f"ERROR publish rejected | topic={self.topic_path} key={ordering_key} "
                f"eventId={event_id} | {type(e).__name__}: {e}"
            )
            return

        with self._pending_lock:
            self._pending.add(future)
        future.add_done_callback(lambda f: self._on_done(f, ordering_key, event_id))

    def _on_done(self, future: Any, ordering_key: str, event_id: str) -> None:
        """
        With ordering enabled a single failed publish PAUSES that ordering key:
        every later message for the same execution comes back as an already-failed
        future carrying PublishToPausedOrderingKeyException. Without a
        resume_publish() the key stays wedged for the life of the client, so the
        terminal checkpoint.clear would never go out either.

        Resuming is safe here because each checkpoint.set is an independent upsert
        of a different stepId — the subscriber sees a gap, not corruption, and a
        lost success just makes the step read back as pending and re-run. Do NOT
        re-publish the failed message from this callback: it would land after
        messages already queued behind it, defeating the ordering key.
        """
        with self._pending_lock:
            self._pending.discard(future)

        try:
            future.result()
            return
        except Exception as e:
            _log(
                f"ERROR publish failed | topic={self.topic_path} key={ordering_key} "
                f"eventId={event_id} | {type(e).__name__}: {e} | this message is LOST; "
                "resuming the ordering key so later messages are not dropped"
            )

        self._schedule_resume(ordering_key)

    def _schedule_resume(self, ordering_key: str) -> None:
        """
        Resume must NOT run on the calling thread.

        This callback can be invoked from inside OrderedSequencer._pause(), whose
        own docstring states "_state_lock must be taken before calling this
        method". _pause cancels every queued batch, and cancel() ->
        Future.set_exception() runs done callbacks synchronously on that thread.
        resume_publish() -> unpause() re-takes the same non-reentrant lock, so an
        inline call would deadlock the Pub/Sub commit thread permanently whenever
        a second message was queued behind the one that failed. Failures are rare,
        so a throwaway daemon thread is the right cost.
        """
        if not ordering_key:
            return
        threading.Thread(
            target=self._resume,
            args=(ordering_key,),
            name="weavex-pubsub-resume",
            daemon=True,
        ).start()

    def _resume(self, ordering_key: str) -> None:
        try:
            self._client.resume_publish(self.topic_path, ordering_key)
            _log(f"resumed ordering key after a publish failure | key={ordering_key}")
        except Exception as e:
            # Expected for messages queued BEHIND the failed one: _pause already
            # failed them all, and the first resume unpauses, so the rest arrive
            # to find RuntimeError("Ordering key is not paused."). Harmless.
            _log(f"resume_publish no-op | key={ordering_key} | {type(e).__name__}: {e}")

    def flush(self, timeout: float = _FLUSH_TIMEOUT) -> bool:
        deadline = time.monotonic() + timeout
        with self._pending_lock:
            futures = list(self._pending)

        for future in futures:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _log(f"WARNING flush timed out | outstanding={len(futures)}")
                return False
            try:
                future.result(timeout=remaining)
            except Exception:
                pass  # already reported by _on_done
        return True


def get_event_publisher() -> EventPublisher:
    """Factory to get the configured EventPublisher implementation. Defaults to Pub/Sub."""
    backend = os.environ.get("WEAVEX_EVENT_PUBLISHER_TYPE", "pubsub").lower()

    if backend == "pubsub":
        return PubSubEventPublisher()
    else:
        raise ValueError(f"Unsupported WEAVEX_EVENT_PUBLISHER_TYPE: {backend}")

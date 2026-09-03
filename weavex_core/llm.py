# weavex_core/llm.py
#
# complete_one_shot — fetches LLM credentials from connect server vault.
# Credentials are cached for 5 minutes (same TTL as execute_api).
# Fully sync — same pattern as execute_api.py.
# Can be called directly from Temporal activities or via the FastAPI route.

import os
import time
import threading
from dataclasses import dataclass
from typing import Optional

import httpx
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_core.language_models import BaseChatModel


# ── Response ───────────────────────────────────────────────────────────────────

@dataclass
class LLMResponse:
    content:      str
    model:        str
    input_tokens: int
    output_tokens:int


# ── Credential cache (mirrors execute_api._CredentialCache exactly) ────────────

@dataclass
class _CachedLLMConfig:
    config:     dict
    fetched_at: float


class _LLMConfigCache:
    def __init__(self, ttl_seconds: int = 300):
        self._cache: dict[str, _CachedLLMConfig] = {}
        self._lock:  threading.Lock               = threading.Lock()
        self._ttl:   int                          = ttl_seconds

    def get(self, integration_id: str) -> Optional[dict]:
        with self._lock:
            entry = self._cache.get(integration_id)
            if not entry:
                return None
            if time.time() - entry.fetched_at > self._ttl:
                del self._cache[integration_id]
                return None
            return entry.config

    def set(self, integration_id: str, config: dict) -> None:
        with self._lock:
            self._cache[integration_id] = _CachedLLMConfig(
                config     = config,
                fetched_at = time.time()
            )

    def evict(self, integration_id: str) -> None:
        with self._lock:
            self._cache.pop(integration_id, None)


_cache = _LLMConfigCache(ttl_seconds=300)


# ── Connect server helpers ─────────────────────────────────────────────────────

def _connect_server_url() -> str:
    url = os.environ.get("WEAVEX_CONNECT_SERVER_URL")
    if not url:
        raise RuntimeError("WEAVEX_CONNECT_SERVER_URL not set")
    return url.rstrip("/")


def _connect_server_auth_headers() -> dict:
    token = os.environ.get("WEAVEX_SKILLS_CONNECT_API_KEY")
    if not token:
        raise RuntimeError("WEAVEX_SKILLS_CONNECT_API_KEY env var not set")
    return {"Authorization": f"Bearer {token}"}


# ── Vault fetch (sync, cached, mirrors _get_credentials in execute_api) ────────

def _fetch_llm_config(integration_id: str) -> dict:
    url = f"{_connect_server_url()}/api/vault/{integration_id}"
    print(f"[llm] fetching config for {integration_id} from {url}", flush=True)

    with httpx.Client(timeout=10) as client:
        response = client.get(url, headers=_connect_server_auth_headers())

    if response.status_code == 404:
        raise ValueError(
            f"No credentials found for integration '{integration_id}' — "
            f"has the skill been connected?"
        )
    if response.status_code != 200:
        raise RuntimeError(
            f"Vault fetch failed for '{integration_id}': HTTP {response.status_code}"
        )

    body   = response.json()
    creds  = body.get("credentials", body)
    config = {
        "provider": creds.get("provider"),
        "model_id": creds.get("modelId"),
        "api_key":  creds.get("apiKey")
    }

    print(
        f"[llm] config fetched for {integration_id}: "
        f"provider={config['provider']} model={config['model_id']}",
        flush=True
    )
    return config


def _get_llm_config(integration_id: str, force_refresh: bool = False) -> dict:
    if not force_refresh:
        cached = _cache.get(integration_id)
        if cached:
            print(f"[llm] using cached config for {integration_id}", flush=True)
            return cached

    config = _fetch_llm_config(integration_id)
    _cache.set(integration_id, config)
    return config


# ── Public API ─────────────────────────────────────────────────────────────────

def complete_one_shot(
        context:        dict,
        integration_id: str,
        system:         str,
        user:           str) -> LLMResponse:
    messages = [
        {"role": "system", "content": system},
        {"role": "user",   "content": user}
    ]
    return complete(context, messages, integration_id)


def complete(
        context:        dict,
        messages:       list[dict],
        integration_id: str) -> LLMResponse:
    if not integration_id:
        raise ValueError("Missing 'integration_id'")

    config   = _get_llm_config(integration_id)
    model_id = config["model_id"]
    api_key  = config["api_key"]
    provider = config["provider"]

    mapping     = {"user": HumanMessage, "system": SystemMessage, "assistant": AIMessage}
    lc_messages = [mapping[m["role"]](content=m["content"]) for m in messages]

    result = _get_model(provider, model_id, api_key).invoke(lc_messages)
    print(result, flush=True)

    usage = result.usage_metadata or {}
    return LLMResponse(
        content       = _extract_content(result),
        model         = model_id,
        input_tokens  = usage.get("input_tokens", 0),
        output_tokens = usage.get("output_tokens", 0),
    )


# ── Model factory ──────────────────────────────────────────────────────────────

def _get_model(provider: str, model_id: str, api_key: str) -> BaseChatModel:
    if provider == "openai":
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(model=model_id, api_key=api_key)
    elif provider == "anthropic":
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(model=model_id, api_key=api_key)
    elif provider == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI
        return ChatGoogleGenerativeAI(model=model_id, api_key=api_key)
    elif provider == "openrouter":
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model    = model_id,
            api_key  = api_key,
            base_url = "https://openrouter.ai/api/v1"
        )
    raise ValueError(f"Unsupported provider: {provider}")


# ── Content extractor ──────────────────────────────────────────────────────────

def _extract_content(result) -> str:
    if isinstance(result.content, str):
        return _strip_json_markdown(result.content)
    if isinstance(result.content, list):
        texts = []
        for block in result.content:
            if isinstance(block, dict) and block.get("type") == "text":
                texts.append(block["text"])
            elif hasattr(block, "type") and block.type == "text":
                texts.append(block.text)
            elif isinstance(block, str):
                texts.append(block)
        if texts:
            return _strip_json_markdown("".join(texts))
    raise ValueError(
        f"Unexpected content format from provider: "
        f"{type(result.content)} — {result.content}"
    )


def _strip_json_markdown(text: str) -> str:
    """
    Strip markdown code fences only when the content looks like JSON.
    Handles: ```json ... ``` and ``` ... ```
    Leaves plain text responses untouched.
    """
    stripped = text.strip()
    # remove ```json or ``` prefix
    if stripped.startswith("```"):
        lines  = stripped.splitlines()
        first  = lines[0].strip()
        # only strip if first line is ``` or ```json (or similar)
        if first in ("```", "```json", "```JSON") or first.startswith("```{"):
            # remove first and last line if last line is ```
            inner = lines[1:]
            if inner and inner[-1].strip() == "```":
                inner = inner[:-1]
            candidate = "\n".join(inner).strip()
            # only return stripped version if it looks like JSON
            if candidate.startswith("{") or candidate.startswith("["):
                return candidate
    return text
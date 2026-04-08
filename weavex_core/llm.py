from langchain_core.messages import HumanMessage, SystemMessage, AIMessage, BaseMessage
from langchain_core.language_models import BaseChatModel
from dataclasses import dataclass
import requests

@dataclass
class LLMResponse:
    content: str
    model: str
    input_tokens: int
    output_tokens: int


def _fetch_llm_config(context: dict, integration_id: str) -> dict:
    api_key = context.get("knit_api_key")
    env = str(context.get("knit_env", "production")).lower()
    region = str(context.get("region", "")).lower()

    if env == "sandbox":
        base_url = "https://api.sandbox.getknit.dev/v1.0"
    elif region == "eu":
        base_url = "https://api.eu.getknit.dev/v1.0"
    else:
        base_url = "https://api.getknit.dev/v1.0"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "X-Knit-Integration-Id": integration_id
    }

    response = requests.get(
        f"{base_url}/llm.config",
        headers=headers
    )

    data = response.json()
    if not data.get("success"):
        raise RuntimeError(f"Failed to fetch LLM config: {data.get('error')}")

    return data["data"]  # expects { model_id, api_key }


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
    raise ValueError(f"Unsupported provider: {provider}")

def complete_one_shot(
        context: dict,
        integration_id: str,
        system: str,
        user: str) -> LLMResponse:
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user}
    ]
    return complete(
        context,
        messages,
        integration_id
    )

def complete(
        context: dict,
        messages: list[dict],
        integration_id: str
) -> LLMResponse:
    if not context.get("knit_api_key"):
        raise ValueError("Missing 'knit_api_key' in context")
    if not integration_id:
        raise ValueError("Missing 'integration_id'")

    llm_config = _fetch_llm_config(context, integration_id)
    model_id = llm_config["model_id"]
    api_key = llm_config["api_key"]
    provider = llm_config["provider"]

    mapping = {"user": HumanMessage, "system": SystemMessage, "assistant": AIMessage}
    lc_messages = [mapping[m["role"]](content=m["content"]) for m in messages]

    result = _get_model(provider, model_id, api_key).invoke(lc_messages)
    usage = result.usage_metadata or {}

    return LLMResponse(
        content=_extract_content(result),
        model=model_id,
        input_tokens=usage.get("input_tokens", 0),
        output_tokens=usage.get("output_tokens", 0),
    )

def _extract_content(result) -> str:
    """Handles plain string and structured content blocks across all providers."""
    if isinstance(result.content, str):
        return result.content

    if isinstance(result.content, list):
        texts = []
        for block in result.content:
            # Dict style — Gemini, Anthropic
            if isinstance(block, dict) and block.get("type") == "text":
                texts.append(block["text"])
            # Object style — some LangChain versions return objects not dicts
            elif hasattr(block, "type") and block.type == "text":
                texts.append(block.text)
            # Plain string block
            elif isinstance(block, str):
                texts.append(block)

        if texts:
            return "".join(texts)

    raise ValueError(f"Unexpected content format from provider: {type(result.content)} — {result.content}")
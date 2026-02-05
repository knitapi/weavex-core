import requests

def get_knit_config(context):
    """Resolves URL and Headers based on context"""
    api_key = context.get("knit_api_key")
    env = str(context.get("knit_env", "production")).lower()
    region = str(context.get("region", "")).lower()

    # URL Mapping logic
    if env == "sandbox":
        base_url = "https://service-mediator.sandbox.getknit.dev"
    elif region == "eu":
        base_url = "https://service-mediator.eu.getknit.dev"
    else:
        base_url = "https://service-mediator.getknit.dev"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    return base_url, headers
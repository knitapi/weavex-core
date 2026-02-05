import requests
from .knit_utils import get_knit_config

def consume(context, event):
    base_url, headers = get_knit_config(context)
    url = f"{base_url}/queue.add"

    payload = {
        "context": context,
        "event": event
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.json()
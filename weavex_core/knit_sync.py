import requests
from .knit_utils import get_knit_config

def store_stats(context, processed, emitted):
    base_url, headers = get_knit_config(context)
    url = f"{base_url}/update.event.count"

    payload = {
        "context": context,
        "event": {
            "processed": processed,
            "emitted": emitted
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.json()
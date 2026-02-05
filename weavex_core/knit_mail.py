import requests
from .knit_utils import get_knit_config

def email(context, to, subject, html_content, attachment=None):
    base_url, headers = get_knit_config(context)
    url = f"{base_url}/send.email"

    payload = {
        "to": to,
        "subject": subject,
        "htmlContent": html_content
    }

    if attachment:
        payload["attachment"] = attachment

    response = requests.post(url, headers=headers, json=payload)
    return response.json()
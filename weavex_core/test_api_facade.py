import json
from weavex_core.api_execution_facade import ApiExecutionFacade
import os

os.environ["WEAVEX_CONNECT_SERVER_URL"] = "http://localhost:8090"

# 1. Configuration: Replace these with your actual credentials/metadata
KNIT_CONTEXT = {
    "knit_api_key": "55a40c91aee261488191a92a37d5904daef8b9bf369778dd5b2ff9f57a0a6d17",
    "execution_id": "manual_test_001",
    "knit_env": "production" # or "production"
}


def print_response(response):
    print(f"Actual Response: {getattr(response, 'actual_resp', None)}")
    print(f"Status Code: {response.status_code}")
    if getattr(response, "headers", None) is not None:
        print("Headers:", json.dumps(response.headers, indent=2))
    print("Body Content:")
    if isinstance(response.body, dict):
        print(json.dumps(response.body, indent=4))
    else:
        print(response.body)


def run_skill_call_read():
    print(f"--- Initiating SKILL Connector Read Call ({KNIT_CONTEXT['knit_env']}) ---")
    try:
        response = ApiExecutionFacade.execute(
            context=KNIT_CONTEXT,
            integration_id="wvx_sk_b19JUDZpOXJ2dk1OWGNDc3NHaGQ4NmdvOnN0cmlwZQ",
            method="GET",
            path="/v1/customers?limit=5"
        )
        print_response(response)
    except ValueError as ve:
        print(f"Validation Error: {ve}")
    except RuntimeError as re:
        print(f"Network/Proxy Error: {re}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def run_skill_call_write():
    print(f"--- Initiating SKILL Connector Write Call ({KNIT_CONTEXT['knit_env']}) ---")
    try:
        response = ApiExecutionFacade.execute(
            context=KNIT_CONTEXT,
            integration_id="wvx_sk_b19JUDZpOXJ2dk1OWGNDc3NHaGQ4NmdvOnN0cmlwZQ",   # your Stripe knit integration_id
            method="POST",
            path="/v1/customers",
            content_type="application/x-www-form-urlencoded",
            body={
                "email": "test.customer@example.com",
                "name": "Test Customer",
                "description": "Created via manual test script",
            },
        )
        print_response(response)
    except ValueError as ve:
        print(f"Validation Error: {ve}")
    except RuntimeError as re:
        print(f"Network/Proxy Error: {re}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def run_knit_call():
    print(f"--- Initiating KNIT Connector Call ({KNIT_CONTEXT['knit_env']}) ---")
    try:
        response = ApiExecutionFacade.execute(
            context=KNIT_CONTEXT,
            integration_id="mg_cwdKThkGYlRVrOKj4HMUkX",   # knit integration_id
            method="GET",
            path="legalentities/404733/departments"
        )
        print_response(response)
    except ValueError as ve:
        print(f"Validation Error: {ve}")
    except RuntimeError as re:
        print(f"Network/Proxy Error: {re}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    run_knit_call()
    print()
    run_skill_call_read()
    print()
    run_skill_call_write()
import json
from weavex_core.api import make_passthrough_call

# 1. Configuration: Replace these with your actual Knit credentials
MY_CONTEXT = {
    "knit_api_key": "4450b29641b0702f8d65390fdf21eb0554563cc1bcbc514611c271880f951d9f",
    "execution_id": "manual_test_001",
    "knit_env": "production" # or "production"
}

def run_test_call():
    print(f"--- Initiating Passthrough Call to {MY_CONTEXT['knit_env']} ---")

    try:
        # 2. Execute the call
        # Example: Calling a hypothetical 'GET /users' on the vendor's API
        response = make_passthrough_call(
            context=MY_CONTEXT,
            integration_id="mg_BMsnPnZR8EIVkls7YBjs1E",
            method="POST",
            path="/v3/objects/contacts/search",
            params={},
            body={
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "email",
                        "operator": "IN",
                        "values": ["kunal@getknit.dev"]
                    }]
                }],
                "properties": ["firstname", "lastname", "mobilephone", "email"],
                "limit": 100
            }
        )

        response = make_passthrough_call(
            context=MY_CONTEXT,
            integration_id="mg_gMe9WA5DbWROfHMonG2zMs",
            method="POST",
            path="/reports/custom?format=JSON&onlyCurrent=true",
            params={},
            body={
                "title": "This is my report",
                "fields": [
                    "firstName",
                    "lastName",
                    "workEmail",
                    "mobilePhone"
                ]
            }
        )

        # response = make_passthrough_call(
        #     context=MY_CONTEXT,
        #     integration_id="mg_gMe9WA5DbWROfHMonG2zMs",
        #     method="POST",
        #     path="datasets/employee",
        #     params={},
        #     body={
        #         "fields": ["firstName", "lastName", "mobilePhone", "workEmail"]
        #     }
        # )

        # 3. Output the results
        print(f"Actual Response: {response.actual_resp}")
        print(f"Status Code: {response.status_code}")
        print("Headers:", json.dumps(response.headers, indent=2))
        print("Body Content:")

        if isinstance(response.body, dict):
            print(json.dumps(response.body, indent=4))
        else:
            print(response.body)

    except ValueError as ve:
        print(f"Validation Error: {ve}")
    except RuntimeError as re:
        print(f"Network/Proxy Error: {re}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    run_test_call()
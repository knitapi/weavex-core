import json
from weavex_core.llm import complete

# 1. Configuration
MY_CONTEXT = {
    "knit_api_key": "0e725a74db1050550bc2f287ec3c8f1e51a1a82fd673d387a7592707029b6606",
    "execution_id": "manual_test_001",
    "integration_id": "mg_qeL5xl6gYIonhtUMIXGzNx",
    "knit_env": "sandbox",
    "region": "",
}

def run_test_call():
    print(f"--- Initiating LLM Call ({MY_CONTEXT['knit_env']}) ---")

    try:
        response = complete(
            context=MY_CONTEXT,
            messages=[
                {
                    "role": "system",
                    "content": "You are a data mapper. Return only valid JSON, no explanation."
                },
                {
                    "role": "user",
                    "content": json.dumps({
                        "source_record": {
                            "id": "emp_1",
                            "first_name": "Kunal",
                            "last_name": "Shah",
                            "work_email": "kunal@getknit.dev"
                        },
                        "target_schema": {
                            "employeeId": "string",
                            "fullName": "string",
                            "emailAddress": "string"
                        },
                        "instruction": "Map the source record to the target schema."
                    }, indent=2)
                }
            ],
            integration_id=MY_CONTEXT["integration_id"]
        )

        print(f"\nModel: {response.model}")
        print(f"Input Tokens: {response.input_tokens}")
        print(f"Output Tokens: {response.output_tokens}")
        print(f"\nMapped Output:")

        # Try to pretty print if JSON, else raw
        try:
            print(json.dumps(json.loads(response.content), indent=4))
        except Exception:
            print(response.content)

    except ValueError as ve:
        print(f"Validation Error: {ve}")
    except RuntimeError as re:
        print(f"LLM/Config Error: {re}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    run_test_call()
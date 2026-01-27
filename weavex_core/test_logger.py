import os
import time
import json
from weavex_core.logging_utils.sdk import WeavexServicesLogger

def run_test():
    # Setup Infrastructure Identity
    os.environ["GCP_PROJECT_ID"] = "weavex-475116"
    os.environ["WEAVEX_LOG_TOPIC"] = "weavex-logs"

    # Setup App Identity (Clustering keys)
    os.environ["WEAVEX_PROJECT_ID"] = "project-123"
    os.environ["WEAVEX_ACCOUNT_ID"] = "acc-test-456"

    print(f"🚀 Initializing Weavex Logger for Project: {os.environ['WEAVEX_PROJECT_ID']}")

    logger = WeavexServicesLogger(
        project_id=os.environ["WEAVEX_PROJECT_ID"],
        logger_type="PUB_SUB"
    )

    try:
        # --- TEST CASE 1: API TABLE ---
        print("\n📡 Sending API traffic log...")
        logger.log_api_traffic(
            log_type="VENDOR_CALL",
            method="POST",
            url="https://api.test.com/v1/sync",
            status_code=201,
            duration_ms=450,
            req_payload={"user_id": 123},
            resp_payload={"status": "created"},
            vendor_name="TestVendor"
        )

        # --- TEST CASE 2: SYNC TABLE ---
        print("📡 Sending SYNC event log...")
        logger.log_sync_event(
            sync_id="sync_abc_123",
            log_type="RECORD_STATUS",
            duration_ms=1200,
            record_id="rec_999",
            status="SUCCESS",
            metadata={"processed_items": 50},
            context={"worker_id": "worker-01"}
        )

        # --- TEST CASE 3: BILLING TABLE ---
        print("📡 Sending BILLABLE event log...")
        logger.log_billable_event(
            source="API_TRIGGER",
            resource_id="res_user_premium",
            quantity=1,
            duration_ms=50,
            metadata={"plan": "enterprise"}
        )

        # --- TEST CASE 4: UNKNOWN TABLE (CATCH-ALL) ---
        print("📡 Sending UNKNOWN log (Testing raw_payload wrapper)...")
        # Direct call to internal log for unknown routing testing
        unknown_payload = {
            "log_table": "UNKNOWN",
            "service_name": "legacy-service",
            "message": "This is an unmapped log",
            "error_code": "ERR_001"
        }
        # In a real SDK, you'd add a log_generic method, but we test the transport logic here
        logger.logger.log(unknown_payload, blocking=True)

        print("\n📡 Testing SERVICE Table Routing (info/error/warning)...")

        # Test INFO level
        logger.info(
            "Workflow started successfully",
            sync_id="sync_abc_123",
            context={"step": "initialization"},
            version="1.0.2" # This goes into 'details' JSON column
        )

        # Test ERROR level
        logger.error(
            "Failed to connect to vendor socket",
            sync_id="sync_abc_123",
            context={"retry_count": 3},
            error_code="ECONNREFUSED"
        )

        print("\n✅ All logs sent successfully. Check BigQuery tables.")



    except Exception as e:
        print(f"❌ Test Error: {e}")
    finally:
        logger.shutdown()

if __name__ == "__main__":
    run_test()
import os
from weavex_core.storage import get_object_store

os.environ["BUCKET_NAME"] = "weavex-flow-storage"
os.environ["WEAVEX_SERVICE_REGION"] = "eu"
os.environ["OBJECT_STORAGE_TYPE"] = "gcs"


def run_test():
    print("--- GCSObjectStore Report Methods Integration Test ---")

    store = get_object_store()
    context = {"sync_job_id": "job_001"}

    # 1. Upload CSV report
    print("\n[1] Uploading CSV report...")
    try:
        csv_content = b"col1,col2\nval1,val2"
        csv_uri = store.upload_report("proj_test", "run_001", context, csv_content, "csv")
        assert csv_uri.startswith("gs://"), f"Expected gs:// URI, got: {csv_uri}"
        print(f"    URI: {csv_uri}")
    except Exception as e:
        print(f"    ERROR: {e}")
        return

    # 2. Upload TXT report
    print("\n[2] Uploading TXT report...")
    try:
        txt_uri = store.upload_report("proj_test", "run_001", context, b"line1\nline2", "txt")
        assert txt_uri.startswith("gs://")
        print(f"    URI: {txt_uri}")
    except Exception as e:
        print(f"    ERROR: {e}")
        return

    # 3. Upload XLSX report
    print("\n[3] Uploading XLSX report...")
    try:
        xlsx_uri = store.upload_report("proj_test", "run_001", context, b"fake_xlsx_bytes", "xlsx")
        assert xlsx_uri.startswith("gs://")
        print(f"    URI: {xlsx_uri}")
    except Exception as e:
        print(f"    ERROR: {e}")
        return

    # 4. Download CSV report
    print("\n[4] Downloading CSV report...")
    try:
        downloaded = store.download_report("proj_test", "run_001", context, csv_uri)
        assert downloaded == csv_content, f"Content mismatch: {downloaded!r} != {csv_content!r}"
        print("    Download content matches original ✓")
    except Exception as e:
        print(f"    ERROR: {e}")
        return

    # 5. Get presigned URL (default expiration)
    print("\n[5] Getting presigned URL (default 3600s)...")
    try:
        url = store.get_report_presigned_url("proj_test", "run_001", context, csv_uri)
        assert url.startswith("https://"), f"Expected https:// URL, got: {url}"
        print(f"    URL: {url}")
    except Exception as e:
        print(f"    ERROR: {e}")
        return

    # 6. Get presigned URL with custom expiration
    print("\n[6] Getting presigned URL (custom 7200s)...")
    try:
        url_custom = store.get_report_presigned_url("proj_test", "run_001", context, csv_uri, expiration_seconds=7200)
        assert url_custom.startswith("https://")
        print(f"    URL: {url_custom}")
    except Exception as e:
        print(f"    ERROR: {e}")
        return

    # 7. Security mismatch — download with wrong project
    print("\n[7] Security check: download with wrong project_id...")
    try:
        store.download_report("wrong_proj", "run_001", context, csv_uri)
        print("    FAIL: Expected PermissionError but none was raised")
    except PermissionError:
        print("    Security check passed ✓")
    except Exception as e:
        print(f"    FAIL: Unexpected exception type: {type(e).__name__}: {e}")

    # 8. Security mismatch — presigned URL with wrong job
    print("\n[8] Security check: presigned URL with wrong sync_job_id...")
    try:
        store.get_report_presigned_url("proj_test", "run_001", {"sync_job_id": "wrong_job"}, csv_uri)
        print("    FAIL: Expected PermissionError but none was raised")
    except PermissionError:
        print("    Security check passed ✓")
    except Exception as e:
        print(f"    FAIL: Unexpected exception type: {type(e).__name__}: {e}")

    # 9. Invalid extension
    print("\n[9] Extension validation: unsupported extension 'pdf'...")
    try:
        store.upload_report("proj_test", "run_001", context, b"data", "pdf")
        print("    FAIL: Expected ValueError but none was raised")
    except ValueError:
        print("    Extension validation passed ✓")
    except Exception as e:
        print(f"    FAIL: Unexpected exception type: {type(e).__name__}: {e}")

    # 10. Invalid URI
    print("\n[10] URI validation: non-gs:// URI for download...")
    try:
        store.download_report("proj_test", "run_001", context, "not-a-gs-uri")
        print("    FAIL: Expected ValueError but none was raised")
    except ValueError:
        print("    URI validation passed ✓")
    except Exception as e:
        print(f"    FAIL: Unexpected exception type: {type(e).__name__}: {e}")

    print("\n--- All tests completed ---")


if __name__ == "__main__":
    run_test()

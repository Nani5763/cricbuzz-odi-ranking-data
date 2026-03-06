import datetime
import json
import functions_framework
from googleapiclient.discovery import build
import google.auth

PROJECT_ID = "cricbuzz-ranking-489310"
REGION = "us-east1"
TEMPLATE_GCS_PATH = "gs://dataflow-templates-us-east1/latest/GCS_Text_to_BigQuery"

DF_PARAMS = {
    "javascriptTextTransformGcsPath": "gs://cricbuzz_ranking_testing/test_batsmen_rankings_testing/udf.js",
    "JSONPath": "gs://cricbuzz_ranking_testing/test_batsmen_rankings_testing/bq.json",
    "javascriptTextTransformFunctionName": "transform",
    "outputTable": "cricbuzz-ranking-489310.Test_batsmen_rankings_testing.test_batsmen_rankings",
    "inputFilePattern": "gs://cricbuzz_ranking_testing/test_batsmen_rankings_testing/test_batsmen_rankings_testing.csv",
    "bigQueryLoadingTemporaryDirectory": "gs://cricbuzz_ranking_testing/test_batsmen_rankings_testing/temp",
}

def _df_client():
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return build("dataflow", "v1b3", credentials=creds, cache_discovery=False)


# ✅ Check if any Dataflow job is already running
def is_dataflow_job_running():
    svc = _df_client()

    request = svc.projects().locations().jobs().list(
        projectId=PROJECT_ID,
        location=REGION,
        filter="ACTIVE"
    )

    response = request.execute()

    if "jobs" in response:
        for job in response["jobs"]:
            if job["name"].startswith("jobtest4"):
                print("Dataflow job already running:", job["name"])
                return True

    return False


def trigger_df_job(cloud_event):
    # ✅ Prevent duplicate jobs
    if is_dataflow_job_running():
        print("Skipping job creation. Dataflow job already running.")
        return "job_already_running"

    svc = _df_client()

    job_name = f"jobtest4-{datetime.datetime.utcnow():%Y%m%d-%H%M%S}"

    print("REGION:", REGION)
    print("Using template:", TEMPLATE_GCS_PATH)

    body = {
        "jobName": job_name,
        "parameters": DF_PARAMS,
        "environment": {
            "tempLocation": "gs://cricbuzz_ranking_testing/temp",
            "zone": "us-east1-c"
        }
    }

    request = svc.projects().locations().templates().launch(
        projectId=PROJECT_ID,
        location=REGION,
        gcsPath=TEMPLATE_GCS_PATH,
        body=body
    )

    response = request.execute()

    print("Launched Dataflow job:")
    print(json.dumps(response, indent=2))

    return job_name


@functions_framework.cloud_event
def hello_auditlog(cloudevent):
    print(f"Event type: {cloudevent['type']}")

    if "subject" in cloudevent:
        print(f"Subject: {cloudevent['subject']}")

    payload = cloudevent.data.get("protoPayload")

    if payload:
        print(f"API method: {payload.get('methodName')}")
        print(f"Resource name: {payload.get('resourceName')}")
        print(f"Principal: {payload.get('authenticationInfo', {}).get('principalEmail')}")

    job = trigger_df_job(cloudevent)

    return {"status": "OK", "job": job}
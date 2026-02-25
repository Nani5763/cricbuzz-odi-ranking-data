import datetime, json
import functions_framework
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google.auth

PROJECT_ID = "gcp-batch-5-project-1"
REGION = "us-east1"
TEMPLATE_GCS_PATH = "gs://dataflow-templates-us-east1/latest/GCS_Text_to_BigQuery"

DF_PARAMS = {
    "javascriptTextTransformGcsPath": "gs://bkt-rank-metadata-crk_new/udf.js",
    "JSONPath": "gs://bkt-rank-metadata-crk_new/bq.json",
    "javascriptTextTransformFunctionName": "transform",
    "outputTable": "gcp-batch-5-project-1.stats_icc_rankings_dataset.test_batting_ranking",
    "inputFilePattern": "gs://bkt-rank-data-test/test_batsmen_rankings.csv",
    "bigQueryLoadingTemporaryDirectory": "gs://bkt-rank-temp-crk_new/temp/",
}

def _df_client():
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return build("dataflow", "v1b3", credentials=creds, cache_discovery=False)

def trigger_df_job(cloud_event, environment=None):
    svc = _df_client()
    job_name = f"test_stats_icc_rankings-{datetime.datetime.utcnow():%Y%m%d-%H%M%S}"

    body = {
        "jobName": job_name,
        "parameters": DF_PARAMS,
        "environment": environment or {},
    }

    # ✅ Call GLOBAL endpoint and pass gcsPath param
    req = svc.projects().templates().launch(
        projectId=PROJECT_ID,
        gcsPath=TEMPLATE_GCS_PATH,   # query param
        body=body
    )
    resp = req.execute()
    print("Launched:", json.dumps(resp, indent=2))
    return job_name

@functions_framework.cloud_event
def hello_auditlog(cloudevent):
    print(f"Event type: {cloudevent['type']}")
    if 'subject' in cloudevent:
        print(f"Subject: {cloudevent['subject']}")
    payload = cloudevent.data.get("protoPayload")
    if payload:
        print(f"API method: {payload.get('methodName')}")
        print(f"Resource name: {payload.get('resourceName')}")
        print(f"Principal: {payload.get('authenticationInfo', {}).get('principalEmail')}")
    job = trigger_df_job(cloudevent)
    return {"status": "OK", "job": job}
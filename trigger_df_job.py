from googleapiclient.discovery import build
import base64
import google.auth
import os

def hello_pubsub():   
 
    service = build('dataflow', 'v1b3')
    project = "my-project-31673-471207"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
         "jobName": "test20",  # Provide a unique name for the job
        "parameters": {
        "javascriptTextTransformGcsPath": "gs://bkt-dataflow-metadata-crk/udf.js",
        "JSONPath": "gs://bkt-dataflow-metadata-crk/bq.json",
        "javascriptTextTransformFunctionName": "transformation",
        "outputTable": "my-project-31673-471207:cricket_dataset.icc_odi_batsmen",
        "inputFilePattern": "gs://bkt-rank-data-crk/batsmen_rankings.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://bkt-rank-data-crk-temp/temp/",
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)

hello_pubsub()
# cricbuzz-odi-ranking-data

### 1️⃣ Using requests + pandas
#### First install:

    pip install pandas requests
#### Code:
    import requests
    import pandas as pd

    API_KEY = "YOUR_API_KEY"

    url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"

    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
    }

    params = {"formatType": "odi"}

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json().get("rank", [])

        if data:
            # Convert JSON list to DataFrame
            df = pd.DataFrame(data)

            # Select only required columns
            df = df[["rank", "name", "country"]]

            # Save to CSV
            df.to_csv("odi_batsmen_rankings_pandas.csv", index=False)

            print("✅ Data saved using pandas")
            print(df.head())
        else:
            print("No data found.")
    else:
        print("API Failed:", response.status_code)


### 2️⃣ PySpark Version 
#### Install Spark first if needed:
    pip install pyspark

#### PySpark Code:

    import requests
    from pyspark.sql import SparkSession

    # Create Spark session
    spark = SparkSession.builder \
        .appName("Cricbuzz ODI Rankings") \
        .getOrCreate()

    API_KEY = "YOUR_API_KEY"

    url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"

    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
    }

    params = {"formatType": "odi"}

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json().get("rank", [])

        if data:
            # Convert JSON to Spark DataFrame
            df = spark.createDataFrame(data)

            # Select required columns
            df_selected = df.select("rank", "name", "country")

            # Show data
            df_selected.show()

            # Save as CSV
            df_selected.write \
                .mode("overwrite") \
                .option("header", True) \
                .csv("odi_batsmen_rankings_spark")

            print("✅ Data saved using PySpark")
        else:
            print("No data available")
    else:
        print("API Failed:", response.status_code)

    spark.stop()


#### Cloud Function Single trigger multiple jobs running below code

    import datetime, json
    import functions_framework
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
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
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        return build("dataflow", "v1b3", credentials=creds, cache_discovery=False)

    def trigger_df_job(cloud_event, environment=None):
        svc = _df_client()
        job_name = f"jobtest4-{datetime.datetime.utcnow():%Y%m%d-%H%M%S}"

        # 👇 ADD DEBUG PRINTS HERE
        print("REGION VALUE:", REGION)
        print("Using template:", TEMPLATE_GCS_PATH)

        body = {
            "jobName": job_name,
            "parameters": DF_PARAMS,
            "environment": {
                "tempLocation": "gs://cricbuzz_ranking_testing/temp",   # REQUIRED
                "zone": "us-east1-c"   # optional but recommended
            }
        }

        # ✅ Call GLOBAL endpoint and pass gcsPath param
        req = svc.projects().locations().templates().launch(
            projectId=PROJECT_ID,
            location = REGION,
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
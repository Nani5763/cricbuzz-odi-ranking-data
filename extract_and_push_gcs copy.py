import os
import requests
import csv
from google.cloud import storage

# ✅ Add your service account JSON path here
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"D:\Chaitu\gcp\Project\cricbuzz-odi-ranking-data\service_account.json"

# API URL
url = 'https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen'

headers = {
	"x-rapidapi-key": "e55e8060c0mshbffbe71cb381b06p1ad3d5jsn3f02ea2beeaa",
	"x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

# ✅ Change ODI → TEST → T20 here
params = {
    'formatType': 'test'   # ← changed
}

# ✅ CSV name updates automatically
csv_filename = f"batsmen_rankings_{params['formatType']}.csv"

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json().get('rank', [])

    if data:
        field_names = ['rank', 'name', 'country']  # Specify required field names
        # Write data to CSV file with only specified field names
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            # writer.writeheader()
            for entry in data:
                writer.writerow({field: entry.get(field, '') for field in field_names})

        print(f"✅ Data fetched successfully and written to '{csv_filename}'")

        bucket_name = 'bkt-rank-data-crk_new'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # ✅ Upload into /test/ folder in GCS
        destination_blob_name = f"{params['formatType']}/{csv_filename}"

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(csv_filename)

        print(f"✅ Uploaded {csv_filename} → gs://{bucket_name}/{destination_blob_name}")
    else:
        print("No data available from the API.")
else:
    print("Failed to fetch data:", response.status_code)

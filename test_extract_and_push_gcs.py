import requests
import csv
from google.cloud import storage

url = 'https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen'

# batsmen|bowlers|allrounders|teams

headers = {
    'x-rapidapi-key': "e55e8060c0mshbffbe71cb381b06p1ad3d5jsn3f02ea2beeaa",
    'x-rapidapi-host': "cricbuzz-cricket.p.rapidapi.com"
}

params = {
    'formatType': 'test'
}

#test|odi|t20 (if isWomen parameter is 1, there will be no odi)


response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json().get('rank', [])  # Extracting the 'rank' data
    csv_filename = 'test_batsmen_rankings.csv'

    if data:
        field_names = ['rank', 'name', 'country']  # Specify required field names

        # Write data to CSV file with only specified field names
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            # writer.writeheader()
            for entry in data:
                writer.writerow({field: entry.get(field) for field in field_names})

        print(f"Data fetched successfully and written to '{csv_filename}'")

        # Upload the CSV file to GCS
        bucket_name = 'bkt-rank-data-test'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'  # The path to store in GCS

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(csv_filename)

        print(f"File {csv_filename} uploaded to GCS bucket {bucket_name} as {destination_blob_name}")
    else:
        print("No data available from the API.")
else:
    print("Failed to fetch data:", response.status_code)

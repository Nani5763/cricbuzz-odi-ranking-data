import json
import requests
import csv

API_KEY = "8da8cad2eamsh37e6625476c152fp1b9d3ejsn9f402157aed1"

with open("config.json", "r") as file:
    configs = json.load(file)

for item in configs:

    category = item["category"]
    format_type = item["formatType"]

    print(f"Processing {category} - {format_type}")

    url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/{category}"

    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
    }

    params = {"formatType": format_type}

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:

        json_data = response.json()

        # 🔥 Handle different response structures
        if "rank" in json_data:
            data = json_data["rank"]
        elif "teams" in json_data:
            data = json_data["teams"]
        else:
            print(f"⚠ No ranking data found for {category}-{format_type}")
            data = []

        # ✅ Create file even if empty
        filename = f"{format_type}_{category}_rankings.csv"

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["rank", "name", "country", "points", "lastUpdatedOn", "id"]
            )
            writer.writeheader()

            for entry in data:
                writer.writerow({
                    "rank": entry.get("rank"),
                    "name": entry.get("name"),
                    "country": entry.get("country"),
                    "points": entry.get("points"),
                    "lastUpdatedOn": entry.get("lastUpdatedOn"),
                    "id": entry.get("id")
                })

        print(f"✅ Created {filename} | Records: {len(data)}")

    else:
        print(f"❌ API Failed: {category}-{format_type}")
        print("Status Code:", response.status_code)
        print("Error:", response.text)
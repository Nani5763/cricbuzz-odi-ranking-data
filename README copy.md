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
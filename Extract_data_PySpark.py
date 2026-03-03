import requests
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Cricbuzz ODI Rankings") \
    .getOrCreate()

API_KEY = "8da8cad2eamsh37e6625476c152fp1b9d3ejsn9f402157aed1"

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
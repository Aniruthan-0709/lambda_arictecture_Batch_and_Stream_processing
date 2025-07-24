import requests
import pandas as pd
import json
from pyspark.sql.functions import to_date
from pyspark.sql import Row

# === CONFIG ===
URL_MAP = {
    "2017": "https://data.cityofnewyork.us/resource/biws-g3hs.json",
    "2018": "https://data.cityofnewyork.us/resource/t29m-gskq.json"
}

BRONZE_PATH = "/mnt/lambda/Bronze/bronze"
METADATA_PATH = "/dbfs/mnt/lambda/Bronze/_metadata/year.json"  # local-accessible path

# === Load current year from metadata ===
try:
    with open(METADATA_PATH, "r") as f:
        metadata = json.load(f)
        current_year = metadata.get("year", "2017")
except FileNotFoundError:
    current_year = "2017"

print(f"Starting ingestion for year: {current_year}")

# === Validate year + URL ===
base_url = URL_MAP.get(current_year)
if not base_url:
    raise Exception(f"No API endpoint found for year: {current_year}")

# === Fetch API Data ===
def fetch_year_data(base_url, rows_per_page=1000, max_pages=500):
    offset = 0
    all_data = []
    for _ in range(max_pages):
        params = {
            "$limit": rows_per_page,
            "$offset": offset
        }
        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            print(f"Failed to fetch at offset {offset}")
            break
        data = response.json()
        if not data:
            break
        all_data.extend(data)
        offset += rows_per_page
    return pd.DataFrame(all_data)

df_pd = fetch_year_data(base_url)

if df_pd.empty:
    print(f"No data returned for year {current_year}.")
else:
    # Convert to Spark DataFrame
    df_spark = spark.createDataFrame(df_pd)
    df_spark = df_spark.withColumn("pickup_date", to_date("pickup_datetime"))

    # Write to Delta (Bronze layer)
    df_spark.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("pickup_date") \
        .save(BRONZE_PATH)

    print(f"Ingested {df_spark.count()} records for year {current_year}")

    # Update year in metadata
    next_year = str(int(current_year) + 1)
    if next_year in URL_MAP:
        with open(METADATA_PATH, "w") as f:
            json.dump({"year": next_year}, f)
        print(f"Updated metadata to next year: {next_year}")
    else:
        print("All years ingested. No further years to process.")

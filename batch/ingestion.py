import requests
import pandas as pd
from pyspark.sql.functions import to_date
from time import sleep

# --- CONFIG ---
BASE_URL = "https://data.cityofnewyork.us/resource/biws-g3hs.json"  # 2017 dataset
ROWS_PER_PAGE = 1000
MAX_PAGES = 500  # ~500,000 rows max (adjust as needed)
STORAGE_PATH = "/mnt/nyctaxi/bronze/trips_2017"

# --- FETCH FUNCTION ---
def fetch_api_data(base_url, rows_per_page=1000, max_pages=100):
    offset = 0
    all_data = []
    for _ in range(max_pages):
        params = {
            "$limit": rows_per_page,
            "$offset": offset
        }
        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            print(f"Failed at offset {offset}, status: {response.status_code}")
            break

        batch = response.json()
        if not batch:
            break

        all_data.extend(batch)
        offset += rows_per_page
        sleep(0.5)  # Be polite to the API
    return pd.DataFrame(all_data)

# --- 1. Fetch from API as Pandas ---
df_pd = fetch_api_data(BASE_URL, ROWS_PER_PAGE, MAX_PAGES)

if df_pd.empty:
    raise Exception("No data retrieved from API.")

# --- 2. Convert to Spark DataFrame ---
df_spark = spark.createDataFrame(df_pd)

# --- 3. Add partition column ---
df_spark = df_spark.withColumn("pickup_date", to_date("pickup_datetime"))

# --- 4. Write to Bronze Delta table ---
df_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("pickup_date") \
    .save(STORAGE_PATH)

print(f"✅ Ingested {df_spark.count()} records from API and saved to {STORAGE_PATH}")

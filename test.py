import sys
import requests
import zipfile
import io
import json
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


import requests
import json

# Fetching the JSON data from the URL
url = "https://api.fda.gov/download.json"
response = requests.get(url)
data = response.json()

# Extract the NDC URL
ndc_url = data['results']['drug']['ndc']['partitions'][0]['file']

# Continue processing as needed
print(ndc_url)

# Initialize Spark Context and Spark Session
sc = SparkContext()
spark = SparkSession(sc)

# URL to download the NDC directory ZIP file
url = "https://download.open.fda.gov/drug/ndc/drug-ndc-0001-of-0001.json.zip"

# Function to download and extract ZIP file
def download_and_extract_zip(url):
    try:
        # Download the ZIP file
        response = requests.get(url)
        response.raise_for_status()

        # Extract the ZIP file contents
        with zipfile.ZipFile(io.BytesIO(response.content)) as the_zip:
            # Extract all files in the ZIP
            for file_name in the_zip.namelist():
                with the_zip.open(file_name) as file:
                    # Read the JSON content
                    json_data = json.load(file)
                    return json_data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"Other error occurred: {err}")

# Download and extract the NDC data
ndc_data = download_and_extract_zip(url)

# Convert to Spark DataFrame
if ndc_data:
    # Assuming the JSON data is a list of dictionaries
    spark_df = spark.read.json(sc.parallelize([json.dumps(ndc_data)]))

    # Show schema and data
    spark_df.printSchema()
    spark_df.show()

# Stop Spark Context
sc.stop()

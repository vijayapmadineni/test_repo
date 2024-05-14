import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit, unix_timestamp
from pyspark.sql.types import *
import boto3
import psycopg2
from datetime import datetime, timedelta
import pytz
import logging
import re
import requests
import zipfile
import io
import json

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job_name=args['JOB_NAME']

# Fetching the JSON data from the URL
src_url = "https://api.fda.gov/download.json"
response = requests.get(src_url)
data = response.json()

# Extract the NDC URL
ndc_url = data['results']['drug']['ndc']['partitions'][0]['file']

# Continue processing as needed
print(ndc_url)

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
                print(f'file_name={file_name}')
                with the_zip.open(file_name) as file:
                    # Read the JSON content
                    json_data = json.load(file)
                    return json_data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"Other error occurred: {err}")

# Download and extract the NDC data
ndc_data = download_and_extract_zip(ndc_url)

# Convert to Spark DataFrame
if ndc_data:
    # # Assuming the JSON data is a list of dictionaries
    spark_df = spark.read.json(sc.parallelize([json.dumps(ndc_data)]))

    # # Show schema and data
    # spark_df.printSchema()
    # spark_df.show()
    print('ndc data was read')

# Stop Spark Context
job.commit()

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read JSON data from the URL
json_df = spark.read.json("https://open.fda.gov/apis/drug/ndc/download/")

# Flatten the JSON
json_df_flattened = json_df.select(
    col("results").getItem(0).getItem("product_ndc").alias("product_ndc"),
    col("results").getItem(0).getItem("product_type").alias("product_type"),
    # add more columns as needed
)

# Load the flattened data into PostgreSQL
db_url = "jdbc:postgresql://your-postgres-host:5432/your-database"
table_name = "your_table_name"
mode = "overwrite"  # Choose overwrite or append based on your requirement

json_df_flattened.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", table_name) \
    .option("user", "your_username") \
    .option("password", "your_password") \
    .mode(mode) \
    .save()


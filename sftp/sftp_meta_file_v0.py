import os
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from datetime import datetime
import pytz
import logging
import csv
import paramiko
import psycopg2
import uuid

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3bucket', 's3_folder', 'aspen_sftp_path', 'aspen_sftp_host', 'cloud_front_url'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Function to get Glue connections and credentials
def get_glue_connection(connectionName):
    # Your existing function remains unchanged

def process_meta_data_files(sftp_folder_path):
    pdf_files = {}
    meta_files = sftp.listdir(sftp_folder_path)
    for file in meta_files:
        if file.startswith('meta_data_') and file.endswith('.csv'):
            with sftp.open(os.path.join(sftp_folder_path, file), 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    pdf_files[row['file_name'].strip()] = row['member_id'].strip()
    return pdf_files

def upload_files_to_s3(pdf_files):
    processed_files = []
    failed_files = []
    for file, memid in pdf_files.items():
        local_path = os.path.join("/tmp", file)
        try:
            sftp.get(os.path.join(sftp_folder_path, file), local_path)
            s3_path = os.path.join(args['s3_folder'], file)
            s3.upload_file(local_path, args['s3bucket'], s3_path)
            processed_files.append(file)
            # Log the file moved to S3 with CloudFront URL
            s3_link = f"https://{args['cloud_front_url']}.cloudfront.net/{args['s3_folder']}/{file}"
            logger.info(f"File moved to S3: {s3_link}")
            # Generate unique identifiers
            filedetail_id = str(uuid.uuid4())
            today_dtm = datetime.now(ny_tz).strftime('%Y-%m-%d %H:%M:%S')
            # Insert data into PostgreSQL
            pg_cursor.execute(f"INSERT INTO kwdm.ecm_fax_filedetails (ecm_fax_filedetails_id, member_id, file_path, file_type, fax_type, created_by, created_dt) VALUES (%s, %s, %s, %s, %s, %s, %s)", (filedetail_id, memid, s3_link, 'pdf', 'providerfax', job_name, today_dtm))
            pg_cursor.execute(f"INSERT INTO kwdm.journey_orchestration (message_eventid, hf_member_num_cd, message_type, channel, message_status, created_dt, actual_scheduled_dt, journey_name, sub_journey_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (filedetail_id, memid, 'PROVIDER_FAX', 'FAX', 'scheduled', today_dtm, today_dtm, 'mtmprovider', 'providerfax' ))
            pg_conn.commit()
            # Remove file from SFTP server
            sftp.remove(os.path.join(sftp_folder_path, file))
            logger.info(f"File removed from SFTP: {file}")
            os.remove(local_path)
        except Exception as e:
            logger.error(f"Failed to process file: {file}. Error: {str(e)}")
            failed_files.append(file)
            os.remove(local_path)  # Remove local file if processing failed

    if failed_files:
        raise Exception(f"Failed to process files: {', '.join(failed_files)}")

# Configure logging
job_name = args['JOB_NAME']
logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(job_name)
logger.setLevel(logging.INFO)

# Get SFTP and Glue credentials
# Your existing code remains unchanged

ny_tz = pytz.timezone('US/Eastern')

# Insert starting job entry into the AWS Glue job log table
# Your existing code remains unchanged

# Connect to SFTP
transport = paramiko.Transport((sftp_host, 22))
transport.connect(username=sftp_username, password=sftp_password)
sftp = paramiko.SFTPClient.from_transport(transport)

pdf_files = process_meta_data_files(sftp_folder_path)

try:
    upload_files_to_s3(pdf_files)
except Exception as e:
    logger.error(f"An error occurred: {str(e)}")
    # Rollback transaction in PostgreSQL if necessary
    pg_conn.rollback()
finally:
    # Close SFTP connection
    sftp.close()
    transport.close()

# Insert ending job entry into the AWS Glue job log table
# Your existing code remains unchanged

job.commit()

import os
import sys
import boto3
import psycopg2
import paramiko
import uuid
import shutil
import re
from datetime import datetime
import pytz
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, unix_timestamp, input_file_name, split, element_at
from pyspark.sql import DataFrame, Row

# Constants
REGION = 'us-east-1'
NY_TZ = pytz.timezone('US/Eastern')

def setup_logging(job_name):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)
    return logger

def get_glue_connection(connection_name):
    glue_client = boto3.client('glue', region_name=REGION)
    response = glue_client.get_connection(Name=connection_name, HidePassword=False)
    creds = {
        'username': response['Connection']['ConnectionProperties']['USERNAME'],
        'password': response['Connection']['ConnectionProperties']['PASSWORD'],
        'url': response['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
    }
    match = re.match(r'jdbc:(\w+)://([^:/]+):(\d+)/([^?]+)', creds['url'])
    creds.update({
        'connection_type': match.group(1),
        'host': match.group(2),
        'port': match.group(3),
        'db': match.group(4)
    })
    return creds

def execute_query(query, creds):
    with psycopg2.connect(database=creds['db'], user=creds['username'], password=creds['password'], host=creds['host'], port=creds['port']) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
            return cur.rowcount

def move_s3_files_to_archive(s3, bucket, prefix):
    arc_prefix = f'{prefix}archive/'
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' in response:
        for file in response['Contents']:
            file_key = file['Key']
            if file_key.endswith('/') or arc_prefix in file_key:
                continue
            filename = file_key.split('/')[-1]
            file_base, file_ext = filename.rsplit('.', 1)
            dtm = datetime.now(NY_TZ).strftime("%Y%m%d%H%M%S")
            new_filename = f"{file_base}_{dtm}.{file_ext}"
            arc_key = f"{arc_prefix}{new_filename}"
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': file_key}, Key=arc_key)
            s3.delete_object(Bucket=bucket, Key=file_key)

def upload_files(sftp, s3, sftp_path, s3_bucket, file_pattern, s3_folder):
    for file_name in sftp.listdir(sftp_path):
        if file_name.startswith(file_pattern) and file_name.endswith('.csv'):
            local_file = os.path.join('/tmp', file_name)
            sftp.get(os.path.join(sftp_path, file_name), local_file)
            s3_path = f"{s3_folder}/{file_name}"
            s3.upload_file(Filename=local_file, Bucket=s3_bucket, Key=s3_path)
            os.remove(local_file)

def upload_pdfs_to_s3(sftp, s3, sftp_path, s3_bucket, cloud_front_url, logger):
    try:
        files = sftp.listdir(sftp_path)
        for file_name in files:
            if file_name.endswith('.pdf'):
                local_file = os.path.join('/tmp', file_name)
                sftp.get(os.path.join(sftp_path, file_name), local_file)
                s3_path = os.path.join('public/pdfs/MTM_provider_fax/', file_name)
                s3.upload_file(local_file, s3_bucket, s3_path)
                s3_link = f"https://{cloud_front_url}.cloudfront.net/{s3_path}"
                logger.info(f"File {file_name} moved to S3: {s3_link}")
                os.remove(local_file)
        return "success"
    except Exception as e:
        logger.error(f"Failed to upload PDFs to S3: {str(e)}")
        return "failed"

def upload_error_file_to_sftp(sftp, local_path, remote_path, logger):
    try:
        sftp.put(local_path, remote_path)
        logger.info("Error file uploaded successfully to SFTP.")
    except Exception as e:
        logger.error(f"Failed to upload error file: {str(e)}")

def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'fax_s3bucket', 'kw_s3bucket', 'aspen_sftp_path',
        'aspen_sftp_host', 'cloud_front_url'
    ])
    logger = setup_logging(args['JOB_NAME'])
    glue_client = boto3.client('glue', region_name=REGION)
    s3 = boto3.client('s3', region_name=REGION)

    # Initialize Spark and Glue Contexts
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    cdm_creds = get_glue_connection("CJA_CONTACT_DATA_MART")
    sftp_creds = get_glue_connection('hf-sfmc-sftp')

    # Database operation example
    job_start_query = "INSERT INTO log_table (job_name, start_time) VALUES (%s, NOW())"
    execute_query(job_start_query, cdm_creds)

    # SFTP and S3 operations
    transport = paramiko.Transport((args['aspen_sftp_host'], 22))
    transport.connect(username=sftp_creds['username'], password=sftp_creds['password'])
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    move_s3_files_to_archive(s3, args['kw_s3bucket'], 'provider_fax_meta_files/')
    upload_files(sftp, s3, args['aspen_sftp_path'], args['kw_s3bucket'], 'meta_data_', 'provider_fax_meta_files')
    upload_pdfs_to_s3(sftp, s3, args['aspen_sftp_path'], args['fax_s3bucket'], args['cloud_front_url'], logger)

    # Cleanup
    sftp.close()
    transport.close()

    logger.info("Script completed successfully")

if __name__ == "__main__":
    main()



def upload_pdfs_to_s3(sftp, s3, sftp_path, s3_bucket, cloud_front_url, db_creds, logger, job_name):
    """
    Uploads PDF files from an SFTP server to an S3 bucket, logs the uploads in a PostgreSQL database,
    and uses a centralized function for executing SQL queries.
    """
    success_count = 0
    failure_count = 0
    
    files = sftp.listdir(sftp_path)
    for file_name in files:
        if file_name.endswith('.pdf'):
            local_file = os.path.join('/tmp', file_name)
            try:
                sftp.get(os.path.join(sftp_path, file_name), local_file)
                s3_path = os.path.join('public/pdfs/MTM_provider_fax/', file_name)
                s3.upload_file(local_file, s3_bucket, s3_path)
                s3_link = f"https://{cloud_front_url}.cloudfront.net/{s3_path}"
                
                # Generate unique identifiers
                filedetail_id = str(uuid.uuid4())
                today_dtm = datetime.now(NY_TZ).strftime('%Y-%m-%d %H:%M:%S')
                
                # Insert data into PostgreSQL
                file_details_query = """
                INSERT INTO kwdm.ecm_fax_filedetails (ecm_fax_filedetails_id, member_id, file_path, file_type, fax_type, created_by, created_dt)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                journey_orch_query = """
                INSERT INTO kwdm.journey_orchestration (message_eventid, hf_member_num_cd, message_type, channel, message_status, created_dt, actual_scheduled_dt, journey_name, sub_journey_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                execute_query(file_details_query, db_creds, (filedetail_id, 'some_member_id', s3_link, 'pdf', 'providerfax', job_name, today_dtm))
                execute_query(journey_orch_query, db_creds, (filedetail_id, 'some_member_id', 'PROVIDER_FAX', 'FAX', 'scheduled', today_dtm, today_dtm, 'mtmprovider', 'providerfax'))
                
                logger.info(f"File {file_name} moved to S3: {s3_link}")
                os.remove(local_file)
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to process file: {file_name}. Error: {str(e)}")
                failure_count += 1

    return success_count, failure_count

def execute_query(query, creds, params=None):
    """
    Executes a SQL query on a PostgreSQL database with optional parameters.
    Args:
    - query: SQL query string.
    - creds: Dictionary containing database credentials.
    - params: Tuple of parameters for the SQL query (optional).
    """
    with psycopg2.connect(database=creds['db'], user=creds['username'], password=creds['password'], host=creds['host'], port=creds['port']) as conn:
        with conn.cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            conn.commit()
            return cur.rowcount

def upload_files(sftp, s3, sftp_path, s3_bucket, file_pattern, s3_folder, logger):
    """
    Upload files from an SFTP server to an S3 bucket based on a specific pattern.
    Raises an exception if the upload fails to stop the job.
    """
    try:
        files = sftp.listdir(sftp_path)
        uploaded_files = []
        for file_name in files:
            if file_name.startswith(file_pattern) and file_name.endswith('.csv'):
                local_file = os.path.join('/tmp', file_name)
                sftp.get(os.path.join(sftp_path, file_name), local_file)
                s3_path = f"{s3_folder}/{file_name}"
                s3.upload_file(Filename=local_file, Bucket=s3_bucket, Key=s3_path)
                os.remove(local_file)
                uploaded_files.append(file_name)

        if not uploaded_files:
            raise Exception("No meta_data files were uploaded to S3.")
        
        logger.info(f"Successfully uploaded files: {uploaded_files}")

    except Exception as e:
        logger.error(f"Failed to upload files: {str(e)}")
        raise Exception(f"Failed to upload files: {str(e)}")


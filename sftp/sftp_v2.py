import os
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, lit, array, struct, col, unix_timestamp
from pyspark.sql.types import *
import boto3
import psycopg2
from datetime import datetime
import pytz
import logging
import re
import paramiko
import uuid
import csv

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3bucket', 's3_folder', 'aspen_sftp_path', 'aspen_sftp_host', 'aspen_sftp_archive_path', 'cloud_front_url'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



# function to get glue connections and credentials
def get_glue_connection(connectionName):
    glueClient = boto3.client('glue', region_name='us-east-1')
    response = glueClient.get_connection(Name=connectionName,HidePassword=False)
    creds = {}
    creds['username'] = response['Connection']['ConnectionProperties']['USERNAME']
    creds['password'] = response['Connection']['ConnectionProperties']['PASSWORD']
    creds['url'] = response['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
    match = re.match(r'jdbc:(\w+)://([^:/]+):(\d+)/([^?]+)', creds['url'])
    creds['connection_type'] = match.group(1)
    creds['host'] = match.group(2)
    creds['port'] = match.group(3)
    creds['db'] = match.group(4)
    return creds

# function to execute postgresql query using psycopg2 module
def cdm_execute_qry(qry,username,password,host,db,port):
    print(f"Execute: {qry}")
    conn = psycopg2.connect(database=db, user=username, password=password, host=host, port=port)
    cur = conn.cursor()
    cur.execute(qry)
    row_cnt=cur.rowcount
    if cur.pgresult_ptr is not None:
        data_qry = cur.fetchone()
    else:
        data_qry=()
    conn.commit()
    cur.close()
    conn.close()
    return row_cnt, data_qry

def process_meta_data_files(sftp_folder_path):
    meta_files = []
    for file in sftp.listdir(sftp_folder_path):
        if file.startswith('meta_data_') and file.endswith('.csv'):
            meta_files.append(file)
    return meta_files

def process_and_upload_files_to_s3(meta_files):
    job_failed = False
    failed_files = []
    for meta_file in meta_files:
        print(f'meta_file: {meta_file}')
        local_path_meta = f"/tmp/{meta_file}"    
        try:
            sftp.get(os.path.join(sftp_folder_path, meta_file), local_path_meta)
            files_to_remove = []
            with open(local_path_meta, 'r') as f:
                reader = csv.DictReader(f, skipinitialspace=True)
                for row in reader:
                    file_name = row['file_name'].strip()
                    member_id = row['member_id'].strip()
                    print(f'file_name: {file_name}; member_id: {member_id}')
                    # verify file_name or member_id are not null
                    if not file_name or not member_id:
                        print(f'Empty file_name or member_id found in record: {row}')
                        job_failed = True
                        if not file_name:
                            failed_files.append('None')
                        else:
                            failed_files.append(file_name)
                        continue
                    try:
                        local_path_pdf = os.path.join("/tmp", file_name)
                        sftp.get(os.path.join(sftp_folder_path, file_name), local_path_pdf)
                        s3_path = os.path.join(args['s3_folder'], file_name)
                        s3.upload_file(local_path_pdf, args['s3bucket'], s3_path)
                        # Log the file moved to S3 with CloudFront URL
                        s3_link = f"https://{args['cloud_front_url']}.cloudfront.net/{args['s3_folder']}/{file_name}"
                        logger.info(f"File moved to S3: {s3_link}")
                        # Generate unique identifiers
                        filedetail_id = str(uuid.uuid4())
                        today_dtm = datetime.now(ny_tz).strftime('%Y-%m-%d %H:%M:%S')
                        # Insert data into PostgreSQL
                        pg_cursor.execute(f"INSERT INTO kwdm.ecm_fax_filedetails (ecm_fax_filedetails_id, member_id, file_path, file_type, fax_type, created_by, created_dt) VALUES (%s, %s, %s, %s, %s, %s, %s)", (filedetail_id, member_id, s3_link, 'pdf', 'providerfax', job_name, today_dtm))
                        pg_cursor.execute(f"INSERT INTO kwdm.journey_orchestration (message_eventid, hf_member_num_cd, message_type, channel, message_status, created_dt, actual_scheduled_dt, journey_name, sub_journey_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (filedetail_id, member_id, 'PROVIDER_FAX', 'FAX', 'scheduled', today_dtm, today_dtm, 'mtmprovider', 'providerfax' ))
                        pg_conn.commit()
                        files_to_remove.append(file_name)
                        sftp.remove(os.path.join(sftp_folder_path, file_name))
                        logger.info(f"File removed from SFTP: {file_name}")
                        os.remove(local_path_pdf)
                    except Exception as e:
                        logger.error(f"Failed to process file: {file_name}. Error: {str(e)}")
                        job_failed = True
                        failed_files.append(file_name)
            if not job_failed:
                dtm = datetime.now(ny_tz).strftime("%Y%m%d%H%M%S")
                s3_path_meta = os.path.join(args['s3_folder'], f"{meta_file.split('.')[0]}_{dtm}.csv")
                s3.upload_file(local_path_meta, args['s3bucket'], s3_path_meta)
                # remove meta data csv file if all entries processed successfully
                sftp.remove(os.path.join(sftp_folder_path, meta_file))
        finally:
            os.remove(local_path_meta)
            if files_to_remove and job_failed:
                remove_entry_from_metadata_csv(meta_file, files_to_remove)
    if job_failed:
        raise Exception(f"Failed to process files: {', '.join(failed_files)}")
                
def remove_entry_from_metadata_csv(csv_file, files_to_remove):
    tmp_file_local = os.path.join('/tmp', csv_file)
    tmp_file_remote = os.path.join(sftp_folder_path, f"tmp_{csv_file}")
    with sftp.open(os.path.join(sftp_folder_path, csv_file), 'r') as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader if row['file_name'].strip() not in files_to_remove]
    with open(tmp_file_local, 'w') as tmp_f:
        writer = csv.DictWriter(tmp_f, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    sftp.put(tmp_file_local, tmp_file_remote)
    
    sftp.remove(os.path.join(sftp_folder_path, csv_file))
    sftp.rename(tmp_file_remote, os.path.join(sftp_folder_path, csv_file))
    
    dtm = datetime.now(ny_tz).strftime("%Y%m%d%H%M%S")
    s3_path_meta = os.path.join(args['s3_folder'], f"{csv_file.split('.')[0]}_{dtm}.csv")
    s3.upload_file(tmp_file_local, args['s3bucket'], s3_path_meta)
    
    os.remove(tmp_file_local)

# configure logging
job_name = args['JOB_NAME']
logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(job_name)
logger.setLevel(logging.INFO)

# get cdm credentials
logger.info("Start: Get cdm Credentials")
cdm_creds = get_glue_connection("CJA_CONTACT_DATA_MART")
cdm_username=cdm_creds['username']
cdm_password=cdm_creds['password']
cdm_url=cdm_creds['url']
cdm_host=cdm_creds['host']
cdm_port=cdm_creds['port']
cdm_db=cdm_creds['db']
logger.info("End: Get cdm Credentials")

logger.info("Start: Get sftp Credentials")
sftp_creds = get_glue_connection('hf-sfmc-sftp')
sftp_username = sftp_creds['username']
sftp_password = sftp_creds['password']
sftp_host=args['aspen_sftp_host']
sftp_host='hfftp.healthfirst.org'
packetsize=1024
port=22
sftp_folder_path=args['aspen_sftp_path']
sftp_archive_path=args['aspen_sftp_archive_path']
logger.info("End: Get sftp Credentials")

s3bucket = args['s3bucket']
s3_folder = args['s3_folder']
cloud_front_url = args['cloud_front_url']


ny_tz = pytz.timezone('US/Eastern')
# insert starting job entry into the aws glue job log table.
logger.info("Start: Insert job start entry into aws_glue_job_log")
job_start_dtm = datetime.now(ny_tz).strftime("%Y-%m-%d %H:%M:%S")
strt_log_qry = f"insert into kwdm.aws_glue_job_log(job_name, start_time, job_status) values('{job_name}', '{job_start_dtm}','started');"
cdm_execute_qry(strt_log_qry, cdm_username, cdm_password, cdm_host, cdm_db, cdm_port)
logger.info("End: Insert job start entry into aws_glue_job_log")

transport = paramiko.Transport((sftp_host, 22))
transport.connect(username=sftp_username, password=sftp_password)
sftp = paramiko.SFTPClient.from_transport(transport)

s3 = boto3.client('s3')
pg_conn = psycopg2.connect(
    host=cdm_host,
    user=cdm_username,
    password=cdm_password,
    dbname=cdm_db
)
pg_cursor = pg_conn.cursor()
meta_files = process_meta_data_files(args['aspen_sftp_path'])
try:
    process_and_upload_files_to_s3(meta_files)
except Exception as e:
    logger.error(f"An error occurred: {str(e)}")
    raise Exception("one or more files are failed to process")
finally:
    # Close SFTP connection
    sftp.close()
    transport.close()
    pg_cursor.close()
    pg_conn.close()

# insert ending job entry into the aws glue job log table.
logger.info("Start: Update job end entry in aws_glue_job_log")
job_end_dtm = datetime.now(ny_tz).strftime("%Y-%m-%d %H:%M:%S")
end_log_qry = f"update kwdm.aws_glue_job_log set source_count=null, insert_count= null, update_count=null, delete_count=null, comments= '', end_time='{job_end_dtm}', job_status='completed' where job_name='{job_name}' and start_time='{job_start_dtm}';"
cdm_execute_qry(end_log_qry, cdm_username, cdm_password, cdm_host, cdm_db, cdm_port)
logger.info("End: Update job end entry in aws_glue_job_log")

job.commit()

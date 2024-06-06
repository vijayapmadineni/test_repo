import os
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, lit, array, struct, col, unix_timestamp, input_file_name, split, element_at, udf
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
import shutil


sys.argv += ['--JOB_NAME', 'Test_job', '--fax_s3bucket', 'hf-dev-sfmc-fax', '--kw_s3bucket', 'hf-dev-cdp-kitewheel',
             '--aspen_sftp_host', 'hfftp.healthfirst.org', '--aspen_sftp_path', '/SFMC/DEV/Provider_Fax',
             '--cloud_front_url', 'd1i7585fs6vfcc']

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'fax_s3bucket', 'kw_s3bucket', 'aspen_sftp_path', 'aspen_sftp_host',
                                     'cloud_front_url'])
print(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# function to get glue connections and credentials
def get_glue_connection(connectionName):
    glueClient = boto3.client('glue', region_name='us-east-1')
    response = glueClient.get_connection(Name=connectionName, HidePassword=False)
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


def cdm_execute_qry(qry, username, password, host, db, port):
    print(f"Execute: {qry}")
    conn = psycopg2.connect(database=db, user=username, password=password, host=host, port=port)
    cur = conn.cursor()
    cur.execute(qry)
    row_cnt = cur.rowcount
    if cur.pgresult_ptr is not None:
        data_qry = cur.fetchone()
    else:
        data_qry = ()
    conn.commit()
    cur.close()
    conn.close()
    return row_cnt, data_qry


def move_s3_files_to_archive(s3bucket, prefix):
    arc_prefix = f'{prefix}archive/'
    response = s3.list_objects_v2(Bucket=s3bucket, Prefix=prefix)
    if 'Contents' in response:
        for file in response['Contents']:
            file_key = file['Key']
            if file_key.endswith('/') or arc_prefix in file_key:
                continue
            filename = file_key.split('/')[-1]
            file_base, file_ext = filename.rsplit('.', 1)
            dtm = datetime.now(ny_tz).strftime("%Y%m%d%H%M%S")
            new_filename = f"{file_base}_{dtm}.{file_ext}"
            arc_key = f"{arc_prefix}{new_filename}"

            s3.copy_object(Bucket=s3bucket,
                           CopySource={'Bucket': s3bucket, 'Key': file_key},
                           Key=arc_key)
            s3.delete_object(Bucket=s3bucket, Key=file_key)
            print(f"Moved {file_key} to {arc_key}")
    else:
        print(f'There are not files to archive from folder: {prefix}')


def upload_meta_data_files_to_s3(sftp_folder_path):
    meta_files = []
    for meta_file in sftp.listdir(sftp_folder_path):
        if meta_file.startswith('meta_data_') and meta_file.endswith('.csv'):
            local_meta_file = os.path.join('/tmp', meta_file)
            sftp.get(os.path.join(sftp_folder_path, meta_file), local_meta_file)
            s3_path_meta_key = f'provider_fax_meta_files/{meta_file}'
            s3.upload_file(local_meta_file, kw_s3bucket, s3_path_meta_key)
            meta_files.append(meta_file)
    if meta_files:
        print(f'List of meta files uploaded to s3: {meta_files}')
    else:
        raise Exception('No meta files uploaded to s3')


def upload_pdfs_to_s3(file_name):
    try:
        local_file = os.path.join('/tmp', file_name)
        sftp.get(os.path.join(sftp_folder_path, file_name), local_file)
        s3_path = os.path.join('public/pdfs/MTM_provider_fax/', file_name)
        s3.upload_file(local_file, args['fax_s3bucket'], s3_path)
        # Log the file moved to S3 with CloudFront URL
        s3_link = f"https://{args['cloud_front_url']}.cloudfront.net/{args['s3_folder']}/{file_name}"
        logger.info(f"File moved to S3: {s3_link}")
        # Generate unique identifiers
        filedetail_id = str(uuid.uuid4())
        today_dtm = datetime.now(ny_tz).strftime('%Y-%m-%d %H:%M:%S')
        # Insert data into PostgreSQL
        pg_cursor.execute(
            f"INSERT INTO kwdm.ecm_fax_filedetails (ecm_fax_filedetails_id, member_id, file_path, file_type, fax_type, created_by, created_dt) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (filedetail_id, member_id, s3_link, 'pdf', 'providerfax', job_name, today_dtm))
        pg_cursor.execute(
            f"INSERT INTO kwdm.journey_orchestration (message_eventid, hf_member_num_cd, message_type, channel, message_status, created_dt, actual_scheduled_dt, journey_name, sub_journey_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (filedetail_id, member_id, 'PROVIDER_FAX', 'FAX', 'scheduled', today_dtm, today_dtm, 'mtmprovider',
             'providerfax'))
        pg_conn.commit()
        sftp.remove(os.path.join(sftp_folder_path, file_name))
        os.remove(local_path_pdf)
        logger.info(f"File removed from SFTP: {file_name}")
        return "success"
    except Exception as e:
        logger.error(f"Failed to process file: {file_name}. Error: {str(e)}")
        return f"failed error: {str(e)}"


def upload_error_file_to_sftp(local_path, remote_path):
    try:
        sftp.put(local_path, remote_path)
        logger.info(f"File uploaded successfully.")
    except Exception as e:
        logger.error(f"Failed to upload file: {local_path}. Error: {str(e)}")


# configure logging
job_name = args['JOB_NAME']
logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(job_name)
logger.setLevel(logging.INFO)

# get cdm credentials
logger.info("Start: Get cdm Credentials")
cdm_creds = get_glue_connection("CJA_CONTACT_DATA_MART")
cdm_username = cdm_creds['username']
cdm_password = cdm_creds['password']
cdm_url = cdm_creds['url']
cdm_host = cdm_creds['host']
cdm_port = cdm_creds['port']
cdm_db = cdm_creds['db']
logger.info("End: Get cdm Credentials")

# get sftp credentials
logger.info("Start: Get sftp Credentials")
sftp_creds = get_glue_connection('hf-sfmc-sftp')
sftp_username = sftp_creds['username']
sftp_password = sftp_creds['password']
sftp_host = args['aspen_sftp_host']
packetsize = 1024
port = 22
sftp_folder_path = args['aspen_sftp_path']
logger.info("End: Get sftp Credentials")

fax_s3bucket = args['fax_s3bucket']
kw_s3bucket = args['kw_s3bucket']
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

move_s3_files_to_archive(kw_s3bucket, 'provider_fax_meta_files/')

try:
    upload_meta_data_files_to_s3(sftp_folder_path)
    df = spark.read.format('csv').options(header='true', inferSchema=True).load(f's3://{kw_s3bucket}/provider_fax_meta_files/meta_data_*').withColumn("meta_file_name", element_at(split(input_file_name(), '/'), -1))
    df.show(n=5, truncate=False)
except Exception as e:
    print(f'error at 205: {str(e)}')
    raise Exception('Error occured at 206')


results_df = spark.createDataFrame([Row(file_name=row['file_name'], status=upload_pdfs_to_s3(row['file_name'])) for row in df.collect()])
results_df.show(n=5, truncate=False)

df = df.join(results_df, on="file_name")
df.show()

logger.info("Start: Write data to targe table")
datetime_NY = datetime.now(ny_tz)
kwdm_df = (df.withColumn("load_datetime", unix_timestamp(lit(datetime_NY.strftime("%Y-%m-%d %H:%M:%S")), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
           .withColumnRenamed('file_name', 'pdf_file_name'))
(kwdm_df.write.format("jdbc")
    .option("url", cdm_url)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable","kwdm.provider_fax_file_track")
    .mode("append")
    .option("user", cdm_username)
    .option("password", cdm_password)
    .option("batchsize", 1000)
    .save())
logger.info("End: Write data to targe table")

logger.info("Start: Filter all error records")
error_df = df.withColumn("member_id", col("member_id").cast("string")).filter(col("status").startswith("failed"))
error_df.show(n=5, truncate=False)
logger.info("Start: Filter all error records")

logger.info("Start: Write error file local")
temp_dir = "/tmp/error_records_output"
error_file_name = f'error_{datetime_NY.strftime("%Y%m%d%H%M%S")}.csv'
error_file_path = os.path.join('/tmp', error_file_name)
error_df.coalesce(1).write.csv(path=temp_dir, mode="overwrite", header=True)
logger.info("End: Write error file local")
part_files = [f for f in os.listdir(temp_dir) if f.startswith("part-")]
if part_files:
    os.rename(os.path.join(temp_dir, part_files[0]), error_file_path)
shutil.rmtree(temp_dir)

logger.info("Start: Upload error file to sftp")
remote_error_file_path = os.path.join(sftp_folder_path, error_file_name)
upload_error_file_to_sftp(error_file_path, remote_error_file_path)
logger.info("End: Upload error file to sftp")

if error_df:
    raise(Exception("Errors: One or more pdfs failed to upload"))


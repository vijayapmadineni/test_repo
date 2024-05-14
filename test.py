import os

# Function to download file from S3 and upload it to SFTP
def upload_to_sftp(s3_bucket, s3_key, local_file_path, remote_file_name, sftp_host, sftp_port, sftp_user, sftp_pass):
    # Download file from S3 to local directory
    s3_client.download_file(s3_bucket, s3_key, local_file_path)

    # Upload file from local directory to SFTP
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_user, password=sftp_pass)
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.put(local_file_path, remote_file_name)
    sftp.close()
    transport.close()

# Define local directory to save the downloaded file
local_file_path = "/tmp/dnc.csv"

# Call the function
upload_to_sftp(s3_bucket, s3_key, local_file_path, "dnc.csv", sftp_host, sftp_port, sftp_user, sftp_pass)

# Remove the downloaded file from local directory
os.remove(local_file_path)
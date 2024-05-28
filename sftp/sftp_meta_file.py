def process_meta_data_files(sftp_folder_path):
    pdf_files = {}
    meta_files = sftp.listdir(sftp_folder_path)
    for file in meta_files:
        if file.startswith('meta_data_') and file.endswith('.csv'):
            with sftp.open(os.path.join(sftp_folder_path, file), 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    pdf_files[row['file_name'].strip()] = {
                        'csv_file': file,
                        'member_id': row['member_id'].strip()
                    }
    return pdf_files

def upload_files_to_s3(pdf_files):
    processed_files = []
    failed_files = []
    processed_csv_files = set()
    for file, data in pdf_files.items():
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
            pg_cursor.execute(f"INSERT INTO kwdm.ecm_fax_filedetails (ecm_fax_filedetails_id, member_id, file_path, file_type, fax_type, created_by, created_dt) VALUES (%s, %s, %s, %s, %s, %s, %s)", (filedetail_id, data['member_id'], s3_link, 'pdf', 'providerfax', job_name, today_dtm))
            pg_cursor.execute(f"INSERT INTO kwdm.journey_orchestration (message_eventid, hf_member_num_cd, message_type, channel, message_status, created_dt, actual_scheduled_dt, journey_name, sub_journey_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (filedetail_id, data['member_id'], 'PROVIDER_FAX', 'FAX', 'scheduled', today_dtm, today_dtm, 'mtmprovider', 'providerfax' ))
            pg_conn.commit()
            # Remove file from SFTP server
            sftp.remove(os.path.join(sftp_folder_path, file))
            logger.info(f"File removed from SFTP: {file}")
            os.remove(local_path)
            processed_csv_files.add(data['csv_file'])  # Track processed CSV files
        except Exception as e:
            logger.error(f"Failed to process file: {file}. Error: {str(e)}")
            failed_files.append(file)
            os.remove(local_path)  # Remove local file if processing failed

    if failed_files:
        # If any file failed, remove only the records that were successfully processed
        for csv_file in processed_csv_files:
            with sftp.open(os.path.join(sftp_folder_path, csv_file), 'r') as f:
                reader = csv.DictReader(f)
                rows_to_keep = [row for row in reader if row['file_name'].strip() not in failed_files]
            # Write back the CSV file with only the rows that were not processed successfully
            with sftp.open(os.path.join(sftp_folder_path, csv_file), 'w') as f:
                writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
                writer.writeheader()
                writer.writerows(rows_to_keep)
        raise Exception(f"Failed to process files: {', '.join(failed_files)}")

    # Remove processed CSV files
    for csv_file in processed_csv_files:
        sftp.remove(os.path.join(sftp_folder_path, csv_file))
        logger.info(f"Processed CSV file removed from SFTP: {csv_file}")

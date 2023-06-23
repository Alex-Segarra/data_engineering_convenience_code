def  functions_iex_reports_sftp_pull (data,context):

    import requests
    import datetime
    import os
    import pandas as pd
    import io
    import subprocess as sp
    from google.cloud import secretmanager
    from google.cloud import bigquery
    from google.cloud import storage
    import pysftp
    import re


    PROJECT_ID = '1234553451' 
    secret_id = 'secret_sftp_export_credentials'
    version_id = 'latest'
    bucket = "sftp_bucket_exports"

    def get_google_secret(secret_id, PROJECT_ID, version_id="latest"):
        
        print(f'\n1. Returning Credentials.')
        # Create the Secret Manager client.
        client = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version.
        name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"

        # Access the secret version.
        response = client.access_secret_version(name=name)

        # Return the decoded payload.
        return response.payload.data.decode('UTF-8')

    def set_sftp(creds):

        #Sets connection options
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        #Creates connection object with credentials from file
        #sftp = pysftp.Connection(host = creds['host'], username = creds['uname'] ,password=creds['pw'] ,cnopts=cnopts)
        sftp = pysftp.Connection(host = creds['URL'], username = creds['USERNAME'] ,password=creds['PASSWORD'] ,cnopts=cnopts)

        print(f'\n2. Connected to SFTP.')

        return(sftp)

    def get_sftp_file_list(sftp):

        print('\n3. Checking SFTP.')

        sftp.chdir('SFTP_Reports')

        file_list = sftp.listdir()

        #file_list = [f for f in file_list if target_file_string in f]

        return(file_list)
        
    def get_bucket_file_list(bucket):

        print(f'\n4. Checking {bucket}.')

        gcs = storage.Client()

        gcs_bucket = gcs.get_bucket(bucket)

        gcs_file_list = [b.name for b in gcs_bucket.list_blobs()]

        return(gcs_file_list)

    def get_missing_file_list(file_list, gcs_file_list):
        
        print(f'\n5. Looking for missing files.')

        missing_files = [f for f in file_list if f not in gcs_file_list]
        
        return(missing_files)

    def sftp_to_bucket(sftp,bucket, missing_files):
        
        os.chdir('/tmp')
        
        print(f"\n6. Grabbing Missing Files")
        
        for i,f in enumerate(missing_files):
            
            local_file = os.path.join('/tmp',f)

            print(f'\n---6.a.{i+1} Pulling {f} from SFTP to {local_file}')
            
            sftp.get(f)
            
            print(f'\n---6.b.{i+1} Cleaning headers for {local_file}')
            
            #Linux Command Line that:
            #'s/#fields://g' :Deletes the #field prefix in the first row
            #'/^#sort/d' :Deletes the #sort order rows
            #'/^[[:space:]]*$/d' Deletes the blank rows
            
            sp.getoutput(f"""sed -i -e 's/#fields://g' -e '/^#sort/d' -e '/^[[:space:]]*$/d' {local_file}""")
            
            print(f'\n---6.c.{i+1} Pushing {local_file} to {bucket}')
            
            storage_client = storage.Client()
            
            gcs_bucket = storage_client.bucket(bucket)

            blob = gcs_bucket.blob(f)
            
            blob.upload_from_filename(local_file)
            
            print(f"\n---*** File {local_file} uploaded to {os.path.join(bucket,f)} ***---")  
        
    def dedupe(prefix, project, dataset):
        
        client = bigquery.Client()
        
        table_name = f"{project}.{dataset}.xsrc_{prefix.lower()}"

        destination_table = client.get_table(table_name) 

        num_rows = destination_table.num_rows

        query = f"""
        CREATE OR REPLACE TABLE {table_name}
        AS
        SELECT DISTINCT * FROM {table_name}
        """

        job = client.query(query)

        destination_table = client.get_table(table_name) 

        print(f'\n7. Deduplicated {num_rows - destination_table.num_rows} from {table_name}')
        
    def bucket_to_gcp(bucket, missing_files):
        
        client = bigquery.Client()
        
        project = "project-1"
        
        dataset = "sftp_reports"
        
        #parse the table names out from the list of files
        file_prefixes = set([re.sub("(.*)(_[0-9]*_[0-9]*\.[a-z]{3,4})","\\1",x) for x in missing_files])
        
        for prefix in file_prefixes:
            
            #grabs separates out files by prefix (for the target table) and creates a list for iterative upload
            upload_file_list = [x for x in missing_files if re.match(prefix,x)]
            
            
            #iteratively loads each table
            for f in upload_file_list:
                
                #sets table name for that specific prefix.  "xsrc" stands for external source.
                table_name = f"{project}.{dataset}.xsrc_{prefix.lower()}"
                
                print(f"\n---*** Sending {f} to {table_name} ***---")  

                job_config = bigquery.LoadJobConfig(
                    autodetect=True
                    #schema= schema
                    #skip_leading_rows=1,
                    # The source format defaults to CSV, so the line below is optional.
                    ,source_format=bigquery.SourceFormat.CSV
                    ,write_disposition = 'WRITE_APPEND'
                    ,create_disposition= 'CREATE_IF_NEEDED'
                    ,field_delimiter="|"
                    , max_bad_records=100
                )

                uri = os.path.join('gs://',bucket,f)

                load_job = client.load_table_from_uri(
                    uri, table_name, job_config=job_config
                )  # Make an API request.

                load_job.result()  # Waits for the job to complete.

                destination_table = client.get_table(table_name)  # Make an API request.

                print(f"\n---*** Loaded {destination_table.num_rows} rows to {table_name}. ***---")
            
            #deduplicating
            dedupe(prefix,project, dataset)
            

    creds = eval(get_google_secret(secret_id, PROJECT_ID))

    sftp = set_sftp(creds)

    file_list = get_sftp_file_list(sftp)

    gcs_file_list = get_bucket_file_list(bucket)

    missing_files = get_missing_file_list(file_list, gcs_file_list)

    if missing_files:

        sftp_to_bucket(sftp,bucket, missing_files)

        bucket_to_gcp(bucket, missing_files)
        
    else:
        
        print('\nNo new files to load')

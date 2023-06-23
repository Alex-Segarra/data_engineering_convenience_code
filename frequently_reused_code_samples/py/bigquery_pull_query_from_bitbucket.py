#!/usr/bin/env python
# coding: utf-8

    def create_table_from_repo(url):

        from google.cloud import bigquery
        from google.cloud import storage
        import datetime
        import re
        import os
        import requests

    #Buckets and Tables to load

        def pull_creds():

            #Parameters
            bucket = 'bucket'
            creds_folder = 'credentials'
            filename = 'bitbucket_credentials'

            file_location = os.path.join(creds_folder,filename)

            #Read metadata from cloud storage
            gcs = storage.Client()

            #Get bucket with name
            gcs_bucket = gcs.get_bucket(bucket)

            #Get bucket data as blob
            blob = gcs_bucket.get_blob(file_location)

            #download credentials as json

            return(eval(blob.download_as_string()))

        def pull_script(url):

            creds = pull_creds()

            req = requests.get(url,auth=(creds['username'], creds['password']))

            return(req.content.decode("utf-8"))

        def generate_table(sql):
            
            client = bigquery.Client()

            job = client.query(sql)  

            job.result() 

            table_name = f"{job.destination.project}.{job.destination.dataset_id}.{job.destination.table_id}"

            print(f"Created Table: {table_name} \nCreated Table at: {datetime.datetime.isoformat(datetime.datetime.now())} \nNumber of Rows: {client.get_table(table_name).num_rows}") 



        sql = pull_script(url)

        generate_table(sql)
"""
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
ENTER BITBUCKET RAW URI's HERE
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
"""
    url_list = []
    
    for s in url_list:

        create_table_from_repo(url=s)

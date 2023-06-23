#!/usr/bin/env python
# coding: utf-8

def function_name(data,context):
  
  """
  Purpose: Runs queries stored as files in GCS.  Can iterate over several files.

  Arguments:
  		
        data:  Mandatory.  Do not change.
        context: Mandatory. Do not change.

  Returns (str): N/A
  """

    from google.cloud import bigquery
    from google.cloud import storage 
    import datetime
    import re
    import os
    import requests

    def create_table_from_repo(url):

        def get_code(file_path):
            """
            Purpose: Grabs code as raw text object from Google Cloud Storage
            
            Arguments: file_path (str) - GCS filepath beginning with 'gs://'
            
            Returns (str): Code as str object
            """
            
            # Regex parses the 'gs://' from the argument url
            result = re.search(r'^(gs://)([a-zA-Z_]+)/(.*)',file_path)
            
            bucket = result.group(2)
            file_location = result.group(3) 

            #file_location = os.path.join(folder,file)

            #Opens GCS Client
            gcs = storage.Client()

            #Get bucket with name
            gcs_bucket = gcs.get_bucket(bucket)

            #Get bucket data as blob
            blob = gcs_bucket.get_blob(file_location)

            return(blob.download_as_string().decode())

        def generate_table(sql):
            
            """
            Purpose: Creates table using sql code passed into main argument
            
            Arguments: sql (str) - sql code as str object
            
            Returns (str): N/A
            """
            
            # Opens the Bigquery Client
            client = bigquery.Client()
            
			#Fires query into bigquery.  Assigns to object 'job' in order to be called as result
            job = client.query(sql)  

            #Result call below is so that function will not proceed until it recieves an 'finished query' signal from Bigquery client
            job.result() 

            #table_name = f"{job.destination.project}.{job.destination.dataset_id}.{job.destination.table_id}"

            #print(f"Created Table: {table_name} \nCreated Table at: {datetime.datetime.isoformat(datetime.datetime.now())} \nNumber of Rows: {client.get_table(table_name).num_rows}") 


        sql = get_code(file_path = url)

        generate_table(sql)
    
    
    #All elements in the below list must be called by 'os.environ.get().'  
    #So that means you can have several calls out to get multiple sql files like: [os.environ.get('sqlfile1'), os.environ.get('sqlfile2')] 
    url_list = [os.environ.get('sql_file')]

    for s in url_list:

        create_table_from_repo(url=s)

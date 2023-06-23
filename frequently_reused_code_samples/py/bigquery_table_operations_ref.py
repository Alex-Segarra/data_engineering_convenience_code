import io
import datetime
import os
import re
import subprocess
import csv
import json
from urllib import request
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

class schema_helper:
    
    def __init__(self):
        print('''This will help you create a schema list.  Just copy and paste the output after you run:
              schema_helper.how_many_fields(num)''')
        from google.cloud.bigquery import SchemaField
        
    def how_many_fields(self,num):
        print(f"""
        Here's a list with {num} SchemaField Entries!!!\n
        The format is as follows:
        Position 1: 'name'
        Position 2: 'field_type':''
        Position 3: 'mode'
        Position 4: 'description'
        Position 5: 'fields'
        Position 6: 'policy_tags'\n
        """)
        return([SchemaField(name = ''
                            ,field_type= ''
                            ,mode = 'NULLABLE/REQUIRED/REPEATED'
                            ,description=None
                            ,fields = ()
                            ,policy_tags=None) for i in range(0,num)])


def create_table(table_id, schema):
    from google.cloud.bigquery import SchemaField
    
    #Pull together table argument

    table = bigquery.Table(table_id, schema = schema)

    #Create Table

    table = client.create_table(table)

    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

def drop_table(table_id):
    client.delete_table(table_id)
    print(f"Dropped table {table.project}.{table.dataset_id}.{table.table_id}")

def deduplicate_table(table_id):
    
    client.query(f"CREATE OR REPLACE TABLE {table_id} AS SELECT DISTINCT * FROM {table_id}")
    
    return(print(f"\n### DEDUPLICATION FINISHED FOR {table_id} ###"))

def copy_table_from_to(from_table, to_table):
    
    client.query(f"CREATE OR REPLACE TABLE {to_table} AS SELECT * FROM {from_table}")
    
    return(print(f"\n### COPIED TABLE {to_table} FROM {from_table} ###"))

def push_local_file_to_table(table_id,filename,file_format):
    """
    table_id = project.dataset.table_name
    
    filename = filepath OR filename
    
    file_format = Must be one of the options in this list: [avro,csv,datastore_backup,json,orc,parquet]
    
    """
    
    if file_format.lower() == 'avro':
        source_format = bigquery.SourceFormat.AVRO
    elif file_format.lower() == 'csv':
        source_format = bigquery.SourceFormat.CSV
    elif file_format.lower() == 'datastore_backup':
        source_format = bigquery.SourceFormat.DATASTORE_BACKUP
    elif file_format.lower() == 'newline_delimited_json' or file_format.lower() == 'json':
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    elif file_format.lower() == 'orc':
        source_format = bigquery.SourceFormat.ORC
    elif file_format.lower() == 'parquet':
        source_format = bigquery.SourceFormat.PARQUET
    
    job_config = bigquery.LoadJobConfig(source_format=source_format)
    
    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    return(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to { table_id}")
  
#In case your local ENV or IAM permissions does not allow you to use the google.cloud library.  Requires SDK.
def bq_query_to_pandas(sql):
    from io import StringIO
    import subprocess
    result = subprocess.getoutput(f'bq query --quiet --format=csv --use_legacy_sql=false "{sql}"')
    result = StringIO(result)
    return(pd.read_csv(result, sep = ","))

class logfile:
    
    def __init__(self, name):
        self.name = name
        self.start_day = datetime.date.today().isoformat()
        self.start_time = datetime.datetime.now().time().isoformat()
      
    def start(self):
        d=self.start_day.replace(r'-','_')
        t=re.sub('\:|\.','_',self.start_time)
        self.logfile = open(f'{self.name}_{d}_{t}.log', 'w')
        
    def write(self,msg):
        self.logfile.write(msg)

    def close(self):
        self.logfile.close()    

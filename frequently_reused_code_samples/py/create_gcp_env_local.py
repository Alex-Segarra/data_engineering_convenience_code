import os
import re
import subprocess

def generate_gcp_env(project=''):
    """
    
    :param project: [Default = ""] This argument specifies the GCP Project you wish to connect to.
    :type project: string
    
    This function:
    
    1) Finds the filepath of the GCP SDK credentials file OR Downloads it if necessary, prompting the user to login to GCP.
    2) Creates a GOOGLE_APPLICATION_CREDENTIALS global variable that is passed into the google.cloud.bigquery client.
    3) Returns the project as a string.
    
    """
    
    #Get SDK Config filepath
    fpath=subprocess.getoutput("gcloud info --format 'value(config.paths.global_config_dir)'")

    #Create the filepath where the application_default_credentials (credentials) file should be
    fpath = os.path.join(fpath,'application_default_credentials.json')

    #Test to see if the file exists, if not, download and begin Authentication procedure
    if os.path.exists(fpath):
        print('application_default_credentials.json file located!')
    else:
        subprocess.getoutput("gcloud auth application-default login")

    #Create GOOGLE_APPLICATION_CREDENTIALS Global Variable to be passed into BigQuery Client
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = fpath
    
    #Set account name
    acct = subprocess.getoutput("gcloud config get-value account")
    
    #If project named as an argument, it will set a new project, else it will go with the default
    if project:
        subprocess.getoutput(f"gcloud config set project {project}")
        os.environ['GOOGLE_CLOUD_PROJECT'] = subprocess.getoutput("gcloud config get-value project") 
    else:
        project= subprocess.getoutput("gcloud config get-value project") 
        project=project
        os.environ['GOOGLE_CLOUD_PROJECT'] = project
    
    os.environ['GOOGLE_CLOUD_PROJECT'] 
    #Checks
    print(
    f"""
    You are set to log in as:

    Account Name: {acct}

    Project Name: {project}
    """
    )
    
    return(project)

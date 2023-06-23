import os
import json
import csv
import re
import collections
import codecs
import datetime
import fileinput
import io

work_dir='/opt/nas/team/analytics/app_data'
  
#AWS
s3_address='s3://s3_bucket/'
aws_buckets=['s3_bucket/buckety_bucko/']
profile='s3_creds'

#Purpose: This creates the appropriate directory if not found.
#Expects: A proper filepath above
for i in aws_buckets:
    csv_dir=os.path.join(work_dir+'/'+i.lower()+'csv/')
    if not os.path.exists(csv_dir):
        print('Making new path: \n'+csv_dir+'\n')
        os.makedirs(csv_dir)
    else:
        print('This path exists! \n' + csv_dir+'\n')

#Purpose: Loop over the possible dir list and pull AWS data
#Expects filespaths and a list of buckets that also exist exactly as they are in s3
def aws_s3_pull():
    newest_files=[]
    for i in aws_buckets:      
        json_path=os.path.join(work_dir + '/'+i.lower()+"csv/")
        new_files=os.popen("/usr/local/bin/aws s3 ls " + s3_address + i+" --recursive --profile "+ profile+" | sort | awk '{print $4}'").read() # Brings a list of files down
        new_files=re.sub('\n',',',new_files) # Regex to clean file list
        new_files=re.sub(',$|ELS/[A-Za-z]*/','',new_files).split(',') #more Regex to clean file names
        current_files=[re.sub('\.json','',d) for d in os.listdir(json_path)] #regex to clean files names and create list of files currently in the directory
        files_to_download=[f for f in new_files if f not in current_files] #Creates a list of files not in the current list of files that are present in AWS
        if not files_to_download or files_to_download==['']:
            print('\nNo new files in :'+s3_address + i)
        elif files_to_download: #PYthonic way of saying if the list is not empty
            print('\nGrabbing files from: '+s3_address + i+'\n'+str(files_to_download))
            for x in files_to_download:
                #call_text='/usr/local/bin/aws s3 cp ' + s3_address + i +str(x)+' '+json_path+str(x)+' --profile '+ profile
                call_text='/usr/local/bin/aws s3 cp ' + s3_address +str(x)+' '+json_path+' --profile '+ profile
                os.system(call_text)
            call_text2='for f in $(find '+json_path+' -type f | grep "*csv.csv"); do rm '+json_path+'*csv.csv; done;'
            os.system(call_text2)
            files_to_download=[json_path+str(x) for x in files_to_download]
            newest_files.extend(files_to_download)
            print(files_to_download)
        
    return(newest_files)

aws_s3_pull()

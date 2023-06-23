import os
import json
import csv
import re
import collections
import codecs
import datetime
import fileinput
import io

#Purpose: These are the fields I want to keep
click_keys = ['timestamp','customer_id','page_name','button_name']

acct_keys = ['timestamp','customer_id','generic_account_field']

#Purpose: Sets up and changes directory to appropriate dirs
#Expects: A filepath
#Windows:

#Unix/Linux
work_dir='/opt/nas/generic_project_folder/generic_app_data/'
s3_address='s3://chp-fake-s3-bucket/app_data_streams/'
aws_buckets=['ClickStream/','User_Account_Data/']
profile='generic_profile'

#Purpose: This creates the appropriate directory if not found.
#Expects: A proper filepath above
for i in aws_buckets:
    csv_dir=os.path.join(work_dir+'/'+i.lower()+'csv/')
    json_dir=os.path.join(work_dir+'/'+i.lower()+'json/')
    if not os.path.exists(csv_dir):
        print('Making new path: \n'+csv_dir+'\n')
        os.makedirs(csv_dir)
    else:
        print('This path exists! \n' + csv_dir+'\n')
    if not os.path.exists(json_dir):
        print('Making new path: \n'+json_dir+'\n')
        os.makedirs(json_dir)
    else:
        print('This path exists! \n' + os.path.join(work_dir+i.lower()+'json/')+'\n')

#Purpose: Loop over the possible dir list and pull AWS data
#Expects filespaths and a list of buckets that also exist exactly as they are in s3
def aws_s3_pull():
    newest_files=[]
    for i in aws_buckets:      
        json_path=os.path.join(work_dir + '/'+i.lower()+"json/")
        new_files=os.popen("aws s3 ls " + s3_address + i+" --recursive --profile "+ profile+" | sort | awk '{print $4}'").read() # Brings a list of files down
        new_files=re.sub('\n',',',new_files) # Regex to clean file list
        new_files=re.sub(',$|generic_app_data/[A-Za-z]*/','',new_files).split(',') #more Regex to clean file names
        current_files=[re.sub('\.json','',d) for d in os.listdir(json_path)] #regex to clean files names and create list of files currently in the directory
        files_to_download=[f for f in new_files if f not in current_files] #Creates a list of files not in the current list of files that are present in AWS
        if not files_to_download or files_to_download==['']:
            print('\nNo new files in :'+s3_address + i)
        elif files_to_download: #PYthonic way of saying if the list is not empty
            print('\nGrabbing files from: '+s3_address + i+'\n'+str(files_to_download))
            for x in files_to_download:
                call_text='aws s3 cp ' + s3_address + i +str(x)+' '+json_path+str(x)+'.json --profile '+ profile 
                os.system(call_text)
            call_text2='for f in $(find '+json_path+' -type f | grep "*json.json"); do rm '+json_path+'*.json.json; done;'
            os.system(call_text2)
            files_to_download=[json_path+str(x)+'.json' for x in files_to_download]
            newest_files.extend(files_to_download)
        
    return(newest_files)
    os.system('printf "Hello,\nThere are the new files from S3 Bucket!" | mailx -s "generic_app_data S3 Bucket Morning Report" alejandro.segarra@cambiahealth.com')

def flatten(key_list,text, parent_key='', sep='_'):
    items=[] #Empty list
    for keys, values in text.items():     #Iterates over keys and values in parallel
        new_key=parent_key+sep+keys if parent_key else keys   #Creates a key that contains the schema.
        if isinstance(values, collections.MutableMapping):  #if is dict... then        
            items.extend(flatten(key_list,values, new_key, sep=sep).items()) #recursively adds the next below layer of json
        else:
            items.append((new_key, values)) #adds new key/value
        
    items=dict(items)
    items={k:v for k,v in items.items() if k in key_list} #Takes and passes only the keys listed above
    return(items)

#Purpose: Load all of the the json messages in a directory, flattens them, then writes them into a file individually.
#Expects: A filepath with JSON that is formatted as a list of json dicts... (i.e. "[{json1} {json2}...{jsonk}]" )
#Supplied: A directory of like .json files.
def write_json_to_csv():
    
    #Assigns proper directories
    json_dir=os.path.join(work_dir +'/'+ x.lower()+'json/')
    csv_dir=os.path.join(work_dir +'/'+ x.lower()+'csv/')

    os.chdir(csv_dir)       

    #Changes the Key mapping depending on the file
    print('Grabbing keys for '+x)
    if x == 'ClickStream/':
        key_list=click_keys
    elif x == 'User_Account_Data/':
        key_list=user_account_keys

    print(key_list)

    #Purpose: Write each individual stanza to the a csv.  Will dynamically adapt to however many files needed to be read and written. Also Dedupes.
    #Expects: An open CSV and a list of files.
    def write(key_list,list_of_files):
        print('Parsing .json to write rows for '+x)
        row_set = set() #Amortized data type to hash data to check for duplication (See dedupe dection below)
        for f in list_of_files:
            if f.endswith(".json"):
                with open(os.path.join(json_dir, f)) as data:
                    print('Reading in: '+f+'\n')
                    data=json.load(data)
                    print('Flattening .json and writing rows!\n')
                    for i in range(len(data)):
                       print('\nFlattening Row: '+str(i))
                       row=flatten(key_list,data[i])
                       #Purpose: Dedupe each line before it's written
                       #Expects a row variable as a dict dict.
                       if tuple(row.items()) not in row_set: 
                           print('\nWriting Row: '+str(i))
                           dict_writer.writerow({k:unicode(v).encode('latin-1','replace') for k,v in row.items()})
                           row_set.add(tuple(row.items()))
                       elif tuple(row.items()) in row_set:
                           print('Found Dupe in line: '+str(i))
                       
    filename=re.sub('/','',x).lower()+'.csv'
    
    if not os.path.exists(filename):
        print(x.lower()+'.csv  does not exist. Pulling all files into new .csv file')
        list_of_files=os.listdir(json_dir)
        with io.open(re.sub('/','',x).lower()+'.csv','w+b') as csv_file:
            dict_writer = csv.DictWriter(csv_file, fieldnames=key_list, extrasaction='ignore') #Takes in the key lists above
            print('\nWriting Header to File')
            dict_writer.writeheader()
            write(key_list,list_of_files)
            
    if os.path.exists(filename):
        print(x.lower()+'.csv exists. Appending newest .json to .csv file')
        list_of_files=filter(lambda y: re.search(json_dir,y), new_files) #filters for the new files in the list of new files
        with io.open(re.sub('/','',x).lower()+'.csv','a+b') as csv_file:
            dict_writer = csv.DictWriter(csv_file, fieldnames=key_list, extrasaction='ignore') #Takes in the key lists above
            write(key_list,list_of_files)

new_files=aws_s3_pull()
print(new_files)
for x in aws_buckets: 
    write_json_to_csv()

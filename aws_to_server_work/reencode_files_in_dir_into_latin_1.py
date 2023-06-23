# -*- coding: utf-8 -*-
"""
Created on Thu Jul 12 09:37:19 2018

@author: r633157
"""
#Necessary Libraries
import os
import json
import re

#dir='//folder/'
dir='/folder/latin_1_reencoded/'
os.chdir(dir)

#create variable from what is in the directory
dir_list=os.listdir(os.curdir)

new_path=dir+'latin_1_reencoded/'

if not os.path.exists(new_path):
    print('Making Path:\n'+ new_path)
    os.makedirs(new_path)
else:
    print('Path Exists!:\n'+ new_path)

#Loops over directory to reencode
for i in dir_list:
    print('\nReading: '+i)
    #Reads in json file
    with open(i) as json_file:
        data = json.load(json_file)
    print('\nEncoding and Writing:'+ re.sub('\.json','',i)+'_encoded.json')    
    #Writes and re-encodes
    with open(new_path+re.sub('\.json','',i)+'_encoded.json', 'w', encoding='latin-1') as json_file:
        json.dump(data,json_file)
    print('\nSuccessfully wrote: '+i+'_encoded.json') 

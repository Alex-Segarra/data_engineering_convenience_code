#/bin/bash

month=$(date +"%m%d%Y")
char="_"

#finds file in director that is less than 5 days old, (can also be +5) executes a copy of the set into another folder recursively
find /opt/nas/team/analytics/Jim -mtime -3 -exec cp -r {} /opt/nas/team/Jim/archive/ \; 

mkdir /opt/nas/team/analytics/Jim/archive/$(date +"%m%Y")

cd /opt/nas/team/analytics/Jim/archive/

for f in *; do mv "$f" ./"$(date +"%m%Y")"/"$month$char$f" ; done

body="Just moved all of these files to ./archives/$(date +"%m%Y"): $(ls)"

echo $body | mailx -s "Moved stuff in Jim's Folder" alejandro.segarra@cambiahealth.com










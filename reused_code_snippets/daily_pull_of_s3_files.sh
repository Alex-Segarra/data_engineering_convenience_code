#!/bin/bash

#Sets the proper directories
hs_output_dir='/opt/nas/team/analytics/hs_output_folder/'
nac_output_dir='/opt/nas/team/analytics/nac_output_folder/'
s3_hs='s3://s3_bucket/hs_folder/'
s3_nac='s3://s3_bucket/nac_folder/'

#Creates a file with a timestamp that Unix can compare against
touch ${hs_output_dir}hs_time_ref.txt
touch ${nac_output_dir}nac_tim_ref.txt

#Pulls shit from S3
aws s3 cp $s3_hs $hs_output_dir --recursive --profile s3_bucket_creds
aws s3 cp $s3_nac $nac_output_dir --recursive --profile s3_bucket_creds

#Moving Files
for f in $(find $hs_output_dir -type f | grep -v 'json'); do mv $f ${f}.json; done;
for f in $(find $nac_output_dir -type f | grep -v 'json'); do mv $f ${f}.json; done;

hs_new_files=$(find $hs_output_dir -type f -newer $hs_output_dir/hs_time_ref.txt -ls)
nac_new_files=$(find $hs_output_dir -type f -newer $nac_output_dir/nac_tim_ref.txt -ls)

printf "Good Morning,\nThese are the new files from the following S3 Bucket:\n$s3_hs\n\n$hs_new_files" | mailx -s "HPL LOG S3 Bucket Morning Report" alejandro.segarra@company.com
printf "Good Morning,\nThese are the new files from the following S3 Bucket:\n$s3_nac\n\n$nac_new_files" | mailx -s "NAC LOG S3 Bucket Morning Report" alejandro.segarra@company.com

rm ${hs_output_dir}hs_time_ref.txt
rm ${nac_output_dir}nac_tim_ref.txt

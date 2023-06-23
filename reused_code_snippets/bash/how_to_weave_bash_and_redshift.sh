#!/bin/bash

###################### OBJECTIVE 1: GET RAW HH COUNTS PER TAG BY ZIP ######################
###################### OBJECTIVE 2: GET RAW HH COUNTS PER TAG ONLY ######################
###################### OBJECTIVE 3: GET RAW INDECES FOR REQUESTED MONTH######################

#SETTING VARIABLES:
cwd=$(echo $(pwd))
file=$1
zip_file=$(echo "$file" | cut -d '.' -f 1)
tags=$"1,2,3"
stations=$"4,5,6"
month=$"201702"
m1=$(echo $month'01')
start=$(date -d $m1 +%Y-%m-%d)
end=$(date -d "$m1 1 month" +%Y-%m-%d)
market="1"
redshift_location="s3://redshift/temporary/asegarra/"

#FANCY DATE MANIPULATION
pmsd=$( as-date first-day-in-previous-month $month)
pmed=$( as-date last-day-in-previous-month $month)
previous_month_start_date=$( date -d $pmsd +%F)
previous_month_end_date=$( date -d $pmed +%F)
msd=$( as-date first-day-in-month $month)
med=$( as-date last-day-in-month $month)
month_start_date=$( date -d $msd +%F)
month_end_date=$( date -d $med +%F)
echo 'Repeat these variables back to me:
pmsd=$pmsd
pmed=$pmed
previous_month_start_date=$previous_month_start_date
previous_month_end_date=$previous_month_end_date
msd=$msd
med=$med
month_start_date=$month_start_date
month_end_date=$month_end_date'

#Upload Zip Codes to Redshift
aws s3 cp ./${zip_file}.txt ${redshift_location}${zip_file}.txt
#set variables

#ping db here
db1_name<<HERE
\set start			'''$start'''
\set end			'''$end'''
\set stations		''$stations''
\set market			'''$market'''
\set tags			$tags

DROP TABLE IF EXISTS MKT_INDEX;
CREATE TEMP TABLE MKT_INDEX AS

SELECT * FROM COMPANY_SECRET_SCHEMA.TABLE_YOU_SHOULDNT_KNOW_ABOUT where tags in (:tags) AND market_no = :market

\COPY (SELECT * FROM MKT_INDEX) TO '${cwd}/mkt_index.txt' (format csv, delimiter '|')

HERE

aws s3 cp ./mkt_index.txt ${redshift_location}mkt_index.txt

#redshift here
as-redshift <<HERE
\set month_start_date           '''$month_start_date'''
\set month_end_date             '''$month_end_date'''
\set previous_month_start_date  '''$previous_month_start_date'''
\set previous_month_end_date    '''$previous_month_end_date'''
--\set zips                     ''$zips''
\set market                     '''$market'''
\set zip_file                    '${zip_file}.txt'
\set tags                        $tags
\set path                        's3://redshift/temporary/asegarra'
\set in_file                     :path/:zip_file
;

drop table if exists mkt_index;
create temp table mkt_index
(	
	columns varchar
);
	
copy mkt_index from '${redshift_location}mkt_index.txt'
credentials :aws_credentials;

select * from mkt_index limit 10;

drop table if exists zips;
create temp table zips
( zips varchar );

copy zips from :'in_file'
credentials :aws_credentials;

select count(*) from zips limit 10;

\\i sql_file.sql

\\i other_sql_file.sql


unload('select * from combined_goodness') to '${redshift_location}${zip_file}_area_indexes.txt'
credentials :aws_credentials
parallel off
allowoverwrite
;

\\i trade_area_hh_counts.sql

unload('select * from counts')
TO '${redshift_location}${zip_file}_feasibility.txt'
credentials :aws_credentials
parallel off
allowoverwrite;

HERE

aws s3 cp ${redshift_location}${zip_file}_area_indexes.txt000 ./
aws s3 cp ${redshift_location}${zip_file}_feasibility.txt000 ./

sed '1itv_market_no|network_no|network_name as Network|station_no|station_call_sign|demographic_tag_no|demographic_tag_name|trade_area_rating|trade_area_index|market_index|index_differential|numer_num_hours|numer_hhs|denom_num_hours|denom_hhs' '${zip_file}_area_indexes.txt000' > $(printf "%s" "$zip_file" "_area_indexes_formatted.txt")
sed '1itag_no|pre_refresh_count|post_refresh_count' ${zip_file}_feasibility.txt000 > $(printf "%s" "$zip_file" "_feasibility_formatted.txt")

echo "Your INDECES and FEASIBILITY are in ${pwd}" | mail -s "Local Trade Area Script Finished"

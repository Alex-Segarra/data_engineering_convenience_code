#WDVM Households script 



aws s3 cp ./wdvm_zips.txt s3://as-redshift/temporary/asegarra/

redshift <<HERE

drop table if exists wdvm_zips;
create temp table wdvm_zips
(zips integer);

copy wdvm_zips from 's3://redshift/temporary/asegarra/wdvm_zips.txt'
credentials :aws_credentials;

drop table if exists that_pull_baby;
create temp table that_pull_baby as

select * from thats.classified

unload ('select * from yeyo;') to 's3://redshift/temporary/asegarra/wdvm_hh_list.txt'
credentials :aws_credentials
parallel off
allowoverwrite;

HERE

aws s3 cp s3://as-redshift/temporary/asegarra/wdvm_hh_list.txt000 ./

sed '1ipostal_code|as_hh_no|headend_no|hh_no|tag_no' wdvm_hh_list.txt000 > wdvm_hh_list2.txt
cut -f 1,3,4 wdvm_hh_list2.txt -d '|' > wdvm_hh_list.txt
rm wdvm_hh_list2.txt


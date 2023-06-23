#!/bin/bash


var=$(find ./*.mp4 -atime +7 | wc -l)

if
        [ $(($var + 0)) -ne 0 ];
then
        find ./*.mp4 -atime +7 -exec gzip {} \;
        printf "Adding the following files at: $(date)" >> gzippin_log.log
        find ./*.mp4 -atime +7 >> gzippin_log.log
fi

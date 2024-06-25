#!/bin/bash

set -x

if [[ ! -d "/mnt/scripts/suites/test_hive_compress_type/data" ]]; then
    echo "/mnt/scripts/suites/test_hive_compress_type/data does not exist"
    cd /mnt/scripts/suites/test_hive_compress_type/ \
    && rm -f data.tar.gz \
    && curl -O https://s3BucketName.s3Endpoint/regression/datalake/pipeline_data/test_hive_compress_type/data.tar.gz \
    && tar xzf data.tar.gz
    cd -
else
    echo "/mnt/scripts/suites/test_hive_compress_type/data exist, continue !"
fi

## mkdir and put data to hdfs
hadoop fs -mkdir -p /user/doris/suites/test_hive_compress_type/
hadoop fs -put /mnt/scripts/suites/test_hive_compress_type/data/* /user/doris/suites/test_hive_compress_type/

# create table
hive -f /mnt/scripts/suites/test_hive_compress_type/create_table.hql


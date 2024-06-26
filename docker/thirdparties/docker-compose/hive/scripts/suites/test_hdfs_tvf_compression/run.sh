#!/bin/bash

set -x

if [[ ! -d "/mnt/scripts/suites/test_hdfs_tvf_compression/test_data" ]]; then
    echo "/mnt/scripts/suites/test_hdfs_tvf_compression/test_data does not exist"
    cd /mnt/scripts/suites/test_hdfs_tvf_compression/ \
    && rm -f test_data.tar.gz \
    && curl -O https://s3BucketName.s3Endpoint/regression/datalake/pipeline_data/test_hdfs_tvf_compression/test_data.tar.gz \
    && tar xzf test_data.tar.gz
    cd -
else
    echo "/mnt/scripts/suites/test_hdfs_tvf_compression/test_data exist, continue !"
fi

## mkdir and put data to hdfs
hadoop fs -mkdir -p /test_data
hadoop fs -put /mnt/scripts/suites/test_hdfs_tvf_compression/test_data/* /test_data/

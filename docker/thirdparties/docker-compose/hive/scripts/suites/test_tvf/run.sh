#!/bin/bash

set -x

if [[ ! -d "/mnt/scripts/suites/test_tvf/tvf" ]]; then
    echo "/mnt/scripts/suites/test_tvf/tvf does not exist"
    cd /mnt/scripts/suites/test_tvf/ \
    && rm -f data.tar.gz \
    && curl -O https://s3BucketName.s3Endpoint/regression/datalake/pipeline_data/test_tvf/data.tar.gz \
    && tar xzf data.tar.gz
    cd -
else
    echo "/mnt/scripts/suites/test_tvf/tvf exist, continue !"
fi

## mkdir and put data to hdfs
hadoop fs -mkdir -p /catalog/tvf/
hadoop fs -put /mnt/scripts/suites/test_tvf/tvf/* /catalog/tvf/

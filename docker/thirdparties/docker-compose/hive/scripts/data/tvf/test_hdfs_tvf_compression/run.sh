#!/bin/bash

set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

if [[ ! -d "${CUR_DIR}/test_data" ]]; then
    echo "${CUR_DIR}/test_data does not exist"
    cd ${CUR_DIR}/ && rm -f test_data.tar.gz \
    && curl -O https://s3BucketName.s3Endpoint/regression/datalake/pipeline_data/test_hdfs_tvf_compression/test_data.tar.gz \
    && tar xzf test_data.tar.gz
    cd -
else
    echo "${CUR_DIR}/test_data exist, continue !"
fi

## mkdir and put data to hdfs
hadoop fs -mkdir -p /test_data
hadoop fs -put "${CUR_DIR}"/test_data/* /test_data/

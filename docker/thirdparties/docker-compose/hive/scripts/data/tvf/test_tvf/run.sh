#!/bin/bash

set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

if [[ ! -d "${CUR_DIR}/tvf" ]]; then
    echo "${CUR_DIR}/tvf does not exist"
    cd ${CUR_DIR}/ && rm -f data.tar.gz \
    && curl -O https://s3BucketName.s3Endpoint/regression/datalake/pipeline_data/test_tvf/data.tar.gz \
    && tar xzf data.tar.gz
    cd -
else
    echo "${CUR_DIR}/tvf exist, continue !"
fi

## mkdir and put data to hdfs
hadoop fs -mkdir -p /catalog/tvf/
hadoop fs -put "${CUR_DIR}"/tvf/* /catalog/tvf/

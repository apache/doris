#!/bin/bash

set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

## mkdir and put data to hdfs (skip if already uploaded)
hadoop fs -mkdir -p /test_data
if [[ -z "$(hadoop fs -ls /test_data 2>/dev/null)" ]]; then
    hadoop fs -put "${CUR_DIR}"/test_data/* /test_data/
fi

#!/bin/bash

set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

## mkdir and put data to hdfs
hadoop fs -mkdir -p /test_data
hadoop fs -put "${CUR_DIR}"/test_data/* /test_data/

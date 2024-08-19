#!/bin/bash
set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

## mkdir and put data to hdfs
cd "${CUR_DIR}" && rm -rf data/ && tar xzf data.tar.gz
hadoop fs -mkdir -p /user/doris/suites/default/
hadoop fs -put "${CUR_DIR}"/data/* /user/doris/suites/default/

# create table
hive -f "${CUR_DIR}/create_table.hql"

#!/bin/bash
set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

## mkdir and put data to hdfs
hadoop fs -mkdir -p /user/doris/suites/default/hive_recursive_directories_table
hadoop fs -mkdir -p /user/doris/suites/default/hive_ignore_absent_partitions_table

# create table
hive -f "${CUR_DIR}"/create_table.hql

hadoop fs -rm -r /user/doris/suites/default/hive_ignore_absent_partitions_table/country=India


#!/bin/bash
set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"


hadoop fs -mkdir -p /user/doris/suites/multi_catalog/
hadoop fs -put "${CUR_DIR}"/data/* /user/doris/suites/multi_catalog/

# create table
hive -f "${CUR_DIR}/create_table.hql"

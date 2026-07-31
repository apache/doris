#!/bin/bash
set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"


# create table
hive -f "${CUR_DIR}"/create_table.hql

hadoop fs -rm -r -f /user/doris/suites/regression/crdmm_data || true
hadoop fs -mkdir -p /user/doris/suites/regression/crdmm_data
hadoop fs -put "${CUR_DIR}"/data/crdmm_data/* /user/doris/suites/regression/crdmm_data/

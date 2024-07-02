#!/bin/bash
set -x

# CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# if [[ ! -d "${CUR_DIR}/data" ]]; then
#     echo "${CUR_DIR}/data does not exist"
#     cd "${CUR_DIR}" && rm -f data.tar.gz \
#     && curl -O https://s3BucketName.s3Endpoint/regression/datalake/pipeline_data/tpch_1000_parquet/part/data.tar.gz \
#     && tar xzf data.tar.gz
#     cd -
# else
#     echo "${CUR_DIR}/data exist, continue !"
# fi

# ## mkdir and put data to hdfs
# hadoop fs -mkdir -p /user/doris/suites/tpch_1000_parquet/
# hadoop fs -put "${CUR_DIR}"/data/* /user/doris/suites/tpch_1000_parquet/

# # create table
# hive -f "${CUR_DIR}"/create_table.hql


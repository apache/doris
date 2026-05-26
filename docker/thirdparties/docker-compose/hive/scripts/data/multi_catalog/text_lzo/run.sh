#!/bin/bash
set -e
set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
HDFS_DIR="/user/doris/preinstalled_data/text_lzo"

hive -f "${CUR_DIR}/create_table.hql"

hadoop fs -mkdir -p "${HDFS_DIR}"
hadoop fs -put -f "${CUR_DIR}"/data/* "${HDFS_DIR}/"

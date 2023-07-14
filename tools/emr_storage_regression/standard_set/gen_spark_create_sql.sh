#!/bin/bash
BUCKET=$1
TYPE=$2
cd "$(dirname "$0")" || exit
sh gen_tbl/gen_ssb_create_sql.sh  "${BUCKET}"/ssb/ssb100_orc ssb100_orc_"${TYPE}" orc > create_"${TYPE}".sql
sh gen_tbl/gen_ssb_create_sql.sh  "${BUCKET}"/ssb/ssb100_parquet ssb100_parquet_"${TYPE}" parquet >> create_"${TYPE}".sql
# tpch
sh gen_tbl/gen_tpch_create_sql.sh "${BUCKET}"/tpch/tpch100_orc tpch100_orc_"${TYPE}" orc >> create_"${TYPE}".sql
sh gen_tbl/gen_tpch_create_sql.sh "${BUCKET}"/tpch/tpch100_parquet tpch100_parquet_"${TYPE}" parquet >> create_"${TYPE}".sql
# clickbench
sh gen_tbl/gen_clickbench_create_sql.sh "${BUCKET}"/clickbench/hits_parquet clickbench_parquet_"${TYPE}" parquet >> create_"${TYPE}".sql
sh gen_tbl/gen_clickbench_create_sql.sh "${BUCKET}"/clickbench/hits_orc  clickbench_orc_"${TYPE}" orc >> create_"${TYPE}".sql
# iceberg
# sh gen_tbl/gen_ssb_create_sql.sh  oss://benchmark-oss/ssb/ssb100_iceberg ssb100_iceberg iceberg >> create_"${TYPE}".sql
# sh gen_tbl/gen_tpch_create_sql.sh oss://benchmark-oss/tpch/tpch100_iceberg tpch100_iceberg iceberg >> create_"${TYPE}".sql
# sh gen_tbl/gen_clickbench_create_sql.sh oss://benchmark-oss/clickbench/hits_iceberg clickbench_iceberg_hdfs >> create_"${TYPE}".sql


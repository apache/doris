#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# See emr_tools.sh
##############################################################

# shellcheck disable=SC2129
BUCKET=$1
TYPE=$2
cd "$(dirname "$0")" || exit
sh gen_tbl/gen_ssb_create_sql.sh "${BUCKET}"/ssb/ssb100_orc ssb100_orc_"${TYPE}" orc >create_"${TYPE}".sql
sh gen_tbl/gen_ssb_create_sql.sh "${BUCKET}"/ssb/ssb100_parquet ssb100_parquet_"${TYPE}" parquet >>create_"${TYPE}".sql
# tpch
sh gen_tbl/gen_tpch_create_sql.sh "${BUCKET}"/tpch/tpch100_orc tpch100_orc_"${TYPE}" orc >>create_"${TYPE}".sql
sh gen_tbl/gen_tpch_create_sql.sh "${BUCKET}"/tpch/tpch100_parquet tpch100_parquet_"${TYPE}" parquet >>create_"${TYPE}".sql
# clickbench
sh gen_tbl/gen_clickbench_create_sql.sh "${BUCKET}"/clickbench/hits_parquet clickbench_parquet_"${TYPE}" parquet >>create_"${TYPE}".sql
sh gen_tbl/gen_clickbench_create_sql.sh "${BUCKET}"/clickbench/hits_orc clickbench_orc_"${TYPE}" orc >>create_"${TYPE}".sql
# iceberg
# sh gen_tbl/gen_ssb_create_sql.sh  oss://benchmark-oss/ssb/ssb100_iceberg ssb100_iceberg iceberg >> create_"${TYPE}".sql
# sh gen_tbl/gen_tpch_create_sql.sh oss://benchmark-oss/tpch/tpch100_iceberg tpch100_iceberg iceberg >> create_"${TYPE}".sql
# sh gen_tbl/gen_clickbench_create_sql.sh oss://benchmark-oss/clickbench/hits_iceberg clickbench_iceberg_hdfs >> create_"${TYPE}".sql

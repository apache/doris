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

# usage: sh run.sh dlf parquet
FE_HOST=$1
USER=$2
PORT=$3
if [[ -z "$4" ]]; then
    echo 'need catalog name'
    exit
else
    catalog_name=$4
fi

if [[ -z "$5" ]]; then
    echo "run all test default"
elif [[ "$5" = 'all' ]]; then
    echo "run all test"
else
    case=$5
fi

if [[ -z ${TYPE} ]]; then
    TYPE=obj
fi
echo "execute ${case} benchmark for ${TYPE}..."

if [[ "${case}" = 'ssb' ]]; then
    # ssb
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".ssb100_parquet_"${TYPE}" queries/ssb_queries.sql
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".ssb100_orc_"${TYPE}" queries/ssb_queries.sql
elif [[ "${case}" = 'ssb_flat' ]]; then
    # ssb_flat
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".ssb100_parquet_"${TYPE}" queries/ssb_flat_queries.sql
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".ssb100_orc_"${TYPE}" queries/ssb_flat_queries.sql
elif [[ "${case}" = 'tpch' ]]; then
    # tpch
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".tpch100_parquet_"${TYPE}" queries/tpch_queries.sql
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".tpch100_orc_"${TYPE}" queries/tpch_queries.sql
elif [[ "${case}" = 'clickbench' ]]; then
    # clickbench
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".clickbench_parquet_"${TYPE}" queries/clickbench_queries.sql
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".clickbench_orc_"${TYPE}" queries/clickbench_queries.sql
else
    # run all
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".ssb100_parquet_"${TYPE}" queries/ssb_queries.sql
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".ssb100_orc_"${TYPE}" queries/ssb_queries.sql

    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".ssb100_parquet_"${TYPE}" queries/ssb_flat_queries.sql
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".ssb100_orc_"${TYPE}" queries/ssb_flat_queries.sql

    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".tpch100_parquet_"${TYPE}" queries/tpch_queries.sql
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".tpch100_orc_"${TYPE}" queries/tpch_queries.sql

    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".clickbench_parquet_"${TYPE}" queries/clickbench_queries.sql
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".clickbench_orc_"${TYPE}" queries/clickbench_queries.sql
fi

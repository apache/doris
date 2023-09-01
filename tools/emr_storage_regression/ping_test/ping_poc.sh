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

## Step 1: create external table and import data
ENDPOINT=$1
REGION=$2
SERVICE=$3
AK=$4
SK=$5
HMS_META_URI=$6
HMS_WAREHOUSE=$7
BEELINE_URI=$8

# set global env to local
# shellcheck disable=SC2269
FE_HOST=${FE_HOST}
# shellcheck disable=SC2269
FE_QUERY_PORT=${FE_QUERY_PORT}
# shellcheck disable=SC2269
USER=${USER}

DLF_ENDPOINT=datalake-vpc.cn-beijing.aliyuncs.com
JINDO_ENDPOINT=cn-beijing.oss-dls.aliyuncs.com

if [[ -z ${HMS_WAREHOUSE} ]]; then
    echo "Need warehouse for ${SERVICE}"
fi
cd "$(dirname "$0")" || gexit

run_spark_create_sql() {
    if [[ ${SERVICE} == 'ali' ]]; then
        PARAM="--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aliyun.dlf.hive.DlfCatalog \
            --conf spark.sql.catalog.iceberg.access.key.id=${AK} \
            --conf spark.sql.catalog.iceberg.access.key.secret=${SK} \
            --conf spark.sql.catalog.iceberg.dlf.endpoint=${DLF_ENDPOINT} \
            --conf spark.sql.catalog.iceberg.dlf.region-id=${REGION} \
            --conf spark.sql.catalog.hms=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.hms.type=hive \
            --conf spark.sql.defaultCatalog=hms \
            --conf spark.sql.catalog.hms.warehouse=${HMS_WAREHOUSE} \
            -f data/create_spark_ping.sql" 2>spark_create.log
    elif [[ ${SERVICE} == 'tx' ]]; then
        PARAM="--jars /usr/local/service/iceberg/iceberg-spark-runtime-3.2_2.12-0.13.1.jar \
              --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
              --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
              --conf spark.sql.catalog.spark_catalog.type=hive \
              --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
              --conf spark.sql.catalog.local.type=hadoop \
              --conf spark.sql.catalog.local.warehouse=/usr/hive/warehouse \
              -f data/create_spark_ping.sql" 2>spark_create.log
    elif [[ ${SERVICE} == 'hw' ]]; then
        PARAM="-f data/create_spark_ping.sql" 2>spark_create.log
    else
        echo "Unknown service type: ${SERVICE}"
        exit 1
    fi
    eval spark-sql "${PARAM}"
}

run_spark_create_sql
run_hive_create_sql() {
    if [[ ${SERVICE} == 'hw' ]]; then
        beeline -u "${BEELINE_URI}" -f data/create_hive_ping.sql 2>hive_create.log
    elif [[ ${SERVICE} == 'ali' ]]; then
        hive -f data/create_hive_ping.sql 2>hive_create.log
    else
        hive -f data/create_hive_ping.sql 2>hive_create.log
    fi
}

run_hive_create_sql

## Step 2: make ping data
spark-sql -f data/data_for_spark.sql >>spark_data.log
hive -f data/data_for_hive.sql >>hive_data.log

run_query() {
    QUERY_NUM=1
    TRIES=2
    sql_file=$1
    catalog=$2
    while read -r query; do
        echo -n "create catalog ${QUERY_NUM},"
        for i in $(seq 1 "${TRIES}"); do
            if [[ -n ${catalog} ]]; then
                query="switch ${catalog};${query}"
            fi
            RES=$(mysql -vvv -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -e "${query}")
            echo -n "${RES}"
            [[ "${i}" != "${TRIES}" ]] && echo -n ","
        done
        QUERY_NUM=$((QUERY_NUM + 1))
    done <"${sql_file}"
}

## Step 3: create external catalog in doris
# shellcheck disable=SC2094
case "${SERVICE}" in
ali)
    sed -e 's#DLF_ENDPOINT#'"${DLF_ENDPOINT}"'#g' emr_catalog.sql >emr_catalog.sql
    sed -e 's#JINDO_ENDPOINT#'"${JINDO_ENDPOINT}"'#g' emr_catalog.sql >emr_catalog.sql
    sed -e 's#ENDPOINT#'"${ENDPOINT}"'#g' -e 's#META_URI#'"${HMS_META_URI}"'#g' -e 's#AK_INPUT#'"${AK}"'#g' -e 's#SK_INPUT#'"${SK}"'#g' create_catalog_aliyun.sql >emr_catalog.sql
    ;;
tx)
    sed -e 's#ENDPOINT#'"${ENDPOINT}"'#g' -e 's#META_URI#'"${HMS_META_URI}"'#g' -e 's#AK_INPUT#'"${AK}"'#g' -e 's#SK_INPUT#'"${SK}"'#g' create_catalog_tx.sql >emr_catalog.sql
    ;;
aws)
    sed -e 's#ENDPOINT#'"${ENDPOINT}"'#g' -e 's#META_URI#'"${HMS_META_URI}"'#g' -e 's#AK_INPUT#'"${AK}"'#g' -e 's#SK_INPUT#'"${SK}"'#g' create_catalog_aws.sql >emr_catalog.sql
    ;;
hw)
    sed -e 's#ENDPOINT#'"${ENDPOINT}"'#g' -e 's#META_URI#'"${HMS_META_URI}"'#g' -e 's#AK_INPUT#'"${AK}"'#g' -e 's#SK_INPUT#'"${SK}"'#g' create_catalog_hw.sql >emr_catalog.sql
    ;;
*)
    echo "Internal error"
    exit 1
    ;;
esac

run_query emr_catalog.sql

## Step 4: query ping
EMR_CATALOG=$(awk '{print $6}' emr_catalog.sql)
# shellcheck disable=SC2116
# required echo here, or the EMR_CATALOG will not be split.
for c in $(echo "${EMR_CATALOG}"); do
    if [[ ${SERVICE} == 'ali' ]]; then
        run_query ping_aliyun.sql "${c}"
    fi
    run_query ping.sql "${c}"
done

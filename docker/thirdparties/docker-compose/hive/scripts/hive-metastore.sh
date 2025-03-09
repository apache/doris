#!/bin/bash
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

set -e -x

parallel=$(getconf _NPROCESSORS_ONLN)

nohup /opt/hive/bin/hive --service metastore &

# wait lockfile
lockfile1="/mnt/scripts/run-data.lock"
while [[ -f "${lockfile1}" ]]; do
    sleep 10
done
touch "${lockfile1}"

# wait metastore start
while ! $(nc -z localhost "${HMS_PORT:-9083}"); do
    sleep 5s
done

# create tables for other cases
# new cases should use separate dir
hadoop fs -mkdir -p /user/doris/suites/

DATA_DIR="/mnt/scripts/data/"
find "${DATA_DIR}" -type f -name "run.sh" -print0 | xargs -0 -n 1 -P "${parallel}" -I {} bash -ec '
    START_TIME=$(date +%s)
    bash -e "{}" || (echo "Failed to executing script: {}" && exit 1)
    END_TIME=$(date +%s)
    EXECUTION_TIME=$((END_TIME - START_TIME))
    echo "Script: {} executed in $EXECUTION_TIME seconds"
'

rm -f "${lockfile1}"

lockfile2="/mnt/scripts/download-data.lock"

# wait lockfile
while [[ -f "${lockfile2}" ]]; do
    sleep 10
done

touch "${lockfile2}"

# if you test in your local，better use # to annotation section about tpch1.db
if [[ ! -d "/mnt/scripts/tpch1.db" ]]; then
    echo "/mnt/scripts/tpch1.db does not exist"
    cd /mnt/scripts/
    curl -O https://s3BucketName.s3Endpoint/regression/datalake/pipeline_data/tpch1.db.tar.gz
    tar -zxf tpch1.db.tar.gz
    rm -rf tpch1.db.tar.gz
    cd -
else
    echo "/mnt/scripts/tpch1.db exist, continue !"
fi

# paimon data file is small and update frequently, so we download it every time
rm -rf "/mnt/scripts/paimon1"
echo "/mnt/scripts/paimon1 does not exist"
cd /mnt/scripts/
curl -O https://s3BucketName.s3Endpoint/regression/datalake/pipeline_data/paimon1.tar.gz
tar -zxf paimon1.tar.gz
rm -rf paimon1.tar.gz
cd -

# download tvf_data
if [[ ! -d "/mnt/scripts/tvf_data" ]]; then
    echo "/mnt/scripts/tvf_data does not exist"
    cd /mnt/scripts/
    curl -O https://s3BucketName.s3Endpoint/regression/datalake/pipeline_data/tvf_data.tar.gz
    tar -zxf tvf_data.tar.gz
    rm -rf tvf_data.tar.gz
    cd -
else
    echo "/mnt/scripts/tvf_data exist, continue !"
fi

rm -f "${lockfile2}"

# put data file
hadoop_put_pids=()
hadoop fs -mkdir -p /user/doris/


## put tpch1
if [[ -z "$(ls /mnt/scripts/tpch1.db)" ]]; then
    echo "tpch1.db does not exist"
    exit 1
fi
hadoop fs -copyFromLocal -f /mnt/scripts/tpch1.db /user/doris/ &
hadoop_put_pids+=($!)

## put paimon1
if [[ -z "$(ls /mnt/scripts/paimon1)" ]]; then
    echo "paimon1 does not exist"
    exit 1
fi
hadoop fs -copyFromLocal -f /mnt/scripts/paimon1 /user/doris/ &
hadoop_put_pids+=($!)

## put tvf_data
if [[ -z "$(ls /mnt/scripts/tvf_data)" ]]; then
    echo "tvf_data does not exist"
    exit 1
fi
hadoop fs -copyFromLocal -f /mnt/scripts/tvf_data /user/doris/ &
hadoop_put_pids+=($!)

## put other preinstalled data
hadoop fs -copyFromLocal -f /mnt/scripts/preinstalled_data /user/doris/ &
hadoop_put_pids+=($!)


# wait put finish
set +e
wait "${hadoop_put_pids[@]}"
set -e
if [[ -z "$(hadoop fs -ls /user/doris/paimon1)" ]]; then
    echo "paimon1 put failed"
    exit 1
fi
if [[ -z "$(hadoop fs -ls /user/doris/tpch1.db)" ]]; then
    echo "tpch1.db put failed"
    exit 1
fi
if [[ -z "$(hadoop fs -ls /user/doris/tvf_data)" ]]; then
    echo "tvf_data put failed"
    exit 1
fi

# create tables
ls /mnt/scripts/create_preinstalled_scripts/*.hql | xargs -n 1 -P "${parallel}" -I {} bash -ec '
    START_TIME=$(date +%s)
    hive -f {} || (echo "Failed to executing hql: {}" && exit 1)
    END_TIME=$(date +%s)
    EXECUTION_TIME=$((END_TIME - START_TIME))
    echo "Script: {} executed in $EXECUTION_TIME seconds"
'

# create view
START_TIME=$(date +%s)
hive -f /mnt/scripts/create_view_scripts/create_view.hql
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))
echo "Script: create_view.hql executed in ${EXECUTION_TIME} seconds"

touch /mnt/SUCCESS

# Avoid container exit
tail -f /dev/null

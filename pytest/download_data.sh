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


set -eo pipefail

PYTEST_HOME=$(dirname "$0")
PYTEST_HOME=$(cd "${PYTEST_HOME}"; pwd)

export PYTEST_HOME

HDFS_DATA="hdfs_data.tar.gz"
KAFKA_DATA="kafka_data.tar.gz"
BOS_DATA="bos_data.tar.gz"
LOCAL_DATA="local_data.tar.gz"

hdfs_download=1
kafka_download=1
bos_download=1
local_download=1

if [[ "${hdfs_download}" -eq 1 ]]; then
    echo "============Download HDFS DATA==========="
    wget -O "${HDFS_DATA}" https://palo-qa.cdn.bcebos.com/pytest_data/hdfs_data.tar.gz
    rm -rf hdfs
    mkdir -p "${PYTEST_HOME}"/hdfs/
    tar xzf "${HDFS_DATA}" -C "${PYTEST_HOME}"/hdfs/ 
    echo "hdfs data download finished, need to put to hdfs"
fi

if [[ "${kafka_download}" -eq 1 ]]; then
    echo "============Download Kafka data=========="
    wget -O "${KAFKA_DATA}" https://palo-qa.cdn.bcebos.com/pytest_data/kafka_data.tar.gz
    rm -rf kafka
    mkdir -p "${PYTEST_HOME}"/kafka
    tar xzf "${KAFKA_DATA}" -C "${PYTEST_HOME}"/kafka/
    echo "kafka data download finished"
fi

if [[ "${bos_download}" -eq 1 ]]; then
    echo "============Download bos data============"
    wget -O "${BOS_DATA}" https://palo-qa.cdn.bcebos.com/pytest_data/bos_data.tar.gz
    rm -rf bos
    mkdir -p "${PYTEST_HOME}"/bos
    tar xzf "${BOS_DATA}" -C "${PYTEST_HOME}"/bos
    echo "bos data download finished, need to put to bos"
fi

if [[ "${local_download}" -eq 1 ]]; then
    echo "============Download local data=========="
    wget -O "${LOCAL_DATA}" https://palo-qa.cdn.bcebos.com/pytest_data/local_data.tar.gz
    if [[ -d "${PYTEST_HOME}"/sys/data ]]; then
        tar xzf "${LOCAL_DATA}" -C "${PYTEST_HOME}"/sys/data
    fi
    echo "local data download finished"
fi




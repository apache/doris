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

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# download tpch1_data
if [[ ! -d "${CUR_DIR}/tpch1.db" ]]; then
    echo "${CUR_DIR}/tpch1.db does not exist"
    cd ${CUR_DIR}/
    curl -O https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/tpch1.db.tar.gz
    tar -zxf tpch1.db.tar.gz
    rm -rf tpch1.db.tar.gz
    cd -
else
    echo "${CUR_DIR}/tpch1.db exist, continue !"
fi

# download tvf_data
if [[ ! -d "${CUR_DIR}/tvf_data" ]]; then
    echo "${CUR_DIR}/tvf_data does not exist"
    cd ${CUR_DIR}/
    curl -O https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/tvf_data.tar.gz
    tar -zxf tvf_data.tar.gz
    rm -rf tvf_data.tar.gz
    cd -
else
    echo "${CUR_DIR}/tvf_data exist, continue !"
fi


# download auxiliary jars
jars=(
    jdom-1.1.jar
    aliyun-java-sdk-core-3.4.0.jar
    aliyun-java-sdk-ecs-4.2.0.jar
    aliyun-java-sdk-ram-3.0.0.jar
    aliyun-java-sdk-sts-3.0.0.jar
    aliyun-sdk-oss-3.4.1.jar
    hadoop-aliyun-3.2.1.jar
    aws-java-sdk-bundle-1.11.375.jar
    hadoop-huaweicloud-3.1.1-hw-54.5.jar
    hadoop-cos-3.1.0-8.3.22.jar
    cos_api-bundle-5.6.244.4.jar
    hadoop-aws-3.2.1.jar
    paimon-hive-connector-3.1-1.3-SNAPSHOT.jar
    gcs-connector-hadoop3-2.2.24-shaded.jar
)

cd ${CUR_DIR}/auxlib
for jar in "${jars[@]}"; do
    curl -O "https://${s3BucketName}.${s3Endpoint}/regression/docker/hive3/${jar}"
done


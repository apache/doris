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

DORIS_PACKAGE=apache-doris-2.1.4-bin-x64
DORIS_DOWNLOAD_URL=https://apache-doris-releases.oss-accelerate.aliyuncs.com

md5_aws_java_sdk="452d1e00efb11bff0ee17c42a6a44a0a"
md5_hadoop_aws="a3e19d42cadd1a6862a41fd276f94382"
md5_hudi_bundle="a9cb8c752d1d7132ef3cfe3ead78a30d"
md5_jdk17="0930efa680ac61e833699ccc36bfc739"
md5_spark="b393d314ffbc03facdc85575197c5db9"
md5_doris="a4d8bc9730aca3a51294e87d7d5b3e8e"

download_source_file() {
    local FILE_PATH="$1"
    local EXPECTED_MD5="$2"
    local DOWNLOAD_URL="$3"

    echo "Download ${FILE_PATH}"

    if [[ -f "${FILE_PATH}" ]]; then
        local FILE_MD5
        FILE_MD5=$(md5sum "${FILE_PATH}" | awk '{ print $1 }')

        if [[ "${FILE_MD5}" = "${EXPECTED_MD5}" ]]; then
            echo "${FILE_PATH} is ready!"
        else
            echo "${FILE_PATH} is broken, Redownloading ..."
            rm "${FILE_PATH}"
            wget "${DOWNLOAD_URL}"/"${FILE_PATH}"
        fi
    else
        echo "Downloading ${FILE_PATH} ..."
        wget "${DOWNLOAD_URL}"/"${FILE_PATH}"
    fi
}

curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
cd "${curdir}" || exit

if [[ ! -d "packages" ]]; then
    mkdir packages
fi
cd packages || exit

download_source_file "aws-java-sdk-bundle-1.12.48.jar" "${md5_aws_java_sdk}" "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.48"
download_source_file "hadoop-aws-3.3.1.jar" "${md5_hadoop_aws}" "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1"
download_source_file "hudi-spark3.4-bundle_2.12-0.14.1.jar" "${md5_hudi_bundle}" "https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.4-bundle_2.12/0.14.1"
download_source_file "openjdk-17.0.2_linux-x64_bin.tar.gz" "${md5_jdk17}" "https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL"
download_source_file "spark-3.4.2-bin-hadoop3.tgz" "${md5_spark}" "https://archive.apache.org/dist/spark/spark-3.4.2"
download_source_file "${DORIS_PACKAGE}.tar.gz" "${md5_doris}" "${DORIS_DOWNLOAD_URL}"

if [[ ! -f "jdk-17.0.2/SUCCESS" ]]; then
    echo "Prepare jdk17 environment"
    if [[ -d "jdk-17.0.2" ]]; then
        echo "Remove broken jdk-17.0.2"
        rm -rf jdk-17.0.2
    fi
    echo "Unpackage jdk-17.0.2"
    tar xzf openjdk-17.0.2_linux-x64_bin.tar.gz
    touch jdk-17.0.2/SUCCESS
fi
if [[ ! -f "spark-3.4.2-bin-hadoop3/SUCCESS" ]]; then
    echo "Prepare spark3.4 environment"
    if [[ -d "spark-3.4.2-bin-hadoop3" ]]; then
        echo "Remove broken spark-3.4.2-bin-hadoop3"
        rm -rf spark-3.4.2-bin-hadoop3
    fi
    echo "Unpackage spark-3.4.2-bin-hadoop3"
    tar -xf spark-3.4.2-bin-hadoop3.tgz
    cp aws-java-sdk-bundle-1.12.48.jar spark-3.4.2-bin-hadoop3/jars/
    cp hadoop-aws-3.3.1.jar spark-3.4.2-bin-hadoop3/jars/
    cp hudi-spark3.4-bundle_2.12-0.14.1.jar spark-3.4.2-bin-hadoop3/jars/
    touch spark-3.4.2-bin-hadoop3/SUCCESS
fi
if [[ ! -f "doris-bin/SUCCESS" ]]; then
    echo "Prepare ${DORIS_PACKAGE} environment"
    if [[ -d "doris-bin" ]]; then
        echo "Remove broken ${DORIS_PACKAGE}"
        rm -rf doris-bin
    fi
    echo "Unpackage ${DORIS_PACKAGE}"
    tar xzf "${DORIS_PACKAGE}".tar.gz
    mv "${DORIS_PACKAGE}" doris-bin
    touch doris-bin/SUCCESS
fi

cd ../

docker compose -f hudi-compose.yml --env-file hudi-compose.env up -d
echo "Create hive table ..."
sleep 5
docker exec -it spark-hudi-hive sh -c "/opt/hadoop-3.3.1/bin/hadoop fs -chmod 777 /tmp/hive"
docker exec -it spark-hudi-hive sh -c "hive -f /opt/scripts/hive-minio.sql"
echo "Build hive catalog in Doris ..."
sleep 5
docker exec -it spark-hudi-hive sh -c "mysql -u root -h doris-hudi-env -P 9030 < /opt/scripts/doris-hudi.sql"
echo "======================================================"
echo "Success to launch spark+doris+hudi+minio environments!"
echo "./login-spark.sh to login into spark"
echo "./login-doris.sh to login into doris"
echo "======================================================"

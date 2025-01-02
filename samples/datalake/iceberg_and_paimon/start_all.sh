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

set -e

DORIS_PACKAGE=apache-doris-2.1.5-bin-x64
DORIS_DOWNLOAD_URL=https://apache-doris-releases.oss-accelerate.aliyuncs.com

download_source_file() {
    local FILE_PATH="$1"
    local EXPECTED_MD5="$2"
    local DOWNLOAD_URL="$3"

    echo "solve for ${FILE_PATH} ..."

    if [[ -f "${FILE_PATH}" ]]; then
        local FILE_MD5
        echo "compare md5sum ..."
        FILE_MD5=$(md5sum "${FILE_PATH}" | awk '{ print $1 }')

        if [[ "${FILE_MD5}" = "${EXPECTED_MD5}" ]]; then
            echo "${FILE_PATH} is ready!"
        else
            echo "${FILE_PATH} is broken, Redownloading ..."
            rm "${FILE_PATH}"
            wget "${DOWNLOAD_URL}"/"${FILE_PATH}"
        fi
    else
        echo "downloading ${FILE_PATH} ..."
        wget "${DOWNLOAD_URL}"/"${FILE_PATH}"
    fi
}

curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
cd "${curdir}" || exit

if [[ ! -d "packages" ]]; then
    mkdir packages
fi
cd packages || exit

download_source_file "${DORIS_PACKAGE}.tar.gz" "0af6706854cedff46ee2210bd949e2e9" "${DORIS_DOWNLOAD_URL}"
download_source_file "jdk-8u202-linux-x64.tar.gz" "0029351f7a946f6c05b582100c7d45b7" "https://repo.huaweicloud.com/java/jdk/8u202-b08"
download_source_file "iceberg-aws-bundle-1.5.2.jar" "7087ac697254f8067d0f813521542263" "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2"
download_source_file "iceberg-flink-runtime-1.18-1.5.2.jar" "8e895288e6770eea69ea05ffbc918c1b" "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.2"
download_source_file "flink-connector-jdbc-3.1.2-1.18.jar" "5c99b637721dd339e10725b81ccedb60" "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18"
download_source_file "paimon-s3-0.8.0.jar" "3e510c634a21cbcdca4fd3b85786a20c" "https://repo1.maven.org/maven2/org/apache/paimon/paimon-s3/0.8.0"
download_source_file "paimon-flink-1.18-0.8.0.jar" "f590d94af1b923a7c68152b558d5b25b" "https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/0.8.0"
download_source_file "paimon-spark-3.5-0.8.0.jar" "963d0c17d69034ecf77816f64863fc51" "https://repo1.maven.org/maven2/org/apache/paimon/paimon-spark-3.5/0.8.0"
download_source_file "flink-shaded-hadoop-2-uber-2.8.3-10.0.jar" "f6f0be5b9cbebfd43e38121b209f4ecc" "https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0"
download_source_file "flink-s3-fs-hadoop-1.18.0.jar" "60b75e0fdc5ed05f1213b593c4b66556" "https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.0"

if [[ ! -f "jdk1.8.0_202/SUCCESS" ]]; then
    echo "Prepare jdk8 environment"
    if [[ -d "jdk1.8.0_202" ]]; then
        echo "Remove broken jdk1.8.0_202"
        rm -rf jdk1.8.0_202
    fi
    echo "Unpackage jdk1.8.0_202"
    tar xzf jdk-8u202-linux-x64.tar.gz
    touch jdk1.8.0_202/SUCCESS
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

if [[ ! -f "jars/SUCCESS" ]]; then
    echo "Prepare jars environment"
    if [[ -d "jars" ]]; then
        echo "Remove broken jars"
        rm -rf jars
    fi
    mkdir jars
    cp ./*.jar jars/
    touch jars/SUCCESS
fi

cd ../

echo "Start docker-compose..."
sudo docker compose -f docker-compose.yml --env-file docker-compose.env up -d

echo "Start init iceberg and paimon tables..."
sudo docker exec -it doris-iceberg-paimon-jobmanager sql-client.sh -f /opt/flink/sql/init_tables.sql | tee -a init.log >/dev/null

echo "Start prepare data for tables..."
sudo docker exec -it doris-iceberg-paimon-spark spark-sql --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions -f /opt/sql/prepare_data.sql | tee -a init.log >/dev/null

echo "============================================================================="
echo "Success to launch doris+iceberg+paimon+flink+spark+minio environments!"
echo "You can:"
echo "    'bash start_doris_client.sh' to login into doris"
echo "    'bash start_flink_client.sh' to login into flink"
echo "    'bash start_spark_paimon_client.sh' to login into spark for paimon"
echo "    'bash start_spark_iceberg_client.sh' to login into spark for iceberg"
echo "============================================================================="

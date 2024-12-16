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

DORIS_PACKAGE=apache-doris-3.0.1-bin-x64
DORIS_DOWNLOAD_URL=https://apache-doris-releases.oss-accelerate.aliyuncs.com

md5_aws_java_sdk="452d1e00efb11bff0ee17c42a6a44a0a"
md5_hadoop_aws="a3e19d42cadd1a6862a41fd276f94382"
md5_jdk17="0930efa680ac61e833699ccc36bfc739"
md5_spark="b393d314ffbc03facdc85575197c5db9"
md5_doris="fecd81c2d043542363422de6f111dbdb"
delta_core="65b8dec752d4984b7958d644848e3978"
delta_storage="a83011a52c66e081d4f53a7dc5c9708a"
antlr4_runtime="718f199bafa6574ffa1111fa3e10276a"
kudu_plugin="2d58bfcac5b84218c5d1055af189e30c"
delta_plugin="6b33448ce42d3d05e7b500ccafbe9698"
hdfs_plugin="ff4a3e3b32dcce27f4df58f17938abde"
kudu_java_example="1afe0a890785e8d0011ea7342ae5e43d"


download_source_file() {
    local FILE_PATH="$1"
    local EXPECTED_MD5="$2"
    local DOWNLOAD_URL="$3"

    echo "Download ${FILE_PATH}"

    if [[ -f "${FILE_PATH}" ]]; then
        local FILE_MD5
        FILE_MD5=$(md5sum "${FILE_PATH}" | awk '{ print $1 }')
        echo "${FILE_PATH} 's md5sum is = ${FILE_MD5} ; expected is = ${EXPECTED_MD5}"
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
download_source_file "openjdk-17.0.2_linux-x64_bin.tar.gz" "${md5_jdk17}" "https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL"
download_source_file "spark-3.4.2-bin-hadoop3.tgz" "${md5_spark}" "https://archive.apache.org/dist/spark/spark-3.4.2"
download_source_file "${DORIS_PACKAGE}.tar.gz" "${md5_doris}" "${DORIS_DOWNLOAD_URL}"
download_source_file "delta-core_2.12-2.4.0.jar" "${delta_core}" "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0"
download_source_file "delta-storage-2.4.0.jar" "${delta_storage}" "https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0"
download_source_file "antlr4-runtime-4.9.3.jar" "${antlr4_runtime}" "https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3"
download_source_file "trino-delta-lake-435-20240724.tar.gz" "${delta_plugin}" "https://github.com/apache/doris-thirdparty/releases/download/trino-435-20240724"
download_source_file "trino-kudu-435-20240724.tar.gz" "${kudu_plugin}" "https://github.com/apache/doris-thirdparty/releases/download/trino-435-20240724"
download_source_file "trino-hdfs-435-20240724.tar.gz" "${hdfs_plugin}" "https://github.com/apache/doris-thirdparty/releases/download/trino-435-20240724"
download_source_file "kudu-java-example-1.0-SNAPSHOT.jar" "${kudu_java_example}" "https://github.com/apache/doris-thirdparty/releases/download/trino-435-20240724"




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
    cp delta-core_2.12-2.4.0.jar spark-3.4.2-bin-hadoop3/jars/
    cp delta-storage-2.4.0.jar spark-3.4.2-bin-hadoop3/jars/
    cp antlr4-runtime-4.9.3.jar spark-3.4.2-bin-hadoop3/jars/
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

mkdir connectors
if [[ ! -f "connectors/trino-delta-lake-435/SUCCESS" ]]; then
    echo "Prepare trino-delta-lake-435 plugin"
    if [[ -d "connectors/trino-delta-lake-435" ]]; then
        echo "Remove broken trino-delta-lake-435"
        rm -rf connectors/trino-delta-lake-435
    fi
    echo "Unpackage trino-delta-lake-435"
    tar xzf trino-delta-lake-435-20240724.tar.gz
    mv trino-delta-lake-435 connectors/trino-delta-lake-435
    touch connectors/trino-delta-lake-435/SUCCESS
fi

if [[ ! -f "connectors/trino-kudu-435/SUCCESS" ]]; then
    echo "Prepare trino-kudu-435 plugin"
    if [[ -d "connectors/trino-kudu-435" ]]; then
        echo "Remove broken trino-kudu-435"
        rm -rf connectors/trino-kudu-435
    fi
    echo "Unpackage trino-kudu-435"
    tar xzf trino-kudu-435-20240724.tar.gz
    mv trino-kudu-435 connectors/trino-kudu-435
    touch connectors/trino-kudu-435/SUCCESS
fi

if [[ ! -f "connectors/trino-delta-lake-435/hdfs/SUCCESS" ]]; then
    echo "Prepare hdfs plugin"
    if [[ -d "connectors/trino-delta-lake-435/hdfs" ]]; then
        echo "Remove broken connectors/trino-delta-lake-435/hdfs"
        rm -rf connectors/trino-delta-lake-435/hdfs
    fi
    echo "Unpackage trino-delta-lake-435/hdfs"
    tar xzf trino-hdfs-435-20240724.tar.gz
    mv hdfs connectors/trino-delta-lake-435/hdfs
    touch connectors/trino-delta-lake-435/hdfs/SUCCESS
fi


cd ../

export KUDU_QUICKSTART_IP=$(ifconfig | grep "inet " | grep -Fv 127.0.0.1 |  awk '{print $2}' | tail -1)

docker compose -f trinoconnector-compose.yml --env-file trinoconnector-compose.env up -d
echo "Create hive table ..."
sleep 5
docker exec -it spark-hive sh -c "/opt/hadoop-3.3.1/bin/hadoop fs -chmod 777 /tmp/hive"
docker exec -it spark-hive sh -c "sh /opt/scripts/create-delta-table.sh"
sleep 5
echo "Build hive catalog in Doris ..."
docker exec -it spark-hive sh -c "mysql -u root -h doris-env -P 9030 < /opt/scripts/doris-sql.sql"
sleep 10
echo "Create kudu table ..."
docker exec -it kudu-master-1 sh -c "/opt/jdk-17.0.2/bin/java -DkuduMasters=kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251 -jar /opt/kudu-java-example-1.0-SNAPSHOT.jar"
sleep 10

echo "======================================================"
echo "Success to launch spark+doris+deltalake+kudu+minio environments!"
echo "./login-spark.sh to login into spark"
echo "./login-doris.sh to login into doris"
echo "======================================================"

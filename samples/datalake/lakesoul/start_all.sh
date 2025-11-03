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

# DORIS_PACKAGE=apache-doris-3.0.2-bin-x64
# DORIS_DOWNLOAD_URL=https://apache-doris-releases.oss-accelerate.aliyuncs.com
DORIS_PACKAGE=apache-doris-3.0.2-lakesoul-bin-x64
DORIS_DOWNLOAD_URL=https://lakesoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/doris
LAKESOUL_VERSION=2.6.1
FLINK_LAKESOUL_JAR=lakesoul-flink-1.17-${LAKESOUL_VERSION}.jar
SPARK_LAKESOUL_JAR=lakesoul-spark-3.3-${LAKESOUL_VERSION}.jar

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
        echo "downloading ${FILE_PATH} ...",
        wget "${DOWNLOAD_URL}"/"${FILE_PATH}"
    fi
}

unpack_tar() {
    local TAR_FILE_PATH="$1"
    local OUT_DIR="$2"

    echo "unpack for ${TAR_FILE_PATH} ..."

    if [[ ! -f "${OUT_DIR}/SUCCESS" ]]; then
        echo "Prepare ${OUT_DIR} environment"
        if [[ -d "${OUT_DIR}" ]]; then
            echo "Remove broken ${OUT_DIR}"
            rm -rf ${OUT_DIR}
        fi
        echo "Unpackage ${TAR_FILE_PATH}"
        tar xzf "${TAR_FILE_PATH}"
        touch "${OUT_DIR}"/SUCCESS
    fi
}


curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
cd "${curdir}" || exit

if [[ ! -d "packages" ]]; then
    mkdir packages
fi
cd packages || exit

download_source_file "${DORIS_PACKAGE}.tar.gz" "b08d23cef21a34cee3a8ce337bed559f" "${DORIS_DOWNLOAD_URL}"

download_source_file "${FLINK_LAKESOUL_JAR}" "de38f63ebd44835e9a37412908a0cdc0" "https://github.com/lakesoul-io/LakeSoul/releases/download/v${LAKESOUL_VERSION}"

download_source_file "${SPARK_LAKESOUL_JAR}" "31a923958fb5398bbd00de073a88a787" "https://github.com/lakesoul-io/LakeSoul/releases/download/v${LAKESOUL_VERSION}"

download_source_file "openjdk-17_linux-x64_bin.tar.gz" "5d1e9bc9be1570768485df4ff665821d" "https://mirrors.huaweicloud.com/openjdk/17/"
unpack_tar "openjdk-17_linux-x64_bin.tar.gz" "jdk-17"

# download_source_file "hadoop-3.3.5.tar.gz" "1b6175712d813e8baec48ed68098ca85" "https://dlcdn.apache.org/hadoop/common/hadoop-3.3.5"
download_source_file "hadoop-3.3.5.tar.gz" "1b6175712d813e8baec48ed68098ca85" "https://lakesoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/doris"
unpack_tar "hadoop-3.3.5.tar.gz" "hadoop-3.3.5"


download_source_file "flink-s3-fs-hadoop-1.17.1.jar" "0a631b07ba3e3b6c54e7c7c920ac6487" "https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.17.1"
download_source_file "parquet-hadoop-bundle-1.12.3.jar" "3a78d684a1938e68c6a57f59863e9106" "https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop-bundle/1.12.3"
download_source_file "flink-parquet-1.17.1.jar" "559fda5535d4018fb923c4ec198340f0" "https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.17.1"


if [[ ! -f "doris-bin/SUCCESS" ]]; then
    echo "Prepare ${DORIS_PACKAGE} environment"
    if [[ -d "doris-bin" ]]; then
        echo "Remove broken ${DORIS_PACKAGE}"
        rm -rf doris-bin
    fi
    echo "Unpackage ${DORIS_PACKAGE}"
    tar xzf "${DORIS_PACKAGE}".tar.gz

    mv "${DORIS_PACKAGE}" doris-bin
    sed -i 's/-lt 2000000 ]]/-lt 60000 ]]/g' doris-bin/be/bin/start_be.sh
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

if [[ ! -f "tpch-dbgen/SUCCESS" ]]; then
    echo "Load tpch data..."
    if [[ -d "tpch-dbgen" ]]; then
        echo "Remove broken tpch-dbgen"
        rm -rf tpch-dbgen
    fi

    git clone https://github.com/databricks/tpch-dbgen.git
    cd tpch-dbgen
    make
    ./dbgen -f -s 0.1

    touch SUCCESS
    cd ../
fi

cd ../

echo "Start docker-compose..."
sudo docker compose -f docker-compose.yml --env-file docker-compose.env up -d

echo "Start prepare data for lakesoul tables..."
sudo docker exec -it doris-lakesoul-spark spark-sql --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.buffer.dir=/opt/spark/work-dir/s3a --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider --conf spark.hadoop.fs.s3a.access.key=admin --conf spark.hadoop.fs.s3a.secret.key=password -f /opt/sql/prepare_data.sql | tee -a init.log >/dev/null


echo "============================================================================="
echo "Success to launch doris+iceberg+paimon+flink+spark+minio environments!"
echo "You can:"
echo "    'bash start_doris_client.sh' to login into doris"
echo "    'bash start_flink_client.sh' to login into flink"
echo "    'bash start_spark_sql.sh' to login into spark"
echo "    'bash start_mysql_client.sh' to login into mysql for test"
echo "============================================================================="

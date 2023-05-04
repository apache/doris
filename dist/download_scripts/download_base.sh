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

###############################################################################
# This script is used to download Apache Doris release binaries
# This is the base script of all download scripts.
# For certain release, it should create a script and wrap
# this script, eg:
# sh download_1.2.3.sh                                                      \
#       apache-doris-fe-1.2.3-bin-x86_64                                    \
#       apache-doris-be-1.2.3-bin-x86_64                                    \
#       apache-doris-dependencies-1.2.3-bin-x86_64                          \
#       https://mirrors.tuna.tsinghua.edu.cn/apache/doris/1.2/1.2.3-rc02/   \
#       apache-doris-1.2.3-bin
###############################################################################

set -eo pipefail

FE="$1"
BE="$2"
DEPS="$3"
DOWNLOAD_LINK_PREFIX="$4"
DOWNLOAD_DIR="$5"
FILE_SUFFIX=".tar.xz"

# Check if curl cmd exists
if [[ -n $(command -v curl >/dev/null 2>&1) ]]; then
    echo "curl command not found on the system"
    exit 1
fi

# Check download dir
if [[ -f "${DOWNLOAD_DIR}" || -d "${DOWNLOAD_DIR}" ]]; then
    read -r -p "Download dir ${DOWNLOAD_DIR} already exists. Overwrite? [y/n] " resp
    if [[ "${resp}" = "y" || "${resp}" = "Y" ]]; then
        rm -rf "${DOWNLOAD_DIR}"
        echo "Origin ${DOWNLOAD_DIR} has been removed and created a new one".
    else
        echo "Please remove the ${DOWNLOAD_DIR} before downloading."
        exit 0
    fi
fi

mkdir "${DOWNLOAD_DIR}"

# Begin to download
FE_LINK="${DOWNLOAD_LINK_PREFIX}${FE}${FILE_SUFFIX}"
BE_LINK="${DOWNLOAD_LINK_PREFIX}${BE}${FILE_SUFFIX}"
DEPS_LINK="${DOWNLOAD_LINK_PREFIX}${DEPS}${FILE_SUFFIX}"

download() {
    MODULE="$1"
    LINK="$2"
    DIR="$3"
    echo "Begin to download ${MODULE} from \"${LINK}\" to \"${DIR}/\" ..."
    total_size=$(curl -sI "${LINK}" | grep -i Content-Length | awk '{print $2}' | tr -d '\r')
    echo "Total size: ${total_size} Bytes"
    echo "curl -# ${LINK} | tar xJ -C ${DIR}/"
    curl -# "${LINK}" | tar xJ -C "${DIR}/"
}

download "FE" "${FE_LINK}" "${DOWNLOAD_DIR}"
download "BE" "${BE_LINK}" "${DOWNLOAD_DIR}"
download "DEPS" "${DEPS_LINK}" "${DOWNLOAD_DIR}"

# Assemble
echo "Begin to assemble the binaries ..."

echo "Move java-udf-jar-with-dependencies.jar to be/lib/ ..."
mv "${DOWNLOAD_DIR}/${DEPS}/java-udf-jar-with-dependencies.jar" "${DOWNLOAD_DIR}/${BE}/lib"

echo "Download complete!"
echo "You can now deploy Apache Doris from ${DOWNLOAD_DIR}/"

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

FE="apache-doris-fe-1.2.4.1-bin-x86_64"
BE="apache-doris-be-1.2.4.1-bin-x86_64"
DEPS="apache-doris-dependencies-1.2.4.1-bin-x86_64"
DOWNLOAD_LINK_PREFIX="https://mirrors.tuna.tsinghua.edu.cn/apache/doris/1.2/1.2.4.1-rc01/"
DOWNLOAD_DIR="apache-doris-1.2.4.1-bin"

# Check and download download_base.sh
DOWNLOAD_BASE_SCRIPTS="download_base.sh"

if [[ ! -f "${DOWNLOAD_BASE_SCRIPTS}" ]]; then
    curl -O https://raw.githubusercontent.com/apache/doris/master/dist/download_scripts/download_base.sh &&
        chmod a+x "${DOWNLOAD_BASE_SCRIPTS}"
fi

# Begin to download
./"${DOWNLOAD_BASE_SCRIPTS}" "${FE}" "${BE}" "${DEPS}" "${DOWNLOAD_LINK_PREFIX}" "${DOWNLOAD_DIR}"

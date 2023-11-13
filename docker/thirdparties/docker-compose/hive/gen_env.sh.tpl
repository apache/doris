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

####################################################################
# This script will generate hadoop-hive.env from hadoop-hive.env.tpl
####################################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
FS_PORT=8020
HMS_PORT=9083

cp "${ROOT}"/hadoop-hive.env.tpl "${ROOT}"/hadoop-hive.env
# Need to set hostname of container to same as host machine's.
# Otherwise, the doris process can not connect to namenode directly.
HOST_NAME="doris--"

{
    echo "FS_PORT=${FS_PORT}"
    echo "HMS_PORT=${HMS_PORT}"
    echo "CORE_CONF_fs_defaultFS=hdfs://${externalEnvIp}:${FS_PORT}"
    echo "HOST_NAME=${HOST_NAME}"
    echo "externalEnvIp=${externalEnvIp}"

} >>"${ROOT}"/hadoop-hive.env

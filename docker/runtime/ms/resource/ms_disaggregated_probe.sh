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

DORIS_HOME=${DORIS_HOME:="/opt/apache-doris"}
CONFIG_FILE="$DORIS_HOME/ms/conf/doris_cloud.conf"
DEFAULT_BRPC_LISTEN_PORT=5000
PROBE_TYPE=$1

log_stderr()
{
    echo "[`date`] $@" >&2
}

function parse_config_file_with_key()
{
    local key=$1
    local value=`grep "^\s*$key\s*=" $CONFIG_FILE | sed "s|^\s*$key\s*=\s*\(.*\)\s*$|\1|g"`
    echo $value
}

function alive_probe()
{
    local brpc_listen_port=$(parse_config_file_with_key "brpc_listen_port")
    brpc_listen_port=${brpc_listen_port:=$DEFAULT_BRPC_LISTEN_PORT}
    if netstat -lntp | grep ":$brpc_listen_port" > /dev/null ; then
        exit 0
    else
        exit 1
    fi
}

function ready_probe()
{
    local brpc_listen_port=$(parse_config_file_with_key "brpc_listen_port")
    brpc_listen_port=${brpc_listen_port:=$DEFAULT_BRPC_LISTEN_PORT}
    if netstat -lntp | grep ":$brpc_listen_port" > /dev/null ; then
        exit 0
    else
        exit 1
    fi
}

if [[ "$PROBE_TYPE" == "ready" ]]; then
    ready_probe
else
    alive_probe
fi

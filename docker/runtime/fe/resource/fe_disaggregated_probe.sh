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

#

DORIS_HOEM=${DORIS_HOME:="/opt/apache-doris"}
CONFIG_FILE="$DORIS_HOME/fe/conf/fe.conf"
DEFAULT_HTTP_PORT=8030
DEFAULT_QUERY_PORT=9030
PROBE_TYPE=$1

log_stderr()
{
    echo "[`date`] $@" >&2
}

function parse_config_file_with_key()
{
    local confkey=$1

    esc_key=$(printf '%s\n' "$confkey" | sed 's/[[\.*^$()+?{|]/\\&/g')
    local confvalue=$(
        grep -v '^[[:space:]]*#' "$CONFIG_FILE" |
        grep -E "^[[:space:]]*${esc_key}[[:space:]]*=" |
        tail -n1 |
        sed -E 's/^[[:space:]]*[^=]+[[:space:]]*=[[:space:]]*//' |
        sed -E 's/[[:space:]]*#.*$//' |
        sed -E 's/^[[:space:]]+|[[:space:]]+$//g'
    )
    log_stderr "[info] read 'fe.conf' config [ $confkey: $confvalue]"
    echo "$confvalue"
}

parse_tls_connection_variables()
{
    ENABLE_TLS=$(parse_config_file_with_key "enable_tls")
    TLS_PRIVATE_KEY_PATH=$(parse_config_file_with_key "tls_private_key_path")
    TLS_CERTIFICATE_PATH=$(parse_config_file_with_key "tls_certificate_path")
    TLS_CA_CERTIFICATE_PATH=$(parse_config_file_with_key "tls_ca_certificate_path")
}

function alive_probe()
{
    local query_port=$(parse_config_file_with_key "query_port")
    query_port=${query_port:=$DEFAULT_QUERY_PORT}
    if netstat -lntp | grep ":$query_port" > /dev/null ; then
        exit 0
    else
        exit 1
    fi
}

ready_probe_with_no_tls()
{
    local http_port=$(parse_config_file_with_key "http_port")
    http_port=${http_port:=$DEFAULT_HTTP_PORT}
    local host=`hostname -f`
    local url="http://${host}:${http_port}/api/health"

    local response=$(curl -s -w "\n%{http_code}" $url)
    local http_code=$(echo "$response" | tail -n1)

    if [[ "$http_code" != "200" ]]; then
        exit 1
    fi

    local res=$(echo "$response" | sed '$d')
    local code=$(jq -r ".code" <<< $res)
    if [[ "x$code" == "x0" ]]; then
        exit 0
    else
        exit 1
    fi
}

ready_probe_with_tls()
{
    local http_port=$(parse_config_file_with_key "http_port")
    http_port=${http_port:=$DEFAULT_HTTP_PORT}
    local host=`hostname -f`
    local url="https://${host}:${http_port}/api/health"
    local response=$(curl --tlsv1.2 --cert $TLS_CERTIFICATE_PATH --cacert $TLS_CA_CERTIFICATE_PATH --key $TLS_PRIVATE_KEY_PATH -s -w "\n%{http_code}" $url)
    local http_code=$(echo "$response" | tail -n1)

    if [[ "$http_code" != "200" ]]; then
        exit 1
    fi

    local res=$(echo "$response" | sed '$d')
    local code=$(jq -r ".code" <<< $res)
    if [[ "x$code" == "x0" ]]; then
        exit 0
    else
        exit 1
    fi
}

function ready_probe()
{
    if [[ "$ENABLE_TLS" == "true" ]]; then
        ready_probe_with_tls
    else
        ready_probe_with_no_tls
    fi
}

parse_tls_connection_variables
if [[ "$PROBE_TYPE" == "ready" ]]; then
    ready_probe
else
    alive_probe
fi

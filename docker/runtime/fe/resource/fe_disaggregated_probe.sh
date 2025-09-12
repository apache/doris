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

function parse_config_file_with_key()
{
    local key=$1
    local value=`grep "^\s*$key\s*=" $CONFIG_FILE | sed "s|^\s*$key\s*=\s*\(.*\)\s*$|\1|g"`
    echo $value
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
    local ip=`hostname -i | awk '{print $1}'`
    local url="http://${ip}:${http_port}/api/health"
    local res=$(curl -s $url)
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
    local res=$(curl --tlsv1.2 --cert $TLS_CERTIFICATE_PATH --cacert $TLS_CA_CERTIFICATE_PATH --key $TLS_PRIVATE_KEY_PATH -s $url)
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

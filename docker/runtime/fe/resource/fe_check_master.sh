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

check_first_fe_status()
{
    local start_time=$(date +%s)
    local expire_timeout=120
    local helper=$1
    local root_password="${DORIS_ROOT_PASSWORD:-}"
    local node_user="${DB_ADMIN_USER:-root}" node_password="${DB_ADMIN_PASSWD:-}"
    local auth_path="${AUTH_PATH:-/etc/basic_auth}"
    local has_node_credentials=false
    if [[ -f "$auth_path/password" ]]; then
        node_password=$(cat -- "$auth_path/password") || return 1
        has_node_credentials=true
        if [[ -f "$auth_path/username" ]]; then
            node_user=$(cat -- "$auth_path/username") || return 1
        fi
    elif [[ -n "${DB_ADMIN_USER:-}" || -n "${DB_ADMIN_PASSWD:-}" ]]; then
        has_node_credentials=true
    fi
    if [[ -n "${DORIS_ROOT_PASSWORD_FILE:-}" ]]; then
        root_password=$(cat -- "$DORIS_ROOT_PASSWORD_FILE") || return 1
    elif [[ -z "$root_password" && "$node_user" == root ]]; then
        root_password=$node_password
    fi
    local output now
    local mysql_args=(--connect-timeout 2 -h "$helper" -P "$FE_QUERY_PORT"
                      --skip-column-names --batch -e 'SHOW FRONTENDS;')
    while true; do
        if output=$(MYSQL_PWD="$root_password" timeout 15 mysql "${mysql_args[@]}" -u root 2>&1); then
            return 0
        fi
        if [[ "$has_node_credentials" == true && "$output" =~ (1045|28000|1820) ]]; then
            if output=$(MYSQL_PWD="$node_password" timeout 15 mysql "${mysql_args[@]}" -u "$node_user" 2>&1); then
                return 0
            fi
        fi
        now=$(date +%s)
        if (( now >= start_time + expire_timeout )); then
            echo "[$(date)] the first container is not started" >&2
            return 1
        fi
        sleep 2
    done
}

check_first_fe_status "$1"


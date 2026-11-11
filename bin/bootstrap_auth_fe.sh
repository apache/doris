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

# Container entrypoints source this file. The FE itself continues to accept only
# a hash in initial_root_password and never reads a password from the environment.
doris_read_root_password() {
    DORIS_BOOTSTRAP_PASSWORD="${DORIS_ROOT_PASSWORD:-}"
    if [[ -n "${DORIS_ROOT_PASSWORD_FILE:-}" ]]; then
        DORIS_BOOTSTRAP_PASSWORD=$(cat -- "$DORIS_ROOT_PASSWORD_FILE") || return 1
        return 0
    fi
    if [[ -n "$DORIS_BOOTSTRAP_PASSWORD" ]]; then
        return 0
    fi
    if [[ "${DB_ADMIN_USER:-root}" == root ]]; then
        DORIS_BOOTSTRAP_PASSWORD="${DB_ADMIN_PASSWD:-}"
    fi
}

doris_root_mysql() {
    MYSQL_PWD="${DORIS_BOOTSTRAP_PASSWORD:-}" command mysql "$@"
}

doris_expand_fe_config_value() {
    local remaining=$1 fe_home=$2 expanded="" reference name replacement
    local variable_pattern='\$\{([^}]*)\}'
    while [[ "$remaining" =~ $variable_pattern ]]; do
        reference=${BASH_REMATCH[0]}
        name=${BASH_REMATCH[1]}
        expanded+="${remaining%%"$reference"*}"
        remaining=${remaining#*"$reference"}
        if [[ "$name" == DORIS_HOME ]]; then
            expanded+="$fe_home"
            continue
        fi
        if [[ ! "$name" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ || ! -v "$name" ]]; then
            printf '%s\n' 'FE container configuration must use literal values or available environment variables.' >&2
            return 1
        fi
        replacement=${!name}
        expanded+="$replacement"
    done
    printf '%s' "$expanded$remaining"
}

doris_fe_custom_config_dir() {
    local config_file=$1 fe_home custom_dir
    fe_home=$(cd -- "$(dirname -- "$config_file")/.." && pwd) || return 1
    custom_dir=$(awk -F= '/^[[:space:]]*custom_config_dir[[:space:]]*=/ {
        value=$0; sub(/^[^=]*=[[:space:]]*/, "", value); sub(/[[:space:]]*$/, "", value)
    } END { print value }' "$config_file") || return 1
    custom_dir="${custom_dir:-$fe_home/conf}"
    doris_expand_fe_config_value "$custom_dir" "$fe_home"
}

doris_fe_config_value() {
    local config_file=$1 key=$2 custom_dir value fe_home
    custom_dir=$(doris_fe_custom_config_dir "$config_file") || return 1
    local files=("$config_file")
    if [[ -f "$custom_dir/fe_custom.conf" ]]; then
        files+=("$custom_dir/fe_custom.conf")
    fi
    value=$(awk -v key="$key" -F= '$0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
        value=$0; sub(/^[^=]*=[[:space:]]*/, "", value); sub(/[[:space:]]*$/, "", value)
    } END { print value }' "${files[@]}") || return 1
    fe_home=$(cd -- "$(dirname -- "$config_file")/.." && pwd) || return 1
    doris_expand_fe_config_value "$value" "$fe_home"
}

doris_append_fe_config() {
    local config_file=$1 key=$2 value=$3 custom_dir
    # DorisFE chooses this directory after fe.conf, before loading fe_custom.conf.
    custom_dir=$(doris_fe_custom_config_dir "$config_file") || return 1
    mkdir -p -- "$custom_dir" || return 1
    (umask 077; printf '\n%s = %s\n' "$key" "$value" >> "$custom_dir/fe_custom.conf")
}

doris_configure_test_mode() {
    local config_file=$1 value
    if [[ "${DORIS_ALLOW_EMPTY_PASSWORD:-false}" != true ]]; then
        return 0
    fi
    value=$(doris_fe_config_value "$config_file" allow_empty_password_for_root_admin) || return 1
    if [[ "${value,,}" == true ]]; then
        return 0
    fi
    doris_append_fe_config "$config_file" allow_empty_password_for_root_admin true || return 1
    printf '%s\n' 'SECURITY WARNING: empty root/admin passwords explicitly enabled for this container' >&2
}

# Only the node creating a cluster calls this function. A new FE joining an
# existing cluster must never be mistaken for a cluster initialization.
doris_prepare_bootstrap_auth() {
    local config_file=$1 meta_dir=$2
    doris_configure_test_mode "$config_file" || return 1
    local configured_meta
    configured_meta=$(doris_fe_config_value "$config_file" meta_dir) || return 1
    if [[ -n "$configured_meta" ]]; then
        meta_dir=$configured_meta
    fi
    if [[ -f "$meta_dir/image/VERSION" ]]; then
        return 0
    fi
    if [[ -n "${DORIS_BOOTSTRAP_PASSWORD:-}" ]]; then
        local password_hash
        password_hash=$(set -o pipefail; printf '%s' "$DORIS_BOOTSTRAP_PASSWORD" \
            | openssl dgst -sha1 -binary | openssl dgst -sha1 \
            | awk '{print "*" toupper($NF)}') || return 1
        if [[ ! "$password_hash" =~ ^\*[[:xdigit:]]{40}$ ]]; then
            printf '%s\n' 'Failed to hash the initial root password' >&2
            return 1
        fi
        doris_append_fe_config "$config_file" initial_root_password "$password_hash" || return 1
        return 0
    fi
    local allow_empty
    allow_empty=$(doris_fe_config_value "$config_file" allow_empty_password_for_root_admin) || return 1
    if [[ "${allow_empty,,}" == true ]]; then
        return 0
    fi
    # Operators can supply the hash directly in their FE configuration instead.
    local configured_hash
    configured_hash=$(doris_fe_config_value "$config_file" initial_root_password) || return 1
    if [[ "$configured_hash" =~ ^\*[[:xdigit:]]{40}$ ]]; then
        return 0
    fi
    printf '%s\n' 'New cluster requires DORIS_ROOT_PASSWORD_FILE, DORIS_ROOT_PASSWORD, or initial_root_password.' \
        'For test containers only, explicitly set DORIS_ALLOW_EMPTY_PASSWORD=true.' >&2
    return 1
}

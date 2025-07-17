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

set -eo pipefail
shopt -s nullglob

DORIS_HOME="/opt/apache-doris"
ENV_PREFIX="DORIS_"
CONFIG_FILE=${DORIS_HOME}/be/conf/be.conf
BE_ALREADY_EXISTS=false

# Obtain necessary and basic information to complete initialization

# logging functions
# usage: doris_[note|warn|error] $log_meg
#    ie: doris_warn "task may be risky!"
#   out: 2023-01-08T19:08:16+08:00 [Warn] [Entrypoint]: task may be risky!
doris_log() {
    local type="$1"
    shift
    # accept argument string or stdin
    local text="$*"
    if [ "$#" -eq 0 ]; then text="$(cat)"; fi
    local dt="$(date -Iseconds)"
    printf '%s [%s] [Entrypoint-init-be]: %s\n' "$dt" "$type" "$text"
}
doris_note() {
    doris_log Note "$@"
}
doris_warn() {
    doris_log Warn "$@" >&2
}
doris_error() {
    doris_log ERROR "$@" >&2
    exit 1
}

# check to see if this file is being run or sourced from another script
_is_sourced() {
    [ "${#FUNCNAME[@]}" -ge 2 ] &&
    [ "${FUNCNAME[0]}" = '_is_sourced' ] &&
    [ "${FUNCNAME[1]}" = 'source' ]
}

show_be_args(){
    doris_note "============= init args ================"
    doris_note "MASTER_FE_HOST " ${MASTER_FE_HOST}
    doris_note "CURRENT_BE_HOST " ${CURRENT_BE_HOST}
    doris_note "CURRENT_BE_PORT " ${CURRENT_BE_PORT}
}

# Execute sql script, passed via stdin
# usage: docker_process_sql sql_script
docker_process_sql() {
    set +e
    mysql -uroot -P9030 -h${MASTER_FE_HOST} --comments "$@" 2>/dev/null
}

register_be_to_fe() {
    set +e
    for i in {1..300}; do
        SQL="alter system add backend '${CURRENT_BE_HOST}:${CURRENT_BE_PORT}';"
        doris_note "Executing SQL: $SQL"
        docker_process_sql <<<"$SQL"

        register_be_status=$?
        if [[ $register_be_status == 0 ]]; then
            doris_note "BE successfully registered to FE!"
            BE_ALREADY_EXISTS=true
            return
        else
            check_be_status
            if [[ $BE_ALREADY_EXISTS == "true" ]]; then
                return
            fi
        fi

        if [[ $(( $i % 20 )) == 1 ]]; then
            doris_note "Register BE to FE is failed. Tried times ${i}, retry..."
        fi
        sleep 1
    done

    if [[ $BE_ALREADY_EXISTS != "true" ]]; then
        doris_error "be can not register in cluster! Byebye..."
    fi 
    
    doris_note "current be has been registered in cluster."
}

check_be_status() {
    set +e

    BE_ALREADY_EXISTS=false
    doris_warn "start check be register status~"
    docker_process_sql <<<"show backends" | grep "[[:space:]]${CURRENT_BE_HOST}[[:space:]]" | grep "[[:space:]]${CURRENT_BE_PORT}[[:space:]]"

    be_join_status=$?
    if [[ "${be_join_status}" == 0 ]]; then
        doris_note "Verify that BE is registered to FE successfully"
        BE_ALREADY_EXISTS=true
    else
        doris_note "current be has not been registered in cluster."
    fi
}

cleanup() {
    doris_note "Container stopped, running stop_be script"
    ${DORIS_HOME}/be/bin/stop_be.sh
}

# Convert a string to lowercase (portable implementation)
to_lowercase() {
    echo "$1" | tr '[:upper:]' '[:lower:]'
}

# Conversion function: Convert config file key to environment variable key
# Example: fqdn → DORIS_FQDN
convert_config_to_env_key() {
    config_key="$1"
    # Add prefix and convert to uppercase (adjust as needed)
    echo "${ENV_PREFIX}${config_key}" | tr '[:lower:]' '[:upper:]'
}

reset_conf_by_env(){
    
    if [ ! -f "$CONFIG_FILE" ]; then
        touch "$CONFIG_FILE"
    fi
    
    # Temporary file to store processed configuration
    TEMP_CONFIG=$(mktemp)
    
    # Mark processed environment variables
    processed_vars=""
    
    # Process existing config lines
    while IFS= read -r line || [ -n "$line" ]; do
        # Skip empty lines/comments
        case "$line" in
            ''|*[![:print:]]*) echo "$line" >> "$TEMP_CONFIG"; continue ;;
            \#*) echo "$line" >> "$TEMP_CONFIG"; continue ;;
        esac
    
        # Extract key/value
        config_key=$(echo "$line" | cut -d= -f1 | tr -d '[:space:]')
        config_value=$(echo "$line" | cut -d= -f2- | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        
        # Get corresponding env variable
        env_var=$(convert_config_to_env_key "$config_key")
        env_value=$(eval echo "\$$env_var")
    
        # Skip if env variable not set
        [ -z "$env_value" ] && echo "$line" >> "$TEMP_CONFIG" && continue
    
        # Only replace if values differ (case-sensitive comparison)
        if [ "$config_value" != "$env_value" ]; then
            echo "$config_key = $env_value" >> "$TEMP_CONFIG"
            doris_note "Replaced: $config_key = $config_value → $env_value"
        else
            echo "$line" >> "$TEMP_CONFIG"
            doris_note "Skipped (same value): $config_key = $config_value"
        fi
        
        processed_vars="$processed_vars $(to_lowercase "$env_var")"
    done < "$CONFIG_FILE"
    
    # Flag to track if we've added the comment line
    added_comment=0
    for env_var in $(env | cut -d= -f1); do
        # Skip variables without prefix
        case "$(to_lowercase "$env_var")" in
            "$(to_lowercase "$ENV_PREFIX")"*) ;;
            *) continue ;;
        esac
    
        # Normalize for case-insensitive checks
        env_var_lower=$(to_lowercase "$env_var")
        
        # Skip if already processed
        case " $processed_vars " in
            *" $env_var_lower "*) continue ;;
        esac
    
        # Extract config key by stripping prefix
        config_key=$(echo "$env_var" | sed "s/^$ENV_PREFIX//" | tr '[:upper:]' '[:lower:]')
        env_value=$(eval echo "\$$env_var")
        
        # Add comment line before first new entry
        if [ $added_comment -eq 0 ]; then
            echo "# Added from environment variables" >> "$TEMP_CONFIG"
            added_comment=1
        fi

        # Add new config entry
        echo "$config_key = $env_value" >> "$TEMP_CONFIG"
        doris_note "Added: $config_key = $env_value (from $env_var)"
    done
    
    # Replace original config file
    mv "$TEMP_CONFIG" "$CONFIG_FILE"
    
    doris_note "Configuration file '$CONFIG_FILE' processing completed by env"
}

_main() {
    trap 'cleanup' SIGTERM SIGINT

    show_be_args
    reset_conf_by_env
    register_be_to_fe

    doris_note "Ready to start BE!"
    export SKIP_CHECK_ULIMIT=true
    ${DORIS_HOME}/be/bin/start_be.sh --console &
    child_pid=$!
    wait $child_pid
    exec "$@"
}

if ! _is_sourced; then
    _main "$@"
fi

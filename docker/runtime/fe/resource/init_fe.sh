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
CONFIG_FILE="${1:-${DORIS_HOME}/fe/conf/fe.conf}"

# Obtain necessary and basic information to complete initialization

# logging functions
# usage: doris_[note|warn|error] $log_meg
#    ie: doris_warn "task may fe risky!"
#   out: 2023-01-08T19:08:16+08:00 [Warn] [Entrypoint]: task may fe risky!
doris_log() {
    local type="$1"
    shift
    # accept argument string or stdin
    local text="$*"
    if [ "$#" -eq 0 ]; then text="$(cat)"; fi
    local dt="$(date -Iseconds)"
    printf '%s [%s] [Entrypoint]: %s\n' "$dt" "$type" "$text"
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

# Check the variables required for   startup
docker_required_variables_env() {
    declare -g RUN_TYPE
    if [ -n "$BUILD_TYPE" ]; then
        RUN_TYPE="K8S"
        if [[ $BUILD_TYPE =~ ^([kK]8[sS])$ ]]; then
            doris_warn "BUILD_TYPE" $BUILD_TYPE
        else
            doris_error "BUILD_TYPE rule error! example: [k8s], Default Value: docker."
        fi
        return
    fi

    RUN_TYPE=DOCKER
    check_arg "MASTER_FE" $MASTER_FE
    check_arg "CURRENT_FE" $CURRENT_FE
    
    if [[ -n "$RECOVERY" ]]; then
        if [[ $RECOVERY =~ true ]]; then
            RUN_TYPE="RECOVERY"
            doris_warn "RECOVERY " $RECOVERY
        else
            doris_error "RECOVERY value error! Only Support 'true'!"
        fi
    fi
    
}

get_doris_fe_args() {

    if [[ $RUN_TYPE == "K8S" ]]; then
        doris_warn "get_doris_fe_args is not supported in K8S mode."
        return
    fi

    declare -g CURRENT_FE_IS_MASTER MASTER_FE_HOST MASTER_FE_EDIT_PORT CURRENT_FE_HOST CURRENT_FE_EDIT_PORT DATABASE_ALREADY_EXISTS 
    if [ ${MASTER_FE} == ${CURRENT_FE} ]; then
        CURRENT_FE_IS_MASTER=true
    else
        CURRENT_FE_IS_MASTER=false
    fi

    MASTER_FE_HOST=$(echo "${MASTER_FE}" | awk -F ':' '{ sub(/ /, ""); print$1}')
    MASTER_FE_EDIT_PORT=$(echo "${MASTER_FE}" | awk -F ':' '{ sub(/ /, ""); print$2}')

    CURRENT_FE_HOST=$(echo "${CURRENT_FE}" | awk -F ':' '{ sub(/ /, ""); print$1}')
    CURRENT_FE_EDIT_PORT=$(echo "${CURRENT_FE}" | awk -F ':' '{ sub(/ /, ""); print$2}')

    if [[ -z $DORIS_PRIORITY_NETWORKS ]]; then
       export DORIS_PRIORITY_NETWORKS=$(echo "${CURRENT_FE_IP}" | awk -F '.' '{print$1"."$2"."$3".0/24"}')
       check_arg "PRIORITY_NETWORKS" $DORIS_PRIORITY_NETWORKS
    fi

    doris_note "MASTER_FE = ${MASTER_FE_HOST}:${MASTER_FE_EDIT_PORT}"
    doris_note "CURRENT_FE = ${CURRENT_FE_HOST}:${CURRENT_FE_EDIT_PORT}"
    doris_note "PRIORITY_NETWORKS = ${DORIS_PRIORITY_NETWORKS}"
    
    if [ -d "${DORIS_HOME}/fe/doris-meta/image" ]; then
        DATABASE_ALREADY_EXISTS='true'
        doris_note "the image is exsit!"
    fi
}

# Execute sql script, passed via stdin
# usage: docker_process_sql sql_script
docker_process_sql() {
    set +e
    mysql -uroot -P9030 -h${MASTER_FE_HOST} --comments "$@" 2>/dev/null
}

register_follower() {
    if [[ $RUN_TYPE == "K8S" ]]; then
        doris_warn "register_follower is not supported in K8S mode."
        return
    fi

    set +e
    if [[ ${CURRENT_FE_IS_MASTER} == true ]]; then
        doris_note "Current FE is Master FE! No need to register again!"
        return
    fi

    for i in {1..60}; do
        sql="alter system add FOLLOWER '${CURRENT_FE}'"
        doris_note "EXECUTE SQL: $sql"

        docker_process_sql <<<$sql

        register_fe_status=$?
        if [[ $register_fe_status == 0 ]]; then
            doris_note "FE successfully registered!"
            CURRENT_FE_ALREADY_EXISTS=true
            break
        else
            check_fe_status
            if [[ $CURRENT_FE_ALREADY_EXISTS == "true" ]]; then
                break
            fi
        fi

        if [[ $(($i % 20)) == 1 ]]; then
            doris_note "ADD FOLLOWER failed, tried ${i} times, retry..."
        fi
        sleep 1
    done

    # don't exit, try start even though register failed in case of not the first time to start
    if [[ $CURRENT_FE_ALREADY_EXISTS != "true" ]]; then
        doris_warn "Failed to register CURRENT_FE to FE! Maybe FE Start Failed!"
    else
        doris_note "current fe has been added in cluster."    
    fi
}

# Check whether the passed parameters are empty to avoid subsequent task execution failures. At the same time,
# enumeration checks can fe added, such as checking whether a certain parameter appears repeatedly, etc.
check_arg() {
    if [ -z $2 ]; then
        doris_error "$1 is null!"
    fi
}

check_fe_status() {
    if [[ $RUN_TYPE == "K8S" ]]; then
        doris_warn "check_fe_status is not supported in K8S mode."
        return
    fi

    set +e
    declare -g CURRENT_FE_ALREADY_EXISTS
    if [[ ${CURRENT_FE_IS_MASTER} == true ]]; then
        doris_note "Current FE is Master FE!  No need check fe status!"
        return
    fi

    docker_process_sql <<<"show frontends" | grep "[[:space:]]${CURRENT_FE_HOST}[[:space:]]" | grep "[[:space:]]${CURRENT_FE_EDIT_PORT}[[:space:]]"
    
    fe_join_status=$?
    if [[ "${fe_join_status}" == 0 ]]; then
        doris_note "Verify that CURRENT_FE is registered to FE successfully"
        CURRENT_FE_ALREADY_EXISTS=true
    else
        doris_warn "current fe has not been added in cluster."
    fi
}

cleanup() {
    doris_note "Container stopped, running stop_fe script"
    ${DORIS_HOME}/fe/bin/stop_fe.sh
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
        eval env_value="\$$env_var"
    
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
    docker_required_variables_env
    trap 'cleanup' SIGTERM SIGINT
    if [[ $RUN_TYPE == "K8S" ]]; then
        ${DORIS_HOME}/fe/bin/start_fe.sh --console &
        child_pid=$!
    else
        get_doris_fe_args
        reset_conf_by_env
        register_follower
        doris_note "Ready to start CURRENT_FE!"
        
        if [[ $RUN_TYPE == "RECOVERY" ]]; then
            ${DORIS_HOME}/fe/bin/start_fe.sh --console --metadata_failure_recovery &
            child_pid=$!
        elif [[ $CURRENT_FE_IS_MASTER == true ]]; then
            ${DORIS_HOME}/fe/bin/start_fe.sh --console &
            child_pid=$!
        else
            ${DORIS_HOME}/fe/bin/start_fe.sh --helper ${MASTER_FE} --console &
            child_pid=$!
        fi
    fi
    wait $child_pid
    exec "$@"
}

if ! _is_sourced; then
    _main "$@"
fi

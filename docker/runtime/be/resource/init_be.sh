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

# Constant Definition
readonly DORIS_HOME="/opt/apache-doris"
readonly MAX_RETRY_TIMES=60
readonly RETRY_INTERVAL=1
readonly MYSQL_PORT=9030
readonly BE_CONFIG_FILE="${DORIS_HOME}/be/conf/be.conf"

# Log Function
log_message() {
    local level="$1"
    shift
    local message="$*"
    if [ "$#" -eq 0 ]; then 
        message="$(cat)"
    fi
    local timestamp="$(date -Iseconds)"
    printf '%s [%s] [Entrypoint]: %s\n' "${timestamp}" "${level}" "${message}"
}

log_info() {
    log_message "INFO" "$@"
}

log_warn() {
    log_message "WARN" "$@" >&2
}

log_error() {
    log_message "ERROR" "$@" >&2
    exit 1
}

# Check whether it is a source file call
is_sourced() {
    [ "${#FUNCNAME[@]}" -ge 2 ] && 
    [ "${FUNCNAME[0]}" = 'is_sourced' ] && 
    [ "${FUNCNAME[1]}" = 'source' ]
}

# Initialize environment variables
init_environment() {
    declare -g database_exists
    if [ -d "${DORIS_HOME}/be/storage/data" ]; then
        database_exists='true'
    fi
}

# Check if the BE node is registered
check_be_registered() {
    # First, ensure that the FE node is available
    local retry_count=0
    while [ $retry_count -lt $MAX_RETRY_TIMES ]; do
        if mysql -uroot -P"${MYSQL_PORT}" -h"${MASTER_FE_IP}" \
            -N -e "SHOW FRONTENDS" 2>/dev/null | grep -w "${MASTER_FE_IP}" &>/dev/null; then
            log_info "Master FE is ready"
            break
        fi
        
        retry_count=$((retry_count + 1))
        if [ $((retry_count % 20)) -eq 1 ]; then
            log_info "Waiting for master FE to be ready... ($retry_count/$MAX_RETRY_TIMES)"
        fi
        sleep "$RETRY_INTERVAL"
    done

    if [ $retry_count -eq $MAX_RETRY_TIMES ]; then
        log_error "Master FE is not ready after ${MAX_RETRY_TIMES} attempts"
    fi

    # Check if BE is registered
    local query_result
    query_result=$(mysql -uroot -P"${MYSQL_PORT}" -h"${MASTER_FE_IP}" \
        -N -e "SHOW BACKENDS" 2>/dev/null | grep -w "${CURRENT_BE_IP}" | grep -w "${CURRENT_BE_PORT}" )
    
    if [ -n "$query_result" ]; then
        log_info "BE node ${CURRENT_BE_IP}:${CURRENT_BE_PORT} is already registered"
        return 0
    fi
    
    return 1
}

# Register BE node to FE
register_be() {
    # First check if the node is registered
    if check_be_registered; then
        return
    fi

    # Try to register BE node
    local retry_count=0
    while [ $retry_count -lt $MAX_RETRY_TIMES ]; do
        if mysql -uroot -P"${MYSQL_PORT}" -h"${MASTER_FE_IP}" \
            -e "ALTER SYSTEM ADD BACKEND '${CURRENT_BE_IP}:${CURRENT_BE_PORT}'" 2>/dev/null; then
            
            # Wait for the BE node to become registered
            local check_count=0
            while [ $check_count -lt 30 ]; do
                if mysql -uroot -P"${MYSQL_PORT}" -h"${MASTER_FE_IP}" \
                    -N -e "SHOW BACKENDS" 2>/dev/null | grep -w "${CURRENT_BE_IP}" | grep -w "${CURRENT_BE_PORT}" &>/dev/null; then
                    log_info "Successfully registered BE node"
                    return 0
                else
                    log_warn "BE node is not ready, retrying... ($check_count/30)"
                fi
                check_count=$((check_count + 1))
                sleep 1
            done
        fi
        
        retry_count=$((retry_count + 1))
        if [ $((retry_count % 20)) -eq 1 ]; then
            log_warn "Failed to register BE node or BE not ready, retrying... ($retry_count/$MAX_RETRY_TIMES)"
        fi
        sleep "$RETRY_INTERVAL"
    done
    
    log_error "Failed to register BE node after ${MAX_RETRY_TIMES} attempts"
}

# Configuring Node Roles
setup_node_role() {
    if [[ ${NODE_ROLE} == 'computation' ]]; then
        log_info "Setting up computation node role"
        echo "be_node_role=computation" >> "$BE_CONFIG_FILE"
    else
        log_info "Setting up mixed node role"
    fi
}

# Print BE configuration information
show_be_config() {
    log_info "==== BE Node Configuration ===="
    log_info "Master FE IP: ${MASTER_FE_IP}"
    log_info "Current BE IP: ${CURRENT_BE_IP}"
    log_info "Current BE Port: ${CURRENT_BE_PORT}"
    log_info "Priority Networks: ${PRIORITY_NETWORKS}"
    log_info "Node Role: ${NODE_ROLE:-mixed}"
    log_info "=========================="
}

# Cleanup Function
cleanup() {
    log_info "Stopping BE node"
    ${DORIS_HOME}/be/bin/stop_be.sh
}

# Main Function
main() {
    trap cleanup SIGTERM SIGINT
    init_environment
    
    # Check the storage directory
    if [ -z "$database_exists" ]; then
        log_info "Initializing BE configuration"
        echo "priority_networks = ${PRIORITY_NETWORKS}" >> "$BE_CONFIG_FILE"
        setup_node_role
        show_be_config
        register_be
    else
        log_info "Storage directory exists, skipping initialization"
    fi

    log_info "Starting BE node"
    export SKIP_CHECK_ULIMIT=true
    ${DORIS_HOME}/be/bin/start_be.sh --console &
    child_pid=$!
    wait $child_pid
}

if ! is_sourced; then
    main "$@"
fi

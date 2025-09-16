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
readonly FE_CONFIG_FILE="${DORIS_HOME}/fe/conf/fe.conf"

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

# Check if the parameter is empty
check_required_param() {
    local param_name="$1"
    local param_value="$2"
    if [ -z "$param_value" ]; then
        log_error "${param_name} is required but not set"
    fi
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
    if [ -d "${DORIS_HOME}/fe/doris-meta/image" ]; then
        database_exists='true'
    fi
}

# Verify IP address format
validate_ip_address() {
    local ip="$1"
    local ipv4_regex="^[1-2]?[0-9]?[0-9](\.[1-2]?[0-9]?[0-9]){3}$"
    local ipv6_regex="^([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)$"
    
    if [[ $ip =~ $ipv4_regex ]] || [[ $ip =~ $ipv6_regex ]]; then
        return 0
    fi
    return 1
}

# Verify port number
validate_port() {
    local port="$1"
    if [[ $port =~ ^[1-6]?[0-9]{1,4}$ ]] && [ "$port" -le 65535 ]; then
        return 0
    fi
    return 1
}

# Verify the necessary environment variables
validate_environment() {
    declare -g run_mode
    
    # RECOVERY Mode Verification
    if [ -n "$RECOVERY" ]; then
        if [[ $RECOVERY =~ ^([tT][rR][uU][eE]|1)$ ]]; then
            run_mode="RECOVERY"
            log_info "Running in Recovery mode"
            return
        fi
    fi

    # K8S mode verification
    if [ -n "$BUILD_TYPE" ]; then
        if [[ $BUILD_TYPE =~ ^([kK]8[sS])$ ]]; then
            run_mode="K8S"
            log_info "Running in K8S mode"
            return
        fi
        log_error "Invalid BUILD_TYPE. Expected: k8s"
    fi

    # Election Mode Verification
    if [[ -n "$FE_SERVERS" && -n "$FE_ID" ]]; then
        validate_election_mode
        return
    fi

    # Specifying a schema for validation
    if [[ -n "$FE_MASTER_IP" && -n "$FE_MASTER_PORT" && 
          -n "$FE_CURRENT_IP" && -n "$FE_CURRENT_PORT" ]]; then
        validate_assign_mode
        return
    fi

    log_error "Missing required parameters. Please check documentation."
}

# Verify election mode configuration
validate_election_mode() {
    run_mode="ELECTION"
    
    # Verify FE_SERVERS format
    local fe_servers_regex="^.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4}(,.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4})*$"
    if ! [[ $FE_SERVERS =~ $fe_servers_regex ]]; then
        log_error "Invalid FE_SERVERS format. Expected: name:ip:port[,name:ip:port]..."
    fi

    # Verify FE_ID
    if ! [[ $FE_ID =~ ^[1-9]{1}$ ]]; then
        log_error "Invalid FE_ID. Must be a single digit between 1-9"
    fi
    
    log_info "Running in Election mode"
}

# Verify the specified mode configuration
validate_assign_mode() {
    run_mode="ASSIGN"
    
    # Verify IP Address
    if ! validate_ip_address "$FE_MASTER_IP"; then
        log_error "Invalid FE_MASTER_IP format"
    fi
    if ! validate_ip_address "$FE_CURRENT_IP"; then
        log_error "Invalid FE_CURRENT_IP format"
    fi

    # Verify port
    if ! validate_port "$FE_MASTER_PORT"; then
        log_error "Invalid FE_MASTER_PORT"
    fi
    if ! validate_port "$FE_CURRENT_PORT"; then
        log_error "Invalid FE_CURRENT_PORT"
    fi
    
    log_info "Running in Assign mode"
}

# Parse a colon-delimited string
parse_colon_separated() {
    local input="$1"
    local -n arr=$2  # 使用nameref来存储结果
    local IFS=':'
    read -r -a arr <<< "$input"
}

# Parsing a comma-delimited string
parse_comma_separated() {
    local input="$1"
    local -n arr=$2  # 使用nameref来存储结果
    local IFS=','
    read -r -a arr <<< "$input"
}

# Configuring the election mode
setup_election_mode() {
    local fe_server_array
    parse_comma_separated "$FE_SERVERS" fe_server_array
    
    # Parsing master node information
    local master_info
    parse_colon_separated "${fe_server_array[0]}" master_info
    master_fe_ip="${master_info[1]}"
    master_fe_port="${master_info[2]}"
    
    # Parse the current node information
    local found=false
    local node_info
    for node in "${fe_server_array[@]}"; do
        parse_colon_separated "$node" node_info
        if [ "${node_info[0]}" = "fe${FE_ID}" ]; then
            current_fe_ip="${node_info[1]}"
            current_fe_port="${node_info[2]}"
            found=true
            break
        fi
    done

    if [ "$found" = "false" ]; then
        log_error "Could not find configuration for fe${FE_ID} in FE_SERVERS"
    fi
    
    is_master_fe=$([[ "$FE_ID" == "1" ]] && echo "true" || echo "false")
}

# Set up a preferred network
setup_priority_networks() {
    local ip_parts
    parse_colon_separated "$1" ip_parts
    priority_networks="${ip_parts[0]}.${ip_parts[1]}.${ip_parts[2]}.0/24"
}

# Configure the specified mode
setup_assign_mode() {
    master_fe_ip="$FE_MASTER_IP"
    master_fe_port="$FE_MASTER_PORT"
    current_fe_ip="$FE_CURRENT_IP"
    current_fe_port="$FE_CURRENT_PORT"
    
    is_master_fe=$([[ "$master_fe_ip" == "$current_fe_ip" ]] && echo "true" || echo "false")
}

# Add RECOVERY mode configuration function
setup_recovery_mode() {
    # In recovery mode, we need to read the configuration from the metadata
    local meta_dir="${DORIS_HOME}/fe/doris-meta"
    if [ ! -d "$meta_dir" ] || [ -z "$(ls -A "$meta_dir")" ]; then
        log_error "Cannot start in recovery mode: meta directory is empty or does not exist"
    fi
    
    log_info "Starting in recovery mode, using existing meta directory"
    is_master_fe="true"  # In recovery mode, it starts as the master node by default
}

# Configuring FE Nodes
setup_fe_node() {
    declare -g master_fe_ip master_fe_port current_fe_ip current_fe_port
    declare -g priority_networks is_master_fe

    case $run_mode in
        "ELECTION")
            setup_election_mode
            ;;
        "ASSIGN")
            setup_assign_mode
            ;;
        "RECOVERY")
            setup_recovery_mode
            ;;
    esac

    # Set priority network (if not in recovery mode)
    if [ "$run_mode" != "RECOVERY" ]; then
        local ip_parts
        IFS='.' read -r -a ip_parts <<< "$current_fe_ip"
        priority_networks="${ip_parts[0]}.${ip_parts[1]}.${ip_parts[2]}.0/24"
    fi
    
    # Print key configuration information
    log_info "==== FE Node Configuration ===="
    log_info "Run Mode: ${run_mode}"
    if [ "$run_mode" = "RECOVERY" ]; then
        log_info "Recovery Mode: true"
        log_info "Meta Directory: ${DORIS_HOME}/fe/doris-meta"
    else
        log_info "Is Master: ${is_master_fe}"
        log_info "Master FE IP: ${master_fe_ip}"
        log_info "Master FE Port: ${master_fe_port}"
        log_info "Current FE IP: ${current_fe_ip}"
        log_info "Current FE Port: ${current_fe_port}"
        log_info "Priority Networks: ${priority_networks}"
        if [ "$run_mode" = "ELECTION" ]; then
            log_info "FE ID: ${FE_ID}"
            log_info "FE Servers: ${FE_SERVERS}"
        fi
    fi
    log_info "=========================="
}

# Start FE node
start_fe() {
    if [ "$run_mode" = "RECOVERY" ]; then
        log_info "Starting FE node in recovery mode"
        ${DORIS_HOME}/fe/bin/start_fe.sh --metadata_failure_recovery
        return
    fi

    if [ "$is_master_fe" = "true" ]; then
        log_info "Starting master FE node"
        ${DORIS_HOME}/fe/bin/start_fe.sh --console
    else
        log_info "Starting follower FE node"
        ${DORIS_HOME}/fe/bin/start_fe.sh --helper "${master_fe_ip}:${master_fe_port}" --console
    fi
}

# Check whether the FE node is registered
check_fe_registered() {
    local retry_count=0
    while [ $retry_count -lt $MAX_RETRY_TIMES ]; do
        # Query the existing FE node list
        local query_result
        query_result=$(mysql -uroot -P"${MYSQL_PORT}" -h"${master_fe_ip}" \
            -N -e "SHOW FRONTENDS" 2>/dev/null | grep -w "${current_fe_ip}" | grep -w "${current_fe_port}" || true)
        
        if [ -n "$query_result" ]; then
            log_info "FE node ${current_fe_ip}:${current_fe_port} is already registered"
            return 0
        fi
        
        retry_count=$((retry_count + 1))
        if [ $((retry_count % 20)) -eq 1 ]; then
            log_info "Waiting for master FE to be ready... ($retry_count/$MAX_RETRY_TIMES)"
        fi
        sleep "$RETRY_INTERVAL"
    done
    
    return 1
}

# Check the metadata directory
check_meta_dir() {
    local meta_dir="${DORIS_HOME}/fe/doris-meta"
    if [ -d "$meta_dir/image" ] && [ -n "$(ls -A "$meta_dir/image")" ]; then
        log_info "Meta directory already exists and not empty"
        return 0
    fi
    return 1
}

# Register FE Node
register_fe() {
    if [ "$is_master_fe" = "true" ]; then
        log_info "Master FE node does not need registration"
        return
    fi

    # First check if the node is registered
    if check_fe_registered; then
        return
    fi

    local retry_count=0
    while [ $retry_count -lt $MAX_RETRY_TIMES ]; do
        if mysql -uroot -P"${MYSQL_PORT}" -h"${master_fe_ip}" \
            -e "ALTER SYSTEM ADD FOLLOWER '${current_fe_ip}:${current_fe_port}'" 2>/dev/null; then
            log_info "Successfully registered FE node"
            return
        fi
        
        retry_count=$((retry_count + 1))
        if [ $((retry_count % 20)) -eq 1 ]; then
            log_warn "Failed to register FE node, retrying... ($retry_count/$MAX_RETRY_TIMES)"
        fi
        sleep "$RETRY_INTERVAL"
    done
    
    log_error "Failed to register FE node after ${MAX_RETRY_TIMES} attempts"
}

# Cleanup Function
cleanup() {
    log_info "Stopping FE node"
    ${DORIS_HOME}/fe/bin/stop_fe.sh
}

# Main Function
main() {
    validate_environment
    trap cleanup SIGTERM SIGINT
    
    if [ "$run_mode" = "K8S" ]; then
        ${DORIS_HOME}/fe/bin/start_fe.sh --console &
        wait $!
    elif [ "$run_mode" = "RECOVERY" ]; then
        setup_fe_node
        start_fe &
        wait $!
    else
        init_environment
        setup_fe_node
        
        # Check the metadata directory
        if check_meta_dir; then
            log_info "Meta directory exists, starting FE directly"
            start_fe &
            wait $!
            return
        fi
        
        # The metadata directory does not exist and needs to be initialized and registered.
        log_info "Initializing meta directory"
        if [ -z "$database_exists" ]; then
            echo "priority_networks = ${priority_networks}" >> "$FE_CONFIG_FILE"
        fi
        
        register_fe
        start_fe &
        wait $!
    fi
}

if ! is_sourced; then
    main "$@"
fi

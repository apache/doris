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

    # Election Mode Verification
    if [[ -n "$FE_SERVERS" && -n "$BE_ADDR" ]]; then
        validate_election_mode
        return
    fi

    # Specifying a schema for validation
    if [[ -n "$FE_MASTER_IP" && -n "$BE_IP" && -n "$BE_PORT" ]]; then
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

    # Verify BE_ADDR format
    if ! validate_ip_address "$(echo "$BE_ADDR" | cut -d: -f1)" || \
       ! validate_port "$(echo "$BE_ADDR" | cut -d: -f2)"; then
        log_error "Invalid BE_ADDR format. Expected: ip:port"
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
    if ! validate_ip_address "$BE_IP"; then
        log_error "Invalid BE_IP format"
    fi

    # Verify port
    if ! validate_port "$BE_PORT"; then
        log_error "Invalid BE_PORT"
    fi
    
    log_info "Running in Assign mode"
}

# Parsing configuration parameters
parse_config() {
    declare -g MASTER_FE_IP CURRENT_BE_IP CURRENT_BE_PORT PRIORITY_NETWORKS

    if [ "$run_mode" = "ELECTION" ]; then
        # Analyze the main FE node information
        MASTER_FE_IP=$(echo "$FE_SERVERS" | cut -d, -f1 | cut -d: -f2)
        
        # Parsing BE node information
        CURRENT_BE_IP=$(echo "$BE_ADDR" | cut -d: -f1)
        CURRENT_BE_PORT=$(echo "$BE_ADDR" | cut -d: -f2)
    else
        MASTER_FE_IP="$FE_MASTER_IP"
        CURRENT_BE_IP="$BE_IP"
        CURRENT_BE_PORT="$BE_PORT"
    fi

    # Set up a preferred network
    PRIORITY_NETWORKS=$(echo "$CURRENT_BE_IP" | awk -F. '{print $1"."$2"."$3".0/24"}')

    # Exporting environment variables
    export MASTER_FE_IP CURRENT_BE_IP CURRENT_BE_PORT PRIORITY_NETWORKS
}

# Check BE status
check_be_status() {
    local retry_count=0
    while [ $retry_count -lt $MAX_RETRY_TIMES ]; do
        if [ "$1" = "true" ]; then
            # Check FE status
            if mysql -uroot -P"${MYSQL_PORT}" -h"${MASTER_FE_IP}" \
                -N -e "SHOW FRONTENDS" 2>/dev/null | grep -w "${MASTER_FE_IP}" &>/dev/null; then
                log_info "Master FE is ready"
                return 0
            fi
        else
            # Check BE status
            if mysql -uroot -P"${MYSQL_PORT}" -h"${MASTER_FE_IP}" \
                -N -e "SHOW BACKENDS" 2>/dev/null | grep -w "${CURRENT_BE_IP}" | grep -w "${CURRENT_BE_PORT}" | grep -w "true" &>/dev/null; then
                log_info "BE node is ready"
                return 0
            fi
        fi
        
        retry_count=$((retry_count + 1))
        if [ $((retry_count % 20)) -eq 1 ]; then
            if [ "$1" = "true" ]; then
                log_info "Waiting for master FE... ($retry_count/$MAX_RETRY_TIMES)"
            else
                log_info "Waiting for BE node... ($retry_count/$MAX_RETRY_TIMES)"
            fi
        fi
        sleep "$RETRY_INTERVAL"
    done
    
    return 1
}

# Processing initialization files
process_init_files() {
    local f
    for f; do
        case "$f" in
            *.sh)
                if [ -x "$f" ]; then
                    log_info "Executing $f"
                    "$f"
                else
                    log_info "Sourcing $f"
                    . "$f"
                fi
                ;;
            *.sql)    
                log_info "Executing SQL file $f"
                mysql -uroot -P"${MYSQL_PORT}" -h"${MASTER_FE_IP}" < "$f"
                ;;
            *.sql.gz)  
                log_info "Executing compressed SQL file $f"
                gunzip -c "$f" | mysql -uroot -P"${MYSQL_PORT}" -h"${MASTER_FE_IP}"
                ;;
            *)         
                log_warn "Ignoring $f"
                ;;
        esac
    done
}

# Main Function
main() {
    validate_environment
    parse_config

    # Start BE Node
    {
        set +e
        bash init_be.sh 2>/dev/null
    } &

    # Waiting for BE node to be ready
    if ! check_be_status false; then
        log_error "BE node failed to start"
    fi

    # Processing initialization files
    if [ -d "/docker-entrypoint-initdb.d" ]; then
        sleep 15  # Wait for the system to fully boot up
        process_init_files /docker-entrypoint-initdb.d/*
    fi

    # Waiting for BE process
    wait
}

if ! is_sourced; then
    main "$@"
fi

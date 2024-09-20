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

DIR=$(
    cd $(dirname $0)
    pwd
)

source $DIR/common.sh

REGISTER_FILE=$DORIS_HOME/status/be-$MY_IP-register

add_local_be() {
    wait_master_fe_ready

    while true; do
        #lsof -i:$BE_HEARTBEAT_PORT
        #if [ $? -ne 0 ]; then
        #    sleep 1
        #    continue
        #fi

        output=$(mysql -P $FE_QUERY_PORT -h $MASTER_FE_IP -u root --execute "ALTER SYSTEM ADD BACKEND '$MY_IP:$BE_HEARTBEAT_PORT';" 2>&1)
        res=$?
        health_log "$output"
        [ $res -eq 0 ] && break
        (echo $output | grep "Same backend already exists") && break
        sleep 1
    done
}

add_cloud_be() {
    if [ -f "${DORIS_HOME}/log/be.out" ]; then
        return
    fi

    # Check if SQL_MODE_NODE_MGR is set to 1
    if [ "$SQL_MODE_NODE_MGR" -eq 1 ]; then
        health_log "SQL_MODE_NODE_MGR is set to 1, skipping cluster creation"
        return
    fi

    cluster_file_name="${DORIS_HOME}/conf/CLUSTER_NAME"
    cluster_name=$(cat $cluster_file_name)
    if [ -z $cluster_name ]; then
        health_log "Empty cluster name, it should specific in file ${cluster_file_name}"
        exit 1
    fi

    cluster_id="${cluster_name}_id"

    wait_create_instance

    nodes='{
    "cloud_unique_id": "'"${CLOUD_UNIQUE_ID}"'",
    "ip": "'"${MY_IP}"'",
    "heartbeat_port": "'"${BE_HEARTBEAT_PORT}"'"
    }'

    lock_cluster

    output=$(curl -s "${META_SERVICE_ENDPOINT}/MetaService/http/add_cluster?token=greedisgood9999" \
        -d '{"instance_id": "default_instance_id",
        "cluster": {
        "type": "COMPUTE",
        "cluster_name": "'"${cluster_name}"'",
        "cluster_id": "'"${cluster_id}"'",
        "nodes": ['"${nodes}"']
    }}')

    health_log "add cluster. output: $output"
    code=$(jq -r '.code' <<<$output)

    # cluster has exists
    if [ "$code" == "ALREADY_EXISTED" ]; then
        output=$(curl -s "${META_SERVICE_ENDPOINT}/MetaService/http/add_node?token=greedisgood9999" \
            -d '{"instance_id": "default_instance_id",
            "cluster": {
            "type": "COMPUTE",
            "cluster_name": "'"${cluster_name}"'",
            "cluster_id": "'"${cluster_id}"'",
            "nodes": ['"${nodes}"']
        }}')
    fi

    unlock_cluster

    health_log "add cluster. output: $output"
    code=$(jq -r '.code' <<<$output)

    if [ "$code" != "OK" ]; then
        health_log "add cluster failed,  exit."
        exit 1
    fi

    output=$(curl -s "${META_SERVICE_ENDPOINT}/MetaService/http/get_cluster?token=greedisgood9999" \
        -d '{"instance_id": "default_instance_id",
            "cloud_unique_id": "'"${CLOUD_UNIQUE_ID}"'",
            "cluster_name": "'"${cluster_name}"'",
            "cluster_id": "'"${cluster_id}"'"
           }')

    health_log "get cluster is: $output"
    code=$(jq -r '.code' <<<$output)

    if [ "$code" != "OK" ]; then
        health_log "get cluster failed,  exit."
        exit 1
    fi
}

stop_backend() {
    health_log "run stop be ..."
    if [ "$STOP_GRACE" = "1" ]; then
        bash $DORIS_HOME/bin/stop_be.sh --grace
    else
        bash $DORIS_HOME/bin/stop_be.sh
    fi
    exit 0
}

wait_process() {
    pid=""
    for ((i = 0; i < 5; i++)); do
        sleep 1s
        pid=$(pgrep doris_be)
        if [ -n "$pid" ]; then
            break
        fi
    done

    wait_pid $pid
}

add_be_to_cluster() {
    if [ -f $REGISTER_FILE ]; then
        return
    fi

    if [ "${IS_CLOUD}" == "1" ]; then
        if [ "${REG_BE_TO_MS}" == "1" ]; then
            add_cloud_be
        fi
    else
        add_local_be
    fi

    touch $REGISTER_FILE
    health_log "register be"
}

main() {
    trap stop_backend SIGTERM

    if [ -n "$LLVM_PROFILE_FILE_PREFIX" ]; then
        export LLVM_PROFILE_FILE="${LLVM_PROFILE_FILE_PREFIX}-$(date +%s)"
    fi

    add_be_to_cluster

    health_log "run start_be.sh"
    bash $DORIS_HOME/bin/start_be.sh --daemon

    wait_process
}

main

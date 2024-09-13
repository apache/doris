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

ARGS=$@

DIR=$(
    cd $(dirname $0)
    pwd
)

source $DIR/common.sh

wait_fdb_ready() {
    while true; do
        if [ -f $HAS_INIT_FDB_FILE ]; then
            health_log "has init fdb"
            return
        fi

        health_log "hasn't init fdb, waiting"

        sleep 1
    done
}

check_init_cloud() {
    if [ -f $HAS_CREATE_INSTANCE_FILE ]; then
        return
    fi

    if [ "$MY_TYPE" != "ms" -o "$MY_ID" != "1" ]; then
        return
    fi

    while true; do

        lock_cluster

        # Check if SQL_MODE_NODE_MGR is set
        if [[ "$SQL_MODE_NODE_MGR" -eq 1 ]]; then
            health_log "SQL_MODE_NODE_MGR is set, skipping create_instance"
            touch $HAS_CREATE_INSTANCE_FILE
            return
        fi

        output=$(curl -s "${META_SERVICE_ENDPOINT}/MetaService/http/create_instance?token=greedisgood9999" \
            -d '{"instance_id":"default_instance_id",
                    "name": "default_instance",
                    "user_id": "'"${DORIS_CLOUD_USER}"'",
                    "obj_info": {
                    "ak": "'"${DORIS_CLOUD_AK}"'",
                    "sk": "'"${DORIS_CLOUD_SK}"'",
                    "bucket": "'"${DORIS_CLOUD_BUCKET}"'",
                    "endpoint": "'"${DORIS_CLOUD_ENDPOINT}"'",
                    "external_endpoint": "'"${DORIS_CLOUD_EXTERNAL_ENDPOINT}"'",
                    "prefix": "'"${DORIS_CLOUD_PREFIX}"'",
                    "region": "'"${DORIS_CLOUD_REGION}"'",
                    "provider": "'"${DORIS_CLOUD_PROVIDER}"'"
                }}')

        unlock_cluster

        health_log "create instance output: $output"
        code=$(jq -r '.code' <<<$output)

        if [ "$code" != "OK" ]; then
            health_log "create instance failed"
            sleep 1
            continue
        fi

        health_log "create doris instance succ, output: $output"
        touch $HAS_CREATE_INSTANCE_FILE
        break
    done
}

stop_cloud() {
    cd $DORIS_HOME
    bash bin/stop.sh
}

wait_process() {
    pid=""
    for ((i = 0; i < 5; i++)); do
        sleep 1s
        pid=$(pgrep _cloud)
        if [ -n "$pid" ]; then
            break
        fi
    done

    wait_pid $pid
}

main() {
    trap stop_cloud SIGTERM

    cd $DORIS_HOME

    wait_fdb_ready

    check_init_cloud &

    health_log "input args: $ARGS"

    bash bin/start.sh $ARGS --daemon

    wait_process
}

main

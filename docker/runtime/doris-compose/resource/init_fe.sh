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

DIR=$(cd $(dirname $0);pwd)

source $DIR/common.sh

add_frontend() {
    while true; do
        read_master_fe_ip
        if [ $? -ne 0 ]; then
            sleep 1
            continue
        fi

        output=`mysql -P $FE_QUERY_PORT -h $MASTER_FE_IP -u root --execute "ALTER SYSTEM ADD FOLLOWER '$MY_IP:$FE_EDITLOG_PORT';" 2>&1`
        res=$?
        health_log "$output"
        [ $res -eq 0 ] && break
        (echo $output | grep "frontend already exists") && break
        sleep 1
    done
}

fe_daemon() {
    set +e
    while true; do
        sleep 1
        output=`mysql -P $FE_QUERY_PORT -h $MY_IP -u root --execute "SHOW FRONTENDS;"`
        code=$?
        health_log "$output"
        if [ $code -ne 0 ]; then
            continue
        fi
        header=`grep IsMaster <<< $output`
        if [ $? -ne 0 ]; then
            health_log "not found header"
            continue
        fi
        host_index=-1
        is_master_index=-1
        i=1
        for field in $header; do
            [[ "$field" = "Host" ]] && host_index=$i
            [[ "$field" = "IsMaster" ]] && is_master_index=$i
            ((i=i+1))
        done
        if [ $host_index -eq -1 ]; then
            health_log "header not found Host"
            continue
        fi
        if [ $is_master_index -eq -1 ]; then
            health_log "header not found IsMaster"
            continue
        fi
        echo "$output" | awk -v is_master="$is_master_index" -v host="$host_index" '{print $is_master $host}' | grep $MY_IP | grep true 2>&1
        if [ $? -eq 0 ]; then
            echo $MY_IP > $MASTER_FE_IP_FILE
            if [ "$MASTER_FE_IP" != "$MY_IP" ]; then
                health_log "change to master, last master is $MASTER_FE_IP"
                MASTER_FE_IP=$MY_IP
            fi
        fi
    done
}

main() {
    if [ "$MY_ID" = "1" -o  -d "${DORIS_HOME}/doris-meta/image" ]; then
        fe_daemon &
        bash $DORIS_HOME/bin/start_fe.sh
    else
        add_frontend
        fe_daemon &
        $DORIS_HOME/bin/start_fe.sh --helper $MASTER_FE_IP:$FE_EDITLOG_PORT
    fi
}

main

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
        output=`mysql -P $FE_QUERY_PORT -h $MY_IP -u root --execute "SHOW FRONTENDS;" | grep -w $MY_IP | awk '{print $8}' 2>&1`
        if [ $? -ne 0 ]; then
            health_log "$output"
        else
            echo $output | grep true
            if [ $? -eq 0 ]; then
                echo $MY_IP > $MASTER_FE_IP_FILE
                if [ "$MASTER_FE_IP" != "$MY_IP" ]; then
                    health_log "change to master, last master is $MASTER_FE_IP"
                    MASTER_FE_IP=$MY_IP
                fi
            fi
        fi
        sleep 3
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

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

REGISTER_FILE=$DORIS_HOME/status/$MY_IP-register

add_backend() {
    while true; do
        read_master_fe_ip
        if [ $? -ne 0 ]; then
            sleep 1
            continue
        fi
        lsof -i:$BE_HEARTBEAT_PORT
        if [ $? -ne 0 ]; then
            sleep 1
            continue
        fi

        output=`mysql -P $FE_QUERY_PORT -h $MASTER_FE_IP -u root --execute "ALTER SYSTEM ADD BACKEND '$MY_IP:$BE_HEARTBEAT_PORT';" 2>&1`
        res=$?
        health_log "$output"
        [ $res -eq 0 ] && break
        (echo $output | grep "Same backend already exists") && break
        sleep 1
    done

    touch $REGISTER_FILE
}

main() {
    if [ ! -f $REGISTER_FILE ]; then
        add_backend &
    fi
    bash $DORIS_HOME/bin/start_be.sh
}

main

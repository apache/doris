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

DORIS_HOME="/opt/apache-doris"

add_frontend() {
    while true; do
        output=`mysql -P $FE_QUERY_PORT -h $MASTER_FE_IP -u root --execute "ALTER SYSTEM ADD FOLLOWER '$MY_IP:$FE_EDITLOG_PORT';" 2>&1`
        res=$?
        date >> $DORIS_HOME/fe/log/fe.out
        echo $output >> $DORIS_HOME/fe/log/fe.out
        [ $res -eq 0 ] && break
        (echo $output | grep "frontend already exists") && break
        sleep 1
    done
}

main() {
    if [ "$MY_IP" = "$MASTER_FE_IP" -o  -d "${DORIS_HOME}/fe/doris-meta/image" ]; then
        $DORIS_HOME/fe/bin/start_fe.sh
    else
        add_frontend
        $DORIS_HOME/fe/bin/start_fe.sh --helper $MASTER_FE_IP:$FE_EDITLOG_PORT
    fi
}

main

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

export MASTER_FE_IP=""
export MASTER_FE_IP_FILE=$DORIS_HOME/status/master_fe_ip

health_log() {
    date >> "$DORIS_HOME/log/health.out"
    echo "$@" >> "$DORIS_HOME/log/health.out"
}

read_master_fe_ip() {
    MASTER_FE_IP=`cat $MASTER_FE_IP_FILE`
    if [ $? -eq 0 ]; then
        health_log "master fe ${MASTER_FE_IP} has ready."
        return 0
    else
        health_log "master fe has not ready."
        return 1
    fi
}


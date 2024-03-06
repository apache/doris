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

init_db() {
    if [ -f $HAS_INIT_FDB_FILE ]; then
        return
    fi

    for ((i = 0; i < 10; i++)); do
        /usr/bin/fdbcli -C ${DORIS_HOME}/conf/fdb.cluster --exec 'configure new single ssd'
        if [ $? -eq 0 ]; then
            touch $HAS_INIT_FDB_FILE
            health_log "fdbcli init cluster succ"
            break
        fi

        health_log "fdbcli init cluster failed"
        sleep 1
    done

    if [ ! -f $HAS_INIT_FDB_FILE ]; then
        health_log "fdbcli init cluster failed, exit"
        exit 1
    fi
}

stop_fdb() {
    exit 0
}

main() {
    trap stop_fdb SIGTERM

    init_db &

    fdbmonitor --conffile ${DORIS_HOME}/conf/fdb.conf --lockfile ${DORIS_HOME}/fdbmonitor.pid
}

main

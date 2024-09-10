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
export HAS_INIT_FDB_FILE=${DORIS_HOME}/status/has_init_fdb
export HAS_CREATE_INSTANCE_FILE=$DORIS_HOME/status/has_create_instance
export LOG_FILE=$DORIS_HOME/log/health.out
export LOCK_FILE=$DORIS_HOME/status/token

health_log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') $@" | tee -a $LOG_FILE
}

# concurrent write meta service server will failed due to fdb txn conflict.
# so add lock to protect writing ms txns.
lock_cluster() {
    health_log "start acquire token"
    while true; do
        if [ -f $LOCK_FILE ]; then
            if [ "a$(cat $LOCK_FILE)" == "a${MY_IP}" ]; then
                health_log "rm $LOCK_FILE generate by myself"
                rm $LOCK_FILE
                continue
            fi

            mt=$(stat -c %Y $LOCK_FILE)
            if [ -z "$mt" ]; then
                health_log "get $LOCK_FILE modify time failed"
                sleep 0.1
                continue
            fi

            now=$(date '+%s')
            diff=$(expr $now - $mt)
            if [ $diff -lt 10 ]; then
                sleep 0.1
                continue
            fi

            rm $LOCK_FILE
            health_log "rm $LOCK_FILE due to exceeds $diff seconds."
        fi

        if [ ! -f $LOCK_FILE ]; then
            echo $MY_IP >$LOCK_FILE
        fi

        sleep 0.1

        if [ "a$(cat $LOCK_FILE)" == "a${MY_IP}" ]; then
            break
        fi

        sleep 0.1
    done

    health_log "now got token"
}

unlock_cluster() {
    if [ ! -f $LOCK_FILE ]; then
        return
    fi

    if [ "a$(cat $LOCK_FILE)" == "a${MY_IP}" ]; then
        rm $LOCK_FILE
    fi
}

wait_master_fe_ready() {
    while true; do
        MASTER_FE_IP=$(cat $MASTER_FE_IP_FILE)
        if [ -n "$MASTER_FE_IP" ]; then
            health_log "master fe ${MASTER_FE_IP} has ready."
            break
        fi
        health_log "master fe has not ready."
        sleep 1
    done
}

wait_create_instance() {
    ok=0
    for ((i = 0; i < 30; i++)); do
        if [ -f $HAS_CREATE_INSTANCE_FILE ]; then
            ok=1
            break
        fi

        health_log "has not create instance, not found file $HAS_CREATE_INSTANCE_FILE"

        sleep 1
    done

    if [ $ok -eq 0 ]; then
        health_log "wait create instance file too long, exit"
        exit 1
    fi

    health_log "check has create instance ok"
}

wait_pid() {
    pid=$1
    health_log ""
    health_log "ps -elf\n$(ps -elf)\n"
    if [ -z $pid ]; then
        health_log "pid not exist"
        exit 1
    fi

    health_log "wait process $pid"
    while true; do
        ps -p $pid >/dev/null
        if [ $? -ne 0 ]; then
            break
        fi
        sleep 1s
    done
    health_log "wait end"
}

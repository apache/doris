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

DORIS_HOME=/opt/apache-doris

log_info()
{
    printf '%s [INFO] [Entrypoint]: %s\n' "$(date -Iseconds)" "$*"
}

log_error()
{
    printf '%s [ERROR] [Entrypoint]: %s\n' "$(date -Iseconds)" "$*"
}

setup_fe()
{
    if [ -z "${INITIALIZED}"]
    then
        echo "priority_networks = 127.0.0.1" >> ${DORIS_HOME}/fe/conf/fe.conf
        log_info "fe initialized"
    fi
}

setup_be()
{
    sysctl -w vm.max_map_count=2000000
	ulimit -n 65536
    if [ -z "${INITIALIZED}"]
    then
        sed -i '/swapon/, +3d' ${DORIS_HOME}/be/bin/start_be.sh
        echo "priority_networks = 127.0.0.1" >> ${DORIS_HOME}/be/conf/be.conf
        log_info "be initialized"
    fi
}

setup()
{
    declare -g INITIALIZED
    if [ -d "${DORIS_HOME}/fe/doris-meta/image" ]
    then
        log_info "cluster is initialized, skip"
        INITIALIZED='true'
        return 0;
    fi
    setup_fe
    setup_be
    log_info "cluster initialize finished"
    INITIALIZED='true'
}

start_fe()
{
    log_info "fe starting"
    ${DORIS_HOME}/fe/bin/start_fe.sh --daemon
    sleep 5s
    if [ ! -f ${DORIS_HOME}/fe/bin/fe.pid ]
    then
        log_error "fe pid file is not exists."
        exit 255
    fi
    declare -g FE_PID
    FE_PID=$(cat ${DORIS_HOME}/fe/bin/fe.pid)
    if [ -z "${FE_PID}" ]
    then
        log_error "fe pid is empty, start fe failed."
        exit 255
    fi
    log_info "fe start success. pid: ${FE_PID}"
    check_count=0
    while [ $(mysql -h127.0.0.1 -P9030 -uroot -e "show frontends" > /dev/null 2>&1;echo $?) -ne 0 ]
    do
        if [ ${check_count} -ge 12 ]
        then
            log_error "QE service is still not started, after waiting for 1 min."
            exit 255
        fi
        sleep 5s
    done
    log_info "QE Service is started."
}

register_be()
{
    if [ $(mysql -h127.0.0.1 -P9030 -uroot -e "show backends" -N -s | wc -l) -gt 0 ]
    then
        log_info "be is already registered, skip."
        return 0
    fi
    register_be_sql="alter system add backend '127.0.0.1:9050'"
    mysql -h127.0.0.1 -P9030 -uroot -e "${register_be_sql}"
    if [ $? -ne 0 ]
    then
        log_error "register be exec failed."
        exit 255
    fi
    log_info "register be exec success."
    check_count=0
    is_alive=$(mysql -h127.0.0.1 -P9030 -uroot -e "show backends \G" -N | awk 'NR==10')
    while [ "${is_alive}" != "true" ]
    do
        if [ ${check_count} -ge 24 ]
        then
            log_error "be is still not alive, after check 2 min."
            exit 255
        fi
        sleep 5s
        let check_count++
        is_alive=$(mysql -h127.0.0.1 -P9030 -uroot -e "show backends \G" -N | awk 'NR==10')
    done
    log_info "be is alive."
}

start_be()
{
    log_info "be starting"
    ${DORIS_HOME}/be/bin/start_be.sh --daemon
    sleep 5s
    if [ ! -f ${DORIS_HOME}/be/bin/be.pid ]
    then
        tail -30 ${DORIS_HOME}/be/log/be.out
        log_error "be pid file is not exists."
        exit 255
    fi
    declare -g BE_PID
    BE_PID=$(cat "${DORIS_HOME}/be/bin/be.pid")
    if [ -z "${BE_PID}" ]
    then
        log_error "be pid is empty, start fe failed."
        exit 255
    fi
    log_info "be start success. pid: ${BE_PID}"
}



check_status()
{
    while [ $(ps -ef | grep $FE_PID | wc -l) -gt 1 ] && [ $(ps -ef | grep $BE_PID | wc -l) -gt 1 ]
    do 
        sleep 15s
    done
    if [ $(ps -ef | grep $FE_PID | wc -l) -le 1 ]
    then
        log_error "fe is down"
    fi
    if [ $(ps -ef | grep $BE_PID | wc -l) -le 1 ]
    then
        log_error "be is down"
    fi
    exit 255
}

start()
{
    start_fe
    start_be
    register_be
    check_status
}

stop()
{
    log_info "start stopping fe"
    ${DORIS_HOME}/fe/bin/stop_fe.sh
    log_info "start stopping be"
    ${DORIS_HOME}/be/bin/stop_be.sh
}

main()
{
    trap "stop" SIGTERM SIGINT
    setup
    start
}

main
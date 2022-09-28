#!/usr/bin/env bash
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

curdir=$(dirname "$0")
curdir=$(
    cd "$curdir"
    pwd
)

OPTS=$(getopt \
    -n $0 \
    -o '' \
    -l 'daemon' \
    -- "$@")

eval set -- "$OPTS"

RUN_DAEMON=0
while true; do
    case "$1" in
    --daemon)
        RUN_DAEMON=1
        shift
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Internal error"
        exit 1
        ;;
    esac
done

export DORIS_HOME=$(
    cd "$curdir/.."
    pwd
)

# export env variables from be.conf
#
# UDF_RUNTIME_DIR
# LOG_DIR
# PID_DIR
export UDF_RUNTIME_DIR=${DORIS_HOME}/lib/udf-runtime
export LOG_DIR=${DORIS_HOME}/log
export PID_DIR=$(
    cd "$curdir"
    pwd
)

# set odbc conf path
export ODBCSYSINI=$DORIS_HOME/conf

# support utf8 for oracle database
export NLS_LANG=AMERICAN_AMERICA.AL32UTF8

while read line; do
    envline=$(echo $line | sed 's/[[:blank:]]*=[[:blank:]]*/=/g' | sed 's/^[[:blank:]]*//g' | egrep "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*=")
    envline=$(eval "echo $envline")
    if [[ $envline == *"="* ]]; then
        eval 'export "$envline"'
    fi
done < $DORIS_HOME/conf/be.conf

if [ -e $DORIS_HOME/bin/palo_env.sh ]; then
    source $DORIS_HOME/bin/palo_env.sh
fi

if [ ! -d $LOG_DIR ]; then
    mkdir -p $LOG_DIR
fi

if [ ! -d $UDF_RUNTIME_DIR ]; then
    mkdir -p ${UDF_RUNTIME_DIR}
fi

rm -f ${UDF_RUNTIME_DIR}/*

pidfile=$PID_DIR/be.pid

if [ -f $pidfile ]; then
    if kill -0 $(cat $pidfile) > /dev/null 2>&1; then
        echo "Backend running as process $(cat $pidfile). Stop it first."
        exit 1
    else
        rm $pidfile
    fi
fi

chmod 755 ${DORIS_HOME}/lib/doris_be
echo "start time: "$(date) >> $LOG_DIR/be.out

if [ ! -f /bin/limit3 ]; then
    LIMIT=
else
    LIMIT="/bin/limit3 -c 0 -n 65536"
fi

## set asan and ubsan env to generate core file
export ASAN_OPTIONS=symbolize=1:abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1
export UBSAN_OPTIONS=print_stacktrace=1

## set TCMALLOC_HEAP_LIMIT_MB to limit memory used by tcmalloc
set_tcmalloc_heap_limit() {
    total_mem_mb=$(free -m | grep Mem | awk '{print $2}')
    mem_limit_str=$(grep ^mem_limit "${DORIS_HOME}"/conf/be.conf)
    digits_unit=${mem_limit_str##*=}
    digits_unit="${digits_unit#"${digits_unit%%[![:space:]]*}"}"
    digits_unit="${digits_unit%"${digits_unit##*[![:space:]]}"}"
    digits=${digits_unit%%[^[:digit:]]*}
    unit=${digits_unit##*[[:digit:] ]}

    mem_limit_mb=0
    case ${unit} in
    t | T) mem_limit_mb=$((digits * 1024 * 1024)) ;;
    g | G) mem_limit_mb=$((digits * 1024)) ;;
    m | M) mem_limit_mb=$((digits)) ;;
    k | K) mem_limit_mb=$((digits / 1024)) ;;
    %) mem_limit_mb=$((total_mem_mb * digits / 100)) ;;
    *) mem_limit_mb=$((digits / 1024 / 1024 / 1024)) ;;
    esac

    if [[ "${mem_limit_mb}" -eq 0 ]]; then
        mem_limit_mb=$((total_mem_mb * 80 / 100))
    fi

    if [[ "${mem_limit_mb}" -gt "${total_mem_mb}" ]]; then
        echo "mem_limit is larger than whole memory of the server. ${mem_limit_mb} > ${total_mem_mb}."
        return 1
    fi
    export TCMALLOC_HEAP_LIMIT_MB=${mem_limit_mb}
}

set_tcmalloc_heap_limit || exit 1

if [ ${RUN_DAEMON} -eq 1 ]; then
    nohup $LIMIT ${DORIS_HOME}/lib/doris_be "$@" >> $LOG_DIR/be.out 2>&1 < /dev/null &
else
    export DORIS_LOG_TO_STDERR=1
    $LIMIT ${DORIS_HOME}/lib/doris_be "$@" 2>&1 < /dev/null
fi

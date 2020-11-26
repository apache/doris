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

CUR_DIR=$(dirname "$0")
CUR_DIR=$(
  cd "$CUR_DIR" || exit
  pwd
)

DORIS_HOME=$(
  cd "$CUR_DIR/.." || exit
  pwd
)
export DORIS_HOME

PID_DIR=$(
  cd "$CUR_DIR" || exit
  pwd
)
export PID_DIR
export PID_FILE=$PID_DIR/be.pid

export UDF_RUNTIME_DIR=${DORIS_HOME}/lib/udf-runtime
export LOG_DIR=${DORIS_HOME}/log
export PID_DIR=$(
  cd "$CUR_DIR" || exit
  pwd
)

# set odbc conf path
export ODBCSYSINI=$DORIS_HOME/conf

# support utf8 for oracle database
export NLS_LANG=AMERICAN_AMERICA.AL32UTF8
RUN_DAEMON="1"
WAIT=1
CMD=""
while read line; do
  ENV_LINE=$(echo $line | sed 's/[[:blank:]]*=[[:blank:]]*/=/g' | sed 's/^[[:blank:]]*//g' | egrep "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*=")
  ENV_LINE=$(eval "echo $ENV_LINE")
  if [[ $ENV_LINE == *"="* ]]; then
    eval 'export "$ENV_LINE"'
  fi
done <$DORIS_HOME/conf/be.conf

function ECHO_INFO() {
  echo -e "${@}"
}

function ECHO_SUCCESS() {
  echo -e "\e[32m${@}\e[0m"
}

function ECHO_ERROR() {
  echo -e "\e[31m${@}\e[0m"
}

function START() {
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

  if [ -f $PID_FILE ]; then
    if kill -0 $(cat $PID_FILE) >/dev/null 2>&1; then
      ECHO_ERROR "Backend running as process $(cat $PID_FILE). Stop it first."
      exit 1
    else
      rm $PID_FILE
    fi
  fi

  chmod 755 ${DORIS_HOME}/lib/palo_be
  ECHO_INFO "start time: "$(date) >>$LOG_DIR/be.out

  if [ ! -f /bin/limit3 ]; then
    LIMIT=
  else
    LIMIT="/bin/limit3 -c 0 -n 65536"
  fi

  if [ ${RUN_DAEMON} -eq 1 ]; then
    nohup $LIMIT ${DORIS_HOME}/lib/palo_be "$@" >>$LOG_DIR/be.out 2>&1 </dev/null &
  else
    $LIMIT ${DORIS_HOME}/lib/palo_be "$@" >>$LOG_DIR/be.out 2>&1 </dev/null
  fi
  pid=$!
  sleep 1
  PID_COMM=$(ps -p $pid -o comm=)
  if [ "palo_be"x != "$PID_COMM"x ]; then
    ECHO_ERROR "start be failed."
    exit 1
  elif [ "${WAIT}" == "1" ] && [ "${RUN_DAEMON}" == "1" ]; then
    timeout 60 grep -q "ThriftServer 'heartbeat' started on port" <(tail -F ${LOG_DIR}/be.INFO)
    if [ "$?" == "0" ]; then
      ECHO_SUCCESS "BE started successfully."
    else
      ECHO_ERROR "BE started failed. Please check the ${LOG_DIR}/be.INFO. Maybe Loading tablet meta."
    fi
  else
    ECHO_SUCCESS "BE started successfully."
  fi
}

function STOP() {
  if [ -f $PID_FILE ]; then
    pid=$(cat $PID_FILE)
    PID_COMM=$(ps -p $pid -o comm=)
    if [ "palo_be"x != "$PID_COMM"x ]; then
      ECHO_ERROR "ERROR: pid process may not be be. "
      exit 1
    fi

    if kill -0 $pid; then
      if kill -9 $pid >/dev/null 2>&1; then

        ECHO_SUCCESS "BE stopped successfully by force, PID: $pid"
        rm $PID_FILE
        exit 0
      else
        ECHO_ERROR "BE stopped failed."
        exit 1
      fi
    else
      ECHO_SUCCESS "BE stopped successfully."
      rm $PID_FILE
    fi
  else
    ECHO_ERROR "BE stopped failed. $PID_FILE does not exist. BE maybe not started."
    exit 1
  fi
}

function RESTART() {
  STOP && START
}

ARGS=$(getopt -o skrdDw: --long start,stop,restart,daemon,nodaemon,wait: -n $0 -- "$@")

if [ $? != 0 ]; then
  echo "Terminating..."
  exit 1
fi

eval set -- "${ARGS}"

while true; do
  case "$1" in
  -s | --start)
    CMD="START"
    shift
    ;;
  -k | --stop)
    CMD="STOP"
    shift
    ;;
  -r | --restart)
    CMD="RESTART"
    shift
    ;;
  -d | --daemon)
    RUN_DAEMON="1"
    shift
    ;;
  -D | --nodaemon)
    RUN_DAEMON="0"
    shift
    ;;
  -w | --wait)
    WAIT="$2"
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *)
    echo "Internal error!"
    exit 1
    ;;
  esac
done

case "$CMD" in
START)
  START
  ;;
STOP)
  STOP
  ;;
RESTART)
  RESTART
  ;;
*)
  ECHO_ERROR "unknown operation."
  ;;
esac


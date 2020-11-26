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
export JAVA_OPTS="-Xmx1024m"

export PID_FILE=$PID_DIR/fe.pid

export LOG_DIR=${DORIS_HOME}/log

while read line; do
  ENV_LINE=$(echo $line | sed 's/[[:blank:]]*=[[:blank:]]*/=/g' | sed 's/^[[:blank:]]*//g' | egrep "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*=")
  ENV_LINE=$(eval "echo $ENV_LINE")
  if [[ $ENV_LINE == *"="* ]]; then
    eval 'export "$ENV_LINE"'
  fi
done <$DORIS_HOME/conf/fe.conf
RUN_DAEMON=1
HELPER=""
WAIT=1
CMD=""

function ECHO_INFO() {
  echo -e "${@}"
}

function ECHO_SUCCESS() {
  echo -e "\e[32m${@}\e[0m"
}

function ECHO_ERROR() {
  echo -e "\e[31m${@}\e[0m"
}

# get jdk version, return version as an Integer.
# 1.8 => 8, 13.0 => 13
function jdk_version() {
  local result
  local java_cmd=$JAVA_HOME/bin/java
  local IFS=$'\n'
  # remove \r for Cygwin
  local lines=$("$java_cmd" -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n')
  if [[ -z $java_cmd ]]; then
    result=no_java
  else
    for line in $lines; do
      if [[ (-z $result) && ($line == *"version \""*) ]]; then
        local ver=$(echo $line | sed -e 's/.*version "\(.*\)"\(.*\)/\1/; 1q')
        # on macOS, sed doesn't support '?'
        if [[ $ver == "1."* ]]; then
          result=$(echo $ver | sed -e 's/1\.\([0-9]*\)\(.*\)/\1/; 1q')
        else
          result=$(echo $ver | sed -e 's/\([0-9]*\)\(.*\)/\1/; 1q')
        fi
      fi
    done
  fi
  echo "$result"
}

function START() {
  if [ -e $DORIS_HOME/bin/palo_env.sh ]; then
    source $DORIS_HOME/bin/palo_env.sh
  fi

  if [ ! -d $LOG_DIR ]; then
    mkdir -p $LOG_DIR
  fi


  # java
  if [ "$JAVA_HOME" = "" ]; then
    ECHO_ERROR "Error: JAVA_HOME is not set."
    exit 1
  fi
  JAVA=$JAVA_HOME/bin/java

  # check java version and choose correct JAVA_OPTS
  java_version=$(jdk_version)
  final_java_opt=$JAVA_OPTS
  if [ $java_version -gt 8 ]; then
    if [ -z "$JAVA_OPTS_FOR_JDK_9" ]; then
      ECHO_ERROR "JAVA_OPTS_FOR_JDK_9 is not set in fe.conf" >>$LOG_DIR/fe.out
      exit 1
    fi
    final_java_opt=$JAVA_OPTS_FOR_JDK_9
  fi
  echo "using java version $java_version" >>$LOG_DIR/fe.out
  echo $final_java_opt >>$LOG_DIR/fe.out

  # add libs to CLASSPATH
  for f in $DORIS_HOME/lib/*.jar; do
    CLASSPATH=$f:${CLASSPATH}
  done
  export CLASSPATH=${CLASSPATH}:${DORIS_HOME}/lib

  PID_FILE=$PID_DIR/fe.pid

  if [ -f $PID_FILE ]; then
    if kill -0 $(cat $PID_FILE) >/dev/null 2>&1; then
      ECHO_ERROR "FE start Failed. Frontend running as process $(cat $PID_FILE). Stop it first."
      exit 1
    fi
  fi

  if [ ! -f /bin/limit ]; then
    LIMIT=
  else
    LIMIT=/bin/limit
  fi

  echo $(date) >>$LOG_DIR/fe.out

  if [ x"$HELPER" != x"" ]; then
    # change it to '-helper' to be compatible with code in Frontend
    HELPER="-helper $HELPER"
  fi

  if [ ${RUN_DAEMON} -eq 1 ]; then
    nohup $LIMIT $JAVA $final_java_opt org.apache.doris.PaloFe ${HELPER} "$@" >>$LOG_DIR/fe.out 2>&1 </dev/null &
  else
    $LIMIT $JAVA $final_java_opt org.apache.doris.PaloFe ${HELPER} "$@" >>$LOG_DIR/fe.out 2>&1 </dev/null
  fi

  pid=$!
  sleep 1
  PID_COMM=$(ps -p $pid -o comm=)

  if [ "$PID_COMM" == "" ] || [ "java" != "$PID_COMM" ]; then
    ECHO_ERROR "start FE failed."
    exit 1
  elif [ "${WAIT}" == "1" ] && [ "${RUN_DAEMON}" == "1" ]; then
    timeout 60 grep -q 'HttpServer started with port' <(tail -F ${LOG_DIR}/fe.log)
    if [ "$?" == "0" ]; then
      ECHO_SUCCESS "FE started successfully."
    else
      ECHO_ERROR "FE started failed. Please check the ${LOG_DIR}/fe.log. Maybe replaying logs."
    fi
  else
    ECHO_SUCCESS "FE started successfully."
  fi

  echo $pid >$PID_FILE

}

function STOP() {
  if [ -f $PID_FILE ]; then
    pid=$(cat $PID_FILE)
    PID_COMM=$(ps -p $pid -o comm=)

    if [ "java" != "$PID_COMM" ]; then
      ECHO_ERROR "ERROR: pid process may not be fe. "
    fi
    kill -9 $pid >/dev/null 2>&1
    sleep 1
    PID_COMM=$(ps -p $pid -o comm=)
    if [ "java" == "$PID_COMM" ]; then
      ECHO_ERROR "ERROR: stop fe failed. "
    else
      ECHO_SUCCESS "FE stopped successfully."
      rm $PID_FILE
    fi
  else
    ECHO_ERROR "FE stopped failed. $PID_FILE does not exist. FE maybe not started."
    exit 1
  fi

}

function RESTART() {
  STOP && START
}

ARGS=$(getopt -o skrdDh:w: --long start,stop,restart,daemon,nodaemon,helper:,wait: -n $0 -- "$@")

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
  -h | --helper)
    HELPER="$2"
    shift 2
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


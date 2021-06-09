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

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'daemon' \
  -l 'helper:' \
  -- "$@")

eval set -- "$OPTS"

RUN_DAEMON=0
HELPER=
while true; do
    case "$1" in
        --daemon) RUN_DAEMON=1 ; shift ;;
        --helper) HELPER=$2 ; shift 2 ;;
        --) shift ;  break ;;
        *) ehco "Internal error" ; exit 1 ;;
    esac
done

export DORIS_HOME=`cd "$curdir/.."; pwd`

# export env variables from fe.conf
#
# JAVA_OPTS
# LOG_DIR
# PID_DIR
export JAVA_OPTS="-Xmx1024m"
export LOG_DIR="$DORIS_HOME/log"
export PID_DIR=`cd "$curdir"; pwd`

while read line; do
    envline=`echo $line | sed 's/[[:blank:]]*=[[:blank:]]*/=/g' | sed 's/^[[:blank:]]*//g' | egrep "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*="`
    envline=`eval "echo $envline"`
    if [[ $envline == *"="* ]]; then
        eval 'export "$envline"'
    fi
done < $DORIS_HOME/conf/fe.conf

if [ -e $DORIS_HOME/bin/palo_env.sh ]; then
    source $DORIS_HOME/bin/palo_env.sh
fi

# java
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi
JAVA=$JAVA_HOME/bin/java

# get jdk version, return version as an Integer.
# 1.8 => 8, 13.0 => 13
jdk_version() {
    local result
    local java_cmd=$JAVA_HOME/bin/java
    local IFS=$'\n'
    # remove \r for Cygwin
    local lines=$("$java_cmd" -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n')
    if [[ -z $java_cmd ]]
    then
        result=no_java
    else
        for line in $lines; do
            if [[ (-z $result) && ($line = *"version \""*) ]]
            then
                local ver=$(echo $line | sed -e 's/.*version "\(.*\)"\(.*\)/\1/; 1q')
                # on macOS, sed doesn't support '?'
                if [[ $ver = "1."* ]]
                then
                    result=$(echo $ver | sed -e 's/1\.\([0-9]*\)\(.*\)/\1/; 1q')
                else
                    result=$(echo $ver | sed -e 's/\([0-9]*\)\(.*\)/\1/; 1q')
                fi
            fi
        done
    fi
    echo "$result"
}

# need check and create if the log directory existed before outing message to the log file.
if [ ! -d $LOG_DIR ]; then
    mkdir -p $LOG_DIR
fi
 
# check java version and choose correct JAVA_OPTS
java_version=$(jdk_version)
final_java_opt=$JAVA_OPTS
if [ $java_version -gt 8 ]; then
    if [ -z "$JAVA_OPTS_FOR_JDK_9" ]; then
        echo "JAVA_OPTS_FOR_JDK_9 is not set in fe.conf" >> $LOG_DIR/fe.out
        exit -1
    fi 
    final_java_opt=$JAVA_OPTS_FOR_JDK_9
fi
echo "using java version $java_version" >> $LOG_DIR/fe.out
echo $final_java_opt >> $LOG_DIR/fe.out

# add libs to CLASSPATH
for f in $DORIS_HOME/lib/*.jar; do
  CLASSPATH=$f:${CLASSPATH};
done
export CLASSPATH=${CLASSPATH}:${DORIS_HOME}/lib


pidfile=$PID_DIR/fe.pid

if [ -f $pidfile ]; then
  if kill -0 `cat $pidfile` > /dev/null 2>&1; then
    echo Frontend running as process `cat $pidfile`.  Stop it first.
    exit 1
  fi
fi

if [ ! -f /bin/limit ]; then
    LIMIT=
else
    LIMIT=/bin/limit
fi

echo `date` >> $LOG_DIR/fe.out

if [ x"$HELPER" != x"" ]; then
    # change it to '-helper' to be compatible with code in Frontend
    HELPER="-helper $HELPER"
fi

if [ ${RUN_DAEMON} -eq 1 ]; then
    nohup $LIMIT $JAVA $final_java_opt org.apache.doris.PaloFe ${HELPER} "$@" >> $LOG_DIR/fe.out 2>&1 </dev/null &
else
    $LIMIT $JAVA $final_java_opt org.apache.doris.PaloFe ${HELPER} "$@" >> $LOG_DIR/fe.out 2>&1 </dev/null
fi

echo $! > $pidfile

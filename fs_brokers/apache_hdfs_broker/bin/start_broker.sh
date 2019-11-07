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
  -- "$@")

eval set -- "$OPTS"

RUN_DAEMON=0
while true; do
    case "$1" in
        --daemon) RUN_DAEMON=1 ; shift ;;
        --) shift ;  break ;;
        *) ehco "Internal error" ; exit 1 ;;
    esac
done

export BROKER_HOME=`cd "$curdir/.."; pwd`
export PID_DIR=`cd "$curdir"; pwd`

export JAVA_OPTS="-Xmx1024m -Dfile.encoding=UTF-8"
export BROKER_LOG_DIR="$BROKER_HOME/log"
# export JAVA_HOME="/usr/java/jdk1.8.0_131"
# java
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java

# add libs to CLASSPATH
for f in $BROKER_HOME/lib/*.jar; do
  CLASSPATH=$f:${CLASSPATH};
done
export CLASSPATH=${CLASSPATH}:${BROKER_HOME}/lib:$BROKER_HOME/conf

while read line; do
    envline=`echo $line | sed 's/[[:blank:]]*=[[:blank:]]*/=/g' | sed 's/^[[:blank:]]*//g' | egrep "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*="`
    envline=`eval "echo $envline"`
    if [[ $envline == *"="* ]]; then
        eval 'export "$envline"'
    fi
done < $BROKER_HOME/conf/apache_hdfs_broker.conf

pidfile=$PID_DIR/apache_hdfs_broker.pid

if [ -f $pidfile ]; then
    if kill -0 `cat $pidfile` > /dev/null 2>&1; then
        echo "Broker running as process `cat $pidfile`.  Stop it first."
        exit 1
    fi
fi

if [ ! -d $BROKER_LOG_DIR ]; then
    mkdir -p $BROKER_LOG_DIR
fi

echo `date` >> $BROKER_LOG_DIR/apache_hdfs_broker.out

if [ ${RUN_DAEMON} -eq 1 ]; then
    nohup $LIMIT $JAVA $JAVA_OPTS org.apache.doris.broker.hdfs.BrokerBootstrap "$@" >> $BROKER_LOG_DIR/apache_hdfs_broker.out 2>&1 </dev/null &
else
    $LIMIT $JAVA $JAVA_OPTS org.apache.doris.broker.hdfs.BrokerBootstrap "$@" >> $BROKER_LOG_DIR/apache_hdfs_broker.out 2>&1 </dev/null
fi

echo $! > $pidfile

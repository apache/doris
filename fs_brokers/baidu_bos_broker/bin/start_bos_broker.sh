#!/usr/bin/env bash

# Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
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

export BROKER_HOME=`cd "$curdir/.."; pwd`

#
# JAVA_OPTS
# LOG_DIR
export JAVA_OPTS="-Xmx1024m -Dfile.encoding=UTF-8"
export LOG_DIR="$BROKER_HOME/log"
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
export CLASSPATH=${CLASSPATH}:${BROKER_HOME}/lib

pidfile=$curdir/bos_broker.pid

if [ -f $pidfile ]; then
    if kill -0 `cat $pidfile` > /dev/null 2>&1; then
        echo "Broker running as process `cat $pidfile`.  Stop it first."
        exit 1
    fi
fi

if [ ! -d $LOG_DIR ]; then
    mkdir -p $LOG_DIR
fi

if [ ! -f /bin/limit3 ]; then
    LIMIT=
else
    LIMIT="/bin/limit3 -c 0 -n 65536"
fi

nohup $LIMIT $JAVA $JAVA_OPTS com.baidu.palo.broker.bos.BrokerBootstrap "$@" >$LOG_DIR/bos_broker.out 2>&1 </dev/null &

echo $! > $pidfile

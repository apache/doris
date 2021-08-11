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

export DOIRS_MANAGER_HOME=`cd "$curdir/.."; pwd`

#
# JAVA_OPTS
# SERVER_PARAMS
# LOG_DIR
# PID_DIR
export JAVA_OPTS="-Xmx1024m"
export LOG_DIR="$DOIRS_MANAGER_HOME/log"
export PID_DIR=`cd "$curdir"; pwd`

# java
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi
JAVA=$JAVA_HOME/bin/java

# need check and create if the log directory existed before outing message to the log file.
if [ ! -d $LOG_DIR ]; then
    mkdir -p $LOG_DIR
fi

echo $JAVA_OPTS >> $LOG_DIR/doris-manager.out

pidfile=$PID_DIR/doris-manager.pid

if [ -f $pidfile ]; then
  if kill -0 `cat $pidfile` > /dev/null 2>&1; then
    echo doris-manager running as process `cat $pidfile`.  Stop it first.
    exit 1
  fi
fi

echo `date` >> $LOG_DIR/doris-manager.out

nohup $JAVA $JAVA_OPTS -jar ${DOIRS_MANAGER_HOME}/lib/dm-server.jar $SERVER_PARAMS >> $LOG_DIR/doris-manager.out 2>&1 </dev/null &

echo $! > $pidfile

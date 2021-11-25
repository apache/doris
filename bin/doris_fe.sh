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

export DORIS_HOME=`cd "$curdir"; pwd`
export PID_DIR=`cd "$curdir"; pwd`
function start() {
    echo "Starting Apache Doris Frontend"
    sh $DORIS_HOME/bin/start_fe.sh --daemon
    if [ -f "$DORIS_HOME/bin/fe.pid" ]; then
       echo "Apache Doris Frontend start fail!"
    else
      sleep 1s
      pid=`cat $DORIS_HOME/bin/fe.pid`
      echo "Doris Fe pid : ${pid}"
      echo "Apache Doris Frontend start success!"
    fi
}

function stop() {
    echo "stop Apache Doris Frontend"
    sh $DORIS_HOME/bin/stop_fe.sh
    if [ -f "$DORIS_HOME/bin/fe.pid" ]; then
       echo "Apache Doris Frontend stop fail!"
    else
       echo "Apache Doris Frontend stop success!"
    fi
}

function restart() {
    echo "Restart Apache Doris Frontend"
    echo "****************************"
    echo "Ready to stop Doris Frontend"
    stop
    sleep 5s
    echo "****************************"
    echo "Ready to start Doris Frontend"
    start
}

case "$1" in
	start )
		echo "****************************"
		start
		echo "****************************"
		;;
	stop )
		echo "****************************"
		stop
		echo "****************************"
		;;
	restart )
		echo "****************************"
		restart
		echo "****************************"
		;;
	* )
		echo "****************************"
		echo "Unknown command, please use: start,stop,restart"
		echo "****************************"
		;;
esac
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
    sh $DORIS_HOME/bin/start_be.sh --daemon
    if [ -f "$DORIS_HOME/bin/be.pid" ]; then
       echo "Apache Doris Backend start fail!"
    else
      sleep 1s
      pid=`cat $DORIS_HOME/bin/be.pid`
      echo "Doris Be pid : ${pid}"
      echo "Apache Doris Backend start success!"
    fi
}

function stop() {
    echo "stop Apache Doris Frontend"
    sh $DORIS_HOME/bin/stop_be.sh
    if [ -f "$DORIS_HOME/bin/be.pid" ]; then
       echo "Apache Doris Backend stop fail!"
    else
       echo "Apache Doris Backend stop success!"
    fi
}

function restart() {
    echo "Restart Apache Doris Backend"
    echo "****************************"
    echo "Ready to stop Doris Backend"
    stop
    sleep 5s
    echo "****************************"
    echo "Ready to start Doris Backend"
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
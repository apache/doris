#!/bin/bash

export DORIS_HOME=`cd "$curdir"; pwd`
export LOG_DIR=${DORIS_HOME}/log
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
      echo `sed -n '$p' $DORIS_HOME/log/be.out`
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
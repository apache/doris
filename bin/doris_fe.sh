#!/bin/bash

export DORIS_HOME=`cd "$curdir"; pwd`
export LOG_DIR=${DORIS_HOME}/log
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
      echo `sed -n '$p' $DORIS_HOME/log/fe.out`
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
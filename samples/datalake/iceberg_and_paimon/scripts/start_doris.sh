#!/usr/bin/env bash

export PATH=$JAVA_HOME/bin:$PATH

echo 'start to copy...'
cp -r /opt/doris-bin /opt/doris

echo 'start fe...'
rm -rf /opt/doris/fe/doris-meta/*
/opt/doris/fe/bin/start_fe.sh --daemon

echo 'start be...'
rm -rf /opt/doris/be/storage/*
/opt/doris/be/bin/start_be.sh --daemon

while [ ! -f "/opt/doris/fe/log/fe.log" ]
do
    echo "wait log..."
    sleep 2
done

QE=$(cat /opt/doris/fe/log/fe.log|grep "QE service start.")
echo $QE
while [ -z "$QE" ]
do
    echo "wait fe..."
    sleep 2
    QE=$(cat /opt/doris/fe/log/fe.log|grep "QE service start.")
done

echo 'doris is started'

MYSQL_ERROR=$(mysql -u root -P 9030 -h doris < /opt/doris-bin/init_doris.sql 2>&1)
ERR=$(echo $MYSQL_ERROR|grep "Can't connect to MySQL")
echo $ERR
while [ ! -z "$ERR" ]
do
    echo "wait mysql..."
    sleep 2
    MYSQL_ERROR=$(mysql -u root -P 9030 -h doris < /opt/doris-bin/init_doris.sql 2>&1)
    ERR=$(echo $MYSQL_ERROR|grep "Can't connect to MySQL")
done

echo 'doris is inited'

tail -F /dev/null
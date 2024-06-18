#!/usr/bin/env bash

/usr/bin/mysqld_safe &
while ! mysqladmin ping -proot --silent; do sleep 1; done

hive --service metatool -updateLocation hdfs://hadoop-master-2:9000/user/hive/warehouse hdfs://hadoop-master:9000/user/hive/warehouse

killall mysqld
while pgrep mysqld; do sleep 1; done

#!/bin/sh
echo "Welcome to Apache-Doris Docker-Compose to quickly build test images!"
echo "Start modifying the configuration files of FE and BE and run FE and BE!"
docker cp /root/init_fe.sh doris-fe:/root/init_fe.sh
docker exec doris-fe bash -c "/root/init_fe.sh"
docker cp /root/init_be.sh doris-be:/root/init_be.sh
docker exec doris-be bash -c "/root/init_be.sh"
sleep 30
echo "Get started with the Apache Doris registration steps!"
mysql -h 172.20.80.2 -P 9030 -uroot -e "ALTER SYSTEM ADD BACKEND \"172.20.80.3:9050\";"
echo "The initialization task of Apache-Doris has been completed, please start to experience it!"

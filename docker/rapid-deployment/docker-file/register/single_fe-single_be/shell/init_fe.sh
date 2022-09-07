#!/bin/bash
echo "Start initializing Apache-Doris FE!"
perl -pi -e "s|# priority_networks = 10.10.10.0/24;192.168.0.0/16|priority_networks = 172.20.80.0/16|g" /usr/local/apache-doris/fe/conf/fe.conf
start_fe.sh --daemon
echo "Apache-Doris FE initialized successfully!"

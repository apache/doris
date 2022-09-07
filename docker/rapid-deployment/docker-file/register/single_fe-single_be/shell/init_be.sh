#!/bin/bash
echo "Start initializing Apache-Doris BE!"
perl -pi -e "s|# priority_networks = 10.10.10.0/24;192.168.0.0/16|priority_networks = 172.20.80.0/16|g" /usr/local/apache-doris/be/conf/be.conf
start_be.sh --daemon
echo "Apache-Doris BE initialized successfully!"

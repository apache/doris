#!/bin/bash

export SQL="ALTER SYSTEM DECOMMISSION BACKEND  '"$BE_IPADDRESS":9050'"
echo $SQL > tmp.sql
mysql -h doris-fe-svc -P 9030 -u root < tmp.sql
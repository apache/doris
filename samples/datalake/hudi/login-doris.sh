#!/usr/bin/env bash

docker exec -it spark-hudi-hive mysql -u root -h doris-hudi-env -P 9030

#!/bin/bash

set -x

## mkdir and put data to hdfs
cd /mnt/scripts/suites/test_hive_remove_partition/ && tar xzf data.tar.gz
hadoop fs -mkdir -p /user/doris/suites/test_hive_remove_partition/
hadoop fs -put /mnt/scripts/suites/test_hive_remove_partition/data/* /user/doris/suites/test_hive_remove_partition/

# create table
hive -f /mnt/scripts/suites/test_hive_remove_partition/create_table.hql


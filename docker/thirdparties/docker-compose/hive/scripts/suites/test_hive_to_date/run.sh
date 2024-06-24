#!/bin/bash

set -x

## mkdir and put data to hdfs
cd /mnt/scripts/suites/test_hive_to_date/ && tar xzf data.tar.gz
hadoop fs -mkdir -p /user/doris/suites/test_hive_to_date/
hadoop fs -put /mnt/scripts/suites/test_hive_to_date/data/* /user/doris/suites/test_hive_to_date/

# create table
hive -f /mnt/scripts/suites/test_hive_to_date/create_table.hql


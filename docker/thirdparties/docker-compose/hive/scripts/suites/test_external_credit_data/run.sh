#!/bin/bash

set -x

## mkdir and put data to hdfs
cd /mnt/scripts/suites/test_external_credit_data/ && tar xzf data.tar.gz
hadoop fs -mkdir -p /user/doris/suites/test_external_credit_data/
hadoop fs -put /mnt/scripts/suites/test_external_credit_data/data/* /user/doris/suites/test_external_credit_data/

# create table
hive -f /mnt/scripts/suites/test_external_credit_data/create_table.hql


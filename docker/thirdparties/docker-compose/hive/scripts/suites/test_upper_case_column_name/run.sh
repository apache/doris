#!/bin/bash

set -x

## mkdir and put data to hdfs
cd /mnt/scripts/suites/test_upper_case_column_name/ && tar xzf hive_data.tar.gz
hadoop fs -mkdir -p /user/doris/suites/test_upper_case_column_name/
hadoop fs -put /mnt/scripts/suites/test_upper_case_column_name/hive_data/* /user/doris/suites/test_upper_case_column_name/

# create table
hive -f /mnt/scripts/suites/test_upper_case_column_name/create_table.hql


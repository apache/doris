#!/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""bloom filter test schema and data"""

import sys
sys.path.append('../')
from lib import palo_config
from lib import palo_client

schema = [('tinyint_key', 'TINYINT'), \
          ('smallint_key', 'SMALLINT'), \
          ('int_key', 'INT'), \
          ('bigint_key', 'BIGINT'), \
          ('char_50_key', 'CHAR(50)'), \
          ('character_key', 'VARCHAR(500)'), \
          ('char_key', 'CHAR'), \
          ('character_most_key', 'VARCHAR(65533)'), \
          ('decimal_key', 'DECIMAL(20, 6)'), \
          ('decimal_most_key', 'DECIMAL(27, 9)'), \
          ('date_key', 'DATE'), \
          ('datetime_key', 'DATETIME'), \
          ('tinyint_value', 'TINYINT', 'SUM'), \
          ('smallint_value', 'SMALLINT', 'SUM'), \
          ('int_value', 'int', 'SUM'), \
          ('bigint_value', 'BIGINT', 'SUM'), \
          ('char_50_value', 'CHAR(50)', 'REPLACE'), \
          ('character_value', 'VARCHAR(500)', 'REPLACE'), \
          ('char_value', 'CHAR', 'REPLACE'), \
          ('character_most_value', 'VARCHAR(65533)', 'REPLACE'), \
          ('decimal_value', 'DECIMAL(20, 6)', 'SUM'), \
          ('decimal_most_value', 'DECIMAL(27, 9)', 'SUM'), \
          ('date_value_max', 'DATE', 'MAX'), \
          ('date_value_replace', 'DATE', 'REPLACE'), \
          ('date_value_min', 'DATE', 'MIN'), \
          ('datetime_value_max', 'DATETIME', 'MAX'), \
          ('datetime_value_replace', 'DATETIME', 'REPLACE'), \
          ('datetime_value_min', 'DATETIME', 'MIN'), \
          ('float_value', 'FLOAT', 'SUM'), \
          ('double_value', 'DOUBLE', 'SUM')]   

column_name_list = [column[0] for column in schema]
key_list = [column[0] for column in schema if len(column) == 2] 
value_list = [column[0] for column in schema if len(column) == 3]
bloom_filter_column_list = list()
for column in schema:
    if len(column) == 2 and column[1] != "TINYINT":
        bloom_filter_column_list.append(column[0])
    # 聚合模型的value列不支持创建bloom filter
    # elif len(column) == 3 and column[2] == "REPLACE":
    #     bloom_filter_column_list.append(column[0])
tinyint_key_list = list()
for column in schema:
    if len(column) == 2 and column[1] == "TINYINT":
        tinyint_key_list.append(column[0])
tinyint_value_list = list()
for column in schema:
    if len(column) == 3 and column[1] == "TINYINT":
        tinyint_value_list.append(column[0])
illegal_bloom_filter_column_list = list()
for column in schema:
    if column[0] not in bloom_filter_column_list:
        illegal_bloom_filter_column_list.append([column[0]])

file_path = palo_config.gen_remote_file_path('sys/all_type.txt')
expected_data_file_list = './data/all_type_834'
expected_data_file_list_3 = './data/PARTITION/partition_type_del_k1'
expected_data_file_list_4 = './data/PARTITION/partition_type_delete_k3_eq_100'

#TINYINT value with REPLACE
schema_tinyint = [('tinyint_key', 'TINYINT'), \
        ("tinyint_value", "TINYINT", "REPLACE")]
tinyint_value_replace_list = list()
for column in schema_tinyint:
    if len(column) == 3 and column[1] == "TINYINT" and column[2] == "REPLACE":
        tinyint_value_replace_list.append(column[0])

#FLOAT DOUBLE value with REPLACE
schema_float = [('tinyint_key', 'TINYINT'), \
        ("float_value", "FLOAT", "REPLACE"), \
        ("double_value", "DOUBLE", "REPLACE")]
float_value_replace_list = list()
for column in schema_float:
    if len(column) == 3 and column[1] == "FLOAT" and column[2] == "REPLACE":
        float_value_replace_list.append(column[0])
double_value_replace_list = list()
for column in schema_float:
    if len(column) == 3 and column[1] == "DOUBLE" and column[2] == "REPLACE":
        double_value_replace_list.append(column[0])

#test_default_value
schema_default_value =  [('increment_key', 'INT', None, "0"), \
          ('smallint_key', 'SMALLINT', None, "-1"), \
          ('int_key', 'INT', None, "-100"), \
          ('bigint_key', 'BIGINT', None, "100"), \
          ('char_50_key', 'CHAR(50)', None, ""), \
          ('character_key', 'VARCHAR(500)', None, "abc"), \
          ('char_key', 'CHAR', None, "Z"), \
          ('character_most_key', 'VARCHAR(65533)', None, ""), \
          ('decimal_key', 'DECIMAL(20, 6)', None, "123.321"), \
          ('decimal_most_key', 'DECIMAL(27, 9)', None, "0"), \
          ('date_key', 'DATE', None, "2015-10-12"), \
          ('datetime_key', 'DATETIME', None, "2015-10-12 13:30:32"), \
          ('tinyint_value', 'TINYINT', 'SUM', "2"), \
          ('smallint_value', 'SMALLINT', 'SUM', "20"), \
          ('int_value', 'int', 'SUM', "200"), \
          ('bigint_value', 'BIGINT', 'SUM', "2000"), \
          ('char_50_value', 'CHAR(50)', 'REPLACE', ""), \
          ('character_value', 'VARCHAR(500)', 'REPLACE', "test"), \
          ('char_value', 'CHAR', 'REPLACE', ""), \
          ('character_most_value', 'VARCHAR(65533)', 'REPLACE', ""), \
          ('decimal_value', 'DECIMAL(20, 6)', 'SUM', "-123.321"), \
          ('decimal_most_value', 'DECIMAL(27, 9)', 'SUM', "0.01"), \
          ('date_value_replace', 'DATE', 'REPLACE', "2015-10-12"), \
          ('date_value_max', 'DATE', 'REPLACE', "2015-10-12"), \
          ('date_value_min', 'DATE', 'REPLACE', "2015-10-12"), \
          ('datetime_value_replace', 'DATETIME', 'REPLACE', "2015-10-12 13:30:32"), \
          ('datetime_value_max', 'DATETIME', 'REPLACE', "2015-10-12 13:30:32"), \
          ('datetime_value_min', 'DATETIME', 'REPLACE', "2015-10-12 13:30:32"), \
          ('float_value', 'FLOAT', 'SUM', "1"), \
          ('double_value', 'DOUBLE', 'SUM', "0")]   
default_value_list = list()
for column in schema_default_value:
    # if column[1] != "TINYINT" and (column[2] is None or column[2] == "REPLACE"):
    if column[1] != "TINYINT" and column[2] is None:
        default_value_list.append(column[0])
default_value_file_path = palo_config.gen_remote_file_path("sys/increment_key.txt")
expected_default_value_file = "data/bloom_filter/default_value.txt"

#partition
partition_info = palo_client.PartitionInfo("tinyint_key", \
        ["p_0", "p_1"], ["0", "MAXVALUE"])
distribution_info = palo_client.DistributionInfo("HASH(tinyint_key)", 10)
#be/ce
be_ce_file_path_list = list()
be_ce_local_file_list = list()
for suffix in ["aa", "ab", "ac", "ad", "ae", "af", "ag", "ah", "ai", "aj", \
        "ak", "al", "am", "an", "ao", "ap", "aq"]:
    be_ce_file_path_list.append(palo_config.gen_remote_file_path("sys/be_ce/all_type_part_%s" \
            % suffix))
    be_ce_local_file_list.append("data/bloom_filter/be_ce/all_type_part_%s" % suffix)

#multi local file path
multi_local_file_list = list()
for suffix in ["a", "b", "c", "d", "e", "f", "g", "h", "i"]:
    multi_local_file_list.append("data/bloom_filter/all_type_part_a%s" % suffix)

#rollup
base_rollup_column_list = ["smallint_key", "date_key", "datetime_key"]
for column in schema:
    if column[0] not in base_rollup_column_list:
        base_rollup_column_list.append(column[0])
rollup_column_list = ["int_key", "date_key", "tinyint_key", "int_value", "float_value"]

rollup_with_bloom_filter = ["int_key", "character_key", "tinyint_key", "datetime_key", \
        "tinyint_value", "float_value"]

rollup_without_bloom_filter = list()
for column in schema:
    if column[0] not in bloom_filter_column_list and (len(column) == 2 or \
        (len(column) == 3 and column[2] != 'REPLACE')):
        rollup_without_bloom_filter.append(column[0])

#schema change
#drop_column_list = ["date_key", "character_value", "decimal_value"]
drop_column_list = ["character_value", "decimal_value"]
bloom_filter_column_list_drop_none_bf = list()
for column in bloom_filter_column_list:
    if column not in drop_column_list:
        bloom_filter_column_list_drop_none_bf.append(column)

modify_column_list = ["smallint_key INT", "character_key VARCHAR(600)", \
        "character_value VARCHAR(600) REPLACE", "smallint_value INT SUM"]

partition_info_for_add = palo_client.PartitionInfo("tinyint_key", \
        ["p_0", "p_1"], ["0", "10"])


if __name__ == '__main__':
    print(bloom_filter_column_list)
    print(rollup_without_bloom_filter)

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

"""
file: stream_test_partition.py
测试 stream load partition
"""

import os
import sys
import time

file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

import pymysql
from data import small_load_complex as DATA
from lib import palo_config
from lib import palo_client
from lib import util

client = None

config = palo_config.config
storage_type = DATA.storage_type

bucket_num = 5
distribution_type = 'hash(k1)'
duplicate_key = 'DUPLICATE KEY(k1, k2, k3, k4, k5, k6, k10, k11, k7)'
unique_key = 'UNIQUE KEY(k1, k2, k3, k4, k5, k6, k10, k11, k7)'

column_list = [("k1", "tinyint"), \
               ("k2", "smallint"), \
               ("k3", "int"),\
               ("k4", "bigint"), \
               ("k5", "decimal(9, 3)"),\
               ("k6", "char(5)"), \
               ("k10", "date"), \
               ("k11", "datetime"),\
               ("k7", "varchar(20)"),\
               ("k8", "double", "max"),\
               ("k9", "float", "sum")
              ]

column_no_agg_list = [("k1", "tinyint"), \
                      ("k2", "smallint"), \
                      ("k3", "int"),\
                      ("k4", "bigint"), \
                      ("k5", "decimal(9, 3)"),\
                      ("k6", "char(5)"), \
                      ("k10", "date"), \
                      ("k11", "datetime"),\
                      ("k7", "varchar(20)"),\
                      ("k8", "double"),\
                      ("k9", "float")
                     ]


def setup_module():
    """
    setUp
    """
    global client, mysql_cursor
    mysql_con=pymysql.connect(host=config.mysql_host, user=config.mysql_user,
                              passwd=config.mysql_password, port=config.mysql_port, 
                              db=config.mysql_db)
    mysql_cursor = mysql_con.cursor()

    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user, \
            password=config.fe_password)
    client.init()


def check_partition_list(table_family_group_name, partition_name_list):
    """
    验证分区是否创建成功
    """
    for partition_name in partition_name_list:
        print("--------------------")
        print(table_family_group_name)
        print("-------------------")
        ret = client.get_partition(table_family_group_name, partition_name)
        assert ret


def create_table_family(table_family_group_name, column_name,
        partition_name_list, partition_value_list, partition_name, key_type):
    """
    todo
    """

    partition_info = palo_client.PartitionInfo(partition_name,
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo(distribution_type, bucket_num)
    client.create_table(table_family_group_name, column_name,
                        partition_info, distribution_info, keys_desc=key_type)
    assert client.show_tables(table_family_group_name)
    check_partition_list(table_family_group_name, partition_name_list)


def check(line1, line2):
    """
    todo
    """
    print(line1)
    print(line2)
    palo_result = client.execute(line1)
    mysql_cursor.execute(line2)
    mysql_result = mysql_cursor.fetchall()
    util.check(palo_result, mysql_result)


def init_data(data_file, database_name, table_family_name,
        partition_name_list, partition_value_list, partition_name):
    """
    small_load and check
    清除，建立数据库，建表，导入，导入完成后判断是否导入成功
    """
    for key_type in [None, duplicate_key, unique_key]:
        client.clean(database_name)
        ret = client.create_database(database_name)
        assert ret
        client.use(database_name)
        if key_type is None:
            column_type_list = column_list
        else:
            column_type_list = column_no_agg_list
        create_table_family(table_family_name, column_type_list, partition_name_list,
                            partition_value_list, partition_name, key_type)
        sub_partitio_list = partition_name_list[0:2]
        ret = client.stream_load(table_family_name, data_file, partition_list=sub_partitio_list,
                                  max_filter_ratio=0.65, column_name_list=DATA.column_name_list)
        assert ret
        line1 = "select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11\
                from %s order by k1" % table_family_name
        line2 = "select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 \
                from baseall where %s < \"%s\" order by k1" % (partition_name, 
                partition_value_list[1])
        check(line1, line2)
    client.clean(database_name)


def test_type_k1():
    """
    {
    "title": "测试建表时，各个不同分区的导入, 使用k1建立分区，并导入",
    "describe": "describe",
    "tag": "function,p1,fuzz"
    }
    """
    """
    测试建表时，各个不同分区的导入
    使用k1，k2，k3，k4，k10，k11建立分区，并导入
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list()
    data_file = "%s/data/small_load_simple/test_small_load_base.data" % file_dir
    partition_name_list = ['partition_a', 'partition_b', 'partition_c']
    partition_value_list = ['5', '10', 'MAXVALUE']
    partition_name = "k1"
    print("***********************")
    print(partition_name)
    init_data(data_file, database_name, table_family_name, partition_name_list,
            partition_value_list, partition_name)


def test_type_k2():
    """
    {
    "title": "test_stream_partition.test_type_k2",
    "describe": "测试建表时，各个不同分区的导入, 使用k2建立分区，并导入",
    "tag": "function,p1,fuzz"
    }
    """
    """
    测试建表时，各个不同分区的导入
    使用k1，k2，k3，k4，k10，k11建立分区，并导入
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list()
    data_file = "%s/data/small_load_simple/test_small_load_base.data" % file_dir
    partition_name_list = ['partition_a', 'partition_b', 'partition_c']
    partition_value_list = ['0', '2000', 'MAXVALUE']
    partition_name = "k2"
    print("***********************")
    print(partition_name)
    init_data(data_file, database_name, table_family_name, partition_name_list,
            partition_value_list, partition_name)


def test_type_k3():
    """
    {
    "title": "test_stream_partition.test_type_k3",
    "describe": "测试建表时，各个不同分区的导入, 使用k3建立分区，并导入",
    "tag": "function,p1,fuzz"
    }
    """
    """
    测试建表时，各个不同分区的导入
    使用k1，k2，k3，k4，k10，k11建立分区，并导入
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list()
    data_file = "%s/data/small_load_simple/test_small_load_base.data" % file_dir
    partition_name_list = ['partition_a', 'partition_b', 'partition_c']
    partition_value_list = ['0', '2000', 'MAXVALUE']
    partition_name = "k3"
    print("***********************")
    print(partition_name)
    init_data(data_file, database_name, table_family_name, partition_name_list,
            partition_value_list, partition_name)


def test_type_k4():
    """
    {
    "title": "test_stream_partition.test_type_k4",
    "describe": "测试建表时，各个不同分区的导入, 使用k4建立分区，并导入",
    "tag": "function,p1,fuzz"
    }
    """
    """
    测试建表时，各个不同分区的导入
    使用k1，k2，k3，k4，k10，k11建立分区，并导入
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list()
    data_file = "%s/data/small_load_simple/test_small_load_base.data" % file_dir
    partition_name_list = ['partition_a', 'partition_b', 'partition_c']
    partition_value_list = ['0', '11011905', 'MAXVALUE']
    partition_name = "k4"
    print("***********************")
    print(partition_name)
    init_data(data_file, database_name, table_family_name, partition_name_list,
            partition_value_list, partition_name)


def test_type_k10():
    """
    {
    "title": "test_stream_partition.test_type_k10",
    "describe": "测试建表时，各个不同分区的导入, 使用k10建立分区，并导入",
    "tag": "function,p1,fuzz"
    }
    """
    """
    测试建表时，各个不同分区的导入
    使用k1，k2，k3，k4，k10，k11建立分区，并导入
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list()
    data_file = "%s/data/small_load_simple/test_small_load_base.data" % file_dir
    partition_name_list = ['partition_a', 'partition_b', 'partition_c']
    partition_value_list = ['1910-01-01', '2012-03-14', 'MAXVALUE']
    partition_name = "k10"
    print("***********************")
    print(partition_name)
    init_data(data_file, database_name, table_family_name, partition_name_list,
            partition_value_list, partition_name)


def test_type_k11():
    """
    {
    "title": "test_type_k11",
    "describe": "测试建表时，各个不同分区的导入, 使用k11建立分区，并导入",
    "tag": "function,p1,fuzz"
    }
    """
    """
    测试建表时，各个不同分区的导入
    使用k1，k2，k3，k4，k10，k11建立分区，并导入
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list()
    data_file = "%s/data/small_load_simple/test_small_load_base.data" % file_dir
    partition_name_list = ['partition_a', 'partition_b', 'partition_c']
    partition_value_list = ['1910-01-01 00:00:00', '2012-03-14 00:00:00', 'MAXVALUE']
    partition_name = "k11"
    print("***********************")
    print(partition_name)
    init_data(data_file, database_name, table_family_name, partition_name_list,
            partition_value_list, partition_name)
    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    setup_module()
    test_type_k1()


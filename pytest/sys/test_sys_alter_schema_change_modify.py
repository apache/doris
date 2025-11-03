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
/***************************************************************************
  *
  * @file test_sys_alter_schema_change_modify.py
  * @date 2020-02-04
  * @brief This file is a test file for Palo modify column.
  * 
  **************************************************************************/
"""
import random

from data import schema_change as DATA
from lib import palo_config
from lib import palo_client
from lib import util

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)


def check_partition_list(table_name, partition_name_list):
    """
    验证分区是否创建成功
    """
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)


def partition_check(table_name, column_name, partition_name_list, \
        partition_value_list, distribution_type, bucket_num, storage_type, schema, table_type="aggregate"):
    """
    检查，验证
    """
    partition_info = palo_client.PartitionInfo(column_name, \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo(distribution_type, bucket_num)
    if table_type == "aggregate":
        client.create_table(table_name, schema, \
                            partition_info, distribution_info)
    elif table_type == "unique":
        client.create_table(table_name, schema, \
            partition_info, distribution_info, keys_desc=DATA.key_1_uniq)
    elif table_type == "dup":
        client.create_table(table_name, schema, \
            partition_info, distribution_info, keys_desc=DATA.key_1_dup) 
    else:
        assert False, "Wrong table type!"
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)


def check(table_name, schema, table_type="aggregate"):
    """
    分区，检查
    """
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d',
                           'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['5', '30', '100', '500', '1000', '2000', 'MAXVALUE']
    partition_check(table_name, 'k1',
                    partition_name_list, partition_value_list,
                    'HASH(k1, k2)', random.randrange(1, 30), 'column', schema, table_type)


def test_modify_varchar_to_date():
    """
    {
    "title": "test_sys_alter_schema_change_modify.test_modify_varchar_to_date",
    "describe": "test_modify_varchar_to_date, 测试varchar到date的字段类型转换，支持6种格式的date："%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d, "%y/%m/%d"",
    "tag": "system,p1"
    }
    """
    """
    test_modify_varchar_to_date
    测试varchar到date的字段类型转换，支持6种格式的date：
    "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d, "%y/%m/%d"
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_1_alter_modify_date)
    sql_insert = "insert into %s values(1,\"1\",\"20200101\",1,1), "\
        "(2,\"2\",\"2020/01/02\",2,2), (3,\"3\",\"2020-01-03\",3,3), "\
        "(4,\"4\",\"200104\",4,4), (5,\"5\",\"20/01/05\",5,5), (6,\"6\",\"20-01-06\",6,6)" % table_name
    r = client_exe.execute(sql_insert)
    assert r == ()
    # 验证插入是否成功，select结果
    sql_select = "select * from %s" % table_name
    ret = client_exe.execute(sql_select)
    assert len(ret) == 6
    # 修改varchar为date
    util.assert_return(True, '',
                       client.schema_change, table_name, modify_column_list = ["k3 date"],
                       is_wait=True)
    # 验证数据类型为date
    column_info = client.get_column_info("k3", table_name, database_name)
    assert column_info[1] == 'DATE'
    # modify之后插入数据
    sql_insert_after_modify =  "insert into %s values(7,\"7\",\"2020-07-07\",7,7)" % table_name
    r = client_exe.execute(sql_insert_after_modify)
    assert r == ()
    client.clean(database_name)


def test_modify_varchar_to_number_err():
    """
    {
    "title": "test_sys_alter_schema_change_modify.test_modify_varchar_to_number_err",
    "describe": "test_modify_varchar_to_number, 测试varchar到数字的字段类型转换，key列不支持改为fload和double类型",
    "tag": "system,p1"
    }
    """
    """
    test_modify_varchar_to_number
    测试varchar到数字的字段类型转换，支持7种格式的数字：
    TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)

    check(table_name, DATA.schema_1_alter_modify_number)
    sql_insert = "insert into %s values(1, \"1\", \"1\", \"1\", \"1\", \"1\", \"1\", \"1\", \"1\", 1, 1)"\
         % table_name
    r = client_exe.execute(sql_insert)
    assert r == ()
    # 验证插入是否成功，select结果
    sql_select = "select * from %s" % table_name
    ret = client_exe.execute(sql_select)
    assert len(ret) == 1
    # 修改varchar为num
    msg = 'Float or double can not used as a key, use decimal instead.'
    util.assert_return(False, '',
                       client.schema_change, table_name, 
                       modify_column_list = ["k3 TINYINT", "k4 SMALLINT", "k5 INT", 
                       "k6 BIGINT", "k7 LARGEINT", "k8 FLOAT", "k9 DOUBLE"])
    client.clean(database_name)


def test_modify_varchar_to_number():
    """
    {
    "title": "test_sys_alter_schema_change_modify.test_modify_varchar_to_number",
    "describe": "test_modify_varchar_to_number, 测试varchar到数字的字段类型转换，支持7种格式的数字:TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE",
    "tag": "system,p1"
    }
    """
    """
    test_modify_varchar_to_number
    测试varchar到数字的字段类型转换，支持7种格式的数字：
    TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)

    alter_modify_number_schema = util.convert_agg_column_to_no_agg_column(DATA.schema_1_alter_modify_number)
    check(table_name, alter_modify_number_schema, 'dup')
    sql_insert = "insert into %s values(1, \"1\", \"1\", \"1\", \"1\", \"1\", \"1\", \"1\", \"1\", 1, 1)"\
         % table_name
    r = client_exe.execute(sql_insert)
    assert r == ()
    # 验证插入是否成功，select结果
    sql_select = "select * from %s" % table_name
    ret = client_exe.execute(sql_select)
    assert len(ret) == 1
    # 修改varchar为num
    util.assert_return(True, '',
                       client.schema_change, table_name,
                       modify_column_list = ["k3 TINYINT", "k4 SMALLINT", "k5 INT",
                       "k6 BIGINT", "k7 LARGEINT", "k8 FLOAT", "k9 DOUBLE"],
                       is_wait=True)
    column_info_tinyint = client.get_column_info("k3", table_name, database_name)
    column_info_smallint = client.get_column_info("k4", table_name, database_name)
    column_info_int = client.get_column_info("k5", table_name, database_name)
    column_info_bigint = client.get_column_info("k6", table_name, database_name)
    column_info_largeint = client.get_column_info("k7", table_name, database_name)
    column_info_float = client.get_column_info("k8", table_name, database_name)
    column_info_double = client.get_column_info("k9", table_name, database_name)
    assert column_info_tinyint[1] == 'TINYINT' and column_info_smallint[1] == 'SMALLINT' \
        and column_info_int[1] == 'INT' and  column_info_bigint[1] == 'BIGINT'  \
        and column_info_largeint[1] == 'LARGEINT' and  column_info_float[1] == 'FLOAT' \
        and column_info_double[1] == 'DOUBLE'
    # modify之后插入数据
    sql_insert_after_modify =  "insert into %s values(2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2)" % table_name
    r = client_exe.execute(sql_insert_after_modify)
    assert r == ()
    client.clean(database_name)


def test_modify_varchar_to_decimal():
    """
    {
    "title": "test_sys_alter_schema_change_modify.test_modify_varchar_to_decimal",
    "describe": "test_modify_varchar_to_decimal, 测试varchar到decimal的字段类型转换，目前不支持decimal类型，执行报错",
    "tag": "system,p1,fuzz"
    }
    """
    """
    test_modify_varchar_to_decimal
    测试varchar到decimal的字段类型转换，目前不支持decimal类型，执行报错
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_1_alter_modify_date)
    sql_insert = "insert into %s values(1,\"1\",\"1.11\",1,1)" % table_name
    r = client_exe.execute(sql_insert)
    assert r == ()
    # 验证插入是否成功，select结果
    sql_select = "select * from %s" % table_name
    ret = client_exe.execute(sql_select)
    assert len(ret) == 1
    # 修改varchar为decimal
    util.assert_return(False, '',
                       client.schema_change, table_name, modify_column_list = ["k3 decimal"],
                       is_wait=True)
    client.clean(database_name)


def test_modify_varchar_to_date_wrong_format():
    """
    {
    "title": "test_sys_alter_schema_change_modify.test_modify_varchar_to_date_wrong_format",
    "describe": "test_modify_varchar_to_date_wrong_format, 测试varchar到date的字段类型转换，原varchar字段不是date类型,  预期转换失败，任务状态为cancelled",
    "tag": "system,p1,fuzz"
    }
    """
    """
    test_modify_varchar_to_date_wrong_format
    测试varchar到date的字段类型转换，原varchar字段不是date类型
    预期转换失败，任务状态为cancelled
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)

    check(table_name, DATA.schema_1_alter_modify_date)
    sql_insert = "insert into %s values(1,\"1\",\"202001011200\",1,1)" % table_name
    r = client_exe.execute(sql_insert)
    assert r == ()
    # 修改varchar为date
    ret = client.schema_change(table_name, modify_column_list=["k3 date"], is_wait=True)
    assert not ret
    # ret1 = client.select_all(table_name)
    # ret2 = client.execute('select 1, 1, cast("2020-01-01" as date), 1, 1.0')
    # util.check(ret1, ret2)
    client.clean(database_name)


def test_modify_varchar_to_date_unique_table():
    """
    {
    "title": "test_sys_alter_schema_change_modify.test_modify_varchar_to_date_unique_table",
    "describe": "test_modify_varchar_to_date_unique_table, 测试varchar到date的字段类型转换，unique表类型",
    "tag": "system,p1"
    }
    """
    """
    test_modify_varchar_to_date_unique_table
    测试varchar到date的字段类型转换，unique表类型
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_1_uniq, "unique")
    sql_insert = "insert into %s values(1,1,\"20200101\",1,1), "\
        "(2,2,\"2020/01/02\",2,2), (3,3,\"2020-01-03\",3,3)" % table_name
    r = client_exe.execute(sql_insert)
    assert r == ()
    # 验证插入是否成功，select结果
    sql_select = "select * from %s" % table_name
    ret = client_exe.execute(sql_select)
    assert len(ret) == 3
    # 修改varchar为date
    util.assert_return(True, '',
                       client.schema_change, table_name, modify_column_list = ["v1 date"],
                       is_wait=True)
    # 验证数据类型为date
    column_info = client.get_column_info("v1", table_name, database_name)
    assert column_info[1] == 'DATE'
    # modify之后插入数据
    sql_insert_after_modify =  "insert into %s values(4,4,\"2020-01-04\",4,4)" % table_name
    r = client_exe.execute(sql_insert_after_modify)
    assert r == ()
    client.clean(database_name)

    
def test_modify_varchar_to_date_duplicate_table():
    """
    {
    "title": "test_sys_alter_schema_change_modify.test_modify_varchar_to_date_duplicate_table",
    "describe": "test_modify_varchar_to_date_duplicate_table,测试varchar到date的字段类型转换，duplicate表类型",
    "tag": "system,p1"
    }
    """
    """
    test_modify_varchar_to_date_duplicate_table
    测试varchar到date的字段类型转换，duplicate表类型
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    
    check(table_name, DATA.schema_1_dup, "dup")
    sql_insert = "insert into %s values(1,1,\"20200101\",1,1), "\
        "(2,2,\"2020/01/02\",2,2), (3,3,\"2020-01-03\",3,3)" % table_name
    r = client_exe.execute(sql_insert)
    assert r == ()
    # 验证插入是否成功，select结果
    sql_select = "select * from %s" % table_name
    ret = client_exe.execute(sql_select)
    assert len(ret) == 3
    # 修改varchar为date
    util.assert_return(True, '',
                       client.schema_change, table_name, modify_column_list = ["v1 date"],
                       is_wait=True)
    # 验证数据类型为date
    column_info = client.get_column_info("v1", table_name, database_name)
    assert column_info[1] == 'DATE'
    # modify之后插入数据
    sql_insert_after_modify =  "insert into %s values(4,4,\"2020-01-04\",4,4)" % table_name
    r = client_exe.execute(sql_insert_after_modify)
    assert r == ()
    client.clean(database_name)


def test_modify_varchar_to_number_overflow():
    """
    {
    "title": "test_sys_alter_schema_change_modify.test_modify_varchar_to_number_overflow",
    "describe": "test_modify_varchar_to_number_overflow, 测试varchar到number类型转换，其中数据有已溢出, 以int为例，最大为2147483647，插入大于2147483647的值之后，再将varchar转换为int",
    "tag": "system,p1,fuzz"
    }
    """
    """
    test_modify_varchar_to_number_overflow
    测试varchar到number类型转换，其中数据有已溢出
    以int为例，最大为2147483647，插入大于2147483647的值之后，再将varchar转换为int
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)

    check(table_name, DATA.schema_1_alter_modify_number)
    sql_insert = "insert into %s values(1, \"1\", \"1\", \"1\", \"2147483648\", \"1\", \"1\", \"1\", \"1\", 1, 1)" % table_name
    r = client_exe.execute(sql_insert)
    assert r == ()
    # 修改varchar为num，数据溢出alter任务失败
    ret = client.schema_change(table_name, modify_column_list=["k5 INT"], is_wait=True)
    assert not ret
    client.clean(database_name)

    
def test_modify_varchar_to_date_notnull_to_null():
    """
    {
    "title": "test_sys_alter_schema_change_modify.test_modify_varchar_to_date_notnull_to_null",
    "describe": "test_modify_varchar_to_date_notnull_to_null, 测试varchar到date的字段类型转换，转换后not null字段变null字段",
    "tag": "system,p1"
    }
    """
    """
    test_modify_varchar_to_date_notnull_to_null
    测试varchar到date的字段类型转换，转换后not null字段变null字段"
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_1_alter_modify_date)
    sql_insert = "insert into %s values(1,\"1\",\"20200101\",1,1)" % table_name
    r = client_exe.execute(sql_insert)
    assert r == ()
    # 验证插入是否成功，select结果
    sql_select = "select * from %s" % table_name
    ret = client_exe.execute(sql_select)
    assert len(ret) == 1
    # 修改字段为null
    util.assert_return(True, '',
                       client.schema_change, table_name, modify_column_list = ["k3 date null"],
                       is_wait=True)
    # 获取字段类型
    column_info = client.get_column_info("k3", table_name, database_name)
    assert column_info[1] == 'DATE'
    # modify之后插入数据
    sql_insert_after_modify =  "insert into %s values(7,\"7\",\"2020-07-07\",7,7)" % table_name
    r = client_exe.execute(sql_insert_after_modify)
    assert r == ()
    client.clean(database_name)

    
def test_modify_varchar_to_date_null_to_notnull():
    """
    {
    "title": "test_sys_alter_schema_change_modify.test_modify_varchar_to_date_null_to_notnull",
    "describe": "test_modify_varchar_to_date_null_to_notnull,测试varchar到date的字段类型转换，添加null记录，转换字段，转换成功",
    "tag": "system,p1,fuzz"
    }
    """
    """
    test_modify_varchar_to_date_null_to_notnull
    测试varchar到date的字段类型转换，添加null记录，转换字段，转换成功"
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_1_alter_modify_date)
    # 修改varchar为null
    util.assert_return(True, '',
                       client.schema_change, table_name, modify_column_list = ["k3 varchar(4096) null"],
                       is_wait=True)
    # 修改varchar为date
    util.assert_return(True, '',
                       client.schema_change, table_name, modify_column_list = ["k3 date"],
                       is_wait=True)
    # todo
    client.clean(database_name)


def test_modify_comment():
    """
    {
    "title": "test_sys_alter_schema_change_modify.test_modify_comment",
    "describe": "修改表的comment和字段的comment, github issue #6387",
    "tag": "system,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    check(table_name, DATA.schema_1)
    #修改表comment
    client.schema_change(table_name, comment='new_comment')
    ret = client.get_comment(database_name, table_name)
    assert ret == 'new_comment', "Modify comment failed"
    client.schema_change(table_name, comment='new_comment_2')
    ret = client.get_comment(database_name, table_name)
    assert ret == 'new_comment_2', "Modify table comment failed"
    #修改字段comment
    modify_column_list = ["k1 comment 'new_k1_comment'"]
    client.schema_change(table_name, modify_column_list=modify_column_list)
    ret = client.get_column_comment(table_name, 'k1')
    assert ret == 'new_k1_comment', "Modify column comment failed"
    modify_column_list = ["v1 comment 'new_v1_comment'"]
    client.schema_change(table_name, modify_column_list=modify_column_list)
    ret = client.get_column_comment(table_name, 'v1')
    assert ret == 'new_v1_comment', "Modify column comment failed"
    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    print("End")


if __name__ == '__main__':
    import pdb
    pdb.set_trace()
    setup_module()
    test_modify_varchar_to_date()
    test_modify_varchar_to_number()
    test_modify_varchar_to_date_wrong_format()
    test_modify_varchar_to_date_unique_table()
    test_modify_varchar_to_date_duplicate_table()
    test_modify_varchar_to_number_overflow()
    test_modify_varchar_to_decimal()
    test_modify_varchar_to_date_notnull_to_null()
    test_modify_varchar_to_date_null_to_notnull()
    teardown_module()

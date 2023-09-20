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
test bloom filter
Date: 2015/10/09 17:32:19
"""

import pymysql
from data import bloom_filter as DATA
from lib import palo_client
from lib import palo_config
from lib import util

LOG = palo_client.LOG
L = palo_client.L

client = None
config = palo_config.config
broker_info = palo_config.broker_info


def setup_module():
    """
    set up
    """
    global client
    global mysql_db
    mysql_db = 'bloom_filter_c_mysql'
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, 
                                    password=config.fe_password, http_port=config.fe_http_port)
    init_mysql(config.mysql_host, config.mysql_port, "./data/bloom_filter/init_mysql.sql",\
            user=config.mysql_user, password=config.mysql_password)


def init_mysql(host, port, sql_file, user="root", password=""):
    """
    初始化数据
    """
    connect = pymysql.connect(host=host, port=port, user=user, passwd=password)
    cursor = connect.cursor()
    cursor.execute('show databases like "%s"' % mysql_db)
    ret = cursor.fetchall()
    if len(ret) == 1:
        return
    init_sql = open(sql_file).read()
    lines = init_sql.split(";")
    for line in lines:
        line = line.strip()
        if not line:
            continue
        cursor.execute(line.replace("test", mysql_db))
        connect.commit()


def execute(host, port, sql, user="root", password=""):
    """
    连接mysql执行语句
    """
    connect = pymysql.connect(host=host, port=port, user=user, passwd=password)
    cursor = connect.cursor()
    try:
        LOG.info(L('mysql check sql', sql=sql))
        cursor.execute(sql)
        return cursor.fetchall()
    except Exception as error:
        assert False, "execute error. %s" % str(error)


def test_add_column():
    """
    {
    "title": "test_sys_bloom_filter_c.test_add_column",
    "describe": "添加普通列",
    "tag": "system,p1"
    }
    """
    """
    添加普通列
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.schema, \
            partition_info=DATA.partition_info, \
            distribution_info=DATA.distribution_info, \
            bloom_filter_column_list=DATA.bloom_filter_column_list)
    assert ret
    length = len(DATA.multi_local_file_list)
    ix = 0
    for local_file in DATA.multi_local_file_list[:int(length / 2)]:
        ix = ix + 1
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    sql = "SELECT %s FROM %s.%s" % (",".join(DATA.column_name_list), database_name, table_name)
    expected = client.execute(sql)
    ret = client.schema_change(table_name, \
            add_column_list=["add_key int default '0' FIRST"], \
            is_wait=True)
    assert ret
    actual = client.execute(sql)
    util.check(expected, actual, force_order=True)
    for local_file in DATA.multi_local_file_list[int(length / 2):]:
        ret = client.stream_load(table_name, local_file, column_name_list=DATA.column_name_list,
                                 max_filter_ratio=0.5)
        assert ret
    sql = "SELECT * FROM %s.%s" % (database_name, table_name)
    palo_result = client.execute(sql)
    mysql_result = execute(config.mysql_host, config.mysql_port, \
            "SELECT 0, %s FROM %s.all_type" % (",".join(DATA.column_name_list), mysql_db), \
            user=config.mysql_user, password=config.mysql_password)
    util.check(palo_result, mysql_result, force_order=True)
    client.clean(database_name)


def test_add_bf_column():
    """
    {
    "title": "test_sys_bloom_filter_c.test_add_bf_column",
    "describe": "添加新列并设置为bloom filter",
    "tag": "system,p1"
    }
    """
    """
    添加新列并设置为bloom filter
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.schema, \
            partition_info=DATA.partition_info, \
            distribution_info=DATA.distribution_info, \
            bloom_filter_column_list=DATA.bloom_filter_column_list)
    assert ret
    length = len(DATA.multi_local_file_list)
    for local_file in DATA.multi_local_file_list[:int(length / 2)]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    sql = "SELECT %s FROM %s.%s" % (",".join(DATA.column_name_list), database_name, table_name)
    expected = client.execute(sql)
    bf_list = ["add_key"]
    for column in DATA.bloom_filter_column_list:
        bf_list.append(column)
    ret = client.schema_change(table_name, \
            add_column_list=["add_key int default '0' FIRST"], \
            bloom_filter_column_list=bf_list, is_wait=True)
    assert ret
    actual = client.execute(sql)
    util.check(expected, actual, force_order=True)
    for local_file in DATA.multi_local_file_list[int(length / 2):]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5,
                                 column_name_list=DATA.column_name_list)
        assert ret
    sql = "SELECT * FROM %s.%s" % (database_name, table_name)
    palo_result = client.execute(sql)
    mysql_result = execute(config.mysql_host, config.mysql_port, \
            "SELECT 0, %s FROM %s.all_type" \
            % (",".join(DATA.column_name_list), mysql_db), \
            user=config.mysql_user, password=config.mysql_password)
    util.check(palo_result, mysql_result, force_order=True)
    client.clean(database_name)


def test_add_bf():
    """
    {
    "title": "test_sys_bloom_filter_c.test_add_bf",
    "describe": "对已存在列添加为bloom filter",
    "tag": "system,p1"
    }
    """
    """
    对已存在列添加为bloom filter
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    
    ret = client.create_table(table_name, DATA.schema, \
            partition_info=DATA.partition_info, \
            distribution_info=DATA.distribution_info)
    assert ret
    length = len(DATA.multi_local_file_list)
    for local_file in DATA.multi_local_file_list[:int(length / 2)]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    sql = "SELECT %s FROM %s.%s" % (",".join(DATA.column_name_list), database_name, table_name)
    expected = client.execute(sql)
    ret = client.schema_change(table_name, \
            bloom_filter_column_list=DATA.bloom_filter_column_list, \
            is_wait=True)
    assert ret
    actual = client.execute(sql)
    util.check(expected, actual, force_order=True)
    for local_file in DATA.multi_local_file_list[int(length / 2):]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5,
                                 column_name_list=DATA.column_name_list)
        assert ret
    sql = "SELECT * FROM %s.%s" % (database_name, table_name)
    palo_result = client.execute(sql)
    mysql_result = execute(config.mysql_host, config.mysql_port, \
            "SELECT %s FROM %s.all_type" \
            % (",".join(DATA.column_name_list), mysql_db), \
            user=config.mysql_user, password=config.mysql_password)
    util.check(palo_result, mysql_result, force_order=True)
    client.clean(database_name)


def test_del_bf():
    """
    {
    "title": "test_sys_bloom_filter_c.test_del_bf",
    "describe": "删除列的bloom filter属性",
    "tag": "system,p1"
    }
    """
    """
    删除列的bloom filter属性
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.schema, \
            partition_info=DATA.partition_info, \
            distribution_info=DATA.distribution_info, \
            bloom_filter_column_list=DATA.bloom_filter_column_list)
    assert ret
    length = len(DATA.multi_local_file_list)
    for local_file in DATA.multi_local_file_list[:int(length / 2)]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    sql = "SELECT %s FROM %s.%s" % (",".join(DATA.column_name_list), database_name, table_name)
    expected = client.execute(sql)
    ret = client.schema_change(table_name, \
            add_column_list=["add_key int default '0' FIRST"], \
            bloom_filter_column_list=["add_key"], is_wait=True)
    assert ret
    actual = client.execute(sql)
    util.check(expected, actual, force_order=True)
    for local_file in DATA.multi_local_file_list[int(length / 2):]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5,
                                 column_name_list=DATA.column_name_list)
        assert ret
    sql = "SELECT * FROM %s.%s" % (database_name, table_name)
    palo_result = client.execute(sql)
    mysql_result = execute(config.mysql_host, config.mysql_port, \
            "SELECT 0, %s FROM %s.all_type" \
            % (",".join(DATA.column_name_list), mysql_db), \
            user=config.mysql_user, password=config.mysql_password)
    util.check(palo_result, mysql_result, force_order=True)
    client.clean(database_name)


def test_add_partition():
    """
    {
    "title": "test_sys_bloom_filter_c.test_add_partition",
    "describe": "添加partition",
    "tag": "system,p1"
    }
    """
    """
    添加partition
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    length = len(DATA.multi_local_file_list)
    ret = client.create_table(table_name, DATA.schema, \
            partition_info=DATA.partition_info_for_add, \
            distribution_info=DATA.distribution_info, \
            bloom_filter_column_list=DATA.bloom_filter_column_list)
    assert ret
    for local_file in DATA.multi_local_file_list[int(length / 2):]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    sql = "SELECT %s FROM %s.%s" % (",".join(DATA.column_name_list), database_name, table_name)
    expected = client.execute(sql)
    ret = client.add_partition(table_name, "p_2", "100")
    ret = client.add_partition(table_name, "p_3", "MAXVALUE")
    assert ret
    actual = client.execute(sql)
    util.check(expected, actual, force_order=True)
    for local_file in DATA.multi_local_file_list[:int(length / 2)]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    palo_result = client.execute(sql)
    mysql_result = execute(config.mysql_host, config.mysql_port, \
            "SELECT %s FROM %s.all_type" \
            % (",".join(DATA.column_name_list), mysql_db), \
            user=config.mysql_user, password=config.mysql_password)
    util.check(palo_result, mysql_result, force_order=True)
    client.clean(database_name)


def test_delete_on_bf_column():
    """
    {
    "title": "test_sys_bloom_filter_c.test_delete_on_bf_column",
    "describe": "在bloom filter列上使用delete",
    "tag": "system,p1"
    }
    """
    """
    在bloom filter列上使用delete
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.schema, \
            partition_info=DATA.partition_info, \
            distribution_info=DATA.distribution_info, \
            bloom_filter_column_list=DATA.bloom_filter_column_list)
    assert ret
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name) 
    ret = client.batch_load(util.get_label(), data_desc_list, max_filter_ratio=0.05, is_wait=True,
                            broker=broker_info)
    assert ret
    sql = "SELECT * FROM %s.%s" % (database_name, table_name)
    import test_sys_delete
    test_sys_delete.setup_module()
    delete_check = test_sys_delete.check
    
    data_1 = client.execute(sql)
    delete_condition_list = [("tinyint_key", "<", "-100")]
    for partition in DATA.partition_info.partition_name_list:
        ret = client.delete(table_name, delete_condition_list, partition_name=partition)
        assert ret
    delete_check(database_name, table_name, delete_condition_list, data_1)
    
    data_2 = client.execute(sql)
    delete_condition_list = [("bigint_key", "<=", "2")]
    for partition in DATA.partition_info.partition_name_list:
        ret = client.delete(table_name, delete_condition_list, partition_name=partition)
        assert ret
    delete_check(database_name, table_name, delete_condition_list, data_2)
    
    data_3 = client.execute(sql)
    delete_condition_list = [("date_key", "=", "1982-04-02")]
    for partition in DATA.partition_info.partition_name_list:
        ret = client.delete(table_name, delete_condition_list, partition_name=partition)
        assert ret
    delete_check(database_name, table_name, delete_condition_list, data_3)
 
    data_4 = client.execute(sql)
    delete_condition_list = [("character_key", "<=", "sk6S0")]
    for partition in DATA.partition_info.partition_name_list:
        ret = client.delete(table_name, delete_condition_list, partition_name=partition)
        assert ret
    delete_check(database_name, table_name, delete_condition_list, data_4)
    client.clean(database_name)


def test_delete_all_data():
    """
    {
    "title": "test_sys_bloom_filter_c.test_delete_all_data",
    "describe": "删除了所有数据",
    "tag": "system,p1"
    }
    """
    """
    删除了所有数据
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.schema, \
            partition_info=DATA.partition_info, \
            distribution_info=DATA.distribution_info, \
            bloom_filter_column_list=DATA.bloom_filter_column_list)
    assert ret
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name) 
    ret = client.batch_load(util.get_label(), data_desc_list, max_filter_ratio=0.05, is_wait=True,
                            broker=broker_info)
    assert ret
    
    sql = "SELECT * FROM %s.%s" % (database_name, table_name)
    import test_sys_delete
    test_sys_delete.setup_module()
    delete_check = test_sys_delete.check
    data_1 = client.execute(sql)
    delete_condition_list = [("smallint_key", "<=", "32767")]
    for partition in DATA.partition_info.partition_name_list:
        ret = client.delete(table_name, delete_condition_list, partition_name=partition)
        assert ret
    delete_check(database_name, table_name, delete_condition_list, data_1)
    
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name) 
    ret = client.batch_load(util.get_label(), data_desc_list, max_filter_ratio=0.05, is_wait=True,
                            broker=broker_info)
    assert ret
    assert client.verify(DATA.expected_data_file_list, table_name)
    client.clean(database_name)


def get_explain_index(sql):
    """
    Get explain index
    """
    result = client.execute('EXPLAIN ' + sql)
    if result is None:
        return None
    rollup_flag = 'rollup: '
    explain_index = None
    for element in result:
        message = element[0].lstrip()
        if message.startswith(rollup_flag):
            explain_index = message[len(rollup_flag):].rstrip(' ')
    return explain_index


def teardown_module():
    """
    tear down
    """
    pass

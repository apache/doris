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

import time
import pymysql
from data import bloom_filter as DATA
from lib import palo_client
from lib import palo_config
from lib import util
from lib import common

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
    mysql_db = 'bloom_filter_a_mysql'
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


def test_base():
    """
    {
    "title": "test_sys_bloom_filter_a.test_base",
    "describe": "支持的所有类型上使用bloom filter，导入查询正确",
    "tag": "system,p1"
    }
    """
    """
    支持的所有类型上使用bloom filter，导入查询正确
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.schema,
                              bloom_filter_column_list=DATA.bloom_filter_column_list)
    assert ret
    #校验schema正确
    schema = client.get_index_schema(table_name)
    for column in schema:
        if column[0] in DATA.bloom_filter_column_list:
            assert column[5].find("BLOOM_FILTER") >= 0, "%s should be bloom filter" % str(column)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name) 
    assert client.batch_load(util.get_label(), data_desc_list, max_filter_ratio=0.05, is_wait=True,
                             broker=broker_info)
    assert client.verify(DATA.expected_data_file_list, table_name)
    client.clean(database_name)


def test_tinyint_bloom_filter():
    """
    {
    "title": "test_sys_bloom_filter_a.test_tinyint_bloom_filter",
    "describe": "tinyint类型列不可以使用bloom filter",
    "tag": "system,p1,fuzz"
    }
    """
    """
    tinyint类型列不可以使用bloom filter
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    try:
        ret = client.create_table(table_name, DATA.schema, \
                bloom_filter_column_list=DATA.tinyint_key_list)
    except Exception as error:
        print(error)
    else:
        assert False
    try:
        ret = client.create_table(table_name, DATA.schema, \
                bloom_filter_column_list=DATA.tinyint_value_list)
    except Exception as error:
        print(error)
    else:
        assert False
    try:
        ret = client.create_table(table_name, DATA.schema_tinyint, \
                bloom_filter_column_list=DATA.tinyint_value_replace_list)
    except Exception as error:
        print(error)
    else:
        assert False
    client.clean(database_name)


def test_illegal_bloom_filter():
    """
    {
    "title": "test_sys_bloom_filter_a.test_illegal_bloom_filter",
    "describe": "非REPLACE聚合方式的value列不可以使用bloom filter",
    "tag": "system,p1,fuzz"
    }
    """
    """
    非REPLACE聚合方式的value列不可以使用bloom filter
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    for bf_list in DATA.illegal_bloom_filter_column_list:
        try:
            ret = client.create_table(table_name, DATA.schema, \
                    bloom_filter_column_list=bf_list)
        except Exception as error:
            print(error)
        else:
            assert False
    client.clean(database_name)


def test_illegal_bloom_filter_1():
    """
    {
    "title": "test_sys_bloom_filter_a.test_illegal_bloom_filter",
    "describe": "聚合表的value列不可以使用bloom filter",
    "tag": "system,p1,fuzz"
    }
    """
    """
    聚合表的value列不支持创建bloom filter
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    for bf_list in DATA.value_list:
        try:
            ret = client.create_table(table_name, DATA.schema, \
                    bloom_filter_column_list=bf_list)
        except Exception as error:
            print(error)
        else:
            assert False
    client.clean(database_name)


def test_float_bloom_filter():
    """
    {
    "title": "test_float_bloom_filter",
    "describe": "FLOAT DOUBLE类型的列不可以使用bloom filter",
    "tag": "system,p1,fuzz"
    }
    """
    """
    FLOAT DOUBLE类型的列不可以使用bloom filter
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    try:
        ret = client.create_table(table_name, DATA.schema_float, \
                bloom_filter_column_list=DATA.float_value_replace_list)
    except Exception as error:
        print(error)
    else:
        assert False
    try:
        ret = client.create_table(table_name, DATA.schema_float, \
                bloom_filter_column_list=DATA.double_value_replace_list)
    except Exception as error:
        print(error)
    else:
        assert False
    client.clean(database_name)


def test_default_value():
    """
    {
    "title": "test_sys_bloom_filter_a.test_default_value",
    "describe": "默认值使用bloom filter列",
    "tag": "system,p1"
    }
    """
    """
    默认值使用bloom filter列
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.schema_default_value, \
            bloom_filter_column_list=DATA.default_value_list)
    assert ret
    data_desc_list = palo_client.LoadDataInfo(DATA.default_value_file_path, table_name, \
            column_name_list=["increment_key"])
    ret = client.batch_load(util.get_label(), data_desc_list, max_filter_ratio=0.05, is_wait=True,
                            broker=broker_info)
    assert ret
    assert client.verify(DATA.expected_default_value_file, table_name)
    client.clean(database_name)


def test_partition_null_replica():
    """
    {
    "title": "test_sys_bloom_filter_a.test_partition_null_replica",
    "describe": "多分区建表，数据导入时部分副本数据为空",
    "tag": "system,p1"
    }
    """
    """
    多分区建表，数据导入时部分副本数据为空
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.schema, \
            partition_info=DATA.partition_info, \
            bloom_filter_column_list=DATA.bloom_filter_column_list)
    assert ret
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name) 
    assert client.batch_load(util.get_label(), data_desc_list, max_filter_ratio=0.05, is_wait=True, 
                             broker=broker_info)
    assert client.verify(DATA.expected_data_file_list, table_name)

    sql = "SELECT tinyint_key from %s.%s where tinyint_key<0" % (database_name, table_name)
    result = client.execute(sql)
    for data in result:
        assert data[0] == -1 or data[0] == -2 or data[0] == -128
    client.clean(database_name)


def test_be_ce():
    """
    {
    "title": "test_sys_bloom_filter_a.test_be_ce",
    "describe": "多次导入BE、CE结束后数据正确，可以skip",
    "tag": "system,p1"
    }
    """
    """
    多次导入BE、CE结束后数据正确
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
    
    ix = 0
    for local_file in DATA.be_ce_local_file_list:
        ix = ix + 1
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    #TODO check finish BE/CE
    assert client.verify(DATA.expected_data_file_list, table_name)
    client.clean(database_name)


def test_rollup_with_bloom_filter():
    """
    {
    "title": "test_sys_bloom_filter_a.test_rollup_with_bloom_filter",
    "describe": "rollup表使用bloom filter",
    "tag": "system,p1"
    }
    """
    """
    rollup表使用bloom filter
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
    #load before rollup
    length = len(DATA.be_ce_local_file_list)
    ix = 0
    for local_file in DATA.be_ce_local_file_list[:int(length / 2)]:
        ix = ix + 1
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    sql = "SELECT int_key, character_key, datetime_key FROM %s.%s \
            group by int_key, character_key, datetime_key" \
            % (database_name, table_name)
    expected = client.execute(sql)
    #rollup
    ret = client.create_rollup_table(table_name, index_name, \
            DATA.rollup_with_bloom_filter, is_wait=True)
    assert ret

    actual = client.execute(sql)
    util.check(expected, actual, force_order=True)
    
    for local_file in DATA.be_ce_local_file_list[int(length / 2):]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    assert client.verify(DATA.expected_data_file_list, table_name)
    #check rollup data
    mysql_result = execute(config.mysql_host, config.mysql_port, \
            "SELECT int_key, character_key, datetime_key \
            from %s.all_type group by int_key, character_key, datetime_key" % mysql_db, \
            user=config.mysql_user, password=config.mysql_password)
    palo_result = client.execute(sql)
    util.check(palo_result, mysql_result, force_order=True)
    client.clean(database_name)


def test_rollup_without_bf():
    """
    {
    "title": "test_sys_bloom_filter_a.test_rollup_without_bf",
    "describe": "rollup中不包含bloom filter",
    "tag": "system,p1"
    }
    """
    """
    rollup中不包含bloom filter
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
    #load before rollup
    length = len(DATA.be_ce_local_file_list)
    for local_file in DATA.be_ce_local_file_list[:int(length / 2)]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    sql = "SELECT tinyint_key, SUM(float_value) FROM %s.%s group by tinyint_key" \
            % (database_name, table_name)
    expected = client.execute(sql)
    #rollup
    ret = client.create_rollup_table(table_name, index_name, 
                                     DATA.rollup_without_bloom_filter, is_wait=True)
    assert ret
    timeout = 400
    while timeout > 0:
        time.sleep(1)
        timeout -= 1
        hit_index = common.get_explain_rollup(client, sql)
        if index_name in hit_index:
            break
    assert index_name in hit_index, "hit index: %s; expected index: %s" % (hit_index, index_name)
    actual = client.execute(sql)
    util.check(actual, expected, force_order=True)
    for local_file in DATA.be_ce_local_file_list[int(length / 2):]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    assert client.verify(DATA.expected_data_file_list, table_name)
    #check rollup data
    mysql_result = execute(config.mysql_host, config.mysql_port, \
            "SELECT tinyint_key, SUM(float_value) \
            from %s.all_type group by tinyint_key" % mysql_db, \
            user=config.mysql_user, password=config.mysql_password)
    palo_result = client.execute(sql)
    util.check(palo_result, mysql_result, force_order=True)
    client.clean(database_name)


def teardown_module():
    """
    tear down
    """
    pass

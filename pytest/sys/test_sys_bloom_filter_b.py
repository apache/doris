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

client = None
config = palo_config.config
broker_info = palo_config.broker_info

LOG = palo_client.LOG
L = palo_client.L


def setup_module():
    """
    set up
    """
    global client
    global mysql_db
    mysql_db = 'bloom_filter_b_mysql'
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, 
                                    password=config.fe_password, http_port=config.fe_http_port)
    init_mysql(config.mysql_host, config.mysql_port, "./data/bloom_filter/init_mysql.sql",
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
        cursor.execute(sql), 
        return cursor.fetchall()
    except Exception as error:
        assert False, "execute error. %s" % str(error)


def test_rollup_base_rollup():
    """
    {
    "title": "test_sys_bloom_filter_b.test_rollup_base_rollup",
    "describe": "基于rollup表建表rollup",
    "tag": "system,p1"
    }
    """
    """
    基于rollup表建表rollup
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
    length = len(DATA.multi_local_file_list)
    for local_file in DATA.multi_local_file_list[:int(length / 2)]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    sql = "SELECT smallint_key, date_key, SUM(int_value), SUM(float_value) FROM %s.%s" \
            % (database_name, table_name)
    sql += " where smallint_key % 2 = 0 group by smallint_key, date_key" 
    expected = client.execute(sql)
    #rollup_1
    ret = client.create_rollup_table(table_name, index_name, \
            DATA.base_rollup_column_list, is_wait=True)
    assert ret
    # hit_index = get_explain_index(sql)
    # assert hit_index == index_name, "hit index: %s; expected index: %s" % (hit_index, index_name)
    actual = client.execute(sql)
    util.check(actual, expected, force_order=True)
    #rollup_2
    sql = "SELECT int_key, date_key, SUM(int_value), SUM(float_value) FROM %s.%s" \
            % (database_name, table_name)
    sql += " where int_key % 7 = 0 group by int_key, date_key" 
    expected = client.execute(sql)
    index_name_2 = "%s_2" % index_name
    ret = client.create_rollup_table(table_name, index_name_2, DATA.rollup_column_list, \
            base_index_name=index_name, is_wait=True)
    assert ret
    # hit_index = get_explain_index(sql)
    # assert hit_index == index_name_2, "hit index: %s; expected index: %s" \
    #         % (hit_index, index_name_2)
    actual = client.execute(sql)
    util.check(actual, expected, force_order=True)
    
    for local_file in DATA.multi_local_file_list[int(length / 2):]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    assert client.verify(DATA.expected_data_file_list, table_name)
    #check rollup data
    palo_result = client.execute(sql)
    sql = "SELECT int_key, date_key, SUM(int_value), SUM(float_value)"
    sql += " from %s.all_type" % mysql_db
    sql += " where int_key % 7 = 0 group by int_key, date_key"
    mysql_result = execute(config.mysql_host, config.mysql_port, \
            sql, user=config.mysql_user, password=config.mysql_password)
    util.check(palo_result, mysql_result, force_order=True)
    client.clean(database_name)


def test_drop_bf_column():
    """
    {
    "title": "test_sys_bloom_filter_b.test_drop_bf_column",
    "describe": "删除使用bloom filter的列",
    "tag": "system,p1"
    }
    """
    """
    删除使用bloom filter的列
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
    column_list = list()
    for column in DATA.schema:
        if column[0] not in DATA.drop_column_list:
            column_list.append(column[0])
    sql = "SELECT %s FROM %s.%s" % (",".join(column_list), database_name, table_name)
    expected = client.execute(sql)
    ret = client.schema_change(table_name, drop_column_list=DATA.drop_column_list, is_wait=True)
    assert ret
    actual = client.execute(sql)
    util.check(expected, actual, force_order=True)
    for local_file in DATA.multi_local_file_list[int(length / 2):]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5, 
                                 column_name_list=DATA.column_name_list)
        assert ret
    key_list = list()
    for column in DATA.schema:
        if len(column) == 2 and column[0] not in DATA.drop_column_list:
            key_list.append(column[0])
    int_value_list = list()
    for column in DATA.schema:
        if len(column) == 3 and column[2] == "SUM" and column[0] not in DATA.drop_column_list:
            int_value_list.append("SUM(%s)" % column[0])

    sql = "SELECT %s, %s FROM %s.%s GROUP BY %s" \
            % (",".join(key_list), ",".join(int_value_list), \
            database_name, table_name, ",".join(key_list))
    palo_result = client.execute(sql)
    mysql_result = execute(config.mysql_host, config.mysql_port, \
            "SELECT %s, %s FROM %s.all_type GROUP BY %s" \
            % (",".join(key_list), ",".join(int_value_list), \
            mysql_db, ",".join(key_list)), \
            user=config.mysql_user, password=config.mysql_password)
    util.check(palo_result, mysql_result, force_order=True)
    client.clean(database_name)


def test_drop_none_bf_column():
    """
    {
    "title": "test_sys_bloom_filter_b.test_drop_none_bf_column",
    "describe": "删除不使用bloom filter的列",
    "tag": "system,p1"
    }
    """
    """
    删除不使用bloom filter的列
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.schema, \
            partition_info=DATA.partition_info, \
            distribution_info=DATA.distribution_info, \
            bloom_filter_column_list=DATA.bloom_filter_column_list_drop_none_bf)
    assert ret
    length = len(DATA.multi_local_file_list)
    ix = 0
    for local_file in DATA.multi_local_file_list[:int(length / 2)]:
        ix = ix + 1
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5)
        assert ret
    column_list = list()
    for column in DATA.schema:
        if column[0] not in DATA.drop_column_list:
            column_list.append(column[0])
    sql = "SELECT %s FROM %s.%s" % (",".join(column_list), database_name, table_name)
    expected = client.execute(sql)
    ret = client.schema_change(table_name, drop_column_list=DATA.drop_column_list, \
            is_wait=True)
    assert ret
    actual = client.execute(sql)
    util.check(expected, actual, force_order=True)
    for local_file in DATA.multi_local_file_list[int(length / 2):]:
        ret = client.stream_load(table_name, local_file, max_filter_ratio=0.5,
                                 column_name_list=DATA.column_name_list)
        assert ret
    palo_result = client.execute(sql)
    mysql_result = execute(config.mysql_host, config.mysql_port, \
            "SELECT %s FROM %s.all_type" % (",".join(column_list), mysql_db), \
            user=config.mysql_user, password=config.mysql_password)
    util.check(palo_result, mysql_result, force_order=True)
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

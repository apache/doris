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
test insert into value
Date: 2019-07-24 13:42:40
"""

import sys
import re
sys.path.append("../")
from lib import palo_client
from lib import palo_config
from lib import util
from lib import palo_job
from lib import common
from data import schema as DATA

client = None
config = palo_config.config


def setup_module():
    """
    set up
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    client.set_variables('enable_insert_strict', 0)


def test_insert_value():
    """
    {
    "title": "test_insert_value",
    "describe": "测试insert into value基本用法",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.int_column_list, set_null=True)
    assert ret
    sql = 'insert into %s values(0, 0, 0, 0, 0)' % table_name
    ret = client.execute(sql)
    assert ret == ()
    sql = 'select * from %s' % table_name
    ret = client.execute(sql)
    assert ret == ((0, 0, 0, 0, 0),)
    sql = 'insert into %s values(0, 1, 1, 1, 1)' % table_name
    ret = client.execute(sql)
    assert ret == ()
    sql = 'select * from %s' % table_name
    ret = client.execute(sql)
    assert ret == ((0, 1, 1, 0, 1),)
    client.clean(database_name)


def test_insert_multi_value():
    """
    {
    "title": "test_insert_multi_value",
    "describe": "测试insert into value插入多条数据, set_enable_strict",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.int_column_list)
    assert ret
    sql = 'insert into %s values(0,0,0,0,0),(1,1,1,1,1),(2,2,2,2,2),(null, null, null, null, null)' % table_name
    ret = client.execute(sql)
    assert ret == ()
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    exp = ((0, 0, 0, 0, 0), (1, 1, 1, 1, 1), (2, 2, 2, 2, 2))
    util.check(ret, exp)
    table_name += '_s'
    ret = client.create_table(table_name, DATA.int_column_list)
    assert ret
    sql = 'set enable_insert_strict=1'
    ret = client.execute(sql)
    assert ret == ()
    sql = 'insert into %s values(0,0,0,0,0),(1,1,1,1,1),(2,2,2,2,2),(null, null, null, null, null)' % table_name
    flag = True
    try:
        ret = client.execute(sql)
        print(ret)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag
    client.clean(database_name)


def test_insert_value_column_list():
    """
    {
    "title": "test_insert_value_column_list",
    "describe": "测试insert into value指定列顺序",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.int_column_list)
    assert ret
    sql = 'insert into %s (v4, v3, v2, v1, k1) values(1,2,3,4,5)' % table_name
    ret = client.execute(sql)
    assert ret == ()
    sql = 'select * from %s' % table_name
    ret = client.execute(sql)
    assert ret == ((5, 4, 3, 2, 1),)
    sql = 'insert into %s (v4, v3, v2, v1) values(1,2,3,4)' % table_name
    flag = True
    try:
        client.execute(sql)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag
    client.clean(database_name)


def test_insert_value_default():
    """
    {
    "title": "test_insert_value_default",
    "describe": "测试insert into value缺省列使用默认值",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.int_column_list, set_null=True)
    assert ret
    sql = 'insert into %s (v4, v3, v2, v1) values(1,2,3,4)' % table_name
    ret = client.execute(sql)
    assert ret == ()
    sql = 'select * from %s' % table_name
    ret = client.execute(sql)
    assert ret == ((None, 4, 3, 2, 1),)
    sql = 'insert into %s (v4, v3, v2, v1) values(2,2,3,2), (NULL, NULL, NULL, 3)' % table_name
    ret = client.execute(sql)
    sql = 'select * from %s order by v1' % table_name
    ret = client.execute(sql)
    assert ret == ((None, 9, 3, 2, None),)
    client.clean(database_name)


def test_insert_value_not_null():
    """
    {
    "title": "test_insert_value_not_null",
    "describe": "测试insert into value的Null值插入",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list)
    assert ret
    sql = 'insert into %s values(null, null, null, null, null, null, null,' \
          ' null, null, null, null)' % table_name
    flag = True
    try:
        ret = client.execute(sql)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag == True, 'expect insert error'
    sql = 'select * from %s' % table_name
    ret = client.execute(sql)
    assert ret == ()
    table_name += '_n'
    ret = client.create_table(table_name, DATA.baseall_column_list, set_null=True)
    assert ret
    sql = 'insert into %s values(null, null, null, null, null, null, null,' \
          ' null, null, null, null)' % table_name
    ret = client.execute(sql)
    assert ret == ()
    sql = 'select * from %s' % table_name
    ret = client.execute(sql)
    assert ret == ((None, None, None, None, None, None, None, None, None, None, None),)
    client.clean(database_name)


def test_insert_select_not_null():
    """
    {
    "title": "test_insert_select_not_null",
    "describe": "测试insert into select的Null值插入",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.int_column_list)
    client.set_variables('enable_insert_strict', 0)
    assert ret
    sql = 'insert into %s SELECT a.k1 AS n1, b.k2 AS n2, 1, 2, 3 FROM %s.baseall a ' \
          'LEFT OUTER JOIN %s.bigtable b ON a.k1 = b.k1 + 10' \
          % (table_name, config.palo_db, config.palo_db)
    ret = client.execute(sql)
    assert ret == ()
    sql1 = 'select * from %s order by k1' % table_name
    ret1 = client.execute(sql1)
    sql2 = 'SELECT a.k1 AS n1, b.k2 AS n2, 1, 2, 3 FROM %s.baseall a ' \
          'LEFT OUTER JOIN %s.bigtable b ON a.k1 = b.k1 + 10 where b.k2 is not null' \
          ' order by n1' % (config.palo_db, config.palo_db)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_insert_value_special_data():
    """
    {
    "title": "test_insert_value_special_data",
    "describe": "测试使用insert into value插入特殊值, insert into tinyint溢出bug",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, set_null=True)
    assert ret
    sql = 'insert into %s(k6) values("hello world")' % table_name
    flag = True
    client.set_variables('enable_insert_strict', 'true')
    try:
        ret = client.execute(sql)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag == True
    client.set_variables('enable_insert_strict', 'false')
    ret = client.execute(sql)
    assert ret == ()
    sql = 'insert into %s(k10) values("2001-01-01 10-00-00")' % table_name
    ret = client.execute(sql)
    assert ret == ()
    sql = 'insert into %s(k5) values(100.1005)' % table_name
    ret = client.execute(sql)
    sql = 'insert into %s(k1) values(-56)' % table_name
    ret = client.execute(sql)
    sql = 'insert into %s(k1) values(200)' % table_name
    flag = True
    try:
        ret = client.execute(sql)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag
    sql = 'select * from %s order by k1, k5, k6, k10' % table_name
    assert common.check_by_file('./data/LOAD/insert_value.data', sql=sql, client=client)
    client.clean(database_name)

    
def teardown_module():
    """tear down"""
    pass


if __name__ == '__main__':
    setup_module()


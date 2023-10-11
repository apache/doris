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

############################################################################
#
#   @file test_sys_duplicate_partition_null.py
#   @date 2019-05-21 15:20:45
#   @brief This file is a test file for palo duplicate key
#
#############################################################################

"""
duplicate模型的分区键为Null,查询删除
"""

from data import schema as DATA
from lib import palo_config
from lib import palo_client
from lib import util

LOG = palo_client.LOG
L = palo_client.L
config = palo_config.config


def setup_module():
    """
    setUp
    """
    pass


def create_workspace(database_name):
    """create client and db, use db"""
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password,http_port=config.fe_http_port)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    return client


def test_duplicate_partition_null():
    """
    {
    "title": "test_sys_duplicate_partition_a.test_duplicate_partition_null",
    "describe": "测试duplicate key的分区列为Null",
    "tag": "system,p1,fuzz"
    }
    """
    """测试duplicate key的分区列为Null"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = create_workspace(database_name)
    partition_info = palo_client.PartitionInfo('k1', ['p1', 'p2', 'p3'], ['0', '100', '1000'])
    ret = client.create_table(table_name, column_list=DATA.int_column_no_agg_list, set_null=True,
                              partition_info=partition_info, keys_desc='DUPLICATE KEY(k1)')
    assert ret, 'create table failed'
    sql = 'insert into %s values(null, 0, 0, 0, 0), (null, 1, 1, 1, 1)' % table_name
    ret = client.execute(sql)
    assert ret == (), ret
    sql1 = 'select * from %s order by v1' % table_name
    ret1 = client.execute(sql1)
    assert len(ret1) == 2, ret1
    sql2 = 'select * from %s where k1 is null order by v1' % table_name
    ret2 = client.execute(sql2)
    assert len(ret2) == 2, ret2
    util.check(ret1, ret2)
    client.clean(database_name)


def test_duplicate_partition_null_delete():
    """
    {
    "title": "test_sys_duplicate_partition_a.test_duplicate_partition_null_delete",
    "describe": "测试duplicate key的分区列为Null",
    "tag": "system,p1,fuzz"
    }
    """
    """测试duplicate key的分区列为Null"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = create_workspace(database_name)
    #create table
    partition_info = palo_client.PartitionInfo('k1', ['p1', 'p2', 'p3'], [0, 100, 1000])
    print(partition_info)
    ret = client.create_table(table_name, column_list=DATA.int_column_no_agg_list, set_null=True,
                              partition_info=partition_info, keys_desc='DUPLICATE KEY(k1)')
    assert ret, 'create table failed'
    # insert data & check
    sql = 'insert into %s values(null, 0, 0, 0, 0), (null, 1, 1, 1, 1)' % table_name
    ret = client.execute(sql)
    assert ret == (), ret
    sql1 = 'select * from %s order by v1' % table_name
    ret1 = client.execute(sql1)
    sql2 = 'select * from %s where k1 is null order by v1' % table_name
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)
    assert len(ret1) == 2, ret1
    # delete in partition p3
    sql = 'DELETE FROM %s PARTITION p3 WHERE k1 is null' % table_name
    ret = client.execute(sql)
    assert ret == (), ret
    sql1 = 'select * from %s order by v1' % table_name
    ret1 = client.execute(sql1)
    util.check(ret1, ret2)
    # delete in partition p2
    sql = 'DELETE FROM %s PARTITION p2 WHERE k1 is null' % table_name
    ret = client.execute(sql)
    assert ret == (), ret
    sql1 = 'select * from %s order by v1' % table_name
    ret1 = client.execute(sql1)
    util.check(ret1, ret2)
    # delete in partition p1
    sql = 'DELETE FROM %s PARTITION p1 WHERE k1 is null' % table_name
    ret = client.execute(sql)
    assert ret == (), ret
    sql1 = 'select * from %s order by v1' % table_name
    ret1 = client.execute(sql1)
    assert ret1 == (), ret1
    client.clean(database_name)


def test_duplicate_partition_null_drop_p():
    """
    {
    "title": "test_sys_duplicate_partition_a.test_duplicate_partition_null_drop_p",
    "describe": "测试duplicate key的分区列为Null",
    "tag": "system,p1,fuzz"
    }
    """
    """测试duplicate key的分区列为Null"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = create_workspace(database_name)
    # create table
    partition_info = palo_client.PartitionInfo('k1', ['p1', 'p2', 'p3'], ['0', '100', '1000'])
    ret = client.create_table(table_name, column_list=DATA.int_column_no_agg_list, set_null=True,
                              partition_info=partition_info, keys_desc='DUPLICATE KEY(k1)')
    assert ret, 'create table failed'
    # insert data & check
    sql = 'insert into %s values(null, 0, 0, 0, 0), (null, 1, 1, 1, 1)' % table_name
    ret = client.execute(sql)
    assert ret == (), ret
    sql1 = 'select * from %s order by v1' % table_name
    ret1 = client.execute(sql1)
    sql2 = 'select * from %s where k1 is null order by v1' % table_name
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)
    assert len(ret1) == 2, ret1
    # drop partitoin p3 & check
    ret = client.drop_partition(table_name, partition_name='p3')
    assert ret
    sql1 = 'select * from %s order by v1' % table_name
    ret1 = client.execute(sql1)
    util.check(ret1, ret2)
    # drop partition p1 & check
    ret = client.drop_partition(table_name, partition_name='p1')
    assert ret
    sql1 = 'select * from %s order by v1' % table_name
    ret1 = client.execute(sql1)
    assert ret1 == (), ret1
    # insert null data & check
    sql = 'insert into %s values(null, 0, 0, 0, 0), (null, 1, 1, 1, 1)' % table_name
    flag = None
    try:
        ret = client.execute(sql)
        flag = False
    except Exception as e:
        print(str(e))
    # drop db
    client.clean(database_name)


def teardown_module():
    """tear down"""
    pass


if __name__ == '__main__':
    setup_module()
    test_duplicate_partition_null_delete() 


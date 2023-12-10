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
  * @file test_sys_partition_multi_col.py
  * @brief Test for list partition
  *
  **************************************************************************/
"""
from data import schema as DATA
from data import load_file as FILE
from lib import palo_client
from lib import util
from lib import palo_config
from lib import common

LOG = palo_client.LOG
L = palo_client.L
config = palo_config.config
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    pass


def teardown_module():
    """
    tearDown
    """
    pass


def check_partition_type(partition_info, database_name, table_name):
    """验证partition基本类型"""
    agg_table = table_name + '_agg'
    dup_table = table_name + '_dup'
    uniq_table = table_name + '_uniq'
    client = common.create_workspace(database_name)
    assert client.create_table(agg_table, DATA.baseall_column_list, partition_info=partition_info)
    assert client.create_table(dup_table, DATA.baseall_column_no_agg_list, partition_info=partition_info,
                               keys_desc=DATA.baseall_duplicate_key)
    assert client.create_table(uniq_table, DATA.baseall_column_no_agg_list, partition_info=partition_info, 
                               keys_desc=DATA.baseall_unique_key)
    for tb in [agg_table, dup_table, uniq_table]:
        load_data_info = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, tb)
        ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
        assert ret
        ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
        assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, uniq_table)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = 'select * from %s.%s order by k1' % (database_name, dup_table)
    sql2 = 'select * from %s.baseall union all select * from %s.baseall' % (config.palo_db, config.palo_db)
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql1 = 'select * from %s.%s order by k1' % (database_name, agg_table)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9*2 from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_list_partition_tinyint():
    """
    {
    "title": "",
    "describe": "3种类型表的tinyint的list分区，创建成功，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [util.gen_tuple_num_str(1, 5), util.gen_tuple_num_str(5, 15),
                            util.gen_tuple_num_str(15, 16)]
    partition_info = palo_client.PartitionInfo('k1', partiton_name, partition_value_list, partition_type='LIST')
    check_partition_type(partition_info, database_name, table_name)

def test_list_partition_smallint():
    """
    {
    "title": "",
    "describe": "3种类型表的smallint的list分区，创建成功，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [('-32767', '32767'), ('255'), ('1985', '1986', '1989', '1991', '1992')]
    partition_info = palo_client.PartitionInfo('k2', partiton_name, partition_value_list, partition_type='LIST')
    check_partition_type(partition_info, database_name, table_name)


def test_list_partition_int():
    """
    {
    "title": "",
    "describe": "3种类型表的int的list分区，创建成功，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [('-2147483647', '2147483647'), ('1001', '3021'), ('1002', '25699', '103', '5014', '1992')]
    partition_info = palo_client.PartitionInfo('k3', partiton_name, partition_value_list, partition_type='LIST')
    check_partition_type(partition_info, database_name, table_name)


def test_list_partition_bigint():
    """
    {
    "title": "",
    "describe": "3种类型表的bigint的list分区，创建成功，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [('11011905', '11011903'), ('-11011903', '11011920', '-11011907'), 
                            ('9223372036854775807', '-9223372036854775807', 
                             '11011902', '7210457', '123456')]
    partition_info = palo_client.PartitionInfo('k4', partiton_name, partition_value_list, partition_type='LIST')
    check_partition_type(partition_info, database_name, table_name)


def test_list_partition_date():
    """
    {
    "title": "",
    "describe": "3种类型表的date的list分区，创建成功，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [('1901-12-31', '1988-03-21'), ('1989-03-21', '1991-08-11', '2012-03-14'),
                            ('2014-11-11', '2015-01-01', '2015-04-02', '3124-10-10', '9999-12-12')]
    partition_info = palo_client.PartitionInfo('k10', partiton_name, partition_value_list, partition_type='LIST')
    check_partition_type(partition_info, database_name, table_name)


def test_list_partition_datetime():
    """
    {
    "title": "",
    "describe": "3种类型表的datetime的list分区，创建成功，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [('1901-01-01 00:00:00', '1989-03-21 13:00:00', '1989-03-21 13:11:00'),
                            ('2000-01-01 00:00:00', '2013-04-02 15:16:52', '2015-03-13 10:30:00'),
                            ('2015-03-13 12:36:38', '2015-04-02 00:00:00', '9999-11-11 12:12:00')]
    partition_info = palo_client.PartitionInfo('k11', partiton_name, partition_value_list, partition_type='LIST')
    check_partition_type(partition_info, database_name, table_name)


def test_list_partition_char():
    """
    {
    "title": "",
    "describe": "3种类型表的char的list分区，创建成功，导入成功，数据正确, todo: bug",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [('true'), ('false'), ('')]
    partition_info = palo_client.PartitionInfo('k6', partiton_name, partition_value_list, partition_type='LIST')
    check_partition_type(partition_info, database_name, table_name)


def test_list_partition_varchar():
    """
    {
    "title": "",
    "describe": "3种类型表的varchar的list分区，创建成功，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [('', ' '), ('du3lnvl', 'jiw3n4', 'lifsno', 'wangjuoo4', 'wangjuoo5'),
                            ('wangynnsf', 'wenlsfnl', 'yanavnd', 'yanvjldjlll', 'yunlj8@nk')]
    partition_info = palo_client.PartitionInfo('k7', partiton_name, partition_value_list, partition_type='LIST')
    check_partition_type(partition_info, database_name, table_name)


def test_list_partition_largeint():
    """
    {
    "title": "",
    "describe": "3种类型表的largeint的list分区，创建成功，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [('11011905', '11011903'), ('-11011903', '11011920', '-11011907'),
                            ('9223372036854775807', '-9223372036854775807',
                             '11011902', '7210457', '123456')]
    partition_info = palo_client.PartitionInfo('k4', partiton_name, partition_value_list, partition_type='LIST')
    agg_table = table_name + '_agg'
    dup_table = table_name + '_dup'
    uniq_table = table_name + '_uniq'
    client = common.create_workspace(database_name)
    column_list = [("k4", "largeint") if i == ("k4", "bigint") else i for i in DATA.baseall_column_list]
    column_no_agg_list = [("k4", "largeint") if i == ("k4", "bigint") else i for i in DATA.baseall_column_no_agg_list]
    assert client.create_table(agg_table, column_list, partition_info=partition_info)
    assert client.create_table(dup_table, column_no_agg_list, partition_info=partition_info,
                               keys_desc=DATA.baseall_duplicate_key)
    assert client.create_table(uniq_table, column_no_agg_list, partition_info=partition_info,
                               keys_desc=DATA.baseall_unique_key)
    for tb in [agg_table, dup_table, uniq_table]:
        load_data_info = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, tb)
        ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
        assert ret
        ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
        assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, uniq_table)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = 'select * from %s.%s order by k1' % (database_name, dup_table)
    sql2 = 'select * from %s.baseall union all select * from %s.baseall' % (config.palo_db, config.palo_db)
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql1 = 'select * from %s.%s order by k1' % (database_name, agg_table)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9*2 from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_list_partition_bool():
    """
    {
    "title": "",
    "describe": "3种类型表的bool的list分区，创建成功，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    partiton_name = ['p1', 'p2']
    partition_value_list = [('true', 'false')]
    partition_info = palo_client.PartitionInfo('k6', partiton_name, partition_value_list, partition_type='LIST')
    agg_table = table_name + '_agg'
    dup_table = table_name + '_dup'
    uniq_table = table_name + '_uniq'
    client = common.create_workspace(database_name)
    column_list = [("k6", "boolean") if i == ("k6", "char(5)") else i for i in DATA.baseall_column_list]
    column_no_agg_list = [("k6", "boolean") if i == ("k6", "char(5)") else i for i in DATA.baseall_column_no_agg_list]
    assert client.create_table(agg_table, column_list, partition_info=partition_info)
    assert client.create_table(dup_table, column_no_agg_list, partition_info=partition_info,
                               keys_desc=DATA.baseall_duplicate_key)
    assert client.create_table(uniq_table, column_no_agg_list, partition_info=partition_info,
                               keys_desc=DATA.baseall_unique_key)
    for tb in [agg_table, dup_table, uniq_table]:
        load_data_info = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, tb)
        ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
        assert ret
        ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
        assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, uniq_table)
    sql2 = 'select k1, k2, k3, k4, k5, case k6 when "true" then 1 when "false" then 0 end k6, ' \
           'k10, k11, k7, k8, k9 from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = 'select * from %s.%s order by k1' % (database_name, dup_table)
    sql2 = 'select k1, k2, k3, k4, k5, case k6 when "true" then 1 when "false" then 0 end k6, ' \
           'k10, k11, k7, k8, k9 from %s.baseall union all ' \
           'select k1, k2, k3, k4, k5, case k6 when "true" then 1 when "false" then 0 end k6, ' \
           'k10, k11, k7, k8, k9 from %s.baseall' % (config.palo_db, config.palo_db)
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql1 = 'select * from %s.%s order by k1' % (database_name, agg_table)
    sql2 = 'select k1, k2, k3, k4, k5, case k6 when "true" then 1 when "false" then 0 end k6, ' \
           'k10, k11, k7, k8, k9*2 from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_list_partition_decimal():
    """
    {
    "title": "",
    "describe": "3种类型表的decimal的list分区，创建失败",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [('-654.654'), ('0'), ('243243.325')]
    partition_info = palo_client.PartitionInfo('k5', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    util.assert_return(False, 'type[DECIMAL32] cannot be a list partition key.',
                       client.create_table, table_name, DATA.baseall_column_list, partition_info=partition_info)
    client.clean(database_name)


def test_list_partition_mul_col():
    """
    {
    "title": "",
    "describe": "3种类型表的多个列的list分区，建表成功，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [(('-32767', '1988-03-21', 'jiw3n4'), ('-32767', '2015-04-02', 'wenlsfnl'),
                             ('255', '1989-03-21', 'wangjuoo5'), ('255', '2015-04-02', ''),
                             ('1985', '2015-01-01', 'du3lnvl')),
                            (('1986', '1901-12-31', 'wangynnsf'), ('1989', '1989-03-21', 'wangjuoo4'),
                             ('1989', '2012-03-14', 'yunlj8@nk'), ('1989', '2015-04-02', 'yunlj8@nk'),
                             ('1991', '1991-08-11', 'wangjuoo4')),
                            (('1991', '2015-04-02', 'wangynnsf'), ('1991', '3124-10-10', 'yanvjldjlll'),
                             ('1992', '9999-12-12', ' '), ('32767', '1991-08-11', 'lifsno'),
                             ('32767', '2014-11-11', 'yanavnd'))]
    partition_info = palo_client.PartitionInfo(['k2', 'k10', 'k7'], partiton_name, 
                                               partition_value_list, partition_type='LIST')
    check_partition_type(partition_info, database_name, table_name)


def test_list_partition_over_num():
    """
    {
    "title": "",
    "describe": "分区列的值溢出，有洞，Null值，正负，0，-0，重复，交叉",
    "tag": "function,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_name = ['p1', 'p2', 'p3', 'p4']
    # 重复值
    partition_value = [('1', '2', '3'), ('10', '10'), ('100'), ('-100', '0')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(False, 'has duplicate item', client.create_table, table_name, DATA.int_column_list, 
                       partition_info=partition_info)
    # 0, -0
    partition_value = [('1', '2', '3'), ('10'), ('100', '-0'), ('-100', '0')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(False, 'is conflict with current partitionKeys', client.create_table, table_name,
                       DATA.int_column_list, partition_info=partition_info)
    # 交叉
    partition_value = [('1', '2', '3'), ('10'), ('100', '-10', '-100'), ('-100', '0')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(False, 'is conflict with current partitionKeys', client.create_table, table_name,
                       DATA.int_column_list, partition_info=partition_info)
    # Null值
    partition_value = [('1', '2', '3'), ('10'), ('100', '-10', 'NULL'), ('-100', '0')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(False, 'Invalid number format: NULL', client.create_table, table_name, DATA.int_column_list, 
                       partition_info=partition_info)
    # 溢出
    partition_value = [('1', '2', '3'), ('10'), ('100', '-10', '2147483648'), ('-100', '0')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(False, 'Number out of range[2147483648]', client.create_table, table_name, DATA.int_column_list, 
                       partition_info=partition_info)
    # 类型不匹配
    partition_value = [('1', '2', '3'), ('10'), ('100', '-10.01'), ('-100', '0')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(False, 'Invalid number format: -10.01', client.create_table, table_name, DATA.int_column_list, 
                       partition_info=partition_info)
    # key为Null
    partition_value = [('1', '2', '3'), ('10'), ('100', '-10'), ('-100', '0')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(False, 'The list partition column must be NOT NULL', client.create_table, table_name,
                       DATA.int_column_list, partition_info=partition_info, set_null=True)
    client.clean(database_name)


def test_list_partition_over_date():
    """
    {
    "title": "",
    "describe": "分区列的值溢出，date/datetime兼容，时区，日期格式，溢出，错误格式的2000-0-0",
    "tag": "function,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_name = ['p1', 'p2']
    # 交叉
    partition_value = [('1990-01-01', '2000-01-01'), ('2000-01-01')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(False, 'conflict with current partitionKeys', client.create_table, table_name,
                       DATA.date_column_list, partition_info=partition_info)
    # date/datetime
    partition_value = [('1990-01-01', '2000-01-01'), ('2000-01-02 08:00:00')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(False, 'date literal [2000-01-02 08:00:00] is invalid', client.create_table, table_name,
                       DATA.date_column_list, partition_info=partition_info)
    util.assert_return(False, 'date literal [1990-01-01] is invalid', client.create_table, table_name,
                       DATA.datetime_column_list, partition_info=partition_info)
    # 0000-01-01 9999-12-31
    partition_value = [('0000-01-01', '2000-01-01'), ('9999-12-31')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(True, None, client.create_table, table_name + '1', DATA.date_column_list,
                       partition_info=partition_info)
    # format
    partition_value = [('1990-01-01', '2000-01-01'), ('20001102')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(True, None, client.create_table, table_name + '2', DATA.date_column_list,
                       partition_info=partition_info)
    partition_value = [('1990-01-01', '2000-01-41'), ('0000-01-00')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(False, 'is invalid', client.create_table, table_name + '3', DATA.date_column_list,
                       partition_info=partition_info)
    partition_value = [('1990-01-01', '2000-01-01'), ('10000-01-01')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(False, 'date literal [10000-01-01] is invalid', client.create_table, table_name + '3',
                       DATA.date_column_list, partition_info=partition_info)
    partition_value = [('1990-01-01', '2000-01-01'), ('00-01-01')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(False, ' ', client.create_table, table_name + '4', DATA.date_column_list,
                       partition_info=partition_info)
    partition_value = [('1990-01-01', '2000-01-01'), ('2000-1-1')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    msg = "The partition key[('2000-01-01')] in partition item[('2000-1-1')] is conflict with current partitionKeys"
    util.assert_return(False, msg, client.create_table, table_name + '5',
                       DATA.date_column_list, partition_info=partition_info)
    client.clean(database_name)


def test_list_partition_over_string():
    """
    {
    "title": "",
    "describe": "分区列的值溢出，string，前缀匹配，empty等",
    "tag": "function,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_name = ['p1', 'p2']
    # 交叉 char & varchar
    partition_value = [('a', 'b'), ('c', ' ', '?', 'a')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    util.assert_return(False, 'is conflict with current partitionKeys', client.create_table, 'tb1_char',
                       DATA.char_least_column_list, partition_info=partition_info)
    util.assert_return(False, 'is conflict with current partitionKeys', client.create_table, 'tb1_varchar',
                       DATA.varchar_least_column_list, partition_info=partition_info)
    # 溢出
    partition_value = [('a', 'b'), ('c', ' ', '?', 'ddd')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    # char
    util.assert_return(True, ' ', client.create_table, 'tb2_char',
                       DATA.char_least_column_list, partition_info=partition_info)
    client.set_variables('enable_insert_strict', 'true')
    util.assert_return(False, 'error totally whack', client.execute, "insert into tb2_char values('d', '1')")
    client.set_variables('enable_insert_strict', 'false')
    util.assert_return(True, ' ', client.execute, "insert into tb2_char values('d', '1')")
    # varchar
    util.assert_return(True, ' ', client.create_table, 'tb2_varchar',
                       DATA.varchar_least_column_list, partition_info=partition_info)
    client.set_variables('enable_insert_strict', 'true')
    util.assert_return(False, 'error totally whack', client.execute, "insert into tb2_varchar values('d', '1')")
    client.set_variables('enable_insert_strict', 'false')
    util.assert_return(True, ' ', client.execute, "insert into tb2_varchar values('d', '1')")

    # 前缀
    partition_value = [('aaa', 'bbb'), ('ccc', ' ', '?', 'aaaa')]
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    # char
    util.assert_return(True, ' ', client.create_table, 'tb3_char',
                       DATA.char_normal_column_list, partition_info=partition_info)
    client.set_variables('enable_insert_strict', 'true')
    util.assert_return(False, 'error totally whack', client.execute, "insert into tb3_char values('aa', '1')")
    client.set_variables('enable_insert_strict', 'false')
    util.assert_return(True, ' ', client.execute, "insert into tb3_char values('aa', '1')")
    # varchar
    util.assert_return(True, ' ', client.create_table, 'tb3_varchar',
                       DATA.varchar_normal_column_list, partition_info=partition_info)
    client.set_variables('enable_insert_strict', 'true')
    util.assert_return(False, 'error totally whack', client.execute, "insert into tb3_varchar values('aa', '1')")
    client.set_variables('enable_insert_strict', 'false')
    util.assert_return(True, ' ', client.execute, "insert into tb3_varchar values('aa', '1')")
    client.clean(database_name)


def test_list_partition_empty():
    """
    {
    "title": "",
    "describe": "没有创建初始的分区",
    "tag": "function,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_name = []
    partition_value = []
    partition_info = palo_client.PartitionInfo('k1', partition_name, partition_value, partition_type='LIST')
    ret = client.create_table(table_name, DATA.date_column_list, partition_info=partition_info)
    assert ret
    load_data_info = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
    assert not ret
    client.clean(database_name)


if __name__ == '__main__':
    test_list_partition_over_string()

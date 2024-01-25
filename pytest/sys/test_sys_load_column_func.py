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
#   @file test_sys_load_column_func.py
#   @date 2019-11-15
#   @brief
#
#############################################################################

"""
测试导入时对replace_if_not_null的column处理
"""
import common
from data import schema as DATA
from lib import palo_config
from lib import palo_job
from lib import palo_client
from lib import util

LOG = palo_client.LOG
L = palo_client.L
config = palo_config.config
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    client.set_variables('enable_insert_strict', 'false')


def check_partition_list(table_name, partition_name_list):
    """
    验证分区是否创建成功
    """
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)


def test_replace_if_not_null_value_with_broker_load():
    """
    {
    "title": "test_sys_load_column_func.test_replace_if_not_null_value_with_broker_load",
    "describe": "test set function replace_if_not_null_value(), 测试broker load时replace_if_not_null列的导入情况",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test set function replace_if_not_null_value()
    测试broker load时replace_if_not_null列的导入情况
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # create table
    table_s = table_name + '_s'
    table_not = table_name + '_not'
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = [100000, 1000000000, 10000000000, 'MAXVALUE']
    duplicate_key = 'AGGREGATE KEY(k1, k2)'
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 3)
    client.create_table(table_s, DATA.replace_if_not_null_column_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_not, DATA.replace_if_not_null_column_list,
                        partition_info, distribution_info, set_null=False, keys_desc=duplicate_key)
    assert client.show_tables(table_s)
    assert client.show_tables(table_not)
    check_partition_list(table_s, partition_name_list)
    check_partition_list(table_not, partition_name_list)

    # broker load
    # replace_if_not_null列not null属性不生效，所以即使设置成了not null，实际上也是null
    set_list = ['v3 = replace_value("-1", NULL)']
    column_name_list = ['k1', 'k2', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    hdfs_file = palo_config.gen_remote_file_path('sys/verify/replace_if_not_null_data_all')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_s,
                                              partition_list=partition_name_list,
                                              column_name_list=column_name_list, column_terminator='\t',
                                              set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.8, strict_mode=True)
    data_desc_list.table_name = table_not
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.5, strict_mode=False)
    # check result
    check_file = './data/LOAD/expe_replace_if_not_null_when_null_value_all_column.data'
    assert client.verify(check_file, table_s)
    check_file = './data/LOAD/expe_replace_if_not_null_when_not_null_value_all_column.data'
    assert client.verify(check_file, table_not)

    column_name_list = ['k1', 'k2', 'v2', 'v10', 'v11']
    hdfs_file = palo_config.gen_remote_file_path('sys/verify/replace_if_not_null_data_some')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_s,
                                              partition_list=partition_name_list,
                                              column_name_list=column_name_list, column_terminator='\t')

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)
    data_desc_list.table_name = table_not
    assert not client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                                 max_filter_ratio=0.5, strict_mode=False)
    # check result
    check_file = './data/LOAD/expe_replace_if_not_null_when_null_value_some_column.data'
    assert client.verify(check_file, table_s)
    check_file = './data/LOAD/expe_replace_if_not_null_when_not_null_value_all_column.data'
    assert client.verify(check_file, table_not)

    client.clean(database_name)


def test_replace_if_not_null_value_with_insert_into():
    """
    {
    "title": "test_sys_load_column_func.test_replace_if_not_null_value_with_insert_into",
    "describe": "test set function replace_if_not_null_value(), 测试insert into时replace_if_not_null列的导入情况",
    "tag": "function,p1"
    }
    """
    """
    test set function replace_if_not_null_value()
    测试insert into时replace_if_not_null列的导入情况
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # create table
    table_s = table_name + '_s'
    table_not = table_name + '_not'
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = [100000, 1000000000, 10000000000, 'MAXVALUE']
    duplicate_key = 'AGGREGATE KEY(k1, k2)'
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 3)
    client.create_table(table_s, DATA.replace_if_not_null_column_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_not, DATA.replace_if_not_null_column_list,
                        partition_info, distribution_info, set_null=False, keys_desc=duplicate_key)
    assert client.show_tables(table_s)
    assert client.show_tables(table_not)
    check_partition_list(table_s, partition_name_list)
    check_partition_list(table_not, partition_name_list)

    file_name = '../hdfs/data/sys/verify/replace_if_not_null_data_all'
    insert_sql = 'insert into %s.%s VALUES %s' % (database_name, table_s, util.file_to_insert_sql_value(file_name))
    ret = client.execute(insert_sql)
    assert ret == ()
    insert_sql = 'insert into %s.%s VALUES %s' % (database_name, table_not, util.file_to_insert_sql_value(file_name))
    ret = client.execute(insert_sql)
    assert ret == ()

    # check result
    check_file = './data/LOAD/expe_replace_if_not_null_when_null_value_all_column_with_insert.data'
    assert client.verify(check_file, table_s)
    check_file = './data/LOAD/expe_replace_if_not_null_when_not_null_value_all_column_with_insert.data'
    assert client.verify(check_file, table_not)

    column_name_list = ['k1', 'k2', 'v2', 'v10', 'v11']
    file_name = '../hdfs/data/sys/verify/replace_if_not_null_data_some'
    insert_sql = 'insert into %s.%s (%s) VALUES %s' % (database_name, table_s, ','.join(column_name_list),
                                                       util.file_to_insert_sql_value(file_name, True))
    ret = client.execute(insert_sql)
    assert ret == ()

    # check result
    check_file = './data/LOAD/expe_replace_if_not_null_when_null_value_some_column_with_insert.data'
    assert client.verify(check_file, table_s)

    client.clean(database_name)


def test_replace_if_not_null_value_with_insert_select():
    """
    {
    "title": "test_sys_load_column_func.test_replace_if_not_null_value_with_insert_select",
    "describe": "test set function replace_if_not_null_value(), 测试insert select时replace_if_not_null列的导入情况",
    "tag": "function,p1"
    }
    """
    """
    test set function replace_if_not_null_value()
    测试insert select时replace_if_not_null列的导入情况
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # create table
    # ba和bs表使用broker load进行导入，是为insert select导入做数据准备
    table_base_all = table_name[0:58] + 'ba'
    table_base_some = table_name[0:58] + 'bs'
    table_s = table_name + '_s'
    table_not = table_name + '_not'
    
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = [100000, 1000000000, 10000000000, 'MAXVALUE']
    duplicate_key = 'AGGREGATE KEY(k1, k2)'
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 3)
    client.create_table(table_base_all, DATA.replace_if_not_null_column_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_base_some, DATA.replace_if_not_null_column_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_s, DATA.replace_if_not_null_column_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_not, DATA.replace_if_not_null_column_list,
                        partition_info, distribution_info, set_null=False, keys_desc=duplicate_key)
    assert client.show_tables(table_s)
    assert client.show_tables(table_not)
    check_partition_list(table_s, partition_name_list)
    check_partition_list(table_not, partition_name_list)

    # ba和bs表使用broker load进行导入，是为insert select导入做数据准备
    set_list = ['v3 = replace_value("-1", NULL)']
    column_name_list = ['k1', 'k2', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    hdfs_file = palo_config.gen_remote_file_path('sys/verify/replace_if_not_null_data_all')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_base_all,
                                              partition_list=partition_name_list,
                                              column_name_list=column_name_list, column_terminator='\t',
                                              set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.8, strict_mode=True)
    column_name_list = ['k1', 'k2', 'v1', 'v2', 'v11']
    set_list = ['v2 = replace_value(NULL, "1")']
    hdfs_file = palo_config.gen_remote_file_path('sys/verify/replace_if_not_null_data_some')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_base_some,
                                              partition_list=partition_name_list,
                                              column_name_list=column_name_list, column_terminator='\t',
                                              set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)

    line = 'select k1, k2, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12 from {table} order by k1, k2'\
        .format(table=table_base_all)
    insert_sql = 'insert into %s.%s %s' % (database_name, table_s, line)
    ret = client.execute(insert_sql)
    assert ret == ()
    line = 'select k1, k2, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12 from {table} order by k1, k2'\
        .format(table=table_base_all)
    insert_sql = 'insert into %s.%s %s' % (database_name, table_not, line)
    ret = client.execute(insert_sql)
    assert ret == ()

    check_file = './data/LOAD/expe_replace_if_not_null_when_null_value_all_column.data'
    assert client.verify(check_file, table_s)
    check_file = './data/LOAD/expe_replace_if_not_null_when_not_null_value_all_column_with_insert_select.data'
    assert client.verify(check_file, table_not)

    line = 'select k1, k2, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12 from {table} order by k1, k2'\
        .format(table=table_base_some)
    insert_sql = 'insert into %s.%s %s' % (database_name, table_s, line)
    ret = client.execute(insert_sql)
    assert ret == ()
    line = 'select k1, k2, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12 from {table} order by k1, k2'\
        .format(table=table_base_some)
    insert_sql = 'insert into %s.%s %s' % (database_name, table_not, line)
    ret = client.execute(insert_sql)
    assert ret == ()

    check_file = './data/LOAD/expe_replace_if_not_null_when_null_value_all_column_with_insert_select2.data'
    assert client.verify(check_file, table_s)
    check_file = './data/LOAD/expe_replace_if_not_null_when_not_null_value_all_column_with_insert_select2.data'
    assert client.verify(check_file, table_not)

    client.clean(database_name)


def test_replace_if_not_null_with_rollup():
    """
    {
    "title": "test_sys_load_column_func.test_replace_if_not_null_with_rollup",
    "describe": "test set function test_replace_if_not_null_with_rollup, 测试表中存在replace_if_not_null列时，建rollup的情况",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test set function test_replace_if_not_null_with_rollup
    测试表中存在replace_if_not_null列时，建rollup的情况
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # create table
    table_s = table_name + '_s'
    table_not = table_name + '_not'
    # create index_name
    index_s = index_name + '_s'
    index_not = index_name + '_not'
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = [100000, 1000000000, 10000000000, 'MAXVALUE']
    duplicate_key = 'AGGREGATE KEY(k1, k2)'
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 3)
    client.create_table(table_s, DATA.replace_if_not_null_column_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_not, DATA.replace_if_not_null_column_list,
                        partition_info, distribution_info, set_null=False, keys_desc=duplicate_key)
    assert client.show_tables(table_s)
    assert client.show_tables(table_not)
    check_partition_list(table_s, partition_name_list)
    check_partition_list(table_not, partition_name_list)
    # broker load
    set_list = ['v3 = replace_value("-1", NULL)']
    column_name_list = ['k1', 'k2', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    hdfs_file = palo_config.gen_remote_file_path('sys/verify/replace_if_not_null_data_all')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_s,
                                              partition_list=partition_name_list,
                                              column_name_list=column_name_list, column_terminator='\t',
                                              set_list=set_list)

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.8, strict_mode=True)
    data_desc_list.table_name = table_not
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.5, strict_mode=False)
    # check result
    check_file = './data/LOAD/expe_replace_if_not_null_when_null_value_all_column.data'
    assert client.verify(check_file, table_s)
    check_file = './data/LOAD/expe_replace_if_not_null_when_not_null_value_all_column.data'
    assert client.verify(check_file, table_not)

    column_name_list = ['k2', 'k1', 'v10', 'v11']
    client.create_rollup_table(table_s, index_s, column_name_list, is_wait=True)
    client.create_rollup_table(table_not, index_not, column_name_list, is_wait=True)

    hdfs_file = palo_config.gen_remote_file_path('sys/verify/replace_if_not_null_data_rollup')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_s,
                                              partition_list=partition_name_list,
                                              column_name_list=['k1', 'k2', 'v10', 'v11'],
                                              column_terminator='\t')

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)
    data_desc_list.table_name = table_not
    assert not client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                                 max_filter_ratio=0.5, strict_mode=False)
    # check rollup result
    check_file = './data/LOAD/expe_replace_if_not_null_when_null_value_rollup.data'
    sql = 'select k2,k1,v10,v11 from %s order by k1 nulls last' % table_s
    assert common.check_by_file(check_file, sql=sql, client=client)
    check_file = './data/LOAD/expe_replace_if_not_null_when_not_null_value_rollup.data'
    sql = 'select k2,k1,v10,v11 from %s order by k1,k2' % table_not
    assert common.check_by_file(check_file, sql=sql, client=client)
    client.clean(database_name)


def test_replace_if_not_null_with_rollup_when_schema_change():
    """
    {
    "title": "test_sys_load_column_func.test_replace_if_not_null_with_rollup_when_schema_change",
    "describe": "test set function test_replace_if_not_null_with_rollup_when_schema_change, 测试replace_if_not_null列进行schema change的情况",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test set function test_replace_if_not_null_with_rollup_when_schema_change
    测试replace_if_not_null列进行schema change的情况
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    # create keys
    keys_list = {
        'key_agg': 'AGGREGATE KEY(k1, k2)',
        'key_dup': 'DUPLICATE KEY(k1, k2)',
        'key_uniq': 'UNIQUE KEY(k1, k2)'
    }

    column_list = {
        'key_agg': DATA.replace_if_not_null_column_list,
        'key_dup': DATA.replace_if_not_null_no_agg_column_list,
        'key_uniq': DATA.replace_if_not_null_no_agg_column_list
    }

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = [100000, 1000000000, 10000000000, 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 3)

    column_name_list = ['k2', 'k1', 'v10', 'v11']

    # dup和uniq都不能设置aggregate type，所以不做验证
    for i in ['agg']:
        table_name = 'table_' + i
        index_name = 'index_' + i
        client.create_table(table_name, column_list['key_' + i],
                            partition_info, distribution_info, set_null=True, keys_desc=keys_list['key_' + i])

        assert client.show_tables(table_name)
        check_partition_list(table_name, partition_name_list)
        client.create_rollup_table(table_name, index_name, column_name_list, is_wait=True)

        util.assert_return(True, '',
                           client.schema_change_add_column, table_name, [('k3', 'INT KEY', None, '3')],
                           after_column_name='k1', is_wait_job=True, is_wait_delete_old_schema=True
                           )

        util.assert_return(True, '',
                           client.schema_change_add_column, table_name, [('v13', 'INT SUM', None, '0')],
                           after_column_name='k2', is_wait_job=True, is_wait_delete_old_schema=True
                           )

        util.assert_return(True, '',
                           client.schema_change_drop_column, table_name, ['v2'],
                           is_wait_job=True, is_wait_delete_old_schema=True)

        util.assert_return(False,
                           'Can not drop key column when table has value column with REPLACE aggregation method',
                           client.schema_change_drop_column, table_name, ['k1'],
                           is_wait_job=True, is_wait_delete_old_schema=True)

        # REPLACE_IF_NOT_NULL列即使设置了not null，实际上也会按null处理，不会报错
        util.assert_return(False, 'Nothing is changed',
                           client.schema_change_modify_column, table_name, 'v6', 'BIGINT REPLACE_IF_NOT_NULL',
                           aggtype='-', column_info='NOT NULL',
                           is_wait_job=True, is_wait_delete_old_schema=True)

        # REPLACE_IF_NOT_NULL列即使设置了not null，实际上也会按null处理，不会报错
        util.assert_return(True, '',
                           client.schema_change_add_column, table_name, [('v14', 'INT REPLACE_IF_NOT_NULL',
                                                                          None, '3')],
                           after_column_name='v10', is_wait_job=True, is_wait_delete_old_schema=True
                           )
        column = client.get_column('v14', table_name, database_name)
        assert palo_job.DescInfo(column).get_null()

    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    setup_module()

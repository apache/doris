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

################################################################################
#
#   @file test_sys_rollup_scenario.py
#   @date 2015/05/29 15:26:21
#   @brief This module provide index(rollup table) related cases for Palo2 testing.
#   部分case要求副本数大于1
################################################################################

"""
This module provide index(rollup table) related cases for Palo2 testing.
"""

import time
import sys
import threading
sys.path.append("../")
from data import rollup_scenario as DATA
from lib import palo_client
from lib import palo_config
from lib import util
from lib import common

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L
client = None
broker_info = palo_config.broker_info


def setup_module():
    """
    Set up
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, 
                                    password=config.fe_password, http_port=config.fe_http_port)


def test_rollup_k2_v3_sum():
    """
    {
    "title": "test_sys_rollup_scenario.test_rollup_k2_v3_sum",
    "describe": "rollup k2 sum(v3)",
    "tag": "system,p1"
    }
    """
    """
    k2 sum(v3)
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.schema_1, keys_desc='AGGREGATE KEY (k1, k2)')
    rename_table_name = table_name + '_rename'
    client.rename_table(rename_table_name, table_name)
    table_name = rename_table_name
    client.create_rollup_table(table_name, index_name, \
            DATA.rollup_field_list_1, is_wait=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_index(table_name, index_name=index_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    sql = 'SELECT k2, SUM(v3) FROM %s GROUP BY k2' % (table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name in shoot_table, 'expect rollup %s, but %s' % (index_name, shoot_table)
    verify_schema = [('k2', 'INT'), \
                ('v3', 'DECIMAL(20,7)', 'SUM')]
    assert client.verify_by_sql(DATA.expected_data_file_list_1_b, sql, verify_schema)
    client.clean(database_name)


def test_rollup_k1_k3_v1_sum():
    """
    {
    "title": "test_sys_rollup_scenario.test_rollup_k1_k3_v1_sum",
    "describe": "rollup k1 k3 sum(v1)",
    "tag": "system,p1"
    }
    """
    """
    k1 k3 sum(v1)
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.schema_2, keys_desc='AGGREGATE KEY (k1, k2, k3)')
    client.create_rollup_table(table_name, index_name, \
            DATA.rollup_field_list_2, is_wait=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_index(table_name, index_name=index_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_2, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_2, table_name)
    assert ret

    sql = 'SELECT k1, k3, SUM(v1) FROM %s GROUP BY k1, k3' % (table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name in shoot_table, 'expect rollup %s, but %s' % (index_name, shoot_table)
    verify_schema = [('k1', 'INT'), \
                ('k3', 'INT'), \
                ('v1', 'INT', 'SUM')]
    assert client.verify_by_sql(DATA.expected_data_file_list_2_b, sql, verify_schema)
    client.clean(database_name)


def test_rollup_after_schema_change():
    """
    {
    "title": "test_sys_rollup_scenario.test_rollup_after_schema_change",
    "describe": "schema change后rollup，验证rollup成功，命中rollup表，数据正确",
    "tag": "system,p1"
    }
    """
    """
    schema change后rollup，验证rollup成功，命中rollup表，数据正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-1', '0', '1', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1)', 10) 
    client.create_table(table_name, DATA.schema_3, keys_desc='AGGREGATE KEY (k1, k2)', \
            partition_info=partition_info, distribution_info=distribution_info)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    assert client.schema_change_add_column(table_name, [('k3', 'INT KEY', None, '3'),], \
            after_column_name='k2', is_wait_job=True, is_wait_delete_old_schema=True)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_2, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_2, table_name)

    client.create_rollup_table(table_name, index_name, \
            DATA.rollup_field_list_2, is_wait=True)
    assert client.get_index(table_name, index_name=index_name)

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(list(DATA.expected_data_file_list_2) * 2, table_name)

    sql = 'SELECT k1, k3, SUM(v1) FROM %s GROUP BY k1, k3' % (table_name)
    time.sleep(10)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    client.clean(database_name)


def test_rollup_while_schema_change():
    """
    {
    "title": "test_sys_rollup_scenario.test_rollup_while_schema_change",
    "describe": "schema change同时rollup",
    "tag": "system,p1,fuzz"
    }
    """
    """
    schema change同时rollup
    """
    class SchemaChangeThread(threading.Thread):
        """
        schema change线程
        """
        def __init__(self, database_name, table_name):
            threading.Thread.__init__(self)
            self.table_name = table_name
            self.database_name = database_name

        def run(self):
            """
            run
            """
            thread_client = palo_client.PaloClient(palo_config.config.fe_host, \
                    palo_config.config.fe_query_port, self.database_name)
            thread_client.init()
            thread_client.schema_change_add_column(self.table_name, [('k3', 'INT KEY', None, '3'),], \
                after_column_name='k2', is_wait_job=True, is_wait_delete_old_schema=True)

    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-1', '0', '1', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1)', 10) 
    client.create_table(table_name, DATA.schema_3, keys_desc='AGGREGATE KEY (k1, k2)', \
            partition_info=partition_info, distribution_info=distribution_info)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    schema_change_thread = SchemaChangeThread(database_name, table_name)
    schema_change_thread.start()

    client.wait_table_schema_change_job(table_name)

    ret = None
    try:
        ret = client.create_rollup_table(table_name, index_name + 'while', \
                DATA.rollup_field_list_3, is_wait=True)
        ret = client.get_index(table_name, index_name=index_name)
    except:
        pass

    assert not ret

    schema_change_thread.join()

    client.create_rollup_table(table_name, index_name, \
            DATA.rollup_field_list_2, is_wait=True)
    assert client.get_index(table_name, index_name=index_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_2, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_2, table_name)

    sql = 'SELECT k1, k3, SUM(v1) FROM %s GROUP BY k1, k3' % (table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name in shoot_table, 'expect rollup %s, but %s' % (index_name, shoot_table)
    client.clean(database_name)


def test_schema_change_to_rollup_table():
    """
    {
    "title": "test_sys_rollup_scenario.test_schema_change_to_rollup_table",
    "describe": "对rollup表schema change,全部命中base表,对rollup表做schema change, 也会影响到base表",
    "tag": "system,p1"
    }
    """
    """
    对rollup表schema change
    全部命中base表
    对rollup表做schema change, 也会影响到base表
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-1', '0', '1', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1)', 10) 
    client.create_table(table_name, DATA.schema_3, keys_desc='AGGREGATE KEY (k1, k2)', \
            partition_info=partition_info, distribution_info=distribution_info)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    client.create_rollup_table(table_name, index_name, \
            DATA.rollup_field_list_4, is_wait=True)
    assert client.get_index(table_name, index_name=index_name)

    assert client.schema_change_add_column(table_name, [('k3', 'INT KEY', None, '3'),], \
            after_column_name='k1', to_table_name=index_name, \
            is_wait_job=True, is_wait_delete_old_schema=True)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_2, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_2, table_name)
    client.clean(database_name)


def test_rollup_after_load():
    """
    {
    "title": "test_sys_rollup_scenario.test_rollup_after_load",
    "describe": "先导入数据再创建上卷表",
    "tag": "system,p1"
    }
    """
    """
    先导入数据再创建上卷表
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-1', '0', '1', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    client.create_table(table_name, DATA.schema_2, partition_info=partition_info, \
            keys_desc='AGGREGATE KEY (k1, k2, k3)')
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_2, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.create_rollup_table(table_name, index_name, \
            DATA.rollup_field_list_2, is_wait=True)
    assert ret, 'create rollup table failed'
    assert client.get_index(table_name, index_name=index_name)

    ret = client.verify(DATA.expected_data_file_list_2, table_name)
    assert ret

    sql = 'SELECT k1, k3, SUM(v1) FROM %s GROUP BY k1, k3' % (table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name in shoot_table, 'expect rollup %s, but %s' % (index_name, shoot_table)
    verify_schema = [('k1', 'INT'), \
                ('k3', 'INT'), \
                ('v1', 'INT', 'SUM')]
    assert client.verify_by_sql(DATA.expected_data_file_list_2_b, sql, verify_schema)
    client.clean(database_name)


def test_rollup_then_delete():
    """
    {
    "title": "test_sys_rollup_scenario.test_rollup_then_delete",
    "describe": "rollup后delete",
    "tag": "system,p1"
    }
    """
    """
    rollup后delete
    rollup index should contains all partition keys
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-100', '-1', '0', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k2', \
            partition_name_list, partition_value_list)
    client.create_table(table_name, DATA.schema_1, partition_info=partition_info, \
            keys_desc='AGGREGATE KEY (k1, k2)')
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    #rollup_field_list_1 = ['k2', 'v3']
    client.create_rollup_table(table_name, index_name, \
            DATA.rollup_field_list_1, is_wait=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_index(table_name, index_name=index_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    assert client.verify(DATA.expected_data_file_list_1, table_name)

    #assert client.delete(table_name, [('k1', '=', '3000'),], 'partition_d')
    #对一个含有index的basetable进行delete操作时
    #删除条件当中的列必须在index中也存在
    assert client.delete(table_name, [('k2', '=', '246'),], 'partition_d')

    sql = 'SELECT k2, SUM(v3) FROM %s GROUP BY k2' % (table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name in shoot_table, 'expect rollup %s, but %s' % (index_name, shoot_table)
    client.clean(database_name)


def test_rollup_while_load():
    """
    {
    "title": "test_sys_rollup_scenario.test_rollup_while_load",
    "describe": "rollup同时导入",
    "tag": "system,p1"
    }
    """
    """
    rollup同时导入
    column[k3] does not exist
    """
    class LoadThread(threading.Thread):
        """
        导入线程
        """
        def __init__(self, database_name, data_desc_list, label, max_filter_ratio=None):
            threading.Thread.__init__(self)
            self.database_name = database_name
            self.data_desc_list = data_desc_list
            self.label = label
            self.max_filter_ratio = max_filter_ratio

        def run(self):
            """
            run
            """
            thread_client = palo_client.PaloClient(palo_config.config.fe_host, \
                    palo_config.config.fe_query_port, self.database_name)
            thread_client.init()
            thread_client.batch_load(self.label, self.data_desc_list, \
                    max_filter_ratio=self.max_filter_ratio, is_wait=True, broker=broker_info)

    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-1', '0', '1', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
#   client.create_table(table_name, DATA.schema_3, partition_info)
    client.create_table(table_name, DATA.schema_2, partition_info=partition_info, \
            keys_desc='AGGREGATE KEY (k1, k2, k3)')
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_2, table_name)
    load_thread = LoadThread(database_name, data_desc_list, util.get_label())
    load_thread.start()

    client.create_rollup_table(table_name, index_name, \
            DATA.rollup_field_list_2, is_wait=True)
    assert client.get_index(table_name, index_name=index_name)

    load_thread.join()

    ret = client.verify(DATA.expected_data_file_list_2, table_name)
    assert ret

    sql = 'SELECT k1, k3, SUM(v1) FROM %s GROUP BY k1, k3' % (table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name in shoot_table, 'expect rollup %s, but %s' % (index_name, shoot_table)
    client.clean(database_name)


def test_rollup_while_delete():
    """
    {
    "title": "test_sys_rollup_scenario.test_rollup_while_delete",
    "describe": "rollup同时delete",
    "tag": "system,p1"
    }
    """
    """
    rollup同时delete
    rollup index should contains all partition keys
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-1', '0', '1', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k2', \
            partition_name_list, partition_value_list)
    client.create_table(table_name, DATA.schema_1, partition_info=partition_info, \
            keys_desc='AGGREGATE KEY (k1, k2)')
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    assert client.create_rollup_table(table_name, index_name, DATA.rollup_field_list_1)

    assert client.delete(table_name, [('k2', '=', '246'),], 'partition_d')
    sql = 'SELECT count(*) FROM %s where k2 = 246' % (table_name)
    assert client.execute(sql) == ((0,),), "Delete failed"

    ret = client.wait_table_rollup_job(table_name, database_name)
    assert ret, "Rollup failed"
    assert client.verify(DATA.expected_data_file_list_3, table_name)
    client.clean(database_name)


def test_cancel_rollup():
    """
    {
    "title": "test_sys_rollup_scenario.test_cancel_rollup",
    "describe": "cancel rollup以及增减分区",
    "tag": "system,p1"
    }
    """
    """
    cancel rollup以及增减分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-1', '0', '50', '100']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    client.create_table(table_name, DATA.schema_2, partition_info=partition_info, \
            keys_desc='AGGREGATE KEY (k1, k2, k3)')
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_2, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    assert client.add_partition(table_name, 'partition_e', '300')

    assert client.create_rollup_table(table_name, index_name, \
            DATA.rollup_field_list_2)

    assert client.cancel_rollup(table_name)

    assert client.drop_partition(table_name, 'partition_d')
    assert client.add_partition(table_name, 'partition_f', 'MAXVALUE')

    assert client.verify(DATA.expected_data_file_list_2, table_name)
    client.clean(database_name)


def test_rollup_rename_issue_4867():
    """
    {
    "title": "test_sys_rollup_scenario.test_rollup_rename_error",
    "describe": "rename rollup name to table name",
    "tag": "system,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    ret = client.create_table(table_name, DATA.schema_1, keys_desc='AGGREGATE KEY (k1, k2)')
    assert ret, 'create table failed'
    client.create_rollup_table(table_name, index_name, DATA.rollup_field_list_1, is_wait=True)

    assert client.show_tables(table_name), 'get table failed'
    assert client.get_index(table_name), 'get table index failed'
    assert client.get_index(table_name, index_name=index_name), 'get rollup index failed'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret, 'data check failed'

    sql = 'SELECT k2, SUM(v3) FROM %s GROUP BY k2' % (table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name in shoot_table, 'expect rollup %s, but %s' % (index_name, shoot_table)
    verify_schema = [('k2', 'INT'), ('v3', 'DECIMAL(20,7)', 'SUM')]
    assert client.verify_by_sql(DATA.expected_data_file_list_1_b, sql, verify_schema)
    msg = 'New name conflicts with rollup index name:'
    util.assert_return(False, msg, client.rename_table, index_name, table_name)
    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    pass


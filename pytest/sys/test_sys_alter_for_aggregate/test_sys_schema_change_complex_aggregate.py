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
  * @file test_sys_schema_change.py
  * @date 2015/02/04 15:26:21
  * @brief This file is a test file for Palo schema changing.
  * 
  **************************************************************************/
"""

import sys
sys.path.append("../")
import time
from data import schema_change as DATA
from lib import palo_config
from lib import palo_client
from lib import palo_task
from lib import util
from lib import palo_job

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L
broker_info = palo_config.broker_info


keys_desc = 'AGGREGATE KEY (tinyint_key, smallint_key, int_key, bigint_key, char_50_key, ' \
    'character_key, char_key, character_most_key, decimal_key, ' \
    'decimal_most_key, date_key, datetime_key)'

def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, 
                                    password=config.fe_password, http_port=config.fe_http_port)


def test_selecting():
    """
    {
    "title": "test_sys_schema_change_complex_aggregate.test_selecting",
    "describe": "功能点：schema change不影响查询",
    "tag": "system,p1,stability"
    }
    """
    """
    功能点：schema change不影响查询
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)

    hash_distribution_info = palo_client.DistributionInfo(\
            distribution_type=DATA.hash_partition_type, \
            bucket_num=DATA.hash_partition_num)

    ret = client.create_table(table_name, DATA.schema, keys_desc=keys_desc, \
            distribution_info=hash_distribution_info, storage_type=DATA.storage_type)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name)
    label = "%s_1" % database_name
    ret = client.batch_load(label, data_desc_list, max_filter_ratio="0.5", is_wait=True, 
                            broker=broker_info)
    assert ret

    sql = "SELECT * FROM %s.%s" % (database_name, table_name)
    select_task = palo_task.SelectTask(config.fe_host, config.fe_query_port, sql, \
            database_name=database_name)
    select_thread = palo_task.TaskThread(select_task)
    select_thread.start()
    client.schema_change_drop_column(table_name, \
            column_name_list=DATA.drop_column_name_list_new)
    select_thread.stop()
    #TODO 验证数据 
    client.clean(database_name)

 
def test_loading():
    """
    {
    "title": "test_sys_schema_change_complex_aggregate.test_loading",
    "describe": "功能点：导入不影响schema change",
    "tag": "system,p1,stability"
    }
    """
    """
    功能点：导入不影响schema change
    测试步骤：
    1. 启动一个线程持续进行导入任务
    2. 等到有任务进入loading状态、做schema change
    验证：
    1. 导入一直正确
    2. schema change后数据正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)

    hash_distribution_info = palo_client.DistributionInfo(\
            distribution_type=DATA.hash_partition_type, \
            bucket_num=DATA.hash_partition_num)
    ret = client.create_table(table_name, DATA.schema, keys_desc=keys_desc, 
            distribution_info=hash_distribution_info, storage_type=DATA.storage_type)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name)
    label = "%s_1" % database_name
    ret = client.batch_load(label, data_desc_list, max_filter_ratio="0.5", is_wait=True, 
                            broker=broker_info)
    assert ret

    load_task = palo_task.BatchLoadTask(config.fe_host, config.fe_query_port, database_name, 
                                        label, data_desc_list, max_filter_ratio="0.05", is_wait=False, 
                                        interval=10, broker=broker_info)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    #等到有任务进入loading 状态
    timeout = 1200
    while not client.get_load_job_list(state="LOADING") and timeout > 0:
        print(client.get_load_job_list(state="LOADING"))
        time.sleep(1)
        timeout -= 1
    if timeout == 0:
        assert 0 == 1, 'can not get loading job'
    client.schema_change_drop_column(table_name, \
            column_name_list=DATA.drop_column_name_list_new)
    load_thread.stop()
    #等到schema change完成
    client.wait_table_schema_change_job(table_name)

    #schema change完成后，没有未完成的导入任务
    loading_job = client.get_unfinish_load_job_list()
    #TODO 验证数据 
    client.clean(database_name)
 

def test_multi_schema_change_in_db():
    """
    {
    "title": "test_sys_schema_change_complex_aggregate.test_multi_schema_change_in_db",
    "describe": "功能点：相同的database中不同的table family同时进行schema change",
    "tag": "system,p1,stability"
    }
    """
    """
    功能点：相同的database中不同的table family同时进行schema change
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)

    table_name_1 = "%s_1" % table_name
    hash_distribution_info = palo_client.DistributionInfo(\
            distribution_type=DATA.hash_partition_type, \
            bucket_num=DATA.hash_partition_num)
    ret = client.create_table(table_name_1, DATA.schema, keys_desc=keys_desc, \
            distribution_info=hash_distribution_info, storage_type=DATA.storage_type)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name_1)
    label = table_name_1
    ret = client.batch_load(label, data_desc_list, max_filter_ratio="0.5", is_wait=True, 
                            broker=broker_info)
    assert ret

    table_name_2 = "%s_2" % table_name
    ret = client.create_table(table_name_2, DATA.schema, keys_desc=keys_desc, \
            distribution_info=hash_distribution_info, storage_type=DATA.storage_type)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name_2)
    label = table_name_2
    ret = client.batch_load(label, data_desc_list, max_filter_ratio="0.5", is_wait=True, 
                            broker=broker_info)
    assert ret
    timeout = 1200
    while client.get_unfinish_load_job_list() and timeout > 0:
        print(client.get_unfinish_load_job_list())
        time.sleep(1)
        timeout -= 1

    assert client.schema_change_drop_column(table_name_1, DATA.drop_column_name_list_new)
    assert client.schema_change_drop_column(table_name_2, DATA.drop_column_name_list_new, \
            is_wait_job=True)
    client.wait_table_schema_change_job(table_name_1)
    new_column_name_list = list(set(DATA.column_name_list).difference(set(\
            DATA.drop_column_name_list_new))) 
    sql = "SELECT %s FROM %s.%s" % (", ".join(new_column_name_list), \
            database_name, table_name_1)

    data_1 = client.execute(sql)
    sql = "SELECT %s FROM %s.%s" % (", ".join(new_column_name_list), \
            database_name, table_name_1)
    data_2 = client.execute(sql)
    #assert data_1 == data_2
    client.clean(database_name)


def test_delete_key():
    """
    {
    "title": "test_sys_schema_change_complex_aggregate.test_delete_key",
    "describe": "功能点：有删除的版本基础上做schema change，新表包括delete key",
    "tag": "system,p1"
    }
    """
    """
    功能点：有删除的版本基础上做schema change，新表包括delete key
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    distribution_info = palo_client.DistributionInfo('HASH(tinyint_key)', 10) 

    rename_table_name = table_name + '_rename'
    if len(rename_table_name) > 60:
        rename_table_name = rename_table_name[-60:]
    assert client.create_table(table_name, DATA.schema, keys_desc=keys_desc, \
            distribution_info=distribution_info)
    client.rename_table(rename_table_name, table_name)
    table_name = rename_table_name

    ret = client.show_tables(table_name)
    assert ret
    ret = client.get_index(table_name)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, max_filter_ratio='0.5', is_wait=True,
                            broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_delete_key_1, table_name, encoding='utf-8')
    assert ret

    ret = client.delete(table_name, DATA.delete_condition_list)
    assert ret
    assert client.verify(DATA.expected_data_file_list_17, table_name)

    ret = client.delete(table_name, DATA.delete_condition_list_2)
    assert ret

    sql = 'ALTER TABLE %s.%s %s' % (database_name, table_name, \
          'ADD COLUMN add_int_key INT KEY DEFAULT "65599" AFTER int_key,' \
          'ADD COLUMN add_smallint_key SMALLINT KEY DEFAULT "1000" AFTER smallint_key,' \
          'ADD COLUMN (add_tinyint_value TINYINT SUM DEFAULT "0",' \
          'add_bigint_value BIGINT REPLACE DEFAULT "321432421342314",' \
          'add_decimal_value DECIMAL(27,9) MAX DEFAULT "32432132.234289",' \
          'add_double_value DOUBLE MIN DEFAULT "10.0",' \
          'add_datetime_value DATETIME REPLACE DEFAULT "2015-04-13 20:00:00",' \
          'add_date_value DATE REPLACE DEFAULT "2015-04-13"),' \
          'DROP COLUMN char_value,' \
          'DROP COLUMN float_value,' \
          'MODIFY COLUMN int_value BIGINT SUM')

    ret = client.execute(sql)
    assert ret == ()
    client.wait_table_schema_change_job(table_name, database_name)

    ret = client.verify(DATA.expected_data_file_list_delete_key_2_new_agg, table_name)
    assert ret
    client.clean(database_name)


def test_cancel_schema_change():
    """
    {
    "title": "test_sys_schema_change_complex_aggregate.test_cancel_schema_change",
    "describe": "在一个tablet比较多的大表上做schemachange，做到一半cancel掉，紧接着又开始做新的schemachange",
    "tag": "system,p1"
    }
    """
    """
    在一个tablet比较多的大表上做schemachange，做到一半cancel掉，紧接着又开始做新的schemachange
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    distribution_info = palo_client.DistributionInfo('HASH(tinyint_key)', 500) 
    ret = client.create_table(table_name, DATA.schema, keys_desc=keys_desc, \
            distribution_info=distribution_info)
    assert ret

    ret = client.show_tables(table_name)
    assert ret
    ret = client.get_index(table_name)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, max_filter_ratio='0.5', is_wait=True,
                            broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_delete_key_1, table_name, encoding='utf-8')
    assert ret

    ret = client.delete(table_name, DATA.delete_condition_list)
    assert ret
    assert client.verify(DATA.expected_data_file_list_17, table_name)

    ret = client.delete(table_name, DATA.delete_condition_list_2)
    assert ret

    sql = 'ALTER TABLE %s.%s %s' % (database_name, table_name, \
          'ADD COLUMN add_int_key INT KEY DEFAULT "65599" AFTER int_key,' \
          'ADD COLUMN add_smallint_key SMALLINT KEY DEFAULT "1000" AFTER smallint_key,' \
          'ADD COLUMN (add_tinyint_value TINYINT SUM DEFAULT "0",' \
          'add_bigint_value BIGINT REPLACE DEFAULT "321432421342314",' \
          'add_decimal_value DECIMAL(27,9) MAX DEFAULT "32432132.234289",' \
          'add_double_value DOUBLE MIN DEFAULT "10.0",' \
          'add_datetime_value DATETIME REPLACE DEFAULT "2015-04-13 20:00:00",' \
          'add_date_value DATE REPLACE DEFAULT "2015-04-13"),' \
          'DROP COLUMN char_value,' \
          'DROP COLUMN float_value,' \
          'MODIFY COLUMN int_value BIGINT SUM')

    assert () == client.execute(sql)
    try_times = 180 
    while try_times > 0:
        time.sleep(1)
        schema_change_job_list = client.get_table_schema_change_job_list(table_name, database_name)
        if not schema_change_job_list:
            try_times -= 1
            continue
        
        last_job_state = schema_change_job_list[-1][palo_job.SchemaChangeJob.State]
        if last_job_state == 'RUNNING':
        #    time.sleep(5)
            break

    assert try_times

    client.cancel_schema_change(table_name, database_name)

    assert () == client.execute(sql)
    client.wait_table_schema_change_job(table_name, database_name)
    ret = client.verify(DATA.expected_data_file_list_delete_key_2_new_agg, table_name)
    assert ret
    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    setup_module()
    test_loading()



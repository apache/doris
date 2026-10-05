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
  * @file test_sys_partition_schema_change.py
  * @date 2015/02/04 15:26:21
  * @brief This file is a test file for Palo schema changing.
  * 
  **************************************************************************/
"""

import sys
import time
import random

sys.path.append("../")
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
        partition_value_list, distribution_type, bucket_num, storage_type):
    """
    检查，验证
    """
    partition_info = palo_client.PartitionInfo(column_name, \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo(distribution_type, bucket_num) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)


def check(table_name):
    """
    分区，检查
    """
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d', \
            'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['5', '30', '100', '500', '1000', '2000', 'MAXVALUE']
    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'HASH(k1, k2)', random.randrange(1, 30), 'column')


def test_delete_key():
    """
    {
    "title": "test_sys_partition_schema_change_delete_aggregate.test_delete_key",
    "describe": "功能点：有删除的版本基础上做schema change，新表包括delete key",
    "tag": "function,p1"
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
    partition_name_list = ['partition_a',]
    partition_value_list = ['MAXVALUE',]
    partition_info = palo_client.PartitionInfo('tinyint_key', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(tinyint_key)', 10) 
    ret = client.create_table(table_name, DATA.schema, partition_info, \
            distribution_info=distribution_info)
    assert ret

    ret = client.show_tables(table_name)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, max_filter_ratio='0.5', is_wait=True,
                            broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_delete_key_1, table_name, encoding='utf-8')
    assert ret

    ret = client.delete(table_name, \
            DATA.delete_condition_list, partition_name='partition_a')
    assert ret
    assert client.verify(DATA.expected_data_file_list_17, table_name)

    sql = 'ALTER TABLE %s.%s %s' % (database_name, table_name, \
          'ADD COLUMN add_int_key INT DEFAULT "65599" AFTER int_key,' \
          'ADD COLUMN add_smallint_key SMALLINT DEFAULT "1000" AFTER smallint_key,' \
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
    client.clean(database_name)
#   ret = client.verify(DATA.expected_data_file_list_delete_key_2, table_name)
#   assert ret


def test_no_delete_key():
    """
    {
    "title": "test_sys_partition_schema_change_delete_aggregate.test_no_delete_key",
    "describe": "功能点：有删除的版本基础上做schema change，新表不包括delete key",
    "tag": "function,p1"
    }
    """
    """
    功能点：有删除的版本基础上做schema change，新表不包括delete key
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    partition_name_list = ['partition_a',]
    partition_value_list = ['MAXVALUE',]
    partition_info = palo_client.PartitionInfo('int_key', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(character_most_key)', 10) 
    assert client.create_table(table_name, DATA.schema, partition_info, \
            distribution_info=distribution_info)
    assert ret

    ret = client.show_tables(table_name)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, max_filter_ratio='0.5', is_wait=True,
                            broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_delete_key_1, table_name, encoding='utf-8')
    assert ret

    ret = client.delete(table_name, \
            DATA.delete_condition_list, partition_name='partition_a')
    assert ret
    assert client.verify(DATA.expected_data_file_list_17, table_name)

    sql = 'ALTER TABLE %s.%s %s' % (database_name, table_name, \
          'ADD COLUMN add_int_key INT DEFAULT "65599" AFTER int_key,' \
          'ADD COLUMN add_smallint_key SMALLINT DEFAULT "1000" AFTER smallint_key,' \
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
    client.clean(database_name)
#   ret = client.verify(DATA.expected_data_file_list_delete_key_3, table_name)
#   assert ret


def test_schema_change_then_delete():
    """
    {
    "title": "test_sys_partition_schema_change_delete_aggregate.test_schema_change_then_delete",
    "describe": "功能点：schema change后delete",
    "tag": "function,p1"
    }
    """
    """
    功能点：schema change后delete
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    assert client.create_database(database_name)
    partition_name_list = ['partition_a', 'partition_b', \
            'partition_c', 'partition_d', 'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['-100', '-1', '0', '1', '500', '1000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('smallint_key', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(character_most_key)', 10) 
    assert client.create_table(table_name, DATA.schema, partition_info, \
            distribution_info=distribution_info)

    assert client.show_tables(table_name)

    assert client.drop_partition(table_name, 'partition_g')
    assert client.add_partition(table_name, 'partition_g', '32767', \
            distribute_type='HASH(character_most_key)', bucket_num=13)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, max_filter_ratio='0.5', is_wait=True,
                             broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_18, table_name, encoding='utf-8')

    sql = 'ALTER TABLE %s.%s %s' % (database_name, table_name, \
          'ADD COLUMN add_int_key INT DEFAULT "65599" AFTER int_key,' \
          'ADD COLUMN add_smallint_key SMALLINT DEFAULT "1000" AFTER smallint_key,' \
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
    client.clean(database_name)
#   client.wait_table_schema_change_job(table_name, database_name)
#   assert client.verify(DATA.expected_data_file_list_19, table_name)

#   assert client.add_partition(table_name, 'partition_h', 'MAXVALUE', \
#           distribute_type='RANDOM', bucket_num=13)
#   assert client.delete(table_name, \
#           [('smallint_key', '<', '550'), ('int_key', '>', '-1')], partition_name='partition_f')

#   assert client.verify(DATA.expected_data_file_list_20, table_name)


def test_delete_whlie_schema_change():
    """
    {
    "title": "test_sys_partition_schema_change_delete_aggregate.test_delete_whlie_schema_change",
    "describe": "功能点：schema change同时delete",
    "tag": "function,p1"
    }
    """
    """
    功能点：schema change同时delete
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    assert client.create_database(database_name)
    partition_name_list = ['partition_a', 'partition_b', \
            'partition_c', 'partition_d', 'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['-100', '-1', '0', '1', '500', '1000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('smallint_key', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(tinyint_key)', 10) 
    assert client.create_table(table_name, DATA.schema, partition_info, \
            distribution_info=distribution_info)

    assert client.show_tables(table_name)

    assert client.drop_partition(table_name, 'partition_g')
    assert client.add_partition(table_name, 'partition_g', '32767', \
            distribute_type='HASH(tinyint_key)', bucket_num=13)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, max_filter_ratio='0.5', is_wait=True, 
                             broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_18, table_name, encoding='utf-8')

    sql = 'ALTER TABLE %s.%s %s' % (database_name, table_name, \
          'ADD COLUMN add_int_key INT DEFAULT "65599" AFTER int_key,' \
          'ADD COLUMN add_smallint_key SMALLINT DEFAULT "1000" AFTER smallint_key,' \
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
    assert client.delete(table_name, [('smallint_key', '<', '550'), \
                ('int_key', '>', '-1')], partition_name='partition_f'), "Delete failed"
    sql = "select count(*) from %s.%s where smallint_key > 500 and smallint_key < 550 and int_key > -1" \
            % (database_name, table_name)
    assert client.execute(sql) == ((0,),), "Delete failed"

    client.wait_table_schema_change_job(table_name, database_name)
    assert client.verify(DATA.expected_data_file_list_21, table_name)
    client.clean(database_name)


def test_schema_change_to_partition_and_no_partition_aggregate_tables():
    """
    {
    "title": "test_sys_partition_schema_change_delete_aggregate.test_schema_change_to_partition_and_no_partition_aggregate_tables",
    "describe": "功能点：同一数据库中的复合分区表和单分区表分别进行schema change操作",
    "tag": "function,p1"
    }
    """
    """
    功能点：同一数据库中的复合分区表和单分区表分别进行schema change操作
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d', \
            'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['5', '30', '100', '500', '1000', '2000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('bigint_key', \
            partition_name_list, partition_value_list)
    table_name_1 = "%s_1" % table_name
    hash_distribution_info = palo_client.DistributionInfo(\
            distribution_type=DATA.hash_partition_type, \
            bucket_num=DATA.hash_partition_num)
    ret = client.create_table(table_name_1, DATA.schema, \
            partition_info, distribution_info=hash_distribution_info, \
            storage_type=DATA.storage_type)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name_1)
    label = table_name_1
    ret = client.batch_load(label, data_desc_list, max_filter_ratio="0.5", is_wait=True,
                            broker=broker_info)
    assert ret

    table_name_2 = "%s_2" % table_name
    distribution_info = palo_client.DistributionInfo('HASH(tinyint_key)', 10) 
    ret = client.create_table(table_name_2, DATA.schema, \
            distribution_info=distribution_info)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name_2)
    label = table_name_2
    ret = client.batch_load(label, data_desc_list, max_filter_ratio="0.5", is_wait=True,
                            broker=broker_info)
    assert ret
    timeout = 1200
    while client.get_unfinish_load_job_list() and timeout > 0:
        time.sleep(1)
        timeout -= 1
    assert client.schema_change_drop_column(table_name_1, DATA.drop_column_name_list_new)
    assert client.schema_change_drop_column(table_name_2, DATA.drop_column_name_list_new, \
            is_wait_job=True)
    client.wait_table_schema_change_job(table_name_1)
    new_column_name_list = list(set(DATA.column_name_list).difference(set(\
            DATA.drop_column_name_list))) 
    sql = "SELECT %s FROM %s.%s" % (", ".join(new_column_name_list), \
            database_name, table_name_1)

    data_1 = client.execute(sql)
    sql = "SELECT %s FROM %s.%s" % (", ".join(new_column_name_list), \
            database_name, table_name_1)
    data_2 = client.execute(sql)
    #assert data_1 == data_2
    client.clean(database_name)


def test_add_partition_while_schema_change():
    """
    {
    "title": "test_sys_partition_schema_change_delete_aggregate.test_add_partition_while_schema_change",
    "describe": "schema change同时增加分区",
    "tag": "function,p1,fuzz"
    }
    """
    """
    schema change同时增加分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d', \
            'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['5', '30', '100', '500', '1000', '5000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 13) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
 
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    column_list = [('k3', 'INT', None, '5')]
    ret = client.schema_change_add_column(table_name, column_list, after_column_name='k2')
    assert ret

    ret = False
    try:
        ret = client.drop_partition(table_name, 'partition_f')
    except:
        pass
    assert not ret
    try:
        ret = client.add_partition(table_name, 'partition_h', '5500')
    except:
        pass
    assert not ret
    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    import pdb
    pdb.set_trace()
    setup_module()
    test_delete_key()


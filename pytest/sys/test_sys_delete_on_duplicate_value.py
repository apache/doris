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
#   @file test_sys_delete_on_duplicate_value.py
#   @date 2020/05/21
#   @brief This file is a test file for delete on duplicate model
#
#############################################################################

"""
duplicate表，按value列删除数据
"""
import random

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
    client.create_table(table_name, DATA.schema_1_dup,
                        distribution_info=distribution_info, keys_desc=DATA.key_1_dup)
    assert client.show_tables(table_name)
    # check_partition_list(table_name, partition_name_list)


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


def check2_palo(line1, line2):
    """
    check2_palo
    :param ret1:
    :param ret2:
    :return:
    """
    ret1 = client.execute(line1)
    ret2 = client.execute(line2)
    util.check(ret1, ret2)


def test_delete_on_duplicate_value_basic():
    """
    {
    "title": "test_sys_delete_on_duplicate_value.test_delete_on_duplicate_value_basic",
    "describe": "验证duplicate类型表根据value删除数据功能",
    "tag": "function,P0"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    check(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    table_name_base = table_name[0:55] + '_base'
    check(table_name_base)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name_base)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name_base)
    assert ret

    condition = 'k1 > 1000'
    sql = 'DELETE FROM %s.%s WHERE %s' % (database_name, table_name, condition)
    util.assert_return(True, '', client.execute, sql)
    line1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s where k1 <= 1000 order by k1' % (database_name, table_name_base)
    check2_palo(line1, line2)

    condition = 'v2 > 1000'
    sql = 'DELETE FROM %s.%s WHERE %s' % (database_name, table_name, condition)
    util.assert_return(False, 'is not key column or storage model is not duplicate or column type is float or double',
                       client.execute, sql)
    line1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s where k1 <= 1000 order by k1' % (database_name, table_name_base)
    check2_palo(line1, line2)

    condition = 'v3 > 5000'
    sql = 'DELETE FROM %s.%s WHERE %s' % (database_name, table_name, condition)
    util.assert_return(True, '',
                       client.execute, sql)
    line1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s where k1 <= 1000 and v3 <= 5000 order by k1' % (database_name, table_name_base)
    check2_palo(line1, line2)

    column_list = [('k3', 'INT KEY', None, '5')]
    ret = client.schema_change_add_column(table_name, column_list,
                                          after_column_name='k1', is_wait_job=True, is_wait_delete_old_schema=True)
    assert ret

    line1 = 'select k1,k2,v1,v2,v3 from %s.%s order by k1' % (database_name, table_name)
    line2 = 'select k1,k2,v1,v2,v3 from %s.%s where k1 <= 1000 and v3 <= 5000 order by k1' % \
            (database_name, table_name_base)
    check2_palo(line1, line2)

    condition = 'v3 > 3000'
    sql = 'DELETE FROM %s.%s WHERE %s' % (database_name, table_name, condition)
    util.assert_return(True, '',
                       client.execute, sql)
    line1 = 'select k1,k2,v1,v2,v3 from %s.%s order by k1' % (database_name, table_name)
    line2 = 'select k1,k2,v1,v2,v3 from %s.%s where k1 <= 1000 and v3 <= 3000 order by k1' % \
            (database_name, table_name_base)
    check2_palo(line1, line2)

    client.clean(database_name)


def test_delete_on_duplicate_value_limit_value_type():
    """
    {
    "title": "test_sys_delete_on_duplicate_value.test_delete_on_duplicate_value_limit_value_type",
    "describe": "验证duplicate类型表根据value删除数据功能时对value类型的限制",
    "tag": "function,fuzz,P1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    check(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    table_name_base = table_name[0:55] + '_base'
    check(table_name_base)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name_base)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name_base)
    assert ret

    condition = 'v2 > 1000'
    sql = 'DELETE FROM %s.%s WHERE %s' % (database_name, table_name, condition)
    util.assert_return(False, 'is not key column or storage model is not duplicate or column type is float or double',
                       client.execute, sql)
    line1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s order by k1' % (database_name, table_name_base)
    check2_palo(line1, line2)

    client.clean(database_name)


def test_delete_on_duplicate_value_limit_rollup():
    """
    {
    "title": "test_sys_delete_on_duplicate_value.test_delete_on_duplicate_value_limit_rollup",
    "describe": "验证duplicate类型表根据value删除数据功能时，对rollup表列的限制",
    "tag": "function,fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    check(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    table_name_base = table_name[0:55] + '_base'
    check(table_name_base)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name_base)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name_base)
    assert ret

    condition = 'v2 > 1000'
    sql = 'DELETE FROM %s.%s WHERE %s' % (database_name, table_name, condition)
    util.assert_return(False, 'is not key column or storage model is not duplicate or column type is float or double',
                       client.execute, sql)
    line1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s order by k1' % (database_name, table_name_base)
    check2_palo(line1, line2)

    view_name_join_k1v3_g = 'join_k1v1v2'
    view_sql = 'select k1,v1,v2 from %s' % table_name
    client.create_materialized_view(table_name, view_name_join_k1v3_g,
                                    view_sql, database_name=database_name, is_wait=True)

    condition = 'v3 > 3000'
    sql = 'DELETE FROM %s.%s WHERE %s' % (database_name, table_name, condition)
    util.assert_return(False, 'Unknown column',
                       client.execute, sql)
    line1 = 'select k1,k2,v1,v2,v3 from %s.%s order by k1' % (database_name, table_name)
    line2 = 'select k1,k2,v1,v2,v3 from %s.%s order by k1' % \
            (database_name, table_name_base)
    check2_palo(line1, line2)

    condition = 'v1 = "wjzzvojftyxqinchvdtzwblqb"'
    sql = 'DELETE FROM %s.%s WHERE %s' % (database_name, table_name, condition)
    util.assert_return(True, '',
                       client.execute, sql)
    line1 = 'select k1,k2,v1,v2,v3 from %s.%s order by k1' % (database_name, table_name)
    line2 = 'select k1,k2,v1,v2,v3 from %s.%s where v1 != "wjzzvojftyxqinchvdtzwblqb" order by k1' % \
            (database_name, table_name_base)
    check2_palo(line1, line2)

    view_name_join_k1v3_g = 'join_k1v2_g'
    view_sql = 'select k1,sum(v2) from %s group by k1' % table_name
    client.create_materialized_view(table_name, view_name_join_k1v3_g,
                                    view_sql, database_name=database_name, is_wait=True)

    condition = 'v2 > 500'
    sql = 'DELETE FROM %s.%s WHERE %s' % (database_name, table_name, condition)
    util.assert_return(False, 'storage model is not duplicate',
                       client.execute, sql)
    line1 = 'select k1,k2,v1,v2,v3 from %s.%s order by k1' % (database_name, table_name)
    line2 = 'select k1,k2,v1,v2,v3 from %s.%s where v1 != "wjzzvojftyxqinchvdtzwblqb" order by k1' % \
            (database_name, table_name_base)
    check2_palo(line1, line2)

    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    # import pdb
    # pdb.set_trace()
    setup_module()
    print(broker_info)


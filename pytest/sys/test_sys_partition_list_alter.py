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
  * @file test_sys_partition_list_alter.py
  * @brief Test for alter list partition
  *
  **************************************************************************/
"""
import time
import pytest

from data import schema as DATA
from data import load_file as FILE
from lib import palo_client
from lib import util
from lib import palo_config
from lib import common
from lib import kafka_config
from lib import palo_job

LOG = palo_client.LOG
L = palo_client.L
config = palo_config.config
broker_name = config.broker_name
broker_info = palo_config.broker_info
TOPIC = 'single-partition-6-%s' % config.fe_query_port


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


def test_list_partition_load():
    """
    {
    "title": "",
    "describe": "list分区的导入，导入指定分区等，broker，stream，insert",
    "tag": "function,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [('-2147483647', '2147483647'), ('1001', '3021'), ('1002', '25699', '103', '5014', '1992')]
    partition_info = palo_client.PartitionInfo('k3', partiton_name, partition_value_list, partition_type='LIST')
    agg_table = table_name + '_agg'
    dup_table = table_name + '_dup'
    uniq_table = table_name + '_uniq'
    client = common.create_workspace(database_name)
    assert client.create_table(agg_table, DATA.baseall_column_list, partition_info=partition_info)
    assert client.create_table(dup_table, DATA.baseall_column_no_agg_list, partition_info=partition_info, 
                               keys_desc=DATA.baseall_duplicate_key)
    assert client.create_table(uniq_table, DATA.baseall_column_no_agg_list, partition_info=partition_info, 
                               keys_desc=DATA.baseall_unique_key)
    # broker
    load_data_info = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, dup_table, partition_list=['p2', 'p3'])
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True, max_filter_ratio=0)
    assert not ret
    load_data_info = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, dup_table, partition_list=['p2', 'p3'])
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True, max_filter_ratio=0.4)
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, dup_table)
    sql2 = 'select * from %s.baseall where k3 not in (2147483647, -2147483647) order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    # stream
    try:
        client.enable_feature_batch_delete(uniq_table)
    except Exception as e:
        pass
    ret = client.stream_load(uniq_table, FILE.baseall_local_file, partition_list=['p1', 'p2', 'p3'])
    assert ret
    ret = client.stream_load(uniq_table, FILE.baseall_local_file, partition_list=['p2', 'p3'],
                             max_filter_ratio=0.4, merge_type='DELETE')
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, uniq_table)
    sql2 = 'select * from %s.baseall where k3 in (2147483647, -2147483647) order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    # insert
    ret = client.execute('insert into %s select * from %s.baseall' % (agg_table, config.palo_db))
    assert ret == ()
    sql1 = 'select * from %s.%s order by k1' % (database_name, agg_table)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_list_partition_routine_load():
    """
    {
    "title": "",
    "describe": "list分区的导入，导入指定分区等,routine load",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [('-2147483647', '2147483647'), ('1001', '3021'), ('1002', '25699', '103', '5014', '1992')]
    partition_info = palo_client.PartitionInfo('k3', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_list, partition_info=partition_info)
    # create routine load
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_partitions(['p2', 'p3'])
    routine_load_property.set_max_error_number(4)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    state = client.get_routine_load_state(routine_load_name)
    ret = (state == 'RUNNING')
    common.assert_stop_routine_load(ret, client, routine_load_name, 'cat not get routine load running')
    # send data to kafka topic
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    # defualt maxBatchIntervalS is 10
    common.wait_commit(client, routine_load_name, 11)
    # stop routine load
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k3 not in (2147483647, -2147483647) order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_list_partition_add():
    """
    {
    "title": "",
    "describe": "增加分区，新增分区与已有分区overlap，duplicate，invalid list value，key数量不匹配",
    "tag": "function,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p2', 'p3']
    partition_value_list = [('1001', '3021'), ('1002', '25699', '103', '5014', '1992')]
    partition_info = palo_client.PartitionInfo('k3', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_no_agg_list, partition_info=partition_info)
    load_data_info = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, partition_list=['p2', 'p3'])
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True, max_filter_ratio=0.4)
    assert ret
    ret = client.add_partition(table_name, 'p1', ('2147483647', '-2147483647'),
                               storage_medium='HDD', partition_type='LIST')
    assert ret
    ret = client.show_partitions(table_name)
    p_name = util.get_attr_condition_value(ret, palo_job.PartitionInfo.PartitionName, 'p1')
    assert p_name
    load_data_info = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, partition_list=['p1'])
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True, max_filter_ratio=0.8)
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    util.assert_return(False, ' conflict with current partitionKeys', client.add_partition, table_name,
                       'p_f', ('1002', '0'), partition_type='LIST')
    util.assert_return(False, 'conflict with current partitionKeys', client.add_partition,
                       table_name, 'p_f', ('1002', '0'), partition_type='LIST')
    util.assert_return(False, 'Duplicate partition name', client.add_partition,
                       table_name, 'p1', ('2', '0'), partition_type='LIST')
    util.assert_return(False, 'Invalid list value format', client.add_partition,
                       table_name, 'p_f', ('2.3', '0'), partition_type='LIST')
    util.assert_return(True, None, client.add_partition, table_name, 'p4', (('2',), ('0',)), partition_type='LIST')
    msg = 'partition key desc list size[2] is not equal to partition column size[1]'
    util.assert_return(False, msg, client.add_partition, table_name, 'p_f', (('2', '1'), ('0',)), partition_type='LIST')
    client.clean(database_name)


def test_list_partition_drop():
    """
    {
    "title": "",
    "describe": "删除分区，重新创建分区，删除不存在的分区",
    "tag": "function,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p1', 'p2', 'p3', 'p4']
    partition_value_list = [('1001', '3021'), ('1002', '25699', '103', '5014', '1992'),
                            ('0'), ('2147483647', '-2147483647')]
    partition_info = palo_client.PartitionInfo('k3', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_no_agg_list, partition_info=partition_info)
    load_data_info = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.drop_partition(table_name, 'p3')
    assert ret
    ret = client.drop_partition(table_name, 'p4')
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k3 not in (2147483647, -2147483647) order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.show_partitions(table_name)
    assert util.get_attr_condition_value(ret, palo_job.PartitionInfo.PartitionName, 'p3') is None
    assert util.get_attr_condition_value(ret, palo_job.PartitionInfo.PartitionName, 'p4') is None
    ret = client.add_partition(table_name, 'p3', ('2147483647', '-2147483647'), partition_type='LIST')
    assert ret 
    ret = client.show_partitions(table_name)
    assert util.get_attr_condition_value(ret, palo_job.PartitionInfo.PartitionName, 'p3')
    util.assert_return(False, 'Error in list of partitions to p_f', client.drop_partition, table_name, 'p_f')
    client.clean(database_name)


def test_list_partition_rename():
    """
    {
    "title": "",
    "describe": "rename partition，再创建分区，再删除分区",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p0']
    partition_value_list = [('1', )]
    partition_info = palo_client.PartitionInfo('k1', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_no_agg_list, partition_info=partition_info)
    ret = client.add_partition(table_name, 'p1', util.gen_tuple_num_str(2, 16), partition_type='LIST')
    assert ret
    assert client.rename_partition('p_new', 'p0', table_name)
    ret = client.show_partitions(table_name)
    assert util.get_attr_condition_value(ret, palo_job.PartitionInfo.PartitionName, 'p_new')
    load_data_info = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.add_partition(table_name, 'p0', ('0'), partition_type='LIST')
    assert ret
    ret = client.drop_partition(table_name, 'p1')
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k1 = 1 order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_list_partition_modify():
    """
    {
    "title": "",
    "describe": "修改分区属性",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p0']
    partition_value_list = [('1',)]
    partition_info = palo_client.PartitionInfo('k1', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_no_agg_list, partition_info=partition_info)
    ret = client.add_partition(table_name, 'p1', util.gen_tuple_num_str(2, 16), 
                               storage_medium='SSD', partition_type='LIST')
    assert ret
    load_data_info = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.modify_partition(table_name, 'p1', replication_num=2, storage_medium='HDD')
    assert ret
    ret = client.show_partitions(table_name)
    assert '2' == util.get_attr_condition_value(ret, palo_job.PartitionInfo.PartitionName,
                                                'p1', palo_job.PartitionInfo.ReplicationNum)
    assert 'HDD' == util.get_attr_condition_value(ret, palo_job.PartitionInfo.PartitionName,
                                                  'p1', palo_job.PartitionInfo.StorageMedium)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_list_partition_alter_partition_col():
    """
    {
    "title": "",
    "describe": "修改分区列属性",
    "tag": "function,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [util.gen_tuple_num_str(1, 5), util.gen_tuple_num_str(5, 15), 
                            util.gen_tuple_num_str(15, 16)]
    partition_info = palo_client.PartitionInfo('k1', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_no_agg_list, partition_info=partition_info)
    msg = 'Can not modify partition column'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'k1', 'int', 
                       is_wait_job=True, column_info='key')
    client.clean(database_name)


def test_list_partition_temp():
    """
    {
    "title": "",
    "describe": "list临时分区操作，创建临时分区，导入，查询，替换",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [util.gen_tuple_num_str(1, 5), util.gen_tuple_num_str(5, 16),
                            util.gen_tuple_num_str(16, 20)]
    partition_info = palo_client.PartitionInfo('k1', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_no_agg_list, partition_info=partition_info)
    client.add_temp_partition(table_name, 'tp1', util.gen_tuple_num_str(5, 10), partition_type='LIST')
    client.add_temp_partition(table_name, 'tp2', util.gen_tuple_num_str(10, 16), partition_type='LIST')
    assert client.stream_load(table_name, FILE.baseall_local_file)
    assert client.stream_load(table_name, FILE.baseall_local_file, max_filter_ratio=0.5, temporary_partitions='tp1,tp2')
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select * from %s.%s temporary partitions(tp1, tp2) order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k1 >= 5 order by k1' % config.palo_db
    common.check2(client, sql1, sql2=sql2)
    msg = '2 partition key lists are not matched.'
    util.assert_return(False, msg, client.modify_temp_partition, database_name, 
                       table_name, ['p2', 'p3'], ['tp1', 'tp2'])
    ret = client.modify_temp_partition(database_name, table_name, ['p2', 'p3'], ['tp1', 'tp2'], strict_range='False')
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_list_partition_rollup():
    """
    {
    "title": "",
    "describe": "创建list分区的rollup，导入，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [util.gen_tuple_num_str(1, 5), util.gen_tuple_num_str(5, 16),
                            util.gen_tuple_num_str(16, 20)]
    partition_info = palo_client.PartitionInfo('k1', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_list, partition_info=partition_info)
    assert client.stream_load(table_name, FILE.baseall_local_file)
    assert client.create_rollup_table(table_name, index_name, ['k6', 'k9'], is_wait=True)
    sql1 = 'select sum(k9) from %s.%s group by k6 order by 1' % (database_name, table_name)
    sql2 = 'select sum(k9) from %s.baseall group by k6 order by 1' % config.palo_db
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_list_partition_backup_restore():
    """
    {
    "title": "",
    "describe": "list分区的备份恢复",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [util.gen_tuple_num_str(1, 5), util.gen_tuple_num_str(5, 16),
                            util.gen_tuple_num_str(16, 20)]
    partition_info = palo_client.PartitionInfo('k1', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_list, partition_info=partition_info)
    assert client.stream_load(table_name, FILE.baseall_local_file)
    assert client.create_rollup_table(table_name, index_name, ['k6', 'k9'], is_wait=True)
    # backup
    ret = client.get_repository()
    if len(ret) == 0:
        repo_name = 'repo_restore'
        client.create_repository(repo_name, broker_name, repo_location=config.repo_location,
                                       repo_properties=config.broker_property)
    else:
        repo_name = ret[-1]
    snapshot_label = util.get_snapshot_label('list_p')
    ret = client.backup(snapshot_label, ['%s PARTITION(p1, p2)' % table_name], repo_name=repo_name, is_wait=True)
    assert ret
    # restore
    restore_list = list()
    new_table_name = 'new_' + table_name
    restore_list.append('%s AS %s' % (table_name, new_table_name))
    ret = client.restore(snapshot_label, repo_name, restore_list, is_wait=True)
    assert ret
    # check
    assert client.get_partition(new_table_name, 'p1')
    assert client.get_partition(new_table_name, 'p2')
    assert not client.get_partition(new_table_name, 'p3')
    sql1 = 'select * from %s order by k1' % table_name
    sql2 = 'select * from %s order by k1' % new_table_name
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_list_partition_export():
    """
    {
    "title": "",
    "describe": "list分区的export",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [util.gen_tuple_num_str(1, 5), util.gen_tuple_num_str(5, 16),
                            util.gen_tuple_num_str(16, 20)]
    partition_info = palo_client.PartitionInfo('k1', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_list, partition_info=partition_info)
    assert client.stream_load(table_name, FILE.baseall_local_file)
    ret = client.export(table_name, FILE.export_to_hdfs_path, partition_name_list=['p1', 'p3'], 
                        broker_info=broker_info)
    assert ret
    client.wait_export(database_name)
    ret = client.show_export(state='FINISHED')
    assert len(ret) == 1
    client.clean(database_name)


def test_list_partition_drop_recover():
    """
    {
    "title": "",
    "describe": "list分区的drop和recover操作",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [util.gen_tuple_num_str(1, 5), util.gen_tuple_num_str(5, 16),
                            util.gen_tuple_num_str(16, 20)]
    partition_info = palo_client.PartitionInfo('k1', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_list, partition_info=partition_info)
    assert client.stream_load(table_name, FILE.baseall_local_file)
    ret = client.drop_partition(table_name, 'p2')
    assert ret
    ret = client.select_all(table_name)
    assert len(ret) == 4
    time.sleep(10)
    ret = client.recover_partition(table_name, 'p2')
    assert ret
    ret = client.select_all(table_name)
    assert len(ret) == 15
    client.clean(database_name)


def test_list_partition_delete():
    """
    {
    "title": "",
    "describe": "list分区的delete",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [util.gen_tuple_num_str(1, 5), util.gen_tuple_num_str(5, 16),
                            util.gen_tuple_num_str(16, 20)]
    partition_info = palo_client.PartitionInfo('k1', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_list, partition_info=partition_info)
    assert client.stream_load(table_name, FILE.baseall_local_file)
    msg = ''
    # where include partition column
    util.assert_return(True, ' ', client.delete, table_name, [("k1", ">", "3"), ("k1", "<=", "13")])
    ret = client.delete(table_name, [("k1", ">", "3"), ("k1", "<=", "13")], '(p2, p3)')
    assert ret
    sql1 = 'select * from %s.%s order by 1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k1 < 4 or k1 > 13 order by 1' % config.palo_db
    common.check2(client, sql1, sql2=sql2)
    # where not include partition column
    util.assert_return(False, ' ', client.delete, table_name, [("k2", ">", "3"), ("k2", "<=", "13")])
    ret = client.delete(table_name, [("k2", ">", "3"), ("k2", "<=", "13")], '(p2, p3)')
    assert ret
    client.set_variables('delete_without_partition', 'true')
    ret = client.delete(table_name, [("k2", ">", "3"), ("k2", "<=", "13")])
    assert ret
    sql1 = 'select * from %s.%s order by 1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k1 < 4 or k1 > 13 order by 1' % config.palo_db
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_list_partition_truncate():
    """
    {
    "title": "",
    "describe": "list分区的truncate",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p1', 'p2', 'p3']
    partition_value_list = [util.gen_tuple_num_str(1, 5), util.gen_tuple_num_str(5, 16),
                            util.gen_tuple_num_str(16, 20)]
    partition_info = palo_client.PartitionInfo('k1', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_list, partition_info=partition_info)
    assert client.stream_load(table_name, FILE.baseall_local_file)
    assert client.truncate(table_name, partition_list=['p1'])
    sql1 = 'select * from %s.%s order by 1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k1 > 4 order by 1' % config.palo_db
    common.check2(client, sql1, sql2=sql2)
    assert client.truncate(table_name)
    ret = client.select_all(table_name)
    assert len(ret) == 0
    client.clean(database_name)


def test_list_partition_select():
    """
    {
    "title": "",
    "describe": "list分区裁剪，优化查询，主要包含=，in，>, <等",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))

    partiton_name = ['p1', 'p2', 'p3', 'p4']
    partition_value_list = [util.gen_tuple_num_str(-128, -64), util.gen_tuple_num_str(-64, 0),
                            util.gen_tuple_num_str(0, 64), util.gen_tuple_num_str(64, 128)]
    partition_info = palo_client.PartitionInfo('k1', partiton_name, partition_value_list, partition_type='LIST')
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.baseall_column_list, partition_info=partition_info)
    assert client.stream_load(table_name, FILE.baseall_local_file)
    sql = 'select * from %s.%s where k1 > 5 order by k1'
    common.check2(client, sql % (database_name, table_name), 
                  sql2=sql % (config.palo_db, 'baseall'))
    sql = 'select * from %s.%s where k1 in (5, 6, 1) order by k1'
    common.check2(client, sql % (database_name, table_name), 
                  sql2=sql % (config.palo_db, 'baseall'))
    sql = 'select * from %s.%s where k1 between 3 and 13 order by k1'
    common.check2(client, sql % (database_name, table_name),
                  sql2=sql % (config.palo_db, 'baseall'))
    sql = 'select * from %s.%s where k1 != 10 order by k1'
    common.check2(client, sql % (database_name, table_name),
                  sql2=sql % (config.palo_db, 'baseall'))
    sql = 'select * from %s.%s where k1 not in (1, 2, 3, 100) order by k1'
    common.check2(client, sql % (database_name, table_name),
                  sql2=sql % (config.palo_db, 'baseall'))
    sql = 'select * from %s.%s where k1 is null order by k1'
    common.check2(client, sql % (database_name, table_name),
                  sql2=sql % (config.palo_db, 'baseall'))


if __name__ == '__main__':
    setup_module()
    test_list_partition_select()

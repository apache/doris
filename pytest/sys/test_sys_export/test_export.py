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
#   @file test_sys_export.py
#   @date 2019-12-02 15:13:01
#   @brief This file is a test file for export
#
#############################################################################

"""
test export
"""

import time
import pytest

from data import schema as DATA
from data import load_file as HDFS_PATH
from lib import palo_config
from lib import palo_client
from lib import util
from lib import palo_job

LOG = palo_client.LOG
L = palo_client.L
config = palo_config.config
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    pass


def create_workspace(database_name):
    """create db return client"""
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    return client


def test_export_partition():
    """
    {
    "title": "test_sys_export.test_export_partition",
    "describe": "export table data",
    "tag": "function,p1"
    }
    """
    """export table partition data"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info,
                              partition_info=DATA.baseall_tinyint_partition_info,
                              set_null=False)
    assert ret
    load_data_desc = palo_client.LoadDataInfo(HDFS_PATH.test_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, is_wait=True, broker=broker_info)
    assert ret
    ret = client.export(table_name, HDFS_PATH.export_to_hdfs_path, partition_name_list=['p1', 'p2', 'p3'], broker_info=broker_info)
    assert ret
    client.wait_export()
    ret = client.show_export(state="FINISHED")
    assert len(ret) == 1
    client.clean(database_name)


def test_export_table():
    """
    {
    "title": "test_sys_export.test_export_table",
    "describe": "export table data",
    "tag": "function,p1"
    }
    """
    """export table data"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info,
                              set_null=True)
    assert ret
    load_data_desc = palo_client.LoadDataInfo(HDFS_PATH.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, is_wait=True, broker=broker_info)
    assert ret
    ret = client.export(table_name, HDFS_PATH.export_to_hdfs_path, broker_info=broker_info)
    assert ret
    client.wait_export()
    ret = client.show_export(state="FINISHED")
    assert len(ret) == 1
    client.clean(database_name)


@pytest.mark.skip()
def test_export_property_timeout():
    """
    {
    "title": "test_sys_export.test_export_property_timeout",
    "describe": "test export property timeout",
    "tag": "autotest"
    }
    """
    """
    test export property timeout
    this is a bug https://github.com/apache/incubator-doris/issues/2788 
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info,
                              partition_info=DATA.baseall_tinyint_partition_info,
                              set_null=True)
    assert ret
    load_data_desc = palo_client.LoadDataInfo(HDFS_PATH.test_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, is_wait=True, broker=broker_info)
    assert ret
    ret = client.export(table_name, HDFS_PATH.export_to_hdfs_path, broker_info=broker_info, 
                        property_dict={"timeout":"5"})
    assert ret
    client.wait_export()
    time.sleep(10)
    ret = client.show_export(state="CANCELLED")
    assert len(ret) == 1
    timeout = palo_job.ExportJob(ret[0]).get_timeout()
    assert timeout == '5'
    client.clean(database_name)


def test_export_property_separator():
    """
    {
    "title": "test_sys_export.test_export_property_separator",
    "describe": "test export property column separator and line delimiter",
    "tag": "function,p1"
    }
    """
    """test export property column separator and line delimiter"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info,
                              partition_info=DATA.baseall_tinyint_partition_info,
                              set_null=True)
    assert ret
    load_data_desc = palo_client.LoadDataInfo(HDFS_PATH.test_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, is_wait=True, broker=broker_info)
    assert ret
    ret = client.export(table_name, HDFS_PATH.export_to_hdfs_path, broker_info=broker_info,
                        property_dict={"timeout": "300", "column_separator": "|", "line_delimiter": "\n"})
    assert ret
    client.wait_export()
    time.sleep(10)
    ret = client.show_export(state='FINISHED')
    assert len(ret) == 1
    export_job = palo_job.ExportJob(ret[0])
    timeout = export_job.get_timeout()
    assert timeout == '300'
    column_separator = export_job.get_column_separator()
    print(column_separator)
    assert column_separator == "|", column_separator
    line_delimiter = export_job.get_line_delimiter()
    assert line_delimiter == "\n", line_delimiter
    client.clean(database_name)


def test_export_property_exec_mem_limit():
    """
    {
    "title": "test_sys_export.test_export_property_exec_mem_limit",
    "describe": "test export property execute memory limit",
    "tag": "function,p1"
    }
    """
    """test export property execute memory limit"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=palo_client.DistributionInfo('HASH(k1)', 20),
                              partition_info=DATA.baseall_tinyint_partition_info,
                              set_null=True)
    assert ret
    load_data_desc = palo_client.LoadDataInfo(HDFS_PATH.test_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, is_wait=True, broker=broker_info)
    assert ret
    ret = client.export(table_name, HDFS_PATH.export_to_hdfs_path, broker_info=broker_info,
                        property_dict={"exec_mem_limit": "1073741824", "tablet_num_per_task": "10"})
    assert ret
    client.wait_export()
    time.sleep(10)
    ret = client.show_export(state='FINISHED')
    assert len(ret) == 1
    export_job = palo_job.ExportJob(ret[0])
    exec_mem_limit = export_job.get_exec_mem_limit()
    assert exec_mem_limit == 1073741824, exec_mem_limit
    tablet_num = export_job.get_tablet_num()
    assert tablet_num == 100, tablet_num
    coord_num = export_job.get_coord_num()
    assert coord_num == tablet_num / 10, coord_num
    client.clean(database_name)


def test_export_label():
    """
    {
    "title": "test_sys_export.test_export_label",
    "describe": "test export label, github issue 6835",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=palo_client.DistributionInfo('HASH(k1)', 20),
                              partition_info=DATA.baseall_tinyint_partition_info,
                              set_null=True)
    assert ret
    load_data_desc = palo_client.LoadDataInfo(HDFS_PATH.test_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, is_wait=True, broker=broker_info)
    assert ret
    label = util.get_label()
    ret = client.export(table_name, HDFS_PATH.export_to_hdfs_path, broker_info=broker_info,
                        property_dict={"label": label})
    assert ret
    client.wait_export()
    ret = client.show_export(state="FINISHED")
    assert len(ret) == 1
    export_job = palo_job.ExportJob(ret[0])
    export_label = export_job.get_label()
    assert export_label == label, 'export label is error'
    client.clean(database_name)


def test_export_where():
    """
    {
    "title": "test_sys_export.test_export_where",
    "describe": "test export where, github 5445",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=palo_client.DistributionInfo('HASH(k1)', 20),
                              partition_info=DATA.baseall_tinyint_partition_info,
                              set_null=True)
    assert ret
    load_data_desc = palo_client.LoadDataInfo(HDFS_PATH.test_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, is_wait=True, broker=broker_info)
    assert ret
    ret = client.export(table_name, HDFS_PATH.export_to_hdfs_path, broker_info=broker_info,
                        where='k1 > 0')
    assert ret
    client.wait_export()
    ret = client.show_export(state="FINISHED")
    assert len(ret) == 1, 'expect export finished'
    client.clean(database_name)


def test_select_when_export():
    """
    {
    "title": "test_sys_export.test_select_when_export",
    "describe": "test select table when export job running",
    "tag": "system,p1"
    }
    """
    """test select table when export job running"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=palo_client.DistributionInfo('HASH(k1)', 20),
                              partition_info=DATA.baseall_tinyint_partition_info,
                              set_null=True)
    assert ret
    load_data_desc = palo_client.LoadDataInfo(HDFS_PATH.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, is_wait=True, broker=broker_info)
    assert ret
    ret = client.export(table_name, HDFS_PATH.export_to_hdfs_path, broker_info=broker_info)
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_load_when_export():
    """
    {
    "title": "test_sys_export.test_load_when_export",
    "describe": "test load when export job running",
    "tag": "system,stability,p1"
    }
    """
    """test load when export job running"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=palo_client.DistributionInfo('HASH(k1)', 20),
                              partition_info=DATA.baseall_tinyint_partition_info,
                              set_null=True)
    assert ret
    load_data_desc = palo_client.LoadDataInfo(HDFS_PATH.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, is_wait=True, broker=broker_info)
    assert ret
    ret = client.export(table_name, HDFS_PATH.export_to_hdfs_path, broker_info=broker_info)
    assert ret
    ret = client.batch_load(util.get_label(), load_data_desc, is_wait=True, broker=broker_info)
    assert ret
    client.wait_export()
    ret = client.show_export(state="FINISHED")
    assert len(ret) == 1    
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9*2 from %s.baseall order by k1' % config.palo_db
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_sc_when_export():
    """
    {
    "title": "test_sys_export.test_sc_when_export",
    "describe": "test schema change when export job running",
    "tag": "system,stability,p1"
    }
    """
    """test schema change when export job running"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=palo_client.DistributionInfo('HASH(k1)', 20),
                              partition_info=DATA.baseall_tinyint_partition_info,
                              set_null=True)
    assert ret
    load_data_desc = palo_client.LoadDataInfo(HDFS_PATH.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, is_wait=True, broker=broker_info)
    assert ret
    ret = client.export(table_name, HDFS_PATH.export_to_hdfs_path, broker_info=broker_info)
    assert ret
    ret = client.drop_partition(table_name, "p1")
    assert ret
    client.wait_export()
    ret = client.show_export(state="FINISHED")
    assert len(ret) == 1
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9 from %s.baseall order by k1' % config.palo_db
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_rollup_when_export():
    """
    {
    "title": "test_sys_export.test_rollup_when_export",
    "describe": "test rollup when export job running",
    "tag": "system,p1,stability"
    }
    """
    """test rollup when export job running"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=palo_client.DistributionInfo('HASH(k1)', 20),
                              partition_info=DATA.baseall_tinyint_partition_info,
                              set_null=True)
    assert ret
    load_data_desc = palo_client.LoadDataInfo(HDFS_PATH.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, is_wait=True, broker=broker_info)
    assert ret
    ret = client.export(table_name, HDFS_PATH.export_to_hdfs_path, broker_info=broker_info)
    assert ret
    ret = client.create_rollup_table(table_name, index_name, ["k2", "k3", "k8", "k9"], is_wait=True)
    assert ret
    client.wait_export()
    ret = client.show_export(state="FINISHED")
    assert len(ret) == 1
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_delete_when_export():
    """
    {
    "title": "test_sys_export.test_delete_when_export",
    "describe": "test delete when export job running",
    "tag": "system,p1,stability"
    }
    """
    """test delete when export job running"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=palo_client.DistributionInfo('HASH(k1)', 20),
                              partition_info=DATA.baseall_tinyint_partition_info,
                              set_null=True)
    assert ret
    load_data_desc = palo_client.LoadDataInfo(HDFS_PATH.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, is_wait=True, broker=broker_info)
    assert ret
    ret = client.export(table_name, HDFS_PATH.export_to_hdfs_path, broker_info=broker_info)
    assert ret
    ret = client.delete(table_name, [('k1', '=', '3')], 'p3')
    assert ret
    client.wait_export()
    ret = client.show_export(state="FINISHED")
    assert len(ret) == 1
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k1 != 3 order by k1' % config.palo_db
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)
    client.clean(database_name)


def teardown_module():
    """tear down"""
    pass


if __name__ == '__main__':
    setup_module()
    test_export_table()
    test_export_partition()
    # test_export_property_timeout()
    # test_export_property_separator()
    # test_export_property_exec_mem_limit()


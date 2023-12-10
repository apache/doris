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
file:test_broker_load_where.py
测试broker load的where过滤。
where过滤的数据属于unselect数据，不记入ratio的计算
"""
import os
import sys
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

from data import schema as DATA
from data import load_file as FILE
from lib import palo_config
from lib import palo_client
from lib import util
from lib import palo_job

config = palo_config.config
broker_info = palo_config.broker_info


def setup_module():
    """
    init config
    """
    global query_db
    if 'FE_DB' in os.environ.keys():
        query_db = os.environ["FE_DB"]
    else:
        query_db = "test_query_qa"


def create_workspace(database_name):
    """get client and create db"""
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)

    client.init()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    return client


def check2(client, sql1, sql2):
    """check 2 sql same result"""
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)


def test_broker_where_and():
    """
    {
    "title": "test_broker_load_where.test_broker_where_and",
    "describe": "where中使用And",
    "tag": "function,p1"
    }
    """
    """
    where中使用And
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k10', 'k11', 'k7', 'k8', 'k9']
    where_clause = 'k1 > 0 and k1 > k2 and k3 > 0 and k4 > 0 and k5 != 0 and k6 < "zsedgwsf" \
                    and k10 >= "1970-01-01" and k11 < "9999-12-31"'
    data_desc_list = palo_client.LoadDataInfo([FILE.baseall_hdfs_file, FILE.test_hdfs_file], table_name, 
                                              column_name_list=column_name_list, 
                                              where_clause=where_clause)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    sql1 = 'select * from %s.%s order by k1, k2, k3, k4, k5' % (database_name, table_name)
    sql2 = 'select * from %s.test where k1 > 0 and k1 > k2 and k3 > 0 and k4 > 0 \
            and k5 != 0 and k6 < "zsedgwsf" and k10 >= "1970-01-01" and \
            k11 < "9999-12-31" order by k1, k2, k3, k4, k5' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_broker_where_or():
    """
    {
    "title": "test_broker_load_where.test_broker_where_or",
    "describe": "where中使用or",
    "tag": "function,p1"
    }
    """
    """
    where中使用or
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    where_clause = 'k1 > 0 or k1 > k2 or k3 > 0 and k4 > 0 and k5 != 0 or k6 < "zsedgwsf" \
                    and k10 >= "1999-01-01" or k11 < "2000-12-31"'
    data_desc_list = palo_client.LoadDataInfo([FILE.baseall_hdfs_file, FILE.test_hdfs_file], table_name,
                                              where_clause=where_clause)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    sql1 = 'select * from %s.%s order by k1, k2, k3, k4, k5' % (database_name, table_name)
    sql2 = 'select * from %s.test where k1 > 0 or k1 > k2 or k3 > 0 and k4 > 0 and k5 != 0 or \
            k6 < "zsedgwsf" and k10 >= "1999-01-01" or k11 < "2000-12-31" \
            order by k1, k2, k3, k4, k5' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_broker_where_between():
    """
    {
    "title": "test_broker_load_where.test_broker_where_between",
    "describe": "where中使用between and",
    "tag": "function,p1"
    }
    """
    """
    where中使用between and
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    where_clause = 'k1 between 0 and 10 or k2 between -10 and 10 and \
                    k10 between "1980-01-01" and "2000-01-01"'
    data_desc_list = palo_client.LoadDataInfo([FILE.baseall_hdfs_file, FILE.test_hdfs_file], table_name,
                                              where_clause=where_clause)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    sql1 = 'select * from %s.%s order by k1, k2, k3, k4, k5' % (database_name, table_name)
    sql2 = 'select * from %s.test where k1 between 0 and 10 or k2 between -10 and 10 and \
            k10 between "1980-01-01" and "2000-01-01" \
            order by k1, k2, k3, k4, k5' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_broker_where_in():
    """
    {
    "title": "test_broker_load_where.test_broker_where_in",
    "describe": "where中使用in",
    "tag": "function,p1"
    }
    """
    """
    where中使用in
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    where_clause = 'k1 in (1,2,3,4,5,6) and k10 in ("1969-06-04", "1922-01-27") and k6 in ("j6vOm", "U2@kv")'
    data_desc_list = palo_client.LoadDataInfo([FILE.baseall_hdfs_file, FILE.test_hdfs_file], table_name,
                                              where_clause=where_clause)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    sql1 = 'select * from %s.%s order by k1, k2, k3, k4, k5' % (database_name, table_name)
    sql2 = 'select * from %s.test where k1 in (1,2,3,4,5,6) and \
            k10 in ("1969-06-04", "1922-01-27") and k6 in ("j6vOm", "U2@kv") \
            order by k1, k2, k3, k4, k5' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_broker_where_like():
    """
    {
    "title": "test_broker_load_where.test_broker_where_like",
    "describe": "where中使用like谓词",
    "tag": "function,p1"
    }
    """
    """
    where中使用like谓词
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    where_clause = 'k6 like "true%" or k6 like "fa_se"'
    data_desc_list = palo_client.LoadDataInfo([FILE.baseall_hdfs_file, FILE.test_hdfs_file], table_name,
                                              where_clause=where_clause)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    sql1 = 'select * from %s.%s order by k1, k2, k3, k4, k5' % (database_name, table_name)
    sql2 = 'select * from %s.test where k6 like "true%%" or k6 like "fa_se" \
            order by k1, k2, k3, k4, k5' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_broker_where_not():
    """
    {
    "title": "test_broker_load_where.test_broker_where_not",
    "describe": "where中使用not",
    "tag": "function,p1"
    }
    """
    """
    where中使用not
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    where_clause = 'k6 not like "true%" and k1 not in (1,2,3,4,6,7) '
    data_desc_list = palo_client.LoadDataInfo([FILE.baseall_hdfs_file, FILE.test_hdfs_file], table_name,
                                              where_clause=where_clause)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    sql1 = 'select * from %s.%s order by k1, k2, k3, k4, k5' % (database_name, table_name)
    sql2 = 'select * from %s.test where k6 not like "true%%" and k1 not in (1,2,3,4,6,7) \
            order by k1, k2, k3, k4, k5' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_broker_where_cmp():
    """
    {
    "title": "test_broker_load_where.test_broker_where_cmp",
    "describe": "where的比较",
    "tag": "function,p1"
    }
    """
    """
    where的比较
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    where_clause = 'k5 >= 0 and k5 < 12.34 or k5 >= 1243.5 and k5 != 0'
    data_desc_list = palo_client.LoadDataInfo([FILE.baseall_hdfs_file, FILE.test_hdfs_file], table_name,
                                              where_clause=where_clause)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    sql1 = 'select * from %s.%s order by k1, k2, k3, k4, k5' % (database_name, table_name)
    sql2 = 'select * from %s.test where k5 >= 0 and k5 < 12.34 or k5 >= 1243.5 and k5 != 0 \
            order by k1, k2, k3, k4, k5' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_broker_where_null():
    """
    {
    "title": "test_broker_load_where.test_broker_where_null",
    "describe": "where中的Null值过滤",
    "tag": "function,p1,fuzz"
    }
    """
    """
    where中的Null值过滤
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    where_clause = 'k5 is not null'
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k10', 'k11', 'k7', 'k8', 'k9', 
                        'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9']
    data_desc_list = palo_client.LoadDataInfo(FILE.all_null_data_hdfs_file, table_name,
                                              column_name_list=column_name_list,
                                              where_clause=where_clause)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret, "expect batch load success"
    sql1 = 'select * from %s.%s order by k1, k2, k3, k4, k5' % (database_name, table_name)
    sql2 = 'select * from %s.test where k5 is null' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_broker_where_null_1():
    """
    {
    "title": "test_broker_load_where.test_broker_where_null_1",
    "describe": "where中的Null值选择",
    "tag": "function,p1"
    }
    """
    """
    where中的Null值选择
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    where_clause = 'k5 is null'
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k10', 'k11', 'k7', 'k8', 'k9',
                        'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9']
    data_desc_list = palo_client.LoadDataInfo(FILE.all_null_data_hdfs_file, table_name,
                                              column_name_list=column_name_list,
                                              where_clause=where_clause)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    sql1 = 'select * from %s.%s order by k1, k2, k3, k4, k5' % (database_name, table_name)
    sql2 = 'select null, null, null, null, null, null, null, null, null, null, null'
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_broker_where_column():
    """
    {
    "title": "test_broker_load_where.test_broker_where_column",
    "describe": "where中的Null值被not Null约束过滤",
    "tag": "function,p1,fuzz"
    }
    """
    """
    where中的Null值被not Null约束过滤
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    where_clause = 'v5 is null'
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k10', 'k11', 'k7', 'k8', 'k9',
                        'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9']
    data_desc_list = palo_client.LoadDataInfo(FILE.all_null_data_hdfs_file, table_name,
                                              column_name_list=column_name_list,
                                              where_clause=where_clause)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    # show load显示失败
    assert not ret
    client.clean(database_name)


def test_broker_where_abnormal():
    """
    {
    "title": "test_broker_load_where.test_broker_where_abnormal",
    "describe": "where过滤的unselect和abnormal",
    "tag": "function,p1,fuzz"
    }
    """
    """
    where过滤的unselect和abnormal
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=False, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    where_clause = 'k5 is not null'
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k10', 'k11', 'k7', 'k8', 'k9',
                        'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9']
    data_desc_list = palo_client.LoadDataInfo(FILE.all_null_data_hdfs_file, table_name,
                                              column_name_list=column_name_list,
                                              where_clause=where_clause)
    label = util.get_label()
    ret = client.batch_load(label, data_desc_list, is_wait=True, broker=broker_info)
    assert not ret
    load_job = client.get_load_job(label)
    etlinfo = palo_job.LoadJob(load_job).get_etlinfo()
    assert 'dpp.abnorm.ALL=10' in etlinfo
    client.clean(database_name)


def test_broker_where_ratio():
    """
    {
    "title": "test_broker_load_where.test_broker_where_ratio",
    "describe": "where过滤的数据属于unselect",
    "tag": "function,p1"
    }
    """
    """
    where过滤的数据属于unselect
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    where_clause = 'k5 >= 0'
    data_desc_list = palo_client.LoadDataInfo([FILE.baseall_hdfs_file], table_name,
                                              where_clause=where_clause)
    label = util.get_label()
    ret = client.batch_load(label, data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    load_job = client.get_load_job(label)
    etlinfo = palo_job.LoadJob(load_job).get_etlinfo()
    assert 'unselected.rows=3' in etlinfo
    sql1 = 'select * from %s.%s order by k1, k2, k3, k4, k5' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k5 >= 0 \
            order by k1, k2, k3, k4, k5' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_broker_where_partition():
    """
    {
    "title": "test_broker_load_where.test_broker_where_partition",
    "describe": "where过滤和指定partition时的ratio值",
    "tag": "function,p1"
    }
    """
    """
    where过滤和指定partition时的ratio值
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info,
                        partition_info=DATA.baseall_tinyint_partition_info)
    assert client.show_tables(table_name)
    partition_name_list = ["p2", "p3"]
    where_clause = 'k5 >= 0'
    data_desc_list = palo_client.LoadDataInfo([FILE.baseall_hdfs_file], table_name, 
                                              partition_list=partition_name_list,
                                              where_clause=where_clause)
    label = util.get_label()
    ret = client.batch_load(label, data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.4)
    assert not ret
    ret = client.batch_load(label, data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.42)
    assert ret
    load_job = client.get_load_job(label)
    etlinfo = palo_job.LoadJob(load_job).get_etlinfo()
    print(etlinfo)
    assert 'unselected.rows=3' in etlinfo
    assert 'dpp.abnorm.ALL=5' in etlinfo
    sql1 = 'select * from %s.%s order by k1, k2, k3, k4, k5' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k5 >= 0 and k1 > -10 and k1 < 10 \
            order by k1, k2, k3, k4, k5' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_broker_multi_description():
    """
    {
    "title": "test_broker_load_where.test_broker_multi_description",
    "describe": "多个文件，导入同一个表中",
    "tag": "function,p1,fuzz"
    }
    """
    """
    多个文件，导入同一个表中
    多个data description
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info,
                        partition_info=DATA.baseall_tinyint_partition_info)
    assert client.show_tables(table_name)
    partition_name_list = ["p2"]
    where_clause = 'k1 >= 2'
    data_desc1 = palo_client.LoadDataInfo([FILE.baseall_hdfs_file], table_name,
                                              partition_list=partition_name_list,
                                              where_clause=where_clause)
    partition_name_list = ["p2", "p3"]
    where_clause = 'k1 > 5'
    data_desc2 = palo_client.LoadDataInfo([FILE.baseall_hdfs_file], table_name,
                                              partition_list=partition_name_list,
                                              where_clause=where_clause)
    label = util.get_label()
    try:
        # 多个description导入的表的部分不应该有重叠，否则报错overlapping
        ret = client.batch_load(label, [data_desc1, data_desc2], is_wait=True, broker=broker_info, 
                                max_filter_ratio=0.8)
        assert not ret
    except Exception as e:
        pass
    partition_name_list = ["p2"]
    where_clause = 'k1 >= 2'
    data_desc1 = palo_client.LoadDataInfo([FILE.baseall_hdfs_file], table_name,
                                              partition_list=partition_name_list,
                                              where_clause=where_clause)
    partition_name_list = ["p3"]
    where_clause = 'k1 > 5'
    data_desc2 = palo_client.LoadDataInfo([FILE.baseall_hdfs_file], table_name,
                                              partition_list=partition_name_list,
                                              where_clause=where_clause)
    label = util.get_label()
    ret = client.batch_load(label, [data_desc1, data_desc2], is_wait=True, broker=broker_info,
                            max_filter_ratio=0.9)
    assert ret
    load_job = client.get_load_job(label)
    etlinfo = palo_job.LoadJob(load_job).get_etlinfo()
    print(etlinfo)
    assert 'unselected.rows=6' in etlinfo
    assert 'dpp.abnorm.ALL=20' in etlinfo
    sql1 = 'select * from %s.%s order by k1, k2, k3, k4, k5' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9 from %s.baseall where k1 > 5 and k1 < 10 \
            order by k1, k2, k3, k4, k5' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)
    

if __name__ == '__main__':
    setup_module()
    # test_broker_where_and()
    # test_broker_where_null_1()
    # test_broker_where_column()
    # test_broker_where_abnormal()
    # test_broker_where_ratio()
    # test_broker_where_partition()
    test_broker_multi_description()

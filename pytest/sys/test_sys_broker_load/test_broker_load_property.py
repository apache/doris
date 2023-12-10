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
file:test_load_property.py
测试broker load的load property
"""
import os
import sys
import pytest
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

from data import schema as DATA
from lib import palo_config
from lib import palo_client
from lib import util
from lib import common

config = palo_config.config
broker_info = palo_config.broker_info

qe_db = "test_query_qa"
qe_table = "baseall"


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


def test_broker_column_mapping_func():
    """
    {
    "title": "test_broker_load_property.test_broker_column_mapping_func",
    "describe": "验证broker导入的部分函数转换是否正确",
    "tag": "function,p1"
    }
    """
    """
    test_broker_column_mapping_func
    验证broker导入的部分函数转换是否正确
    :return:
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v10', 'v11', 'v7', 'v8', 'v9']
    set_list = ['k1=v1*2', 'k2=round(v2/10)', 'k3=ceil(v5)', 'k4=round(ln(abs(v4)))',
                'k5=sin(v3)', 'k6=ucase(v6)', 'k7=substr(v7,3, 8)', 'k10=date_add(v10, interval "13" week)',
                'k11=adddate(v11, interval 31 day)', 'k8=floor(v8)', 'k9=abs(v9)']
    hdfs_file = palo_config.gen_remote_file_path('qe/baseall.txt')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_name,
                                              column_name_list=column_name_list, column_terminator='\t',
                                              set_list=set_list)

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)

    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select 2*k1, round(k2/10), ceil(k5), round(ln(abs(k4))), sin(k3), ucase(k6), ' \
           'date_add(k10, interval "13" week), adddate(k11, interval 31 day), substr(k7, 3, 8),' \
           'floor(k8), abs(k9) from %s.baseall order by k1' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def time_zone_broker_load(database_name, table_name, broker_cloumn_map, sql,\
    table_schema=None, client=None, zone="+00:00"):
    """"
    更改时区验证对broker load的影响,包含建表，导入数据，查询sql，再次导入数据，查询sql
    broker_cloumn_map： broker_cloumn mapping
    sql：broker load完毕要执行的sql语句
    """
    if not table_schema:
        table_schema = DATA.baseall_column_list
    # 建表
    partition_name_list = ['partition_a']
    partition_value_list = ['MAXVALUE']

    partition_info = palo_client.PartitionInfo('k2',
                                               partition_name_list, partition_value_list)
    distribution_type_d = 'HASH(k1, k2, k5)'
    distribution_info_d = palo_client.DistributionInfo(distribution_type_d, 3)
    client.create_table(table_name, table_schema, partition_info, distribution_info_d)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)
    ###导入数据
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k10', 'k11', 'k7', 'k8', 'k9']
    hdfs_file = palo_config.gen_remote_file_path('qe/baseall.txt')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_name,
                                              column_name_list=column_name_list, column_terminator='\t',
                                              set_list=broker_cloumn_map)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                                  max_filter_ratio=0.05, strict_mode=True)
    sql_count = 'select count(*) from %s.%s' % (database_name, table_name)
    res = client.execute(sql_count)
    assert res[0] == (15,), "excepetd %s == 15" % res[0]
    ###第一次获取数据
    res_before = client.execute(sql)
    ##设置时区,第二次获取数据
    zone = '+00:00'
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True, timezone=zone)
    res_after = client.execute(sql)
    return res_before, res_after


def test_broker_column_mapping_case_when():
    """
    {
    "title": "test_broker_load_property.test_broker_column_mapping_case_when",
    "describe": "验证broker导入的case_when转换是否正确",
    "tag": "function,p1"
    }
    """
    """
    test_broker_column_mapping_func
    验证broker导入的case_when转换是否正确
    :return:
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v10', 'v11', 'v7', 'v8', 'v9']
    set_list = ['k1=case when cast(v1 as int) > 10 then cast((v1 + "100") as varchar) '
                'when cast(v1 as int) > 1 and cast(v1 as int) < 5 then cast(-v1 as varchar) '
                'else cast(v1 as varchar) end',
                'k2=v2', 'k3=v3', 'k4=v4', 'k5=v5',
                'k6=case v6 when "true" then "True" when "false" then "False" else "None" end',
                'k7=case when v7 like "%wang%" then "wang" else v7 end', 'k10=v10',
                'k11=v11', 'k8=v8', 'k9=v9']
    hdfs_file = palo_config.gen_remote_file_path('qe/baseall.txt')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_name,
                                              column_name_list=column_name_list, column_terminator='\t',
                                              set_list=set_list)

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)

    sql1 = 'select * from %s.%s order by 1, 2, 3, 4' % (database_name, table_name)
    sql2 = 'select case when k1 > 10 then k1 + 100 when k1 > 1 and k1 < 5 then -k1 else k1 end a,' \
           ' k2, k3, k4, k5, case k6 when "true" then "True" when "false" then "False" else "None" end b, ' \
           'k10, k11, case when k7 like "%%wang%%" then "wang" else k7 end c, k8, k9 ' \
           'from %s.baseall order by 1, 2, 3, 4' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_broker_column_mapping_case_if():
    """
    {
    "title": "test_broker_load_property.test_broker_column_mapping_case_if",
    "describe": "验证broker导入的if函数转换是否正确",
    "tag": "function,p1"
    }
    """
    """
    test_broker_column_mapping_func
    验证broker导入的if函数转换是否正确
    :return:
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v10', 'v11', 'v7', 'v8', 'v9']
    set_list = ['k1=nullif(v1, "100")', 'k2=if(v2 > "0", v2, cast(-v2 as varchar))',
                'k3=v3', 'k4=v4', 'k5=v5',
                'k6=nullif(v6, "false")',
                'k7=v7', 'k10=v10', 'k11=v11', 'k8=v8', 'k9=v9']
    hdfs_file = palo_config.gen_remote_file_path('qe/baseall.txt')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_name,
                                              column_name_list=column_name_list, column_terminator='\t',
                                              set_list=set_list)

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)

    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select nullif(k1, 100), if(k2>0, k2, -k2), k3, k4, k5, nullif(k6,"false"),' \
           ' k10, k11, k7, k8, k9 from %s.baseall order by k1' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_broker_column_mapping_case_null():
    """
    {
    "title": "test_broker_load_property.test_broker_column_mapping_case_null",
    "describe": "验证broker导入的ifnull判断转换是否正确",
    "tag": "function,p1"
    }
    """
    """
    test_broker_column_mapping_func
    验证broker导入的ifnull判断转换是否正确
    :return:
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    ret = client.create_table(table_name, DATA.types_kv_column_list,
                              set_null=True, distribution_info=DATA.baseall_distribution_info)
    # assert client.show_tables(table_name)
    assert ret, 'create table failed'
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8',
                        'k9', 'k10', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6',
                        'v7', 'v8', 'v9', 'tmp']
    set_list = ['v10=ifnull(tmp, "0")']
    hdfs_file = palo_config.gen_remote_file_path('sys/null/data_5')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_name,
                                              column_name_list=column_name_list, column_terminator='\t',
                                              set_list=set_list)

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)

    expect_file = '%s/data/NULL/verify_5_3' % file_dir
    assert client.verify(expect_file, table_name), 'verify without schema failed'
    client.clean(database_name)


def test_broker_column_mapping_case_json():
    """
    {
    "title": "test_broker_load_property.test_broker_column_mapping_case_json",
    "describe": "验证broker导入的json转换是否正确",
    "tag": "function,p1"
    }
    """
    """
    test_broker_column_mapping_func
    验证broker导入的json转换是否正确
    :return:
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    duplicate_key = 'DUPLICATE KEY(k1, k2)'
    client.create_table(table_name, DATA.json_column_no_agg_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info,
                        keys_desc=duplicate_key)
    assert client.show_tables(table_name)
    # assert ret, 'create table failed'
    column_name_list = ['tmp']

    set_list = ['k1=get_json_int(tmp, "$.col1")',
                'k2=get_json_string(tmp, "$.col2")',
                'k3=get_json_double(tmp, "$.col3")']
    hdfs_file = palo_config.gen_remote_file_path('sys/load/test_json.data')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_name,
                                              column_name_list=column_name_list, column_terminator='\t',
                                              set_list=set_list)

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)

    sql = 'select * from %s.%s order by k1 nulls last, k2' % (database_name, table_name)
    expect_file = '%s/data/BROKER_LOAD/expe_test_json.data' % file_dir
    assert common.check_by_file(expect_file, sql=sql), 'verify without schema failed'
    client.clean(database_name)


def test_set_time_zone_broker_load_now():
    """
    {
    "title": "test_broker_load_property.test_set_time_zone_broker_load_now",
    "describe": "设置time_zone变量,验证now() 函数对broker load任务的影响。",
    "tag": "function,p1"
    }
    """
    """
    设置time_zone变量,验证now() 函数对broker load任务的影响。
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    set_column_list = ["k11=now()"]
    sql = "select k11 from %s order by k11 limit 1" % table_name
    zone = "+00:00"
    gap = 28800  ##8个时区对应的时间戳
    res_before, res_after = time_zone_broker_load(database_name, table_name, \
        set_column_list, sql, client=client)
    print(res_before, res_after)
    ##两者差值
    util.check2_time_zone(res_before, res_after, gap)
    client.clean(database_name)


def test_set_time_zone_broker_load_FROM_UNIXTIME():
    """
    {
    "title": "test_broker_load_property.test_set_time_zone_broker_load_FROM_UNIXTIME",
    "describe": "设置time_zone变量,验证FROM_UNIXTIME() 函数对broker load任务的影响。",
    "tag": "function,p1"
    }
    """
    """
    设置time_zone变量,验证FROM_UNIXTIME() 函数对broker load任务的影响。
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    set_column_list = ["k11=FROM_UNIXTIME(2019, '%Y-%m-%d %H:%i:%s')"]
    sql = "select k11 from %s order by k11 limit 1" % table_name
    zone = "+00:00"
    gap = 28800  ##8个时区对应的时间戳
    res_before, res_after = time_zone_broker_load(database_name, table_name,\
        set_column_list, sql, client=client)
    print(res_before, res_after)
    ##两者差值
    util.check2_time_zone(res_before, res_after, gap)
    client.clean(database_name)


def test_set_time_zone_broker_load_CONVERT_TZ():
    """
    {
    "title": "test_broker_load_property.test_set_time_zone_broker_load_CONVERT_TZ",
    "describe": "设置time_zone变量,验证CONVERT_TZ() 函数对broker load任务的影响。",
    "tag": "function,p1"
    }
    """
    """
    设置time_zone变量,验证CONVERT_TZ() 函数对broker load任务的影响。
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    set_column_list = ["k11=CONVERT_TZ('2019-01-01 12:00:00','+01:00','+10:00')"]
    sql = "select k11 from %s order by k11 limit 1" % table_name
    zone = "+00:00"
    gap = 28800  ##8个时区对应的时间戳
    res_before, res_after = time_zone_broker_load(database_name, table_name,\
        set_column_list, sql, client=client)
    ##两者差值
    util.check2_time_zone(res_before, res_after, gap)
    client.clean(database_name)


def test_set_time_zone_broker_load_UNIX_TIMESTAMP():
    """
    {
    "title": "test_broker_load_property.test_set_time_zone_broker_load_UNIX_TIMESTAMP",
    "describe": "设置time_zone变量,验证UNIX_TIMESTAMP() 函数对broker load任务的影响。",
    "tag": "function,p1"
    }
    """
    """
    设置time_zone变量,验证UNIX_TIMESTAMP() 函数对broker load任务的影响。
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    set_column_list = ["k3=UNIX_TIMESTAMP('2019-01-01 08:00:00')"]
    sql = "select k3 from %s order by k3 limit 1" % table_name
    zone = "+00:00"
    gap = 28800  ##8个时区对应的时间戳
    res_before, res_after = time_zone_broker_load(database_name, table_name,\
        set_column_list, sql, client=client)
    ##两者差值
    util.check(res_before, res_after)
    client.clean(database_name)


@pytest.mark.skip()
def test_sql_mode_broker_load():
    """
    {
    "title": "test_broker_load_property.test_sql_mode_broker_load",
    "describe": "验证sql mode模式下broker导入是否正确",
    "tag": "autotest"
    }
    """
    """
    test_sql_mode_broker_load
    验证sql mode模式下broker导入是否正确
    :return:
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = create_workspace(database_name)
    # create table
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    ##set sql_mode
    assert client.set_sql_mode() == ()
    res = client.get_sql_mode()
    assert "PIPES_AS_CONCAT" in res[0]
    ##load data
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v10', 'v11', 'v7', 'v8', 'v9']
    set_list = ['k1=(v1 || 10)', 'k2=(v2)',
                'k3=v3', 'k4=v4', 'k5=v5',
                'k6=nullif(v6, "false")',
                'k7=v7', 'k10=v10', 'k11=v11', 'k8=v8', 'k9=v9']
    hdfs_file = palo_config.gen_remote_file_path('qe/baseall.txt')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_name,
                                              column_name_list=column_name_list, column_terminator='\t',
                                              set_list=set_list)

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)

    sql1 = 'select k1 from %s.%s order by k1' % (database_name, table_name)
    ##hdfs上相同的数据导入到baseall上
    sql2 = 'select k1*100 + 10 from %s.%s order by k1' % (qe_db, qe_table)
    ##bug, todo,rd fixing, issue:https://github.com/apache/incubator-doris/issues/2428
    #check2(client, sql1, sql2)
    client.clean(database_name)


if __name__ == "__main__":
    test_sql_mode_broker_load()
    #test_set_time_zone_broker_load_now()
    test_set_time_zone_broker_load_FROM_UNIXTIME()
    test_set_time_zone_broker_load_CONVERT_TZ()
    test_set_time_zone_broker_load_UNIX_TIMESTAMP()

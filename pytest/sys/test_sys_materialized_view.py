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
############################################################################
#
#   @file test_sys_materialized_view.py
#   @date 2020-03-05
#   目前只支持duplicate和aggregate
#############################################################################
"""
import sys
import time
import re
from data import schema_change as DATA

sys.path.append("../")
from lib import palo_config
from lib import palo_client
from lib import util

client = None

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


def execute(line):
    """execute palo sql and return reuslt"""
    palo_result = client.execute(line)
    return palo_result


def set_materialized_view_flag():
    """
    set_test_materialized_view_flag
    测试字段，用来验证新、旧选择器选择结果是否一致
    :return:
    """
    line = 'set test_materialized_view=true'
    execute(line)


def get_explain_value(result, key):
    """get value from explain"""
    for item in result:
        if key + ': ' in item[0]:
            return item[0].split(key + ': ')[1].strip()
    return 'empty'


def get_preaggregation_and_rollup(ret):
    """from explain get preaggregation and rollup"""
    LOG.info(L('', preaggregation=get_explain_value(ret, 'PREAGGREGATION'), 
               rollup=get_explain_value(ret, 'TABLE')))
    tb_idx = get_explain_value(ret, 'TABLE')
    pattern = re.compile(r'[(](.*?)[)]', re.S)
    rollup = re.findall(pattern, tb_idx)
    return [get_explain_value(ret, 'PREAGGREGATION'), rollup[0]]


def check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup):
    """check preaggregation and rollup"""
    ret = client.explain_query(sql)
    flag_preaggregation = False
    flag_rollup = False
    for item in ret:
        if 'PREAGGREGATION' in item[0]:
            print(item[0])
            if expected_preaggregation_and_rollup[0] in item[0]:
                flag_preaggregation = True

        if 'TABLE' in item[0]:
            print(item[0])
            for table in expected_preaggregation_and_rollup[1]:
                if table in item[0]:
                    flag_rollup = True
    print(flag_preaggregation, flag_rollup)
    assert flag_preaggregation, 'expect PREAGGREGATION: %s ' % expected_preaggregation_and_rollup[0]
    assert flag_rollup, 'expect rollup: %s ' % expected_preaggregation_and_rollup[1]


def compare_preaggregation_and_rollup(sql, m_table_name, r_table_name):
    """
    compare_preaggregation_and_rollup
    :param sql1:
    :param sql2:
    :return:
    """
    ret1 = client.explain_query(sql % m_table_name)
    ret2 = client.explain_query(sql % r_table_name)
    preaggregation1, rollup1 = get_preaggregation_and_rollup(ret1)
    preaggregation2, rollup2 = get_preaggregation_and_rollup(ret2)
    assert preaggregation1 == preaggregation2, 'the preaggregation for mv and rollup is not same: %s || %s'\
                                               % (preaggregation1, preaggregation2)
    # 命中base表时，两张表的前缀不一样m_/r_，所以从第三个字母开始匹配
    rollup_prefix1 = rollup1[2:]
    rollup_prefix2 = rollup2[2:]
    assert rollup_prefix1 == rollup_prefix2, 'the rollup for mv and rollup is not same: %s || %s' % (rollup1, rollup2)


def check_rollup(sql, expect_rollup_list):
    """验证SQL命中的rollup是否符合预期"""
    ret = client.explain_query(sql)
    preaggregation, rollup = get_preaggregation_and_rollup(ret)
    # assert preaggregation == "ON", 'expect preaggregation ON but %s' % preaggregation
    assert rollup in expect_rollup_list, 'expect %s but %s' % (expect_rollup_list, rollup)


def create_rollup(table_name, rollup_name, columns, database_name=None, is_wait=False):
    """
    create_rollup
    :param table_name:
    :param columns:
    :param database_name:
    :param is_wait:
    :return:
    """
    client.use(database_name)
    rollup_name = 'r_' + rollup_name
    ret = client.create_rollup_table(table_name=table_name,
                                     rollup_table_name=rollup_name,
                                     column_name_list=columns,
                                     database_name=database_name, is_wait=False)

    if not ret:
        LOG.info(L("CREATE ROLLUP fail.", database_name=database_name,
                   rollup_table_name=rollup_name, ret=ret))
        assert 0 == 1, 'create rollup fail'
        return False
    ret = True
    if is_wait:
        ret = client.wait_table_rollup_job(table_name, database_name=database_name)
        assert ret, 'create rollup fail'
        return ret
    LOG.info(L("CREATE ROLLUP succ.", database_name=database_name,
               table_name=table_name, rollup_table_name=rollup_name))
    return ret


def test_sys_materialized_view_duplicate_function():
    """
       {
       "title": "test_sys_materialized_view_duplicate_function",
       "describe": "验证物化视图基本功能，duplicate类型表",
       "tag": "function,p0"
       }
    """
    """test_sys_materialized_view_duplicate_function"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 10)
    client.create_table(table_name, DATA.schema_1_dup, distribution_info=distribution_info, \
                        keys_desc='DUPLICATE KEY (k1, k2)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    # set_test_materialized_view_flag()

    view_name_k1 = 'k1_v3'
    view_sql = 'select k1,v3 from %s' % table_name
    client.create_materialized_view(table_name, view_name_k1, view_sql, database_name=database_name, is_wait=True)

    # 验证一些查询语句是否能命中mv
    sql_list = []
    sql_list.append('select k1 + 1 from {0} where v3 > 1')
    sql_list.append('select k1, sum(v3) + 1 from {0} where k1 - 1 = 0 and v3 * 2 + 1 > 1 group by k1')
    # sql_list.append('select k1 * v3 from {0} group by k1 * v3')
    sql_list.append('select v3, count(distinct k1) from {0} group by v3')
    sql_list.append('select v3, sum(distinct k1) from {0} group by v3')
    sql_list.append('select v3, sum(k1+1) from {0} group by grouping sets((v3));')
    sql_list.append('select k1 from {0} union select k2 from {0}')
    sql_list.append('select * from (select k1 from {0} union all select k2 from {0}) a where k1 / 2 = 1')
    sql_list.append('select k1 from (select k1 from {0} union all select k2 from {0}) a group by k1')
    sql_list.append('select * from (select k1,sum(v3) from {0} group by k1) a '
                    'join (select k2,max(v2) from {0} group by k2) b on a.k1 = b.k2')
    sql_list.append('select * from (select k1,sum(v2) from {0} group by k1 order by k1) a '
                    'join (select k1,max(v3) from {0} group by k1) b using (k1)')
    expected_preaggregation_and_rollup = ['ON', [view_name_k1]]
    for sql in sql_list:
        whole_sql = sql.format(table_name)
        check_preaggregation_and_rollup(whole_sql, expected_preaggregation_and_rollup)

    # 测试: join时命中mv的情况
    view_name_join_k1v3_g = 'join_k1v3_g'
    view_sql = 'select k1,sum(v3) from %s group by k1' % table_name
    client.create_materialized_view(table_name, view_name_join_k1v3_g, view_sql,
                                    database_name=database_name, is_wait=True)
    view_name_join_k2v3_g = 'join_k2v3_g'
    view_sql = 'select k2,max(v3) from %s group by k2' % table_name
    client.create_materialized_view(table_name, view_name_join_k2v3_g, view_sql,
                                    database_name=database_name, is_wait=True)

    sql_all_mv = 'select * from (select k1, sum(v3) from {0} group by k1) a ' \
                 'join (select k2,max(v3) from {0} group by k2) b on a.k1=b.k2'.format(table_name)
    expected_preaggregation_and_rollup = ['ON', [view_name_join_k1v3_g]]
    check_preaggregation_and_rollup(sql_all_mv, expected_preaggregation_and_rollup)
    expected_preaggregation_and_rollup = ['ON', [view_name_join_k2v3_g]]
    check_preaggregation_and_rollup(sql_all_mv, expected_preaggregation_and_rollup)

    sql_right_mv = 'select * from (select k1, max(v3) from {0} group by k1) a ' \
                   'join (select k2,max(v3) from {0} group by k2) b on a.k1=b.k2'.format(table_name)
    expected_preaggregation_and_rollup = ['ON', [table_name]]
    check_preaggregation_and_rollup(sql_right_mv, expected_preaggregation_and_rollup)
    expected_preaggregation_and_rollup = ['ON', [view_name_join_k2v3_g]]
    check_preaggregation_and_rollup(sql_right_mv, expected_preaggregation_and_rollup)

    sql_left_mv = 'select * from (select k1, sum(v3) from {0} group by k1) a ' \
                  'join (select k2,sum(v3) from {0} group by k2) b on a.k1=b.k2'.format(table_name)
    expected_preaggregation_and_rollup = ['ON', [view_name_join_k1v3_g]]
    check_preaggregation_and_rollup(sql_left_mv, expected_preaggregation_and_rollup)
    expected_preaggregation_and_rollup = ['ON', [table_name]]
    check_preaggregation_and_rollup(sql_left_mv, expected_preaggregation_and_rollup)

    # 测试: 从备选的 MVs 表中找到最小的 Rowcount 的表
    view_name_k1k2_g = 'k1k2_g'
    view_sql = 'select k1,k2 from %s group by k1,k2' % table_name
    client.create_materialized_view(table_name, view_name_k1k2_g, view_sql, database_name=database_name, is_wait=True)
    view_name_k2_g = 'k2_g'
    view_sql = 'select k2 from %s group by k2' % table_name
    client.create_materialized_view(table_name, view_name_k2_g, view_sql, database_name=database_name, is_wait=True)

    sql = 'select k2 from {0} group by k2'.format(table_name)
    expected_preaggregation_and_rollup = ['ON', [view_name_k2_g]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    client.clean(database_name)


def test_sys_materialized_view_aggregate_function():
    """
       {
       "title": "test_sys_materialized_view_aggregate_function",
       "describe": "验证物化视图基本功能，aggregate类型表",
       "tag": "function,p0"
       }
    """
    """test_sys_materialized_view_aggregate_function"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 10)
    client.create_table(table_name, DATA.schema_1, distribution_info=distribution_info, \
                        keys_desc='AGGREGATE KEY (k1, k2)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    # set_test_materialized_view_flag()

    view_name_k1 = 'k1_v2'
    view_sql = 'select k1,sum(v2) from %s group by k1' % table_name
    client.create_materialized_view(table_name, view_name_k1, view_sql, database_name=database_name, is_wait=True)

    # 验证一些查询语句是否能命中mv，期望：不命中
    sql_list = []
    sql_list.append('select k1 + 1 from {0} where v2 > 1')
    sql_list.append('select k1 + 1 from {0} where v2 > 1 group by k1')
    sql_list.append('select k1, sum(v2) + 1 from {0} where k1 - 1 = 0 and v2 * 2 + 1 > 1 group by k1')
    sql_list.append('select k1 * v2 from {0} group by k1 * v2')
    sql_list.append('select v2, count(distinct k1) from {0} group by v2')
    expected_preaggregation_and_rollup = ['OFF', [table_name]]
    for sql in sql_list:
        whole_sql = sql.format(table_name)
        check_preaggregation_and_rollup(whole_sql, expected_preaggregation_and_rollup)

    # 验证一些查询语句是否能命中mv，期望：命中
    sql_list = []
    sql_list.append('select k1 + 1 from {0} group by k1')
    sql_list.append('select k1, sum(v2) + 1 from {0} where k1 - 1 = 0 group by k1')
    sql_list.append('select count(distinct k1) from {0} group by k1')
    sql_list.append('select sum(distinct k1) from {0} group by k1')
    sql_list.append('select k1, sum(v2) + 1 from {0} group by grouping sets((k1));')
    sql_list.append('select k1 from {0} group by k1 union select k1 from {0} group by k1')
    sql_list.append('select * from (select k1 from {0} group by k1 union all'
                    ' select k1 from {0} group by k1) a where k1 / 2 = 1')
    sql_list.append('select * from (select k1,sum(v2) from {0} group by k1) a '
                    'join (select k1,sum(v3) from {0} group by k1) b on a.k1 = b.k1')
    sql_list.append('select * from (select k1,sum(v2) from {0} group by k1) a '
                    'join (select k1,sum(v3) from {0} group by k1) b using (k1)')
    expected_preaggregation_and_rollup = ['ON', [view_name_k1]]
    for sql in sql_list:
        whole_sql = sql.format(table_name)
        check_preaggregation_and_rollup(whole_sql, expected_preaggregation_and_rollup)

    # 测试: join时命中mv的情况
    view_name_join_k1v3_g = 'join_k1v3_g'
    view_sql = 'select k1,sum(v3) from %s group by k1' % table_name
    client.create_materialized_view(table_name, view_name_join_k1v3_g, view_sql,
                                    database_name=database_name, is_wait=True)
    view_name_join_k2v3_g = 'join_k2v3_g'
    view_sql = 'select k2,sum(v3) from %s group by k2' % table_name
    client.create_materialized_view(table_name, view_name_join_k2v3_g, view_sql,
                                    database_name=database_name, is_wait=True)

    sql_all_mv = 'select * from (select k1, sum(v3) from {0} group by k1) a ' \
                 'join (select k2,sum(v3) from {0} group by k2) b on a.k1=b.k2'.format(table_name)
    expected_preaggregation_and_rollup = ['ON', [view_name_join_k1v3_g]]
    check_preaggregation_and_rollup(sql_all_mv, expected_preaggregation_and_rollup)
    expected_preaggregation_and_rollup = ['ON', [view_name_join_k2v3_g]]
    check_preaggregation_and_rollup(sql_all_mv, expected_preaggregation_and_rollup)

    sql_right_mv = 'select * from (select k1, max(v3) from {0} group by k1) a ' \
                   'join (select k2,sum(v3) from {0} group by k2) b on a.k1=b.k2'.format(table_name)
    expected_preaggregation_and_rollup = ['ON', [table_name]]
    check_preaggregation_and_rollup(sql_right_mv, expected_preaggregation_and_rollup)
    expected_preaggregation_and_rollup = ['ON', [view_name_join_k2v3_g]]
    check_preaggregation_and_rollup(sql_right_mv, expected_preaggregation_and_rollup)

    sql_left_mv = 'select * from (select k1, sum(v3) from {0} group by k1) a ' \
                  'join (select k2,max(v3) from {0} group by k2) b on a.k1=b.k2'.format(table_name)
    expected_preaggregation_and_rollup = ['ON', [view_name_join_k1v3_g]]
    check_preaggregation_and_rollup(sql_left_mv, expected_preaggregation_and_rollup)
    expected_preaggregation_and_rollup = ['ON', [table_name]]
    check_preaggregation_and_rollup(sql_left_mv, expected_preaggregation_and_rollup)

    # 测试: 从备选的 MVs 表中找到最小的 Rowcount 的表
    view_name_k2v1_g = 'k2v1_g'
    view_sql = 'select k2,v1 from %s group by k2,v1' % table_name
    client.create_materialized_view(table_name, view_name_k2v1_g, view_sql, database_name=database_name, is_wait=True)
    view_name_k2_g = 'k2_g'
    view_sql = 'select k2 from %s group by k2' % table_name
    client.create_materialized_view(table_name, view_name_k2_g, view_sql, database_name=database_name, is_wait=True)

    sql = 'select k2 from {0} group by k2'.format(table_name)
    expected_preaggregation_and_rollup = ['ON', [view_name_k2_g]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    client.clean(database_name)


def test_sys_materialized_view_duplicate():
    """
       {
       "title": "test_sys_materialized_view_duplicate",
       "describe": "验证物化视图基本功能，duplicate类型表",
       "tag": "function,p1"
       }
    """
    """test_sys_materialized_view_duplicate"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 10)
    client.create_table(table_name, DATA.schema_1_dup, distribution_info=distribution_info, \
                        keys_desc='DUPLICATE KEY (k1, k2)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    # set_test_materialized_view_flag()

    view_name_k1 = 'k1'
    view_sql = 'select k1 from %s group by k1' % table_name
    client.create_materialized_view(table_name, view_name_k1, view_sql, database_name=database_name, is_wait=True)

    # deduplicate mv
    view_name_k1_k2 = 'k1_k2'
    view_sql = 'select k1,k2 from %s group by k1,k2' % table_name
    client.create_materialized_view(table_name, view_name_k1_k2, view_sql, database_name=database_name, is_wait=True)

    view_name_k1_sumv2 = 'k1_sumv2'
    view_sql = 'select k1,sum(v2) from %s group by k1' % table_name
    client.create_materialized_view(table_name, view_name_k1_sumv2, view_sql, database_name=database_name, is_wait=True)

    view_name_k1_v3 = 'k1_v3'
    view_sql = 'select k1,v3 from %s' % table_name
    client.create_materialized_view(table_name, view_name_k1_v3, view_sql, database_name=database_name, is_wait=True)

    client.use(database_name)

    sql = 'select k1 from %s' % table_name
    expected_preaggregation_and_rollup = ['ON', [view_name_k1, view_name_k1_v3]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    sql = 'select k1 from %s group by k1' % table_name
    expected_preaggregation_and_rollup = ['ON', [view_name_k1]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    sql = 'select k1,k2 from %s' % table_name
    expected_preaggregation_and_rollup = ['ON', [table_name]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    sql = 'select k1,sum(v2) from %s group by k1' % table_name
    expected_preaggregation_and_rollup = ['ON', [view_name_k1_sumv2]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    sql = 'select k1,min(v2) from %s group by k1' % table_name
    expected_preaggregation_and_rollup = ['ON', [table_name]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    sql = 'select k1,v3 from %s' % table_name
    expected_preaggregation_and_rollup = ['ON', [view_name_k1_v3]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    sql = 'select k1,max(v3) from %s group by k1' % table_name
    expected_preaggregation_and_rollup = ['ON', [view_name_k1_v3]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    client.clean(database_name)


def test_sys_materialized_view_aggregate():
    """
       {
       "title": "test_sys_materialized_view_aggregate",
       "describe": "验证物化视图基本功能，aggregate类型表",
       "tag": "function,p1"
       }
    """
    """test_sys_materialized_view_aggregate"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 10)
    client.create_table(table_name, DATA.schema_1, distribution_info=distribution_info, \
                        keys_desc='AGGREGATE KEY (k1, k2)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    # set_test_materialized_view_flag()

    view_name_k1 = 'k1'
    view_sql = 'select k1 from %s group by k1' % table_name
    client.create_materialized_view(table_name, view_name_k1, view_sql, database_name=database_name, is_wait=True)

    view_name_k1_k2 = 'k1_k2'
    view_sql = 'select k1,k2 from %s group by k1,k2' % table_name
    util.assert_return(False, 'MV contains all keys in base table with same order for aggregation or unique table '
                              'is useless.', client.create_materialized_view, table_name, view_name_k1_k2, view_sql,
                       database_name=database_name, is_wait=True)

    view_name_k1_sumv2 = 'k1_sumv2'
    view_sql = 'select k1,sum(v2) from %s group by k1' % table_name
    client.create_materialized_view(table_name, view_name_k1_sumv2, view_sql, database_name=database_name, is_wait=True)

    view_name_k1_v3 = 'k1_v3'
    view_sql = 'select k1,v3 from %s' % table_name
    util.assert_return(False, 'The materialized view of aggregation or unique table must has grouping columns',
                       client.create_materialized_view, table_name, view_name_k1_v3, view_sql,
                       database_name=database_name, is_wait=True)

    view_name_k1_minv3 = 'k1_minv3' 
    view_sql = 'select k1,min(v3) from %s group by k1' % table_name
    util.assert_return(False, 'Aggregate function require same with slot aggregate type',
                       client.create_materialized_view, table_name, view_name_k1_minv3, view_sql,
                       database_name=database_name, is_wait=True)

    view_sql = 'select k1,sum(v3) from %s group by k1' % table_name
    client.create_materialized_view(table_name, view_name_k1_v3, view_sql, database_name=database_name, is_wait=True)

    client.use(database_name)

    sql = 'select k1 from %s' % table_name
    expected_preaggregation_and_rollup = ['OFF', [table_name]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    sql = 'select k1 from %s group by k1' % table_name
    expected_preaggregation_and_rollup = ['ON', [view_name_k1]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    sql = 'select k1,k2 from %s' % table_name
    expected_preaggregation_and_rollup = ['OFF', [table_name]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    sql = 'select k1,sum(v2) from %s group by k1' % table_name
    expected_preaggregation_and_rollup = ['ON', [view_name_k1_sumv2]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)
    sql = 'select k1,min(v2) from %s group by k1' % table_name
    expected_preaggregation_and_rollup = ['OFF', [table_name]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    sql = 'select k1,v3 from %s' % table_name
    expected_preaggregation_and_rollup = ['OFF', [table_name]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)
    sql = 'select k1,sum(v3) from %s group by k1' % table_name
    expected_preaggregation_and_rollup = ['ON', [view_name_k1_v3]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)
    sql = 'select k1,max(v3) from %s group by k1' % table_name
    expected_preaggregation_and_rollup = ['OFF', [table_name]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)
    sql = 'select k1,min(v3) from %s group by k1' % table_name
    expected_preaggregation_and_rollup = ['OFF', [table_name]]
    check_preaggregation_and_rollup(sql, expected_preaggregation_and_rollup)

    client.clean(database_name)


def test_sys_materialized_view_and_rollup_duplicate():
    """
       {
       "title": "test_sys_materialized_view_and_rollup_duplicate",
       "describe": "验证物化视图基本功能，与rollup进行对比，duplicate类型表",
       "tag": "function,p1"
       }
    """
    """test_sys_materialized_view_and_rollup_duplicate"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    m_table_name = 'm_' + table_name[2:]
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 10)
    client.create_table(m_table_name, DATA.schema_1_dup, distribution_info=distribution_info,
                        keys_desc='DUPLICATE KEY (k1, k2)')

    time.sleep(1)

    assert client.show_tables(m_table_name)
    assert client.get_index(m_table_name)

    client.use(database_name)

    view_name_k1 = 'k1'
    view_sql = 'select k1 from %s group by k1' % m_table_name
    client.create_materialized_view(m_table_name, 'm_' + view_name_k1, view_sql,
                                    database_name=database_name, is_wait=True)

    view_name_k1_k2 = 'k1_k2'
    view_sql = 'select k1,k2 from %s group by k1,k2' % m_table_name
    client.create_materialized_view(m_table_name, 'm_' + view_name_k1_k2, view_sql,
                                    database_name=database_name, is_wait=True)

    view_name_k1_v3 = 'k1_v3'
    view_sql = 'select k1,v3 from %s' % m_table_name
    client.create_materialized_view(m_table_name, 'm_' + view_name_k1_v3, view_sql, 
                                    database_name=database_name, is_wait=True)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, m_table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    set_list = ['k1=k1+1']
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, m_table_name, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    time.sleep(1)

    sql_list = []
    sql_list.append(('select k1 from %s where k1 < 10' % m_table_name, ['m_k1_v3']))
    sql_list.append(('select k1 from %s group by k1' % m_table_name, ['m_k1']))
    sql_list.append(('select k1,k2 from %s' % m_table_name, [m_table_name]))
    sql_list.append(('select k1,k2 from %s group by k1,k2' % m_table_name, ['m_k1_k2']))
    sql_list.append(('select k1,min(k2) from %s group by k1' % m_table_name, ['m_k1_k2']))
    sql_list.append(('select k1,v3 from %s' % m_table_name, ['m_k1_v3']))
    sql_list.append(('select k1,max(v3) from %s group by k1' % m_table_name, ['m_k1_v3']))

    for sql in sql_list:
        check_rollup(sql[0], sql[1])
    client.clean(database_name)


def test_sys_materialized_view_and_rollup_aggregate():
    """
       {
       "title": "test_sys_materialized_view_and_rollup_aggregate",
       "describe": "验证物化视图基本功能，与rollup进行对比，aggregate类型表",
       "tag": "function,p1"
       }
    """
    """test_sys_materialized_view_and_rollup_aggregate"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    m_table_name = 'm_' + table_name[2:]
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 10)
    client.create_table(m_table_name, DATA.schema_1, distribution_info=distribution_info,
                        keys_desc='AGGREGATE KEY (k1, k2)')

    time.sleep(1)

    assert client.show_tables(m_table_name)
    assert client.get_index(m_table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, m_table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    set_list = ['k1=k1+1']
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, m_table_name, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    view_name_k1 = 'k1'
    view_sql = 'select k1 from %s group by k1' % m_table_name
    client.create_materialized_view(m_table_name, 'm_' + view_name_k1, view_sql, 
                                    database_name=database_name, is_wait=True)

    view_name_k1_k2 = 'k1_k2'
    view_sql = 'select k1,k2 from %s group by k1,k2' % m_table_name
    util.assert_return(False, 'MV contains all keys in base table with same order for aggregation or unique table '
                              'is useless.', client.create_materialized_view, table_name, view_name_k1_k2, view_sql,
                       database_name=database_name, is_wait=True)

    view_name_k1_sumv2 = 'k1_sumv2'
    view_sql = 'select k1,sum(v2) from %s group by k1' % m_table_name
    client.create_materialized_view(m_table_name, 'm_' + view_name_k1_sumv2, view_sql,
                                    database_name=database_name, is_wait=True)

    view_name_k1_v3 = 'k1_v3'
    view_sql = 'select k1,sum(v3) from %s group by k1' % m_table_name
    client.create_materialized_view(m_table_name, 'm_' + view_name_k1_v3, view_sql, 
                                    database_name=database_name, is_wait=True)

    client.use(database_name)
    time.sleep(5)
    sql_list = []
    sql_list.append(('select k1 from %s' % m_table_name, [m_table_name]))
    sql_list.append(('select k1 from %s group by k1' % m_table_name, ['m_k1', 'm_k1_v3', 'm_k1_sumv2']))
    sql_list.append(('select k1,k2 from %s' % m_table_name, [m_table_name]))
    sql_list.append(('select k1,sum(v2) from %s group by k1' % m_table_name, ['m_k1_sumv2']))
    sql_list.append(('select k1,min(v2) from %s group by k1' % m_table_name, [m_table_name,]))
    sql_list.append(('select k1,v3 from %s' % m_table_name, [m_table_name,]))
    sql_list.append(('select k1,sum(v3) from %s group by k1' % m_table_name, ['m_k1_v3']))
    sql_list.append(('select k1,max(v3) from %s group by k1' % m_table_name, [m_table_name,]))

    for sql in sql_list:
        check_rollup(sql[0], sql[1])
    client.clean(database_name)


def test_sys_materialized_view_duplication_with_duplicate_func():
    """
    {
       "title": "test_sys_materialized_view_duplication_with_duplicate_col",
       "describe": "验证物化视图基本功能，duplicate类型表,支持基列使用多个agg函数以及agg函数中包含子表达式",
       "tag": "function,p1"
       }
    """
    """test_sys_materialized_view_duplication_with_duplicate_col"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    m_table_name = 'm_' + table_name[2:]
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 10)
    client.create_table(m_table_name, DATA.schema_1_dup, distribution_info=distribution_info, \
                        keys_desc='DUPLICATE KEY (k1, k2)')

    time.sleep(1)

    assert client.show_tables(m_table_name)
    assert client.get_index(m_table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, m_table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    view_name_k1smk2 = 'k1smk2'
    view_sql = 'select k1 ,sum(k2), min(k2) from %s group by k1' % m_table_name
    client.create_materialized_view(table_name, 'm_' + view_name_k1smk2, view_sql,
                                    database_name=database_name, is_wait=True)

    view_name_k1sak2 = 'k1sak2'
    view_sql = 'select k1,sum(abs(k2)) from %s group by k1' % m_table_name
    client.create_materialized_view(table_name, 'm_' + view_name_k1sak2, view_sql,
                                    database_name=database_name, is_wait=True)

    view_name_sapk1k2 = 'sapk1k2'
    view_sql = 'select abs(k1)+1,sum(abs(k2+1)) from %s group by abs(k1)+1' % m_table_name
    client.create_materialized_view(table_name, 'm_' + view_name_sapk1k2, view_sql,
                                    database_name=database_name, is_wait=True)

    client.use(database_name)
    time.sleep(5)
    sql_list = []
    sql_list.append(('select k1 from %s' % m_table_name, [m_table_name]))
    sql_list.append(('select k1,k2 from %s' % m_table_name, [m_table_name]))
    sql_list.append(('select k1,sum(k2) from %s group by k1' % m_table_name, ['m_k1smk2']))
    sql_list.append(('select k1,min(k2) from %s group by k1' % m_table_name, ['m_k1smk2']))
    sql_list.append(('select k1,sum(k2),min(k2) from %s group by k1' % m_table_name, ['m_k1smk2']))
    sql_list.append(('select k1,sum(abs(k2)) from %s group by k1' % m_table_name, ['m_k1sak2']))
    sql_list.append(('select abs(k1)+1,sum(abs(k2+1))  from %s group by abs(k1) + 1' % m_table_name, ['m_sapk1k2']))

    for sql in sql_list:
        check_rollup(sql[0], sql[1])
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
    test_sys_materialized_view_aggregate()

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
#   @file test_query_lateral_view.py
#   @date 2021-12-17 11:32:00
#   @brief This file is a test file for lateral view
#
#############################################################################

"""
lateral view 行转列测试
"""
import os
import sys
import subprocess
sys.path.append("../lib/")
import palo_qe_client
from palo_qe_client import PaloQE
from palo_qe_client import QueryBase
from palo_query_plan import PlanInfo
import query_util as util

LOG = palo_qe_client.LOG
L = palo_qe_client.L
file_path = os.path.split(os.path.realpath(__file__))[0]

database_name = "test_lateral_view"
table_name = "lateral_view_data"
check_name = "lateral_view_check"

if 'FE_DB' in os.environ.keys():
    query_db = os.environ["FE_DB"]
else:
    query_db = "test_query_qa"


def setup_module():
    """
    init config
    """
    global runner
    runner = QueryBase()
    if 'FE_HOST' not in os.environ.keys():
        os.environ["FE_HOST"] = runner.query_host
    if 'FE_QUERY_PORT' not in os.environ.keys():
        os.environ["FE_QUERY_PORT"] = str(runner.query_port)
    if 'FE_PASSWORD' not in os.environ.keys():
        os.environ["FE_PASSWORD"] = runner.query_passwd
    runner.query_palo.db_name = "test_lateral_view"
    # runner.checkok('set global enable_lateral_view=true')
    cmd = 'sh ../data/init_lateral_view.sh'
    check1 = 'select count(*) from test_lateral_view.lateral_view_data'
    check2 = 'select count(*) from test_lateral_view.lateral_view_check'
    try:
        res1 = runner.query_palo.do_sql(check1)
        res2 = runner.query_palo.do_sql(check2)
        if res1 != ((1000,),) or res2 != ((10238,),):
            subprocess.getstatusoutput(cmd)
            runner.wait_end('LOAD')
    except:
        subprocess.getstatusoutput(cmd)
        runner.wait_end('LOAD')
    res1 = runner.query_palo.do_sql(check1)
    res2 = runner.query_palo.do_sql(check2)
    assert res1 == ((1000,),) and res2 == ((10238,),), 'init lateral view table fail'
    runner.checkok('use test_lateral_view')


def create(database_name, table_name, column_list, distribution_info='HASH(k1)', keys_desc='', partition_info=None):
    """
    create table
    """
    sql = "create table %s.%s (" % (database_name, table_name)
    for column in column_list:
        sql = '%s %s,' % (sql, util.column_to_sql(column))
    sql = '%s) %s' % (sql[:-1], keys_desc)
    if partition_info:
        sql = '%s %s' % (sql, str(partition_info))
    sql = '%s distributed by %s' % (sql, distribution_info)
    runner.checkok(sql)


def check_table_function_node(sql, check_value, column_name='', frag_no=2, database_name='test_lateral_view', \
                table_name='lateral_view_data', separator="','", check=False):
    """
    判断查询计划里table_function_node
    """
    plan_res = runner.query_palo.get_query_plan(sql)
    assert plan_res, "%s is NUll" % str(plan_res)
    assert len(check_value) > 0, "%s is null" % str(check_value)
    plan = PlanInfo(plan_res)
    if not frag_no:
        table_function_node_res = plan.get_frag_table_function_node(plan_res)
    else:
        fragment_res = plan.get_one_fragment(frag_no)
        table_function_node_res = plan.get_frag_table_function_node(fragment_res)
    if not check:
        if str(check_value).upper() == "EXPLODE_SPLIT":        
            # check_value = "%s(`default_cluster:%s`.`%s`.`%s`, %s)" % (check_value, database_name, table_name, \
            #         column_name, separator)
            check_value = '%s(%s, %s' % (check_value, column_name, separator)
        else:
            # check_value = "%s(`default_cluster:%s`.`%s`.`%s`" % (check_value, database_name, table_name, column_name)
           check_value = '%s(%s' % (check_value, column_name)
    LOG.info(L('', check_value=str(check_value).upper(), table_function_node=str(table_function_node_res).upper()))
    assert (str(check_value).upper() in str(table_function_node_res).upper()), \
           'expect: %s, actural: %s' % (check_value, table_function_node_res)


def test_lateral_view_explode_split():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_explode_split",
    "describe": "行转列函数功能测试，查询语句包含：1.仅行转列结果列2.仅普通列3.普通列及行转列结果列",
    "tag": "p0,function"
    }
    """
    runner.checkok('use %s' % database_name)
    #explode_split
    line1 = "select k1,e1 from %s lateral view explode_split_outer(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s where k4 is not null order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1 from %s lateral view explode_split_outer(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    line1 = "select k1 from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1 from %s where k4 is not null order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select e1 from %s lateral view explode_split_outer(k4, ',') tmp as e1 order by e1" % table_name
    line2 = "select k4 from %s order by k4" % check_name
    runner.check2_palo(line1, line2)
    line1 = "select e1 from %s lateral view explode_split(k4, ',') tmp as e1 order by e1" % table_name
    line2 = "select k4 from %s where k4 is not null order by k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')


def test_lateral_view_explode_bitmap():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_explode_bitmap",
    "describe": "行转列函数功能测试，查询语句包含：1.仅行转列结果列2.仅普通列3.普通列及行转列结果列",
    "tag": "p0,function"
    }
    """
    runner.checkok('use %s' % database_name)
    # explode_bitmap bitmap_from_string
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap(bitmap_from_string', 'k8')
    line1 = "select k1 from %s lateral view explode_bitmap(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1 from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap(bitmap_from_string', 'k8')
    line1 = "select e1 from %s lateral view explode_bitmap(bitmap_from_string(k8)) tmp as e1 order by e1" % table_name
    line2 = "select k8 from %s where k8 is not null order by k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap(bitmap_from_string', 'k8')
    # explode_bitmap bitmap_union
    line1 = "select k1,e1 from %s lateral view explode_bitmap(k9) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k8 from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap', 'k9')
    line1 = "select k1 from %s lateral view explode_bitmap(k9) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1 from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap', 'k9')
    line1 = "select e1 from %s lateral view explode_bitmap(k9) tmp as e1 order by e1" % table_name
    line2 = "select k8 from %s where k8 is not null order by k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap', 'k9')


def test_lateral_view_explode_bitmap_outer():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_explode_bitmap_outer",
    "describe": "行转列函数功能测试，查询语句包含：1.仅行转列结果列2.仅普通列3.普通列及行转列结果列",
    "tag": "p0,function"
    }
    """
    runner.checkok('use %s' % database_name)
    # explode_bitmap bitmap_from_string
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8')
    line1 = "select k1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8')
    line1 = "select e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1 order by e1" \
            % table_name
    line2 = "select k8 from %s order by k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8')
    # explode_bitmap bitmap_union
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(k9) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9')
    line1 = "select k1 from %s lateral view explode_bitmap_outer(k9) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9')
    line1 = "select e1 from %s lateral view explode_bitmap_outer(k9) tmp as e1 order by e1" % table_name
    line2 = "select k8 from %s order by k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9')


def test_lateral_view_explode_json():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_explode_json",
    "describe": "行转列函数功能测试，查询语句包含：1.仅行转列结果列2.仅普通列3.普通列及行转列结果列",
    "tag": "p0,function"
    }
    """
    runner.checkok('use %s' % database_name)
    # explode_json_array_string
    line1 = "select k1,e1 from %s lateral view explode_json_array_string(k5) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k5 from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5')
    line1 = "select k1 from %s lateral view explode_json_array_string(k5) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1 from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5')
    line1 = "select e1 from %s lateral view explode_json_array_string(k5) tmp as e1 order by e1" % table_name
    line2 = "select k5 from %s where k6 is not null order by k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5')
    # explode_json_array_int
    line1 = "select k1,e1 from %s lateral view explode_json_array_int(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s where k6 is not null order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6')
    line1 = "select k1 from %s lateral view explode_json_array_int(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1 from %s where k6 is not null order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6')
    line1 = "select e1 from %s lateral view explode_json_array_int(k6) tmp as e1 order by e1" % table_name
    line2 = "select k6 from %s where k6 is not null order by k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6')
    # explode_json_array_double
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k7 from %s where k6 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7')
    line1 = "select k1 from %s lateral view explode_json_array_double(k7) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1 from %s where k6 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7')
    line1 = "select e1 from %s lateral view explode_json_array_double(k7) tmp as e1 order by e1" % table_name
    line2 = "select k7 from %s where k6 is not null order by k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7')


def test_lateral_view_explode_json_outer():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_explode_json_outer",
    "describe": "行转列函数功能测试，查询语句包含：1.仅行转列结果列2.仅普通列3.普通列及行转列结果列",
    "tag": "p0,function"
    }
    """
    runner.checkok('use %s' % database_name)
    # explode_json_array_string
    line1 = "select k1,e1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k5 from %s order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5')
    line1 = "select k1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1 from %s order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5')
    line1 = "select e1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1 order by e1" % table_name
    line2 = "select k5 from %s order by k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5')
    # explode_json_array_int
    line1 = "select k1,e1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6')
    line1 = "select k1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1 from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6')
    line1 = "select e1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1 order by e1" % table_name
    line2 = "select k6 from %s order by k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6')
    # explode_json_array_double
    line1 = "select k1,e1 from %s lateral view explode_json_array_double_outer(k7) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k7 from %s order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7')
    line1 = "select k1 from %s lateral view explode_json_array_double_outer(k7) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1 from %s order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7')
    line1 = "select e1 from %s lateral view explode_json_array_double_outer(k7) tmp as e1 order by e1" % table_name
    line2 = "select k7 from %s order by k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7')


def test_aggregate_key():
    """
    {
    "title": "test_query_lateral_view:test_aggregate_key",
    "describe": "aggregate表行转列，执行行转列的列为key列、非分桶列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_aggregate_key'
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k5 from %s order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s where k6 is not null order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k7 from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', 2, database_name, table_name)


def test_aggregate_key_bucket():
    """
    {
    "title": "test_query_lateral_view:test_aggregate_key_bucket",
    "describe": "aggregate表行转列，执行行转列的列为key列、分桶列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_aggregate_key_bucket'
    line1 = "select k1,e1 from %s lateral view explode_split_outer(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split_outer', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k5 from %s order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s where k6 is not null order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double_outer(k7) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k7 from %s order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7', 2, database_name, table_name)


def test_aggregate_value_replace():
    """
    {
    "title": "test_query_lateral_view:test_aggregate_value_replace",
    "describe": "aggregate表行转列，执行行转列的列为value列，聚合类型为replace",
    "tag": "p1,function"
    }
    """
    table_name = 'test_aggregate_value_replace'
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k5 from %s order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s where k6 is not null order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double_outer(k7) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k7 from %s order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7', 2, database_name, table_name)


def test_aggregate_value():
    """
    {
    "title": "test_query_lateral_view:test_aggregate_value",
    "describe": "aggregate表行转列，执行行转列的列为value列，聚合类型为max、min",
    "tag": "p1,function"
    }
    """
    table_name = 'test_aggregate_value'
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string(k5) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k5 from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k7 from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(k9) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9', 2, database_name, table_name)


def test_unique_key():
    """
    {
    "title": "test_query_lateral_view:test_unique_key",
    "describe": "unique表行转列，执行行转列的列为key列、非分桶列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_unique_key'
    line1 = "select k1,e1 from %s lateral view explode_split_outer(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split_outer', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k5 from %s order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s where k6 is not null order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double_outer(k7) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k7 from %s order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7', 2, database_name, table_name)


def test_unique_key_bucket():
    """
    {
    "title": "test_query_lateral_view:test_unique_key_bucket",
    "describe": "unique表行转列，执行行转列的列为key列、分桶列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_unique_key_bucket'
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string(k5) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k5 from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k7 from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', 2, database_name, table_name)


def test_unique_value():
    """
    {
    "title": "test_query_lateral_view:test_unique_value",
    "describe": "unique表行转列，执行行转列的列为value列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_unique_value'
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string(k5) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k5 from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k7 from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', 2, database_name, table_name)


def test_duplicate_key():
    """
    {
    "title": "test_query_lateral_view:test_duplicate_key",
    "describe": "duplicate表行转列，执行行转列的列为key列、非分桶列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_duplicate_key'
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string(k5) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k5 from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k7 from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', 2, database_name, table_name)


def test_duplicate_key_bucket():
    """
    {
    "title": "test_query_lateral_view:test_duplicate_key_bucket",
    "describe": "duplicate表行转列，执行行转列的列为key列、分桶列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_duplicate_key_bucket'
    line1 = "select k1,e1 from %s lateral view explode_split_outer(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split_outer', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k5 from %s order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s where k6 is not null order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double_outer(k7) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k7 from %s order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7', 2, database_name, table_name)


def test_duplicate_value():
    """
    {
    "title": "test_query_lateral_view:test_duplicate_value",
    "describe": "duplicate表行转列，执行行转列的列为value列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_duplicate_value'
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string(k5) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k5 from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k7 from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', 2, database_name, table_name)


def test_range_partition():
    """
    {
    "title": "test_query_lateral_view:test_range_partition",
    "describe": "range分区表行转列，执行行转列的列为非分区列、非分桶列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_range_partition'
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string(k5) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k5 from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k7 from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(k9) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9', 2, database_name, table_name)


def test_range_partition_bucket():
    """
    {
    "title": "test_query_lateral_view:test_range_partition_bucket",
    "describe": "range分区表行转列，执行行转列的列为非分区列、分桶列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_range_partition_bucket'
    line1 = "select k1,e1 from %s lateral view explode_split_outer(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split_outer', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k5 from %s order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s where k6 is not null order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double_outer(k7) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k7 from %s order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7', 2, database_name, table_name)


def test_list_partition():
    """
    {
    "title": "test_query_lateral_view:test_list_partition",
    "describe": "list分区表行转列，执行行转列的列为非分区列、非分桶列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_list_partition'
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string(k5) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k5 from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k7 from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(k9) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9', 2, database_name, table_name)


def test_list_partition_bucket():
    """
    {
    "title": "test_query_lateral_view:test_list_partition_bucket",
    "describe": "list分区表行转列，执行行转列的列为非分区列、分桶列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_list_partition_bucket'
    line1 = "select k1,e1 from %s lateral view explode_split_outer(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split_outer', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k5 from %s order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s where k6 is not null order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double_outer(k7) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k7 from %s order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7', 2, database_name, table_name)


def test_multi_partition():
    """
    {
    "title": "test_query_lateral_view:test_multi_partition",
    "describe": "复合分区表行转列，执行行转列的列为非分区列、非分桶列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_multi_partition'
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string(k5) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k5 from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k7 from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(k9) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9', 2, database_name, table_name)


def test_multi_partition_bucket():
    """
    {
    "title": "test_query_lateral_view:test_multi_partition_bucket",
    "describe": "复合分区表行转列，执行行转列的列为非分区、分桶列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_multi_partition_bucket'
    line1 = "select k1,e1 from %s lateral view explode_split_outer(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split_outer', 'k4', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string(k8)) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k8 from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap(bitmap_from_string', 'k8', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k5 from %s order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k6 from %s where k6 is not null order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double_outer(k7) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,k7 from %s order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7', 2, database_name, table_name)


def test_explode_split_type():
    """
    {
    "title": "test_query_lateral_view:test_explode_split_type",
    "describe": "explode_split函数执行行转列的列类型测试",
    "tag": "p1,function"
    }
    """
    table_name = 'test_explode_split_type'
    check_name = 'test_explode_split_type_check'
    line1 = "select k1,e1 from %s lateral view explode_split(k2, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k2 from %s order by k1,k2" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_split(k3, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k2 from %s order by k1,k2" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k3', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,k2 from %s order by k1,k2" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', 2, database_name, table_name)
    columns = ['k2', 'k3', 'k4', 'k5', 'k10', 'k11', 'k8', 'k9']
    for column in columns:
        line1 = "select k1,e1 from %s.baseall lateral view explode_split(%s, ',')tmp as e1 order by k1" \
                % (query_db, column)
        line2 = "select k1,cast(%s as char) from %s.baseall order by k1" % (column, query_db)
        runner.check2_palo(line1, line2)
        check_table_function_node(line1, 'explode_split', column, 2, query_db, 'baseall')


def test_lateral_view_null():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_null",
    "describe": "行转列null值测试，1.行转列的列为null,2.普通列为null",
    "tag": "p1,function"
    }
    """
    table_name = 'test_lateral_view_null'
    runner.checkok('drop table if exists %s' % table_name)
    column_list = [('k1', 'int'), ('k2', 'char')]
    create(database_name, table_name, column_list)
    sql = "insert into %s values (1, NULL)" % table_name
    runner.checkok(sql)
    line1 = "select k1,e1 from %s lateral view explode_split_outer(k2, ',')tmp as e1" % table_name
    line2 = "select k1,NULL from %s" % table_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split_outer', 'k2', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_split(k2, ',')tmp as e1" % table_name
    line3 = "select k1,NULL from %s where 1 = 2" % table_name
    runner.check2_palo(line1, line3)
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name)

    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k2)) tmp as e1" % table_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k2', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string(k2)) tmp as e1" % table_name
    runner.check2_palo(line1, line3)
    check_table_function_node(line1, 'explode_bitmap(bitmap_from_string', 'k2', 2, database_name, table_name)

    line1 = "select k1,e1 from %s lateral view explode_json_array_string(k2)tmp as e1" % table_name
    runner.check2_palo(line1, line3)
    check_table_function_node(line1, 'explode_json_array_string', 'k2', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string_outer(k2)tmp as e1" % table_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k2', 2, database_name, table_name)

    line1 = "select k1,e1 from %s lateral view explode_json_array_int_outer(k2)tmp as e1" % table_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k2', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int(k2)tmp as e1" % table_name
    runner.check2_palo(line1, line3)
    check_table_function_node(line1, 'explode_json_array_int', 'k2', 2, database_name, table_name)

    line1 = "select k1,e1 from %s lateral view explode_json_array_double_outer(k2)tmp as e1" % table_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k2', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k2)tmp as e1" % table_name
    runner.check2_palo(line1, line3)
    check_table_function_node(line1, 'explode_json_array_double', 'k2', 2, database_name, table_name)
    runner.checkok('drop table if exists %s' % table_name)


def test_lateral_view_null_1():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_null_1",
    "describe": "行转列null值测试，1.行转列的列为null,2.普通列为null",
    "tag": "p1,function"
    }
    """
    table_name_2 = 'test_lateral_view_null_2'
    column_list = [('k1', 'int'), ('k2', 'varchar(20)'), ('k3', 'varchar(20)'), ('k4', 'varchar(20)'), \
            ('k5', 'varchar(20)'), ('k6', 'bitmap bitmap_union')]
    runner.checkok('drop table if exists %s' % table_name_2)
    create(database_name, table_name_2, column_list)
    sql = "insert into %s values (NULL, 'a,b', '[\"a\",\"b\"]', '[1,2]', '[1.1,2.2]', bitmap_from_string('1,2'))" \
            % table_name_2
    runner.checkok(sql)
    line1 = "select k1,e1 from %s lateral view explode_split(k2, ',')tmp as e1" % table_name_2
    line2 = "select null,'a' union all select null,'b'"
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name_2)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string(k3)tmp as e1" % table_name_2
    line2 = "select null,'a' union all select null,'b'"
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k3', 2, database_name, table_name_2)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int(k4)tmp as e1" % table_name_2
    line2 = "select null,1 union all select null,2"
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k4', 2, database_name, table_name_2)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k5)tmp as e1" % table_name_2
    line2 = "select null,1.1 union all select null,2.2"
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k5', 2, database_name, table_name_2)
    line1 = "select k1,e1 from %s lateral view explode_bitmap(k6)tmp as e1" % table_name_2
    line2 = "select null,1 union all select null,2"
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap', 'k6', 2, database_name, table_name_2)
    runner.checkok('drop table if exists %s' % table_name_2)


def test_explode_split_cn_symbol():
    """
    {
    "title": "test_query_lateral_view:test_explode_split_cn_symbol",
    "describe": "explode_split函数对中文字符进行行转列",
    "tag": "p1,function"
    }
    """
    table_name = 'test_explode_split_cn_symbol'
    runner.checkok('drop table if exists %s' % table_name)
    column_list = [('k1', 'int'), ('k2', 'varchar(200)'), ('k3', 'varchar(200)')]
    create(database_name, table_name, column_list)
    data = "你我他你我他你，我他你我他，，你我他abc你我他，你你,我。他他他。"
    data_2 = '["他你，", "他你", "他你", "他abc你", "。他他他。", "你", "他，，你", "他，你你,"]'
    sql = "insert into %s values (1, '%s', '%s')" % (table_name, data, data_2)
    runner.checkok(sql)
    line1 = "select k1,e1 from %s lateral view explode_split(k2, '我') tmp as e1 order by e1" % table_name
    line2 = "select 1,'。他他他。' union all select 1,'他abc你' union all select 1,'他你' union all select \
            1,'他你' union all select 1,'他你，' union all select 1,'他，你你,' union all select 1,'他，，你' \
            union all select 1,'你'"
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name, separator="'\\U6211'")
    line1 = "select k1,e1 from %s lateral view explode_json_array_string(k3) tmp as e1 order by e1" % table_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k3', 2, database_name, table_name)
    line1 = "select k1,e1 from %s lateral view explode_split(k2, '，') tmp as e1 order by e1" % table_name
    line2 = "select 1,'' union all select 1,'你你,我。他他他。' union all select 1,'你我他abc你我他' \
            union all select 1,'你我他你我他你' union all select 1,'我他你我他'"
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name, separator="'\\uff0c'")
    line1 = "select k1,e1 from %s lateral view explode_split(k2, ',') tmp as e1 order by e1" % table_name
    line2 = "select 1,'你我他你我他你，我他你我他，，你我他abc你我他，你你' union all select 1,'我。他他他。'"
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name)
    runner.checkok('drop table if exists %s' % table_name)


def test_explode_split_separator():
    """
    {
    "title": "test_query_lateral_view:test_explode_split_separator",
    "describe": "explode_split函数分隔符测试，包括无分隔符，空字符串，转义字符tab，长字符串，数字",
    "tag": "p1,function"
    }
    """
    table_name = 'test_explode_split_separator'
    runner.checkok('drop table if exists %s' % table_name)
    column_list = [('k1', 'int'), ('k2', 'varchar(50)')]
    create(database_name, table_name, column_list)
    sql = "insert into %s values (1, 'a b'), (2, 'a\tb'), (3, 'axyzbxzb'), (4, 'a8b9c')" % table_name
    runner.checkok(sql)
    line = "select k1,e1 from %s lateral view explode_split(k2) tmp as e1 order by k1,e1" % table_name
    runner.checkwrong(line)
    line = "select k1,e1 from %s lateral view explode_split(k2, ) tmp as e1 order by k1,e1" % table_name
    runner.checkwrong(line)
    line1 = "select k1,e1 from %s lateral view explode_split(k2, '') tmp as e1 order by k1,e1" % table_name
    line2 = "select 1,' ' union all select 1,'a' union all select 1,'b' union all select 2,'\t' union all select 2,'a' \
            union all select 2,'b' union all select 3,'a' union all select 3,'b' union all select 3,'b' union all select \
            3,'x' union all select 3,'x' union all select 3,'y' union all select 3,'z' union all select 3,'z' union all \
            select 4,'8' union all select 4,'9' union all select 4,'a' union all select 4,'b' union all select 4,'c'"
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name, separator="''")
    line1 = "select k1,e1 from %s lateral view explode_split(k2, '\t') tmp as e1 order by k1,e1" % table_name
    line2 = "select 1,'a b' union all select 2,'a' union all select 2,'b' union all select 3,'axyzbxzb' union all \
            select 4,'a8b9c'"
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name, separator="'\\t'")
    line1 = "select k1,e1 from %s lateral view explode_split(k2, 'xyz') tmp as e1 order by k1,e1" % table_name
    line2 = "select 1,'a b' union all select 2,'a\tb' union all select 3,'a' union all select 3,'bxzb' union all \
            select 4,'a8b9c'"
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name, separator="'xyz'")
    line1 = "select k1,e1 from %s lateral view explode_split(k2, 8) tmp as e1 order by k1,e1" % table_name
    line2 = "select 1,'a b' union all select 2,'a\tb' union all select 3,'axyzbxzb' union all select 4,'a' union all \
            select 4,'b9c'"
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name, separator="8")
    runner.checkok('drop table if exists %s' % table_name)


def test_column_as_separator():
    """
    {
    "title": "test_query_lateral_view:test_column_as_separator",
    "describe": "explode_split函数进行行转列，将一列数据作为分隔符",
    "tag": "p1,function"
    }
    """
    table_name = 'test_column_as_separator'
    runner.checkok('drop table if exists %s' % table_name)
    column_list = [('k1', 'int'), ('k2', 'varchar(50)'), ('k3', 'varchar(50)')]
    create(database_name, table_name, column_list)
    sql = "insert into %s values (1,'xyz,abc,de',','),(2,'a,b',','),(3,'c,a,b','a')" % table_name
    runner.checkok(sql)
    line1 = "select k1,e1 from %s lateral view explode_split(k2,k3) tmp as e1 order by k1,e1" % table_name
    line2 = "select 1,'abc' union all select 1,'de' union all select 1,'xyz' union all select 2,'a' union all select \
            2,'b' union all select 3,',b' union all select 3,'c,"
    separator = '`default_cluster:%s`.`%s`.`k3`' % (database_name, table_name)
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name, separator)
    line1 = "select k3,e1 from %s lateral view explode_split(k2,k3) tmp as e1 order by k3,e1" % table_name
    line2 = "select ',','a' union all select ',','abc' union all select ',','b' union all select ',','de' union all \
            select ',','xyz' union all select 'a',',b' union all select 'a','c,'"
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name, separator)
    runner.checkok('drop table if exists %s' % table_name)


def test_multi_lateral_view_split():
    """
    {
    "title": "test_query_lateral_view:test_multi_lateral_view_split",
    "describe": "一个sql语句中包含2个lateral view结构",
    "tag": "p1,function"
    }
    """
    line1 = "select k1,e1,e2 from %s lateral view explode_split(k4, ',') tmp1 as e1 lateral view \
            explode_split(k4, ',') tmp2 as e2 order by k1,e1,e2" % table_name
    line2 = "select a.k1,a.k4,b.k4 from %s a join %s b on a.k3=b.k3 order by k1,a.k4,b.k4" \
            % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1,e1,e2 from %s lateral view explode_split(k4, ',') tmp1 as e1 lateral view \
            explode_json_array_string_outer(k5) tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k4,b.k5 from %s a join %s b on a.k3=b.k3 order by k1,k4,k5 limit 65535" \
            % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5')
    line1 = "select k1,e1,e2 from %s lateral view explode_split(k4, ',') tmp1 as e1 lateral view \
            explode_json_array_int(k6) tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k4,b.k6 from %s a join %s b on a.k3=b.k3 where a.k6 is not null and b.k6 is not null " \
            "order by k1,k4,k6 limit 65535" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    check_table_function_node(line1, 'explode_json_array_int', 'k6')
    line1 = "select k1,e1,e2 from %s lateral view explode_split(k4, ',') tmp1 as e1 lateral view \
            explode_json_array_double_outer(k7) tmp2 as e2 order by k1,e1,e2" % table_name
    line2 = "select a.k1,a.k4,b.k7 from %s a join %s b on a.k3=b.k3 order by k1,k4,k7" \
            % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7')
    line1 = "select k1,e1,e2 from %s lateral view explode_split(k4, ',') tmp1 as e1 lateral view \
            explode_bitmap(k9) tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k4,b.k8 from %s a join %s b on a.k3=b.k3 where a.k8 is not null and b.k8 is not null " \
            "order by k1,k4,k8 limit 65535" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    check_table_function_node(line1, 'explode_bitmap', 'k9')


def test_multi_lateral_view_json():
    """
    {
    "title": "test_query_lateral_view:test_multi_lateral_view_json",
    "describe": "一个sql语句中包含2个lateral view结构",
    "tag": "p1,function"
    }
    """
    # explode_json_array_string + lateral view
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_string_outer(k5) tmp1 as e1 lateral view \
            explode_split(k4, ',') tmp2 as e2 order by k1,e1,e2" % table_name
    line2 = "select a.k1,a.k5,b.k4 from %s a join %s b on a.k3=b.k3 order by k1,k5,k4" \
            % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5')
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_string_outer(k5) tmp1 as e1 lateral view \
            explode_json_array_string(k5) tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k5,b.k5 from %s a join %s b on a.k3=b.k3 where b.k6 is not null " \
            "order by k1,a.k5,b.k5 limit 65535" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5')
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_string_outer(k5) tmp1 as e1 lateral view \
            explode_json_array_int_outer(k6) tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k5,b.k6 from %s a join %s b on a.k3=b.k3 order by k1,k5,k6 limit 65535" \
            % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5')
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6')
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_string(k5) tmp1 as e1 lateral view \
            explode_json_array_double(k7) tmp2 as e2 order by k1,e1,e2" % table_name
    line2 = "select a.k1,a.k5,b.k7 from %s a join %s b on a.k3=b.k3 where a.k6 is not null and b.k7 is not null " \
            "order by k1,k5,k7" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5')
    check_table_function_node(line1, 'explode_json_array_double', 'k7')
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_string_outer(k5) tmp1 as e1 lateral view \
            explode_bitmap(k9) tmp2 as e2 order by k1,e1,e2" % table_name
    line2 = "select a.k1,a.k5,b.k8 from %s a join %s b on a.k3=b.k3 where b.k8 is not null " \
            "order by k1,k5,k8" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5')
    check_table_function_node(line1, 'explode_bitmap', 'k9')
    # explode_json_array_int + lateral view
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_int(k6) tmp1 as e1 lateral view \
            explode_split(k4, ',') tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k6,b.k4 from %s a join %s b on a.k3=b.k3 where a.k6 is not null " \
            "order by k1,k6,k4 limit 65535" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6')
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_int_outer(k6) tmp1 as e1 lateral view \
            explode_json_array_string(k5) tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k6,b.k5 from %s a join %s b on a.k3=b.k3 where b.k6 is not null " \
            "order by k1,k6,k5 limit 65535" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6')
    check_table_function_node(line1, 'explode_json_array_string', 'k5')
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_int(k6) tmp1 as e1 lateral view \
            explode_json_array_int_outer(k6) tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k6,b.k6 from %s a join %s b on a.k3=b.k3 where a.k6 is not null " \
            "order by k1,a.k6,b.k6 limit 65535" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6')
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_int_outer(k6) tmp1 as e1 lateral view \
            explode_json_array_double_outer(k7) tmp2 as e2 order by k1,e1,e2" % table_name
    line2 = "select a.k1,a.k6,b.k7 from %s a join %s b on a.k3=b.k3 order by k1,k6,k7" \
            % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6')
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7')
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_int(k6) tmp1 as e1 lateral view \
            explode_bitmap(k9) tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k6,b.k8 from %s a join %s b on a.k3=b.k3 where a.k6 is not null and b.k8 is not null "\
            "order by k1,k6,k8 limit 65535" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6')
    check_table_function_node(line1, 'explode_bitmap', 'k9')
    # explode_json_array_double + lateral view
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_double(k7) tmp1 as e1 lateral view \
            explode_split(k4, ',') tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k7,b.k4 from %s a join %s b on a.k3=b.k3 where a.k7 is not null " \
            "order by k1,k7,k4 limit 65535" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7')
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_double_outer(k7) tmp1 as e1 lateral view \
            explode_json_array_string_outer(k5) tmp2 as e2 order by k1,e1,e2" % table_name
    line2 = "select a.k1,a.k7,b.k5 from %s a join %s b on a.k3=b.k3 order by k1,k7,k5" \
            % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7')
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5')
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_double_outer(k7) tmp1 as e1 lateral view \
            explode_json_array_int(k6) tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k7,b.k6 from %s a join %s b on a.k3=b.k3 where b.k7 is not null " \
            "order by k1,k7,k6 limit 65535" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7')
    check_table_function_node(line1, 'explode_json_array_int', 'k6')
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_double(k7) tmp1 as e1 lateral view \
            explode_json_array_double_outer(k7) tmp2 as e2 order by k1,e1,e2" % table_name
    line2 = "select a.k1,a.k7,b.k7 from %s a join %s b on a.k3=b.k3 where b.k7 is not null " \
            "order by k1,a.k7,b.k7" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7')
    line1 = "select k1,e1,e2 from %s lateral view explode_json_array_double_outer(k7) tmp1 as e1 lateral view \
            explode_bitmap_outer(k9) tmp2 as e2 order by k1,e1,e2" % table_name
    line2 = "select a.k1,a.k7,b.k8 from %s a join %s b on a.k3=b.k3 order by k1,k7,k8" \
            % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7')
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9')


def test_multi_lateral_view_bitmap():
    """
    {
    "title": "test_query_lateral_view:test_multi_lateral_view_bitmap",
    "describe": "一个sql语句中包含2个lateral view结构",
    "tag": "p1,function"
    }
    """
    # explode_bitmap + lateral view
    line1 = "select k1,e1,e2 from %s lateral view explode_bitmap_outer(k9) tmp1 as e1 lateral view \
            explode_split(k4, ',') tmp2 as e2 order by k1,e1,e2" % table_name
    line2 = "select a.k1,a.k8,b.k4 from %s a join %s b on a.k3=b.k3 order by k1,k8,k4" \
            % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9')
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1,e1,e2 from %s lateral view explode_bitmap(k9) tmp1 as e1 lateral view \
            explode_json_array_string_outer(k5) tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k8,b.k5 from %s a join %s b on a.k3=b.k3 where a.k8 is not null " \
            "order by k1,k8,k5 limit 65535" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap', 'k9')
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5')
    line1 = "select k1,e1,e2 from %s lateral view explode_bitmap(k9) tmp1 as e1 lateral view \
            explode_json_array_int(k6) tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k8,b.k6 from %s a join %s b on a.k3=b.k3 where a.k8 is not null and b.k6 is not null " \
            " order by k1,k8,k6 limit 65535" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap', 'k9')
    check_table_function_node(line1, 'explode_json_array_int', 'k6')
    line1 = "select k1,e1,e2 from %s lateral view explode_bitmap_outer(k9) tmp1 as e1 lateral view \
            explode_json_array_double_outer(k7) tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k8,b.k7 from %s a join %s b on a.k3=b.k3 order by k1,k8,k7 limit 65535" \
            % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9')
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7')
    line1 = "select k1,e1,e2 from %s lateral view explode_bitmap(k9) tmp1 as e1 lateral view \
            explode_bitmap_outer(k9) tmp2 as e2 order by k1,e1,e2 limit 65535" % table_name
    line2 = "select a.k1,a.k8,b.k8 from %s a join %s b on a.k3=b.k3 where a.k8 is not null " \
            "order by k1,a.k8,b.k8 limit 65535" % (check_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap', 'k9')


def test_many_lateral_view():
    """
    {
    "title": "test_query_lateral_view:test_many_lateral_view",
    "describe": "一个sql语句中包含多个lateral view结构",
    "tag": "p1,function"
    }
    """
    table_name = 'test_many_lateral_view'
    check_name = table_name + '_check'
    runner.checkok('drop table if exists %s' % table_name)
    runner.checkok('drop table if exists %s' % check_name)
    column_list = [('k1', 'int'), ('k2', 'int'), ('k3', 'int'), ('k4', 'varchar(500)'), ('k5', 'varchar(500)'), \
            ('k6', 'varchar(500)'), ('k7', 'varchar(500)'), ('k8', 'varchar(500)'), ('k9', 'bitmap bitmap_union')]
    column_list_2 = [('k1', 'int'), ('k2', 'int'), ('k3', 'int'), ('k4', 'varchar(50)'), ('k5', 'varchar(50)'), \
            ('k6', 'bigint'), ('k7', 'double'), ('k8', 'bigint')]
    create(database_name, table_name, column_list)
    create(database_name, check_name, column_list_2)
    sql = "insert into %s select * from test_lateral_view.lateral_view_data where k3 = 2 or k3 = 4" % table_name
    runner.checkok(sql)
    sql = "insert into %s select * from test_lateral_view.lateral_view_check where k3 = 2 or k3 = 4" % check_name
    runner.checkok(sql)
    line1 = "select k1,e1,e2,e3,e4,e5,e6,e7,e8,e9 from %s lateral view explode_split(k4,',') tmp1 as e1 lateral view \
        explode_json_array_string(k5) tmp2 as e2 lateral view explode_json_array_int(k6) tmp3 as e3 lateral view \
        explode_json_array_double(k7) tmp4 as e4 lateral view explode_json_array_double(k7) tmp5 as e5 lateral view \
        explode_json_array_int(k6) tmp6 as e6 lateral view explode_json_array_string(k5) tmp7 as e7 lateral view \
        explode_split(k4, ',') tmp8 as e8 lateral view explode_bitmap(k9) tmp9 as e9 order by \
        k1,e1,e2,e3,e4,e5,e6,e7,e8,e9" % table_name
    line2 = "select a.k1,a.k4,b.k5,c.k6,d.k7,e.k7,f.k6,g.k5,h.k4,i.k8 from {0} a join {0} b join {0} c join {0} d join \
        {0} e join {0} f join {0} g join {0} h join {0} i on a.k3=b.k3 and b.k3=c.k3 and c.k3=d.k3 and d.k3=e.k3 and \
        e.k3=f.k3 and f.k3=g.k3 and g.k3=h.k3 and h.k3=i.k3 order by a.k1,a.k4,b.k5,c.k6,d.k7,e.k7,f.k6,g.k5,h.k4, \
        i.k8".format(check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', 2, database_name, table_name)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', 2, database_name, table_name)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', 2, database_name, table_name)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', 2, database_name, table_name)
    check_table_function_node(line1, 'explode_bitmap', 'k9', 2, database_name, table_name)
    runner.checkok('drop table if exists %s' % table_name)
    runner.checkok('drop table if exists %s' % check_name)


def test_lateral_view_after_function():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_after_function",
    "describe": "对列执行函数操作，再进行行转列",
    "tag": "p1,function"
    }
    """
    line1 = "select k1,e1 from %s lateral view explode_split(reverse(k4), ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,reverse(k4) as k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split(reverse', 'k4')
    line1 = "select k1,e1 from %s lateral view explode_json_array_string(replace(k5, 'a', 'b')) tmp as e1 order by \
            k1,e1" % table_name
    line2 = "select k1,replace(k5, 'a', 'b') as k5 from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string(replace', 'k5')
    line1 = "select k1,e1 from %s lateral view explode_json_array_int_outer(replace(k6, '-', '')) tmp as e1 " \
            "order by k1,e1" % table_name
    line2 = "select k1,cast(abs(k6) as bigint) as k6 from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer(replace', 'k6')
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(replace(k7, '-','')) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,cast(abs(k7) as double) as k7 from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double(replace', 'k7')


def test_function_after_lateral_view():
    """
    {
    "title": "test_query_lateral_view:test_function_after_lateral_view",
    "describe": "执行行转列后，对新列执行函数操作",
    "tag": "p1,function"
    }
    """
    line1 = "select k1,lcase(e1) from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,lcase(k4) from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1,md5(e1) from %s lateral view explode_split(k4, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,md5(k4) from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1,reverse(e1) from %s lateral view explode_json_array_string(k5) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,reverse(k5) from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5')
    line1 = "select k1,replace(e1, 'a', 'b') from %s lateral view explode_json_array_string_outer(k5) tmp as e1 " \
            "order by k1,e1" % table_name
    line2 = "select k1,replace(k5, 'a', 'b') from %s order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5')
    line1 = "select k1,conv(e1,10,16) from %s lateral view explode_json_array_int_outer(k6) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,conv(k6,10,16) from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6')
    line1 = "select k1,abs(e1) from %s lateral view explode_json_array_int(k6) tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,abs(k6) from %s where k6 is not null order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6')
    line1 = "select k1,ceil(e1) from %s lateral view explode_json_array_double(k7) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,ceil(k7) from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7')
    line1 = "select k1,bin(e1) from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1 " \
            "order by k1,e1" % table_name
    line2 = "select k1,bin(k8) from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8')
    line1 = "select k1,minutes_add('2000-01-01',e1) from %s lateral view explode_bitmap(k9) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,minutes_add('2000-01-01',k8) from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap', 'k9')


def test_function_with_lateral_view_subquery():
    """
    {
    "title": "test_query_lateral_view:test_function_with_lateral_view_subquery",
    "describe": "行转列的结果作为子查询，再进行函数操作",
    "tag": "p1,function"
    }
    """
    line1 = "select k1,lcase(e1) from (select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1) a \
            order by k1,e1" % table_name
    line2 = "select k1,lcase(k4) from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1,md5(e1) from (select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1) a \
            order by k1,e1" % table_name
    line2 = "select k1,md5(k4) from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1,reverse(e1) from (select k1,e1 from %s lateral view explode_json_array_string(k5) tmp as e1) a \
            order by k1,e1" % table_name
    line2 = "select k1,reverse(k5) from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5')
    line1 = "select k1,replace(e1, 'a', 'b') from (select k1,e1 from %s lateral view explode_json_array_string_outer(k5) \
            tmp as e1) a order by k1,e1" % table_name
    line2 = "select k1,replace(k5, 'a', 'b') from %s order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5')
    line1 = "select k1,conv(e1,10,16) from (select k1,e1 from %s lateral view explode_json_array_int(k6) tmp as e1) a \
            order by k1,e1" % table_name
    line2 = "select k1,conv(k6,10,16) from %s where k6 is not null order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6')
    line1 = "select k1,abs(e1) from (select k1,e1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1) a \
            order by k1,e1" % table_name
    line2 = "select k1,abs(k6) from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6')
    line1 = "select k1,ceil(e1) from (select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1) a \
            order by k1,e1" % table_name
    line2 = "select k1,ceil(k7) from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7')
    line1 = "select k1,bin(e1) from (select k1,e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp \
            as e1) a order by k1,e1" % table_name
    line2 = "select k1,bin(k8) from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8')
    line1 = "select k1,minutes_add('2000-01-01',e1) from (select k1,e1 from %s lateral view explode_bitmap(k9) tmp \
            as e1) a order by k1,e1" % table_name
    line2 = "select k1,minutes_add('2000-01-01',k8) from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap', 'k9')


def test_lateral_view_for_subquery():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_for_subquery",
    "describe": "对子查询执行行转列操作",
    "tag": "p1,function"
    }
    """
    line1 = "select k1,e1 from (select k1,k4 from %s) a lateral view explode_split(k4, ',') tmp as e1" % table_name
    line2 = "select k1,k4 from %s" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_split(`k4`, ',')", check=True)
    line1 = "select k1,e1 from (select k1,k5 from %s) a lateral view explode_json_array_string_outer(k5) tmp as e1" \
            % table_name
    line2 = "select k1,k5 from %s" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_string_outer(`k5`)", check=True)
    line1 = "select k1,e1 from (select k1,k6 from %s) a lateral view explode_json_array_int(k6) tmp as e1" \
            % table_name
    line2 = "select k1,k6 from %s where k6 is not null" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_int(`k6`)", check=True)
    line1 = "select k1,e1 from (select k1,k7 from %s) a lateral view explode_json_array_double_outer(k7) tmp as e1" \
            % table_name
    line2 = "select k1,k7 from %s" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_double_outer(`k7`)", check=True)
    line1 = "select k1,e1 from (select k1,k8 from %s) a lateral view explode_bitmap(bitmap_from_string(k8)) tmp \
            as e1" % table_name
    line2 = "select k1,k8 from %s where k8 is not null" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_bitmap(bitmap_from_string(`k8`))", check=True)
    line1 = "select k1,e1 from (select k1,k9 from %s) a lateral view explode_bitmap_outer(k9) tmp as e1" % table_name
    line2 = "select k1,k8 from %s" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_bitmap_outer(`k9`)", check=True)


def test_lateral_view_in_subquery():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_in_subquery",
    "describe": "子查询中包含行转列操作",
    "tag": "p1,function"
    }
    """
    line1 = "select k1,e1 from (select k1,e1 from %s lateral view explode_split_outer(k4, ',') tmp as e1) a \
            order by k1,e1" % table_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split_outer', 'k4')
    line1 = "select k1,e1 from (select k1,e1 from %s lateral view explode_json_array_string(k5) tmp as e1) a \
            order by k1,e1" % table_name
    line2 = "select k1,k5 from %s where k6 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5')
    line1 = "select k1,e1 from (select k1,e1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1) a \
            order by k1,e1" % table_name
    line2 = "select k1,k6 from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6')
    line1 = "select k1,e1 from (select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1) a \
            order by k1,e1" % table_name
    line2 = "select k1,k7 from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7')
    line1 = "select k1,e1 from (select k1,e1 from %s lateral view explode_bitmap_outer(bitmap_from_string(k8)) tmp as e1) a \
            order by k1,e1" % table_name
    line2 = "select k1,k8 from %s order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8')
    line1 = "select k1,e1 from (select k1,e1 from %s lateral view explode_bitmap(k9) tmp as e1) a \
            order by k1,e1" % table_name
    line2 = "select k1,k8 from %s where k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap', 'k9')


def test_normal_column_operate():
    """
    {
    "title": "test_query_lateral_view:test_normal_column_operate",
    "describe": "行转列语句包含对普通列的聚合操作",
    "tag": "p1,function"
    }
    """
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1  where k1 > 0 \
            order by k1,e1" % table_name
    line2 = "select k1,k4 from %s where k1 > 0 order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1,e1 from %s lateral view explode_split_outer(k4, ',') tmp as e1  having k1 < 0 \
            order by k1,e1" % table_name
    line2 = "select k1,k4 from %s having k1 < 0 order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split_outer', 'k4')
    line1 = "select k1,max(e1) from %s lateral view explode_json_array_string(k5) tmp as e1 group by k1 \
            order by k1" % table_name
    line2 = "select k1,max(k5) from %s where k6 is not null group by k1 order by k1" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5')
    line1 = "select max(k3),k2,max(e1) from %s lateral view explode_json_array_int_outer(k6) tmp as e1 group by k2 \
            order by k2" % table_name
    line2 = "select max(k3),k2,max(k6) from %s group by k2 order by k2" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', frag_no=3)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1 where k1 > 100 and k2 <0 \
            order by k1,e1" % table_name
    line2 = "select k1,k7 from %s where k1 > 100 and k2 <0 and k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7')
    line1 = "select k1,e1 from %s lateral view explode_bitmap_outer(k9) tmp as e1  where mod(k1,2)=0 \
            order by k1,e1 desc" % table_name
    line2 = "select k1,k8 from %s where mod(k1,2)=0 order by k1,k8 desc" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9')


def test_lateral_view_column_operate():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_column_operate",
    "describe": "行转列语句中包含对行转列的结果列的聚合操作",
    "tag": "p1,function"
    }
    """
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 group by k1,e1 \
            order by k1,e1" % table_name
    line2 = "select k1,k4 from %s group by k1,k4 order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 where length(e1) > 10 \
            order by k1,e1" % table_name
    line2 = "select k1,k4 from %s where length(k4) > 10 order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1,e1 from %s lateral view explode_json_array_string(k5) tmp as e1 where length(e1) > 10 \
            order by k1,e1" % table_name
    line2 = "select k1,k5 from %s where length(k5) > 10 order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5')
    line1 = "select k1,e1 from %s lateral view explode_json_array_int(k6) tmp as e1 where length(e1) < 5 \
            order by k1,e1" % table_name
    line2 = "select k1,k6 from %s where length(k6) < 5 order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6')
    line1 = "select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1 where abs(e1)>123456 \
            order by k1,e1" % table_name
    line2 = "select k1,k7 from %s where abs(k7)>123456 order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7')
    line1 = "select k1,e1 from %s lateral view explode_bitmap(k9) tmp as e1 where left(cast(e1 as varchar), 1)='1' \
            order by k1,e1" % table_name
    line2 = "select k1,k8 from %s where left(cast(k8 as varchar), 1)='1' order by k1,k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap', 'k9')


def test_agg_after_lateral_view():
    """
    {
    "title": "test_query_lateral_view:test_agg_after_lateral_view",
    "describe": "行转列sql包含子查询，再进行聚合操作",
    "tag": "p1,function,fuzz"
    }
    """
    line1 = "select k1,e1 from (select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1) a group by k1,e1 \
            order by k1,e1" % table_name
    line2 = "select k1,k4 from %s group by k1,k4 order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select k1,e1 from (select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1 group by k1,e1) a \
            order by k1,e1" % table_name
    line2 = "select k1,k4 from %s group by k1,k4 order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select max(e1) from (select k2,e1 from %s lateral view explode_split(k4, ',') tmp as e1)a group by k2 \
            order by k2" % table_name
    line2 = "select max(k4) from %s group by k2 order by k2" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', frag_no=3)
    line1 = "select max(k1),e1 from %s lateral view explode_split(k4, ',') tmp as e1 group by e1 \
            order by e1" % table_name
    line2 = "select max(k1),k4 from %s group by k4 order by k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', frag_no=3)
    line1 = "select k1,e1 from (select k1,e1 from %s lateral view explode_json_array_string(k5) tmp as e1) a \
            group by k1,e1 order by k1,e1" % table_name
    line2 = "select k1,k5 from %s where k6 is not null group by k1,k5 order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5')
    line1 = "select k1,e1 from " \
            "(select k1,e1 from %s lateral view explode_json_array_int_outer(k6) tmp as e1 group by k1,e1) " \
            "a order by k1,e1" % table_name
    line2 = "select k1,k6 from %s group by k1,k6 order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6')
    line1 = "select max(e1) from (select k2,e1 from %s lateral view explode_json_array_double(k7) tmp as e1)a \
            group by k2 order by k2" % table_name
    line2 = "select max(k7) from %s where k7 is not null group by k2 order by k2" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', frag_no=3)
    line1 = "select max(k1),e1 from %s lateral view explode_bitmap_outer(k9) tmp as e1 group by e1 order by e1" \
            % table_name
    line2 = "select max(k1),k8 from %s group by k8 order by k8" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9', frag_no=3)
    line = "select max(k1),e1 from (select k1,k2,k4 from %s)a lateral view explode_split(k4, ',') tmp as e1 group by e1 \
            order by e1" % table_name
    runner.checkwrong(line)
    line = "select max(k1),e1 from (select k1,k2,k5 from %s)a lateral view explode_json_array_string(k5) tmp as e1 \
            group by e1 order by e1" % table_name
    runner.checkwrong(line)
    line = "select max(k1),e1 from (select k1,k2,k6 from %s)a lateral view explode_json_array_int(k6) tmp as e1 \
            group by e1 order by e1" % table_name
    runner.checkwrong(line)
    line = "select max(k1),e1 from (select k1,k2,k7 from %s)a lateral view explode_json_array_double(k7) tmp as e1 \
            group by e1 order by e1" % table_name
    runner.checkwrong(line)
    line = "select max(k1),e1 from (select k1,k2,k9 from %s)a lateral view explode_bitmap(k9) tmp as e1 group by e1 \
            order by e1" % table_name
    runner.checkwrong(line)


def test_lateral_view_after_join():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_after_join",
    "describe": "对join后的表执行行转列操作",
    "tag": "p1,function"
    }
    """
    join_name = "lateral_view_join"
    runner.checkok('drop table if exists %s' % join_name)
    column_list = [('k1', 'int'), ('k2', 'int')]
    create(database_name, join_name, column_list)
    sql = "insert into %s values (1,1),(2,4),(3,8),(4,16),(5,32),(6,64),(7,128),(8,256),(9,512),(10,1024),(11,1),\
            (12,2),(13,3),(14,4),(15,5),(16,6)" % join_name
    runner.checkok(sql)
    line1 = "select k1,k3,e1 from (select a.k1,a.k3,a.k4 from %s a join %s b on a.k3=b.k1)t lateral view \
            explode_split(k4, ',') tmp as e1" % (table_name, join_name)
    line2 = "select a.k1,a.k3,a.k4 from %s a join %s b on a.k3=b.k1" % (check_name, join_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_split", check=True)
    line1 = "select k1,k3,e1 from (select a.k1,b.k3,a.k4 from %s a join %s b on a.k4=b.k4 and a.k3=b.k3)t lateral view \
            explode_split(k4, ',') tmp as e1" % (table_name, table_name)
    line2 = "select k1,k3,k4 from %s" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_split", check=True)
    line1 = "select k1,k3,e1 from (select a.k1,a.k3,a.k5 from %s a left join %s b on a.k3=b.k1)t lateral view \
            explode_json_array_string_outer(k5) tmp as e1" % (table_name, join_name)
    line2 = "select a.k1,a.k3,a.k5 from %s a left join %s b on a.k3=b.k1" % (check_name, join_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_string_outer", check=True)
    line1 = "select k1,k3,e1 from (select a.k1,b.k3,a.k5 from %s a left join %s b on a.k5=b.k5 and a.k3=b.k3)t \
            lateral view explode_json_array_string(k5) tmp as e1" % (table_name, table_name)
    line2 = "select k1,k3,k5 from %s where k6 is not null" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_string", check=True)
    line1 = "select k1,k3,e1 from (select a.k1,a.k3,a.k6 from %s a right join %s b on a.k3=b.k1)t lateral view \
            explode_json_array_int_outer(k6) tmp as e1" % (table_name, join_name)
    line2 = "select a.k1,a.k3,a.k6 from %s a right join %s b on a.k3=b.k1" % (check_name, join_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_int_outer", check=True)
    line1 = "select k1,k3,cast(e1 as bigint) from (select a.k1,b.k3,a.k6 from %s a right join %s b on a.k6=b.k6 \
            and a.k3=b.k3)t lateral view explode_json_array_int(k6) tmp as e1" % (table_name, table_name)
    line2 = "select b.k1,b.k3,b.k6 from %s a right join %s b on a.k6=b.k6 where b.k6 is not null" \
            % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_int", check=True)
    line1 = "select k1,k3,e1 from (select a.k1,a.k3,a.k7 from %s a inner join %s b on a.k3=b.k1)t lateral view \
            explode_json_array_double(k7) tmp as e1" % (table_name, join_name)
    line2 = "select a.k1,a.k3,a.k7 from %s a inner join %s b on a.k3=b.k1" % (check_name, join_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_double", check=True)
    line1 = "select k1,k3,e1 from (select a.k1,b.k3,a.k7 from %s a inner join %s b on a.k7=b.k7 and a.k3=b.k3)t \
            lateral view explode_json_array_double_outer(k7) tmp as e1" % (table_name, table_name)
    line2 = "select k1,k3,k7 from %s" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_double_outer", check=True)
    line1 = "select k1,k3,e1 from (select a.k1,a.k3,a.k9 from %s a cross join %s b)t lateral view \
            explode_bitmap(k9) tmp as e1" % (table_name, join_name)
    line2 = "select a.k1,a.k3,a.k8 from %s a cross join %s b where a.k8 is not null" \
            % (check_name, join_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_bitmap", check=True)
    runner.checkok('drop table if exists %s' % join_name)


def test_join_after_lateral_view():
    """
    {
    "title": "test_query_lateral_view:test_join_after_lateral_view",
    "describe": "先执行行转列，再执行join，join的列为普通列",
    "tag": "p1,function"
    }
    """
    join_name = "lateral_view_join"
    runner.checkok('drop table if exists %s' % join_name)
    column_list = [('k1', 'int'), ('k2', 'int')]
    create(database_name, join_name, column_list)
    sql = "insert into %s values (1,1),(2,4),(3,8),(4,16),(5,32),(6,64),(7,128),(8,256),(9,512),(10,999),(11,1),\
            (12,2),(13,3),(14,4),(15,5),(16,6)" % join_name
    runner.checkok(sql)
    line1 = "select a.k1,a.k3,a.e1 from (select k1,k3,e1 from %s lateral view explode_split(k4,',') tmp as e1) a \
            join %s b on a.k3=b.k1 order by a.k1,a.k3,a.e1" % (table_name, join_name)
    line2 = "select a.k1,a.k3,a.k4 from %s a join %s b on a.k3=b.k1 order by a.k1,a.k3,a.k4" % (check_name, join_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select a.k1,a.k3,a.e1 from " \
            "(select k1,k3,e1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1) "\
            "a left join %s b on a.k3=b.k1 order by a.k1,a.k3,a.e1" % (table_name, join_name)
    line2 = "select a.k1,a.k3,a.k5 from %s a left join %s b on a.k3=b.k1 order by a.k1,a.k3,a.k5" \
            % (check_name, join_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5')
    line1 = "select a.k1,b.k2,a.e1 from (select k1,k3,e1 from %s lateral view explode_split(k4,',') tmp as e1) a \
            right join %s b on a.k3=b.k2 order by a.k1,b.k2,a.e1" % (table_name, join_name)
    line2 = "select a.k1,b.k2,a.k4 from %s a right join %s b on a.k3=b.k2 order by a.k1,b.k2,a.k4" \
            % (check_name, join_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', frag_no=4)
    line1 = "select a.k1,b.k2,a.e1 from (select k1,k3,e1 from %s lateral view explode_json_array_int(k6) tmp as e1) a \
            right join %s b on a.k3=b.k2 order by a.k1,b.k2,a.e1" % (table_name, join_name)
    line2 = "select a.k1,b.k2,a.k6 from %s a right join %s b on a.k3=b.k2 order by a.k1,b.k2,a.k6" \
            % (check_name, join_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', frag_no=4)
    line1 = "select a.k1,b.k2,a.e1 from (select k1,k3,e1 from %s lateral view explode_json_array_double(k7) tmp as e1) \
            a inner join %s b on a.k3=b.k2 order by a.k1,b.k2,a.e1" % (table_name, join_name)
    line2 = "select a.k1,b.k2,a.k7 from %s a inner join %s b on a.k3=b.k2 order by a.k1,b.k2,a.k7" \
            % (check_name, join_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7')
    line1 = "select a.k1,b.k2,a.e1 from (select k1,k3,e1 from %s lateral view explode_bitmap(bitmap_from_string(k8)) \
            tmp as e1) a cross join %s b order by a.k1,b.k2,a.e1" % (table_name, join_name)
    line2 = "select a.k1,b.k2,a.k8 from %s a cross join %s b where a.k8 is not null order by a.k1,b.k2,a.k8" \
            % (check_name, join_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap(bitmap_from_string', 'k8')
    line1 = "select b.k1,a.k2,b.e1 from %s a join (select k1,k3,e1 from %s lateral view explode_bitmap(k9) tmp as e1) \
            b on a.k2=b.k3 order by b.k1,a.k2,b.e1" % (join_name, table_name)
    line2 = "select b.k1,a.k2,b.k8 from %s a join %s b on a.k2=b.k3 order by b.k1,a.k2,b.k8" % (join_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap', 'k9', frag_no=3)
    line1 = "select a.k1,a.k2,b.e1 from %s a left join (select k1,k3,e1 from %s lateral view explode_split(k4, ',') tmp \
            as e1) b on a.k2=b.k3 order by a.k1,a.k2,b.e1" % (join_name, table_name)
    line2 = "select a.k1,a.k2,b.k4 from %s a left join %s b on a.k2=b.k3 order by a.k1,a.k2,b.k4" \
            % (join_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4', frag_no=3)
    line1 = "select a.k1,b.k3,b.e1 from %s a right join (select k1,k3,e1 from %s lateral view \
            explode_json_array_string(k5) tmp as e1) b on a.k2=b.k3 order by a.k1,b.k3,b.e1" % (join_name, table_name)
    line2 = "select a.k1,b.k3,b.k5 from %s a right join %s b on a.k2=b.k3 where b.k6 is not null " \
            "order by a.k1,b.k3,b.k5" % (join_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', frag_no=3)
    line1 = "select a.k1,a.k2,b.e1 from %s a inner join (select k1,k3,e1 from %s lateral view \
            explode_json_array_int(k6) tmp as e1) b on a.k2=b.k3 order by a.k1,a.k2,b.e1" % (join_name, table_name)
    line2 = "select a.k1,a.k2,b.k6 from %s a inner join %s b on a.k2=b.k3 order by a.k1,a.k2,b.k6" \
            % (join_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', frag_no=3)
    line1 = "select a.k1,a.k2,b.e1 from %s a cross join (select k1,k3,e1 from %s lateral view \
            explode_json_array_double_outer(k7) tmp as e1) b order by a.k1,a.k2,b.e1" % (join_name, table_name)
    line2 = "select a.k1,a.k2,b.k7 from %s a cross join %s b order by a.k1,a.k2,b.k7" % (join_name, check_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double_outer', 'k7', frag_no=3)
    runner.checkok('drop table if exists %s' % join_name)


def test_join_after_lateral_view_column():
    """
    {
    "title": "test_query_lateral_view:test_join_after_lateral_view",
    "describe": "先执行行转列，再执行join，join的列为行转列生成的列",
    "tag": "p1,function"
    }
    """
    join_name = "lateral_view_join_2"
    runner.checkok('drop table if exists %s' % join_name)
    column_list = [('k1', 'int'), ('k2', 'int'), ('k3', 'int'), ('k4', 'varchar(50)'), ('k5', 'varchar(50)'), \
            ('k6', 'bigint'), ('k7', 'double'), ('k8', 'bigint')]
    create(database_name, join_name, column_list)
    sql = "insert into %s select * from %s where k3 < 10" % (join_name, check_name)
    runner.checkok(sql)
    line1 = "select a.k1,a.e1 from (select k1,e1 from %s lateral view explode_split(k4,',') tmp as e1) a \
            join %s b on a.e1=b.k4 order by a.k1,a.e1" % (table_name, join_name)
    line2 = "select a.k1,a.k4 from %s a join %s b on a.k4=b.k4 order by a.k1,a.k4" % (check_name, join_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "select a.k1,a.e1 from " \
            "(select k1,k3,e1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1) a "\
            "left join %s b on a.e1=b.k5 and a.k3<10 order by a.k1,a.e1" % (table_name, join_name)
    line2 = "select a.k1,a.k5 from %s a left join %s b on a.k5=b.k5 and a.k3<10 order by a.k1,a.k5" \
            % (check_name, join_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5')
    line1 = "select a.k1,b.k6 from (select k1,e1 from %s lateral view explode_json_array_int(k6) tmp as e1) a \
            right join %s b on a.e1=b.k6 order by a.k1,b.k6" % (table_name, join_name)
    line2 = "select a.k1,b.k6 from %s a right join %s b on a.k6=b.k6 order by a.k1,b.k6" % (check_name, join_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', frag_no=4)
    line1 = "select a.k1,a.e1 from (select k1,e1 from %s lateral view explode_json_array_double(k7) tmp as e1) a \
            inner join %s b on a.e1=b.k7 order by a.k1,a.e1" % (table_name, join_name)
    line2 = "select a.k1,a.k7 from %s a right join %s b on a.k7=b.k7 order by a.k1,a.k7" % (check_name, join_name)
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k7')
    runner.checkok('drop table if exists %s' % join_name)


def test_lateral_view_multi():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_multi",
    "describe": "行转列中嵌套行转列",
    "tag": "p1,function"
    }
    """
    table_name = 'lateral_view_multi_data'
    check_name = 'lateral_view_multi_check'
    line1 = "select k1,e2 from (select k1,e1 from %s lateral view explode_split(k4, ',') tmp1 as e1) a lateral view \
            explode_split(e1, '-') tmp2 as e2" % table_name
    line2 = "select k1,k4 from %s" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_split(`e1`, '-')", check=True)
    line1 = "select k1,e2 from (select k1,e1 from %s lateral view explode_split(k5, '-')tmp1 as e1) a lateral view \
            explode_bitmap(bitmap_from_string(e1)) tmp2 as e2" % table_name
    line2 = "select k1,k5 from %s where k5 is not null" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_bitmap(bitmap_from_string(`e1`))", check=True)
    line1 = "select k1,e2 from (select k1,e1 from %s lateral view explode_split(k6, '|')tmp1 as e1) a lateral view \
            explode_json_array_int_outer(e1) tmp2 as e2" % table_name
    line2 = "select k1,k6 from %s" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_int_outer(`e1`)", check=True)
    line1 = "select k1,e2 from (select k1,e1 from %s lateral view explode_split(k7, '|')tmp1 as e1) a lateral view \
            explode_json_array_double(e1) tmp2 as e2" % table_name
    line2 = "select k1,k7 from %s where k7 is not null" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_double(`e1`)", check=True)
    line1 = "select k1,e2 from (select k1,e1 from %s lateral view explode_json_array_string_outer(k8) tmp1 as e1) a \
            lateral view explode_split_outer(e1, '-') tmp2 as e2" % table_name
    line2 = "select k1,k4 from %s" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_split_outer(`e1`, '-')", check=True)

    current_table = 'lateral_view_current'
    column_list = [('k1', 'int'), ('k2', 'varchar(400)')]
    runner.checkok('drop table if exists %s' % current_table)
    create(database_name, current_table, column_list)
    line = "insert into %s select k1,e1 from %s lateral view explode_split_outer(k4, ',')tmp1 as e1" \
            % (current_table, table_name)
    runner.checkok(line)
    line1 = "select k1,e2 from %s lateral view explode_split(k2, '-') tmp2 as e2 order by k1,e2" % current_table
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k2', 2, table_name=current_table, separator="'-'")

    runner.checkok('drop table if exists %s' % current_table)
    create(database_name, current_table, column_list)
    line = "insert into %s select k1,e1 from %s lateral view explode_split(k5, '-')tmp1 as e1" \
            % (current_table, table_name)
    runner.checkok(line)
    line1 = "select k1,e2 from %s lateral view explode_bitmap(bitmap_from_string(k2)) tmp2 as e2 order by k1,e2" \
            % current_table
    line2 = "select k1,k5 from %s where k5 is not null order by k1,k5" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_bitmap(bitmap_from_string', 'k2', 2, table_name=current_table)

    runner.checkok('drop table if exists %s' % current_table)
    create(database_name, current_table, column_list)
    line = "insert into %s select k1,e1 from %s lateral view explode_split(k6, '|')tmp1 as e1" \
            % (current_table, table_name)
    runner.checkok(line)
    line1 = "select k1,e2 from %s lateral view explode_json_array_int_outer(k2) tmp2 as e2 order by k1,e2" \
            % current_table
    line2 = "select k1,k6 from %s order by k1,k6" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k2', 2, table_name=current_table)

    runner.checkok('drop table if exists %s' % current_table)
    create(database_name, current_table, column_list)
    line = "insert into %s select k1,e1 from %s lateral view explode_split(k7, '|')tmp1 as e1" \
            % (current_table, table_name)
    runner.checkok(line)
    line1 = "select k1,e2 from %s lateral view explode_json_array_double(k2) tmp2 as e2 order by k1,e2" % current_table
    line2 = "select k1,k7 from %s where k7 is not null order by k1,k7" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_double', 'k2', 2, table_name=current_table)

    runner.checkok('drop table if exists %s' % current_table)
    create(database_name, current_table, column_list)
    line = "insert into %s select k1,e1 from %s lateral view explode_json_array_string_outer(k8)tmp1 as e1" \
            % (current_table, table_name)
    runner.checkok(line)
    line1 = "select k1,e2 from %s lateral view explode_split_outer(k2, '-') tmp2 as e2 order by k1,e2" \
            % current_table
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split_outer', 'k2', 2, table_name=current_table, separator="'-'")
    runner.checkok('drop table if exists %s' % current_table)


def test_lateral_view_after_union():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_after_union",
    "describe": "对union后的表执行行转列操作",
    "tag": "p1,function"
    }
    """
    # bug,使用union时会使k3列变为0,非行转列的bug,延期修复
    line1 = "select k3,e1 from (select * from %s where k3<100 union select * from %s where k3>900) a lateral view \
            explode_split(k4, ',') tmp as e1" % (table_name, table_name)
    line2 = "select k3,k4 from %s where k3<100 or k3>900" % check_name
    # runner.check2_palo(line1, line2, True)
    line1 = "select k3,e1 from (select * from %s where k3=100 union select * from %s where k3=900) a lateral view \
            explode_json_array_string(k5) tmp as e1" % (table_name, table_name)
    line2 = "select k3,k5 from %s where k3=100 or k3=900" % check_name
    # runner.check2_palo(line1, line2, True)
    line1 = "select k3,e1 from (select * from %s where k1>10000 union select * from %s where k1<0 and k3<900) a \
            lateral view explode_bitmap(bitmap_from_string(k8)) tmp as e1" % (table_name, table_name)
    line2 = "select k3,k8 from %s where k1>10000 union select k3,k8 from %s where k1<10 and k3<900" \
            % (check_name, check_name)
    # runner.check2_palo(line1, line2, True)
    line1 = "select k1,k3,e1 from (select * from %s where k3<500 union all select * from %s where k3>400) a \
            lateral view explode_split(k4, ',') tmp as e1" % (table_name, table_name)
    line2 = "select k1,k3,k4 from %s where k3<500 union all select k1,k3,k4 from %s where k3>400" \
            % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_value = 'explode_split(<slot 21> `default_cluster:test_lateral_view`.`lateral_view_data`.`k4`'
    check_table_function_node(line1, check_value, check=True)
    line1 = "select k3,e1 from (select * from %s where k3<500 union all select * from %s where k3>400) a lateral view \
            explode_json_array_int_outer(k6) tmp as e1" % (table_name, table_name)
    line2 = "select k3,k6 from %s where k3<500 union all select k3,k6 from %s where k3>400" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_value = 'explode_json_array_int_outer(<slot 23> `default_cluster:test_lateral_view`.`lateral_view_data`.`k6`'
    check_table_function_node(line1, check_value, check=True)
    line1 = "select k3,e1 from (select * from %s where k3<100 union all select * from %s where k3>900) a lateral view \
            explode_json_array_double(k7) tmp as e1" % (table_name, table_name)
    line2 = "select k3,k7 from %s where k3<100 and k7 is not null union all " \
            "select k3,k7 from %s where k3>900 and k7 is not null" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_value = 'explode_json_array_double(<slot 24> `default_cluster:test_lateral_view`.`lateral_view_data`.`k7`'
    check_table_function_node(line1, check_value, check=True)
    line1 = "select k1,k3,e1 from (select * from %s where k1>10000 union all select * from %s where k3<900) a \
            lateral view explode_bitmap_outer(k9) tmp as e1" % (table_name, table_name)
    line2 = "select k1,k3,k8 from %s where k1>10000 union all select k1,k3,k8 from %s where k3<900" \
            % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_value = 'explode_bitmap_outer(<slot 26> `default_cluster:test_lateral_view`.`lateral_view_data`.`k9`'
    check_table_function_node(line1, check_value, check=True)


def test_union_after_lateral_view():
    """
    {
    "title": "test_query_lateral_view:test_union_after_lateral_view",
    "describe": "行转列后执行union操作",
    "tag": "p1,function"
    }
    """
    line1 = "select k3,e1 from %s lateral view explode_split(k4, ',')tmp1 as e1 where k3<100 union select k3,e1 \
            from %s lateral view explode_split(k4, ',') tmp1 as e1 where k3>900" % (table_name, table_name)
    line2 = "select k3,k4 from %s where k3<100 union select k3,k4 from %s where k3>900" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_split', 'k4', frag_no=4)
    check_table_function_node(line1, 'explode_split', 'k4', frag_no=5)
    line1 = "select k3,e1 from %s lateral view explode_json_array_string_outer(k5) tmp1 as e1 where k3<100 " \
            "union select k3,e1 from %s lateral view explode_split(k4, ',') tmp1 as e1 where k3>900" \
            % (table_name, table_name)
    line2 = "select k3,k5 from %s where k3<100 union select k3,k4 from %s where k3>900" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_split', 'k4', frag_no=4)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5', frag_no=5)
    line1 = "select k3,e1 from %s lateral view explode_json_array_int(k6)tmp1 as e1 where k3<100 union select k3,e1 \
            from %s lateral view explode_bitmap(k9) tmp1 as e1 where k3>900" % (table_name, table_name)
    line2 = "select k3,k6 from %s where k3<100 and k6 is not null " \
            "union select k3,k8 from %s where k3>900 and k8 is not null" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_bitmap', 'k9', frag_no=4)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', frag_no=5)
    line1 = "select k3,e1 from %s lateral view explode_bitmap_outer(k9)tmp1 as e1 where k3=100 union select k3,e1 \
            from %s lateral view explode_json_array_int_outer(k6) tmp1 as e1 where k3<50" % (table_name, table_name)
    line2 = "select k3,k8 from %s where k3=100 union select k3,k6 from %s where k3<50" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', frag_no=4)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9', frag_no=5)
    line1 = "select k3,e1 from %s lateral view explode_json_array_string(k5)tmp1 as e1 where k3<100 union select k3,e1 \
            from %s lateral view explode_json_array_string(k5) tmp1 as e1 where k3<200" % (table_name, table_name)
    line2 = "select k3,k5 from %s where k3<100 and k6 is not null " \
            "union select k3,k5 from %s where k3<200 and k6 is not null" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', frag_no=4)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', frag_no=5)
    line1 = "select k3,e1 from %s lateral view explode_json_array_int_outer(k6)tmp1 as e1 where k3=100 union select k3,e1 \
            from %s lateral view explode_json_array_int_outer(k6) tmp1 as e1 where k3>1000" % (table_name, table_name)
    line2 = "select k3,k6 from %s where k3=100 union select k3,k6 from %s where k3>1000" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', frag_no=4)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', frag_no=5)
    line1 = "select k3,e1 from %s lateral view explode_json_array_double(k7)tmp1 as e1 where k3<100 union select k3,e1 \
            from %s lateral view explode_json_array_double(k7) tmp1 as e1 where k3<100" % (table_name, table_name)
    line2 = "select k3,k7 from %s where k3<100 and k7 is not null " \
            "union select k3,k7 from %s where k3<100 and k7 is not null" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', frag_no=4)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', frag_no=5)
    line1 = "select k3,e1 from %s lateral view explode_bitmap_outer(k9)tmp1 as e1 where k3<100 union select k3,e1 \
            from %s lateral view explode_bitmap_outer(k9) tmp1 as e1" % (table_name, table_name)
    line2 = "select k3,k8 from %s where k3<100 union select k3,k8 from %s" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9', frag_no=4)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9', frag_no=5)
    line1 = "select k3,e1 from %s lateral view explode_json_array_string(k5)tmp1 as e1 where k3<100 union select \
            k3,e1 from %s lateral view explode_json_array_int(k6) tmp1 as e1 where k3>200" % (table_name, table_name)
    line2 = "select k3,k5 from %s where k3<100 and k6 is not null " \
            "union select k3,k6 from %s where k3>200 and k6 is not null" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', frag_no=4)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', frag_no=5)
    line1 = "select k3,e1 from %s lateral view explode_bitmap_outer(k9)tmp1 as e1 where k3<100 union select \
            k3,e1 from %s lateral view explode_split_outer(k4, ',') tmp1 as e1 where k3>900" % (table_name, table_name)
    line2 = "select k3,k8 from %s where k3<100 union select k3,k4 from %s where k3>900" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_split_outer', 'k4', frag_no=4)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9', frag_no=5)


def test_union_all_after_lateral_view():
    """
    {
    "title": "test_query_lateral_view:test_union_all_after_lateral_view",
    "describe": "行转列后执行union all操作",
    "tag": "p1,function"
    }
    """
    line1 = "select k3,e1 from %s lateral view explode_split(k4, ',')tmp1 as e1 where k3<100 union all select k3,e1 \
            from %s lateral view explode_split(k4, ',') tmp1 as e1 where k3>900" % (table_name, table_name)
    line2 = "select k3,k4 from %s where k3<100 union all select k3,k4 from %s where k3>900" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_split', 'k4', frag_no=3)
    check_table_function_node(line1, 'explode_split', 'k4', frag_no=4)
    line1 = "select k3,e1 from %s lateral view explode_json_array_string_outer(k5)tmp1 as e1 where k3<100 " \
            "union all select k3,e1 from %s lateral view explode_split_outer(k4, ',') tmp1 as e1 where k3>900" \
            % (table_name, table_name)
    line2 = "select k3,k5 from %s where k3<100 union all select k3,k4 from %s where k3>900" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_split_outer', 'k4', frag_no=3)
    check_table_function_node(line1, 'explode_json_array_string_outer', 'k5', frag_no=4)
    line1 = "select k3,e1 from %s lateral view explode_json_array_int(k6)tmp1 as e1 where k3<100 union all select k3,e1 \
            from %s lateral view explode_bitmap(k9) tmp1 as e1 where k3>900" % (table_name, table_name)
    line2 = "select k3,k6 from %s where k3<100 and k6 is not null " \
            "union all select k3,k8 from %s where k3>900 and k6 is not null" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_bitmap', 'k9', frag_no=3)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', frag_no=4)
    line1 = "select k3,e1 from %s lateral view explode_bitmap_outer(k9)tmp1 as e1 where k3=100 union all select k3,e1 \
            from %s lateral view explode_json_array_int_outer(k6) tmp1 as e1 where k3<50" % (table_name, table_name)
    line2 = "select k3,k8 from %s where k3=100 union all select k3,k6 from %s where k3<50" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', frag_no=3)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9', frag_no=4)
    line1 = "select k3,e1 from %s lateral view explode_json_array_string(k5)tmp1 as e1 where k3<100 union all select \
            k3,e1 from %s lateral view explode_json_array_string(k5) tmp1 as e1 where k3<200" % (table_name, table_name)
    line2 = "select k3,k5 from %s where k3<100 and k6 is not null " \
            "union all select k3,k5 from %s where k3<200 and k6 is not null" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', frag_no=3)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', frag_no=4)
    line1 = "select k3,e1 from %s lateral view explode_json_array_int_outer(k6)tmp1 as e1 where k3=100 " \
            "union all select k3,e1 from %s lateral view explode_json_array_int_outer(k6) tmp1 as e1 where k3>1000" \
            % (table_name, table_name)
    line2 = "select k3,k6 from %s where k3=100 union all select k3,k6 from %s where k3>1000" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', frag_no=3)
    check_table_function_node(line1, 'explode_json_array_int_outer', 'k6', frag_no=4)
    line1 = "select k3,e1 from %s lateral view explode_json_array_double(k7)tmp1 as e1 where k3<100 union all select \
            k3,e1 from %s lateral view explode_json_array_double(k7) tmp1 as e1 where k3<100" % (table_name, table_name)
    line2 = "select k3,k7 from %s where k3<100 and k7 is not null " \
            "union all select k3,k7 from %s where k3<100 and k7 is not null" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', frag_no=3)
    check_table_function_node(line1, 'explode_json_array_double', 'k7', frag_no=4)
    line1 = "select k3,e1 from %s lateral view explode_bitmap_outer(k9)tmp1 as e1 where k3<100 union all select k3,e1 \
            from %s lateral view explode_bitmap_outer(k9) tmp1 as e1" % (table_name, table_name)
    line2 = "select k3,k8 from %s where k3<100 union all select k3,k8 from %s" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9', frag_no=3)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9', frag_no=4)
    line1 = "select k3,e1 from %s lateral view explode_json_array_string(k5)tmp1 as e1 where k3<100 union all select \
            k3,e1 from %s lateral view explode_json_array_int(k6) tmp1 as e1 where k3<200" % (table_name, table_name)
    line2 = "select k3,k5 from %s where k3<100 and k6 is not null " \
            "union all select k3,k6 from %s where k3<200 and k6 is not null" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_json_array_int', 'k6', frag_no=3)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', frag_no=4)
    line1 = "select k3,e1 from %s lateral view explode_bitmap_outer(k9)tmp1 as e1 where k3<100 union all select \
            k3,e1 from %s lateral view explode_split_outer(k4, ',') tmp1 as e1 where k3>900" % (table_name, table_name)
    line2 = "select k3,k8 from %s where k3<100 union all select k3,k4 from %s where k3>900" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_split_outer', 'k4', frag_no=3)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9', frag_no=4)


def test_view_with_lateral_view():
    """
    {
    "title": "test_query_lateral_view:test_view_with_lateral_view",
    "describe": "基于行转列创建view",
    "tag": "p1,function"
    }
    """
    view_name = 'view_split'
    line = "drop view if exists %s" % view_name
    runner.checkok(line)
    line = "create view %s (k1,e1) as select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1" \
            % (view_name, table_name)
    runner.checkok(line)
    line1 = "select * from %s order by k1,e1" % view_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)

    view_name = 'view_string'
    line = "drop view if exists %s" % view_name
    runner.checkok(line)
    line = "create view %s (k1,k3,e1) as select k1,k3,e1 from %s lateral view explode_json_array_string_outer(k5) \
            tmp as e1 where k3<100" % (view_name, table_name)
    runner.checkok(line)
    line1 = "select * from %s order by k1,k3,e1" % view_name
    line2 = "select k1,k3,k5 from %s where k3<100 order by k1,k3,k5" % check_name
    runner.check2_palo(line1, line2)

    view_name = 'view_int'
    line = "drop view if exists %s" % view_name
    runner.checkok(line)
    line = "create view %s (k1,k3,e1) as select k1,k3,reverse(e1) from %s lateral view explode_json_array_int(k6) \
            tmp as e1" % (view_name, table_name)
    runner.checkok(line)
    line1 = "select * from %s order by k1,k3,e1" % view_name
    line2 = "select k1,k3,reverse(k6) from %s where k6 is not null order by k1,k3,reverse(k6)" % check_name
    runner.check2_palo(line1, line2)

    view_name = 'view_double'
    line = "drop view if exists %s" % view_name
    runner.checkok(line)
    line = "create view %s (e1) as select e1 from %s lateral view explode_json_array_double_outer(k7) \
            tmp as e1" % (view_name, table_name)
    runner.checkok(line)
    line1 = "select * from %s order by e1" % view_name
    line2 = "select k7 from %s order by k7" % check_name
    runner.check2_palo(line1, line2)

    view_name = 'view_bitmap'
    line = "drop view if exists %s" % view_name
    runner.checkok(line)
    line = "create view %s (k1,k3,e1) as select k1,k3,e1 from %s lateral view explode_bitmap(k9) tmp as e1 \
            where k3>100" % (view_name, table_name)
    runner.checkok(line)
    line1 = "select k1,e1 from %s where k3<900 order by k1,e1" % view_name
    line2 = "select k1,k8 from %s where k3<900 and k3>100 and k8 is not null order by k1,k8" % check_name
    runner.check2_palo(line1, line2)


def test_lateral_view_with_view():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_with_view",
    "describe": "创建view后进行行转列",
    "tag": "p1,function"
    }
    """
    view_name = 'lateral_view_with_view'
    line = "drop view if exists %s" % view_name
    runner.checkok(line)
    line = "create view %s (k1,k3,k4,k5,k6,k7,k8,k9) as select k1,k3,k4,k5,k6,k7,k8,k9 from %s" \
            % (view_name, table_name)
    runner.checkok(line)
    line1 = "select k1,e1 from %s lateral view explode_split(k4, ',') tmp as e1" % view_name
    line2 = "select k1,k4 from %s" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_split(`k4`, ',')", check=True)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string_outer(k5) tmp as e1" % view_name
    line2 = "select k1,k5 from %s" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_string_outer(`k5`)", check=True)
    line1 = "select k3,e1 from %s lateral view explode_json_array_int(k6) tmp as e1 where k3<100" % view_name
    line2 = "select k3,k6 from %s where k3<100 and k6 is not null" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_int(`k6`)", check=True)
    line1 = "select k1,reverse(e1) from %s lateral view explode_json_array_double_outer(k7) tmp as e1" % view_name
    line2 = "select k1,reverse(k7) from %s" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_json_array_double_outer(`k7`)", check=True)
    line1 = "select e1 from %s lateral view explode_bitmap(k9) tmp as e1" % view_name
    line2 = "select k8 from %s where k8 is not null" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, "explode_bitmap(`k9`)", check=True)


def test_insert_select_lateral_view():
    """
    {
    "title": "test_query_lateral_view:test_insert_select_lateral_view",
    "describe": "行转列后导出数据",
    "tag": "p1,function"
    }
    """
    temp_name = 'lateral_view_temp'
    runner.checkok('drop table if exists %s' % temp_name)
    column_list = [('k1', 'int'), ('k2', 'varchar(100)')]
    column_list_2 = [('k1', 'int'), ('k2', 'bigint')]
    create(database_name, temp_name, column_list)
    sql = "insert into %s select k1,e1 from %s lateral view explode_split(k4, ',')tmp1 as e1" % (temp_name, table_name)
    runner.checkok(sql)
    line1 = "select * from %s order by k1,k2" % temp_name
    line2 = "select k1,k4 from %s order by k1,k4" % check_name
    runner.check2_palo(line1, line2)
    runner.checkok('drop table if exists %s' % temp_name)
    create(database_name, temp_name, column_list)
    sql = "insert into %s select k3,e1 from %s lateral view explode_json_array_string_outer(k5)tmp1 as e1 \
            where k3<100" % (temp_name, table_name)
    runner.checkok(sql)
    line1 = "select * from %s order by k1,k2" % temp_name
    line2 = "select k3,k5 from %s  where k3<100 order by k3,k5" % check_name
    runner.check2_palo(line1, line2)
    runner.checkok('drop table if exists %s' % temp_name)


def test_numerical_range():
    """
    {
    "title": "test_query_lateral_view:test_numerical_range",
    "describe": "测试各行转列函数的数值上限",
    "tag": "p1,function"
    }
    """
    table_name = 'test_numerical_range'
    column_list = [('k1', 'int')]
    runner.checkok('drop table if exists %s' % table_name)
    create(database_name, table_name, column_list)
    sql = "insert into %s values (1)" % table_name
    runner.checkok(sql)
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string('9223372036854775807, \
            9223372036854775808')) tmp as e1 order by k1,e1" % table_name
    line2 = "select 1,cast(-9223372036854775808 as bigint) union all select 1,cast(9223372036854775807 as bigint)"
    runner.check2_palo(line1, line2)
    # bug, bitmap cannot suport negtive -1
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string('-1,1')) tmp as e1 order by \
            k1,e1" % table_name
    runner.checkok(line1)
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string('3,1')) tmp as e1 order by \
            k1,e1" % table_name
    line2 = "select 1,1 union select 1, 3"
    runner.check2_palo(line1, line2, True)
    ret = runner.query_palo.do_sql(line1)
    print(ret)
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string('0,1,1')) tmp as e1 order by \
            k1,e1" % table_name
    line2 = "select 1,0 union all select 1,1"
    runner.check2_palo(line1, line2)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int('[9223372036854775807,9223372036854775808]') \
            tmp as e1 order by k1,e1" % table_name
    line2 = "select 1,null union all select 1,cast(9223372036854775807 as bigint)"
    runner.check2_palo(line1, line2)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int('[-9223372036854775809,-9223372036854775808]') \
            tmp as e1 order by k1,e1" % table_name
    line2 = "select 1,null union all select 1,cast(-9223372036854775808 as bigint)"
    runner.check2_palo(line1, line2)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double('[0.123456789123456,0.100000000000003,\
            0.999999999999999]') tmp as e1 order by k1,e1" % table_name
    line2 = "select 1,0.100000000000003 union all select 1,0.123456789123456 union all select 1,0.999999999999999"
    runner.check2_palo(line1, line2)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double('[99999999999999.9,0.1000000000000001,\
            -99999999999999.9,-0.1000000000000001]') tmp as e1 order by k1,e1" % table_name
    line2 = "select 1,-99999999999999.9 union all select 1,-0.1000000000000001 union all select 1,0.1000000000000001 \
            union all select 1,99999999999999.9"
    runner.check2_palo(line1, line2)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double('[123123123123123.1,123123123123.1234,\
            -123123123.1231234]') tmp as e1 order by k1,e1" % table_name
    line2 = "select 1,-123123123.1231234 union all select 1,123123123123.1234 union all select 1,123123123123123.1"
    runner.check2_palo(line1, line2)
    # runner.checkok('drop table if exists %s' % table_name)


def test_incorrect_data_type():
    """
    {
    "title": "test_query_lateral_view:test_incorrect_data_type",
    "describe": "列类型错误时，执行行转列操作",
    "tag": "p1,function,fuzz"
    }
    """
    table_name = 'test_incorrect_data_type'
    column_list = [('k1', 'int')]
    runner.checkok('drop table if exists %s' % table_name)
    create(database_name, table_name, column_list)
    sql = "insert into %s values (1)" % table_name
    runner.checkok(sql)
    line1 = "select k1,e1 from %s lateral view explode_split(123, ',') tmp as e1" % table_name
    line2 = "select 1,'123'"
    runner.check2_palo(line1, line2)
    line1 = "select k1,bitmap_to_string(e1) from %s lateral view explode_split(bitmap_from_string('1,2,3'), ',') \
            tmp as e1" % table_name
    runner.checkwrong(line1)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string('[a,b,c]') tmp as e1" % table_name
    line2 = "select * from %s where 1=2" % table_name
    runner.check2_palo(line1, line2)
    line1 = "select k1,e1 from %s lateral view explode_json_array_string_outer('[a,b,c]') tmp as e1" % table_name
    line2 = "select 1,null"
    runner.check2_palo(line1, line2)
    line1 = 'select k1,e1 from %s lateral view explode_json_array_string_outer(\'"a","b","c"\') tmp as e1' % table_name
    line2 = "select 1,null"
    runner.check2_palo(line1, line2)
    line1 = 'select k1,e1 from %s lateral view explode_json_array_int(\'["a","b","c"]\') tmp as e1' % table_name
    line2 = "select 1,null union all select 1,null union all select 1,null"
    runner.check2_palo(line1, line2)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int_outer('a,b,c') tmp as e1" % table_name
    line2 = "select 1,null"
    runner.check2_palo(line1, line2)
    line1 = "select k1,e1 from %s lateral view explode_json_array_int('[1.1,2.2,3.3]') tmp as e1" % table_name
    line2 = "select 1,null union all select 1,null union all select 1,null"
    runner.check2_palo(line1, line2)
    line1 = 'select k1,e1 from %s lateral view explode_json_array_double(\'["a","b","c"]\') tmp as e1' % table_name
    line2 = "select 1,null union all select 1,null union all select 1,null"
    runner.check2_palo(line1, line2)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double_outer('a,b,c') tmp as e1" % table_name
    line2 = "select 1,null"
    runner.check2_palo(line1, line2)
    line1 = "select k1,e1 from %s lateral view explode_json_array_double('[1,2,3]') tmp as e1" % table_name
    line2 = "select 1,null union all select 1,null union all select 1,null"
    runner.check2_palo(line1, line2)
    line1 = "select k1,e1 from %s lateral view explode_bitmap('1,2,3') tmp as e1" % table_name
    runner.checkwrong(line1)
    line1 = "select k1,e1 from %s lateral view explode_bitmap(bitmap_from_string('a,b,c')) tmp as e1" % table_name
    line2 = "select k1 from %s where 1=2" % table_name
    runner.check2_palo(line1, line2)
    runner.checkok('drop table if exists %s' % table_name)


def test_date_type():
    """
    {
    "title": "test_query_lateral_view:test_date_type",
    "describe": "行转列后为时间类型数据，进行时间函数操作",
    "tag": "p1,function"
    }
    """
    table_name = 'test_date_type'
    column_list = [('k1', 'int'), ('k2', 'varchar(100)'), ('k3', 'varchar(100)')]
    create(database_name, table_name, column_list)
    sql = "insert into %s values (1, '2022-01-01,2022-02-02,2021-12-12', '[\"2022-01-01 13:00:00\",\
            \"2022-02-02 05:50:05\",\"2021-12-12 10:10:10\"]'), (2, '2022-01-20,2022-02-01', \
            '[\"2022-01-20 20:10:00\",\"2022-02-01 19:59:59\"]')" % table_name
    runner.checkok(sql)
    check_name = table_name + '_check'
    column_list = [('k1', 'int'), ('k2', 'date'), ('k3', 'datetime')]
    create(database_name, check_name, column_list)
    sql = "insert into %s values (1, '2022-01-01', '2022-01-01 13:00:00'), (1, '2022-02-02', '2022-02-02 05:50:05'), \
            (1, '2021-12-12', '2021-12-12 10:10:10'), (2, '2022-01-20', '2022-01-20 20:10:00'), (2, '2022-02-01', \
            '2022-02-01 19:59:59')" % check_name
    runner.checkok(sql)
    line1 = "select k1,dayname(e1) from %s lateral view explode_split(k2, ',') tmp as e1 order by k1,e1" % table_name
    line2 = "select k1,dayname(k2) from %s order by k1,k2" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name)
    line1 = "select k1,datediff(e1, '2022-01-01') from %s lateral view explode_split(k2, ',') tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,datediff(k2, '2022-01-01') from %s order by k1,k2" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_split', 'k2', 2, database_name, table_name)
    line1 = "select k1,date_add(e1, 5) from %s lateral view explode_json_array_string(k3) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,date_add(k3, 5) from %s order by k1,k3" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k3', 2, database_name, table_name)
    line1 = "select k1,hours_diff(e1, '2022-01-01') from %s lateral view explode_json_array_string(k3) tmp as e1 \
            order by k1,e1" % table_name
    line2 = "select k1,hours_diff(k3, '2022-01-01') from %s order by k1,k3" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k3', 2, database_name, table_name)
    line1 = "select k1,unix_timestamp(e1) from %s lateral view explode_json_array_string(k3) tmp as e1 order by k1,e1" \
            % table_name
    line2 = "select k1,unix_timestamp(k3) from %s order by k1,k3" % check_name
    runner.check2_palo(line1, line2)
    check_table_function_node(line1, 'explode_json_array_string', 'k3', 2, database_name, table_name)
    runner.checkok('drop table if exists %s' % table_name)
    runner.checkok('drop table if exists %s' % check_name)


def test_subquery():
    """
    {
    "title": "test_query_lateral_view:test_subquery",
    "describe": "执行行转列，包含子查询：子查询分别位于where和with中",
    "tag": "p1,function"
    }
    """
    #where中包含子查询
    line1 = "select k1,k3,e1 from %s lateral view explode_split(k4, ',') tmp as e1 where k3 in (select k2 from %s) " \
            % (table_name, table_name)
    line2 = "select k1,k3,k4 from %s where k3 in (select k2 from %s)" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_split', 'k4', None)
    line1 = "select k1,k3,e1 from %s lateral view explode_json_array_string(k5) tmp as e1 where k3 in \
            (select k2 from %s) " % (table_name, table_name)
    line2 = "select k1,k3,k5 from %s where k3 in (select k2 from %s)" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_json_array_string', 'k5', None)
    line1 = "select k1,k3,e1 from %s lateral view explode_bitmap(k9) tmp as e1 where k3 in (select k2 from %s \
            where k2<10) " % (table_name, table_name)
    line2 = "select k1,k3,k8 from %s where k3 in (select k2 from %s where k2<10)" % (check_name, check_name)
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_bitmap', 'k9', None)
    #with中包含子查询
    line1 = "with tmp as (select * from %s where k3>900) select k3,e1 from tmp lateral view explode_split(k4, ',') \
            tmp2 as e1" % table_name
    line2 = "select k3,k4 from %s where k3>900" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "with tmp as (select * from %s where k3>900) select k3,e1 from tmp lateral view explode_json_array_int(k6) \
            tmp2 as e1" % table_name
    line2 = "select k3,k6 from %s where k3>900 and k6 is not null" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_json_array_int', 'k6')
    line1 = "with tmp as (select * from %s where k3<900) select k1,e1 from tmp lateral view \
            explode_bitmap_outer(bitmap_from_string(k8)) tmp2 as e1" % table_name
    line2 = "select k1,k8 from %s where k3<900" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_bitmap_outer(bitmap_from_string', 'k8')
    # with中包含行转列子查询
    line1 = "with tmp as (select k3,e1 from %s lateral view explode_split(k4, ',') tmp2 as e1) select * from tmp \
            where k3>900" % table_name
    line2 = "select k3,k4 from %s where k3>900" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_split', 'k4')
    line1 = "with tmp as (select k1,k3,e1 from %s lateral view explode_json_array_double(k7) tmp2 as e1) \
            select * from tmp where k3>900" % table_name
    line2 = "select k1,k3,k7 from %s where k3>900 and k7 is not null" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_json_array_double', 'k7')
    line1 = "with tmp as (select k1,e1 from %s lateral view explode_bitmap_outer(k9) tmp2 as e1) select e1 from tmp" \
            % table_name
    line2 = "select k8 from %s" % check_name
    runner.check2_palo(line1, line2, True)
    check_table_function_node(line1, 'explode_bitmap_outer', 'k9')


def test_lateral_view_in_different_table():
    """
    {
    "title": "test_query_lateral_view:test_lateral_view_in_different_table",
    "describe": "行转列的列位于其他表，执行失败",
    "tag": "p1,function,fuzz"
    }
    """
    line = "select k1,e1 from %s lateral view explode_split(%s.k4, ',') tmp as e1" % (check_name, table_name)
    runner.checkwrong(line)    
    line = "select k1,e1 from %s lateral view explode_json_array_string(%s.k5) tmp as e1" % (check_name, table_name)
    runner.checkwrong(line)
    line = "select k1,e1 from %s lateral view explode_json_array_int(%s.k6) tmp as e1" % (check_name, table_name)
    runner.checkwrong(line)
    line = "select k1,e1 from %s lateral view explode_json_array_double(%s.k7) tmp as e1" % (check_name, table_name)
    runner.checkwrong(line)
    line = "select k1,e1 from %s lateral view explode_bitmap(%s.k9) tmp as e1" % (check_name, table_name)
    runner.checkwrong(line)
    line = "select k1,e1,e2 from %s lateral view explode_split(k4, ',') tmp1 as e1 lateral view \
            explode_split(%s.k4, ',') tmp2 as e2" % (table_name, check_name)
    runner.checkwrong(line)

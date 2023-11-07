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
验证查询计划
"""
import sys
sys.path.append("../lib/")
from palo_qe_client import QueryBase
import palo_query_plan

table_name = "baseall"
join_name = "test"


def setup_module():
    """setup"""
    global runner
    runner = QueryBase()


def test_issue_7929():
    """
    {
    "title": "test_issue_7929",
    "describe": "explain verbose empty node, expect 4:EMPTYSET tuple ids: 0 1 5",
    "tag": "system,p1"
    }
    """
    sql = "desc verbose select * from baseall t1 left join " \
          "(select max(k1) over() as x from bigtable)a on t1.k1=a.x where 1=0"
    ret = runner.get_sql_result(sql)
    verbose_plan = palo_query_plan.PlanInfo(ret)
    fragment0 = verbose_plan.get_one_fragment(1)
    empty_set = verbose_plan.get_frag_empty_set(fragment0)
    # empty_set is a tuple (u'     tuple ids: 0 1 5 ',)
    sql = 'show variables like "enable_vectorized_engine"'
    ret = runner.get_sql_result(sql)
    if len(ret) == 1 and ret[0][1] == 'true':
        expect = 'tuple ids: 5'
    else:
        expect = 'tuple ids: 0 1 4'
    assert expect in empty_set[0], 'expect EMPTYSET tuple ids: 0 1 4, but actural not'
    sql = "select * from baseall t1 left join " \
          "(select max(k1) over() as x from bigtable)a on t1.k1=a.x where 1=0"
    runner.checkok(sql)


def test_issue_7837():
    """
    {
    "title": "test_issue_7837",
    "describe": "explain verbose showing implicit cast",
    "tag": "system,p1"
    }
    """
    sql = "desc verbose select * from baseall where k1 = '1'"
    ret = runner.get_sql_result(sql)
    verbose_plan = palo_query_plan.PlanInfo(ret)
    fragment0 = verbose_plan.get_one_fragment(1)
    predicates = verbose_plan.get_frag_olapScanNode_predicates(fragment0)
    # predicates is a tuple (u'     PREDICATES: `k3` = 1',)
    assert '`k1` = 1' in predicates[0]
    sql = "select * from baseall where k1 = '1'"
    runner.check(sql)


def teardown_module():
    """tear down"""
    pass


if __name__ == '__main__':
    setup_module()
    test_issue_7837()

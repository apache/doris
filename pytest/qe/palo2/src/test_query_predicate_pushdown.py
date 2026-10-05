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
case about predicate push
谓词下推
"""

import os
import pymysql
import sys
import time
sys.path.append("../lib/")
from palo_qe_client import QueryBase
import palo_query_plan
from warnings import filterwarnings
filterwarnings('ignore', category=pymysql.Warning)


sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage

table_name = "baseall"
join_name = "test"

if 'FE_DB' in os.environ.keys():
    db_name = os.environ["FE_DB"]
else:
    db_name = "test_query_qa"


def setup_module():
    """
    init config
    """
    global runner
    global msg_flag
    runner = QueryExecuter()
    timeout = 600
    print('check show data...')
    while timeout > 0:
        timeout -= 1
        flag = True
        ret = runner.query_palo.do_sql('show data')
        for data in ret:
            if data[0] == table_name and data[1] == '0.000 ':
                time.sleep(1)
                flag = False
            elif data[0] == join_name and data[1] == '0.000 ':
                time.sleep(1)
                flag = False
            else:
                pass
        if flag:
            print('check show data ok')
            break
    if not flag:
        print('cannot get table msg')
    msg_flag = flag


class QueryExecuter(QueryBase):
    def __init__(self):
        self.get_clients()

    def check_predicate(self, sql, check_value, verify=True, frag_no=1, ScanNode_predicate=True, scannode_no=1):
        """判断查询计划里，下推的谓词predicate,
        line：查询sql
        check_value：下推的谓词value值：eg，PREDICATES: `k6` IS NULL，value要大写
        verify:是否验证包含 
        frag_no: 第几个fragment
        ScanNode_predicate:是否是scannode下的predicate
        eg:
        ScanNode:PREDICATES: `k6` IS NULL ,对应的check_dict=['k6` IS NULL']
        select:PREDICATES: <slot 8> > 8 ,对应的check_dict=['<slot 8> > 8']
        """
        plan_res = self.query_palo.get_query_plan(sql)
        assert plan_res, "%s is NUll"  % str(plan_res)
        assert len(check_value) > 0, "%s is null" % str(check_value)
        fragment_res = palo_query_plan.PlanInfo(plan_res).get_one_fragment(frag_no)
        if ScanNode_predicate:
            predicate_res = palo_query_plan.PlanInfo(plan_res).get_frag_olapScanNode_predicates(fragment_res, 
                                                                                                scannode_no)
        else:
             predicate_res = palo_query_plan.PlanInfo(plan_res).get_frag_Select_predicates(fragment_res)
        if verify:
            print(str(check_value).upper())
            print(str(predicate_res).upper())
            if isinstance(check_value, list):
                flag = False
                for v in check_value:
                    if str(v).upper() in str(predicate_res).upper():
                        flag = True
                assert flag, 'expect: %s, actural: %s' % (check_value, predicate_res)
            else:
                assert (str(check_value).upper() in str(predicate_res).upper()), \
                       'expect: %s, actural: %s' % (check_value, predicate_res)
        else:
            assert (str(check_value).upper() not in str(predicate_res).upper()), \
                   'expect not %s, actural: %s' % (check_value, predicate_res)


def test_predicate_type():
    """
    {
    "title": "test_query_predicate_pushdown.test_predicate_type",
    "describe": "test_predicate_type,测试点：各个类型支持谓词下推",
    "tag": "system,p1,fuzz"
    }
    """
    """
    test_predicate_type
    测试点：各个类型支持谓词下推
    """
    for index in range(11):
        if index in [0]:
            line_1 = 'select * from (select k1, sum(k2) over (partition by k%s) \
                as ss from %s)s  where s.k%s > 10 order by k1' % \
                (index + 1, join_name, index + 1)
            line_2 = 'select * from (select k1, sum(k2) over (partition by k%s) \
                    as ss from %s where k%s > 10)s order by k1' % \
                   (index + 1, join_name, index + 1)
        elif index in [5, 6]:
            line_1 = "select * from (select  k%s, sum(k2) over (partition by k%s) \
                as ss from %s)s  where s.k%s > 'a' order by k%s" % \
                (index + 1, index + 1, join_name, index + 1, index + 1)
            line_2 = "select * from (select k%s, sum(k2) over (partition by k%s) \
                    as ss from %s where k%s > 'a')s order by k%s" % \
                    (index + 1, index + 1, join_name, index + 1, index + 1)
        elif index in [9]:
            line_1 = "select * from (select k%s, sum(k2) over (partition by k%s) \
                as ss from %s)s  where s.k%s > '2010-01-01' order by k%s" % \
                (index + 1, index + 1, join_name, index + 1, index + 1)
            line_2 = "select * from (select k%s, sum(k2) over (partition by k%s) \
                    as ss from %s where k%s > '2010-01-01')s order by k%s" % \
                    (index + 1, index + 1, join_name, index + 1, index + 1)
        elif index in [10]:
            line_1 = "select * from (select k%s, sum(k2) over (partition by k%s) \
                as ss from %s)s  where s.k%s > '2010-01-01 09:00:00' order by k%s" % \
               (index + 1, index + 1, join_name, index + 1, index + 1)
            line_2 = "select * from (select k%s, sum(k2) over (partition by k%s) \
                    as ss from %s where k%s > '2010-01-01 09:00:00')s order by k%s" % \
                    (index + 1, index + 1, join_name, index + 1, index + 1)
        else:
            line_1 = 'select * from (select k%s, sum(k2) over (partition by k%s) \
                as ss from %s)s  where s.k%s > 10 order by k%s' % \
                (index + 1, index + 1, join_name, index + 1, index + 1)
            line_2 = 'select * from (select k%s, sum(k2) over (partition by k%s) \
                    as ss from %s where k%s > 10)s order by k%s' % \
                   (index + 1, index + 1, join_name, index + 1, index + 1)
        frag_no = 3
        runner.check_predicate(line_1, '`k%s`' % (index + 1), frag_no=frag_no)
        runner.check2_palo(line_1, line_2)
    line_1 = "select k1, k2, sum(k2) over (partition by k1) as ss from %s\
       where k1 + k2 > 10" % table_name
    runner.check_predicate(line_1, '`k1` + `k2` > 10', frag_no=3)
    line_2 = "select s.k1, s.k2, s.ss from (select k1, k2, sum(k2) over (partition by k1) as ss\
       from %s)s  where s.k1 + s.k2 > 10" % table_name
    runner.check_predicate(line_2, '`k1` > 1', verify=False)

    sql = "select * from (select k1, k1, sum(k2) over (partition by k1) as ss from %s)s \
          where s.k1 > 10 order by k1;" % table_name
    runner.checkwrong(sql)
    sql = "select * from (select k1, k2, sum(k2) over (partition by k1) \
       as ss from %s where ss > 10)s" % table_name
    runner.checkwrong(sql)


def test_predicate_windows_two_partition_sum():
    """
    {
    "title": "test_query_predicate_pushdown.test_predicate_windows_two_partition_sum",
    "describe": "窗口函数下 2个partition相加,测试点：各个类型",
    "tag": "system,p1"
    }
    """
    """
    窗口函数下 2个partition相加
    测试点：各个类型
    """
    for index in range(11):
        if index in [0]:
            line_1 = 'select * from (select k1, sum(k2) over (partition by k%s + k%s) \
                as ss from %s)s  where s.k%s > 8 order by k1' % \
                (index + 1, index + 2, table_name, index + 1)
            line_2 = 'select * from (select k1, sum(k2) over (partition by k%s + k%s) \
                    as ss from %s where k%s > 8)s order by k1' % \
                   (index + 1, index + 2, table_name, index + 1)
            ####plan
            runner.check_predicate(line_1, '`k%s`' % (index + 1), verify=False)
            runner.check_predicate(line_1, '> 8', frag_no=2, ScanNode_predicate=False)
        elif index in [5, 6]:
            line_1 = "select * from (select k1, k%s, sum(k2) over (partition by k%s + k%s) \
                as ss from %s)s  where s.k%s > 'a' order by k1" % \
                (index + 1, index + 2, index + 1, table_name, index + 1)
            line_2 = "select * from (select k1, k%s, sum(k2) over (partition by k%s + k%s) \
                    as ss from %s where k%s > 'a')s order by k1" % \
                    (index + 1, index + 2, index + 1, table_name, index + 1)
            ####plan
            runner.check_predicate(line_1, '`k%s`' % (index + 1), verify=False)
            runner.check_predicate(line_1, "> 'a'", frag_no=2, ScanNode_predicate=False)
        elif index in [9]:
            line_1 = "select * from (select k1, k%s, sum(k2) over (partition by k%s + k%s) \
                as ss from %s)s  where s.k%s > '2010-01-01'order by k1" % \
                (index + 1, index + 1, index + 2, table_name, index + 1)
            line_2 = "select * from (select k1, k%s, sum(k2) over (partition by k%s + k%s) \
                    as ss from %s where k%s > '2010-01-01')s order by k1" % \
                    (index + 1, index + 1, index + 2, table_name, index + 1)
            ####plan
            runner.check_predicate(line_1, '`k%s`' % (index + 1), verify=False)
            runner.check_predicate(line_1, "> '2010-01-01 00:00:00'", frag_no=2, ScanNode_predicate=False)
        elif index in [10]:
            line_1 = "select * from (select k1, k%s, sum(k2) over (partition by k%s + k%s) \
                as ss from %s)s  where s.k%s > '2010-01-01 09:00:00' order by k1" % \
               (index + 1, index + 1, index + 1, table_name, index + 1)
            line_2 = "select * from (select k1, k%s, sum(k2) over (partition by k%s + k%s) \
                    as ss from %s where k%s > '2010-01-01 09:00:00')s order by k1" % \
                    (index + 1, index + 1, index + 1, table_name, index + 1)
            ####plan
            runner.check_predicate(line_1, '`k%s`' % (index + 1), verify=False)
            runner.check_predicate(line_1, "> '2010-01-01 09:00:00'", frag_no=2, ScanNode_predicate=False)
        else:
            line_1 = 'select * from (select k1, k%s, sum(k2) over (partition by k%s + k%s) \
                as ss from %s)s  where s.k%s > 2000 order by k1' % \
                (index + 1, index + 1, index + 2, table_name, index + 1)
            line_2 = 'select * from (select k1, k%s, sum(k2) over (partition by k%s + k%s) \
                    as ss from %s where k%s > 2000)s order by k1' % \
                   (index + 1, index + 1, index + 2, table_name, index + 1)
            ####plan
            runner.check_predicate(line_1, '`k%s`' % (index + 1), verify=False)
            runner.check_predicate(line_1, '> 2000', frag_no=2, ScanNode_predicate=False)
        if index in [4, 6]:
            #NULL windows
            continue
        runner.check2_palo(line_1, line_2)

    line_1 = "select * from (select k1, k2, sum(k2) over (partition by k1 + k2)\
         as ss from %s)s  where s.k1 + s.k2 > 10 order by k1 limit 1" % table_name
    line_2 = "select * from (select k1, k2, sum(k2) over (partition by k1 + k2)\
         as ss from %s where k1 + k2 > 10)s order by k1 limit 1" % table_name
    runner.check_predicate(line_1, '`k1` + `k2`', verify=False)
    runner.check2_palo(line_1, line_2)


def test_predicate_windows_partitioned_by_multi_columns():
    """
    {
    "title": "test_query_predicate_pushdown.test_predicate_windows_partitioned_by_multi_columns",
    "describe": "test_predicate_two_partition_comma_separate,两个partition，逗号分隔，依次用于partition by,测试点：各个类型，where等其他谓词",
    "tag": "system,p1"
    }
    """
    """
    test_predicate_two_partition_comma_separate,两个partition，逗号分隔，依次用于partition by
    测试点：各个类型，where等其他谓词
    """
    for index in range(11):
        if index in [0]:
            line_1 = 'select * from (select k1, sum(k2) over (partition by k%s, k%s) \
                as ss from %s)s  where s.k%s > 10 or s.k%s > 30 order by k1' % \
                (index + 1, index + 2, join_name, index + 1, index + 1)
            line_2 = 'select * from (select k1, sum(k2) over (partition by k%s, k%s) \
                    as ss from %s where k%s > 10 or k%s > 30)s order by k1' % \
                   (index + 1, index + 2, join_name, index + 1, index + 1)
        elif index in [5, 6]:
            line_1 = "select * from (select k%s, sum(k2) over (partition by k%s, k%s) \
                as ss from %s)s  where s.k%s like 'a' order by k%s" % \
                (index + 1, index + 1, index + 2, join_name, index + 1, index + 1)
            line_2 = "select * from (select k%s, sum(k2) over (partition by k%s, k%s) \
                    as ss from %s where k%s like 'a')s order by k%s" % \
                    (index + 1, index + 1, index + 2, join_name, index + 1, index + 1)
        elif index in [9]:
            line_1 = "select * from (select k%s, sum(k2) over (partition by k%s, k%s) \
                as ss from %s)s  where s.k%s > '2010-01-01' order by k%s" % \
                (index + 1, index + 1, index + 2, join_name, index + 1, index + 1)
            line_2 = "select * from (select k%s, sum(k2) over (partition by k%s, k%s) \
                    as ss from %s where k%s > '2010-01-01')s order by k%s" % \
                    (index + 1, index + 1, index + 2, join_name, index + 1, index + 1)
        elif index in [10]:
            line_1 = "select * from (select k%s, sum(k2) over (partition by k%s, k%s) \
                as ss from %s)s  where s.k%s > '2010-01-01 09:00:00' order by k%s" % \
               (index + 1, index + 1, index + 1, join_name, index + 1, index + 1)
            line_2 = "select * from (select k%s, sum(k2) over (partition by k%s, k%s) \
                    as ss from %s where k%s > '2010-01-01 09:00:00')s order by k%s" % \
                    (index + 1, index + 1, index + 1, join_name, index + 1, index + 1)
        else:
            line_1 = 'select * from (select k%s, sum(k2) over (partition by k%s, k%s) \
                as ss from %s)s  where s.k%s in (10, 30) order by k%s' % \
                (index + 1, index + 1, index + 2, join_name, index + 1, index + 1)
            line_2 = 'select * from (select k%s, sum(k2) over (partition by k%s, k%s) \
                    as ss from %s where k%s in (10, 30))s order by k%s' % \
                   (index + 1, index + 1, index + 2, join_name, index + 1, index + 1)
        runner.check_predicate(line_1, '`k%s`' % (index + 1), frag_no=3)
        runner.check2_palo(line_1, line_2)
        
    line_1 = "select * from (select k1, k2, sum(k2) over (partition by k1, k2)\
         as ss from %s)s  where s.k1 + s.k2 > 10 order by k1 limit 1" % table_name
    line_2 = "select * from (select k1, k2, sum(k2) over (partition by k1, k2)\
         as ss from %s where k1 + k2 > 10)s order by k1 limit 1" % table_name
    runner.check_predicate(line_1, '`k1` + `k2`', frag_no=3)
    runner.check2_palo(line_1, line_2)


def test_complex_predicate():
    """
    {
    "title": "test_query_predicate_pushdown.test_complex_predicate",
    "describe": "test_complex_predicate, where里有and，谓词会下推,测试点：2个where",
    "tag": "system,p1"
    }
    """
    """
    test_complex_predicate, where里有and，谓词会下推
    测试点：2个where
    """
    line_1 = "select * from (select k1, k2, sum(k2) over (partition by k1, k2)\
         as ss from %s)s  where s.k1> 10 and s.k2 > 10 order by k1" % table_name
    line_2 = "select * from (select k1, k2, sum(k2) over (partition by k1, k2)\
         as ss from %s where k1>10 and k2 > 10)s order by k1" % table_name
    runner.check_predicate(line_1, '`k1` > 10, `k2` > 10', frag_no=3)
    runner.check2_palo(line_1, line_2)

    line_1 = "select * from (select k1, k2, sum(k2) over (partition by k1 + k2)\
         as ss from %s)s  where s.k1> 8 and s.k2 > 10 order by k1" % table_name
    line_2 = "select * from (select k1, k2, sum(k2) over (partition by k1 + k2)\
         as ss from %s where k1>8 and k2 > 10)s order by k1" % table_name
    runner.check_predicate(line_1, '> 10', frag_no=2, ScanNode_predicate=False)
    runner.check_predicate(line_1, '`k1`', verify=False)
    runner.check_predicate(line_1, '> 10', frag_no=2, ScanNode_predicate=False)
    runner.check2_palo(line_1, line_2)

    line_1 = "select * from (select k1, k2, sum(k2) over (partition by k1)\
         as ss from %s)s  where s.k1> 10 and s.k2 > 10 order by k1" % table_name
    line_2 = "select * from (select k1, k2, sum(k2) over (partition by k1)\
         as ss from %s where k1>10 and k2 > 10)s order by k1" % table_name
    runner.check_predicate(line_1, '`k1` > 10', frag_no=3)
    runner.check2_palo(line_1, line_2)

    line_1 = "select * from (select k1, k2, sum(k2) over (partition by k1)\
         as ss from %s where k1> 10)s  where s.k2 > 10 order by k1" % table_name
    line_2 = "select * from (select k1, k2, sum(k2) over (partition by k1)\
         as ss from %s where k1>10 and k2 > 10)s order by k1" % table_name
    runner.check_predicate(line_1, '`k1` > 10', frag_no=3)
    runner.check2_palo(line_1, line_2)

    line_1 = "select * from (select k1, k2, sum(k2) over (partition by k1 + k2)\
         as ss from %s where k1> 10)s  where s.k2 > 10 order by k1" % table_name
    line_2 = "select * from (select k1, k2, sum(k2) over (partition by k1 + k2)\
         as ss from %s where k1>10 and k2 > 10)s order by k1" % table_name
    runner.check_predicate(line_1, '> 10', frag_no=2, ScanNode_predicate=False)
    runner.check_predicate(line_1, '> 10', frag_no=2, ScanNode_predicate=False)
    runner.check_predicate(line_1, '`k1` > 10', frag_no=3)
    runner.check_predicate(line_1, '`k2`', verify=False)
    runner.check2_palo(line_1, line_2)

    line = "select * from (select k1, k2 from %s group by k1, k2)s  \
       where s.k1 > 10 and hex(1) > 1" % table_name
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, '`k1` > 10', frag_no=3)
    else:
        runner.check_predicate(line, '`k1` > 10', frag_no=2)
    line = "select * from (select k1, k2 from %s where k1 > 10\
       group by k1, k2 having hex(1) > 1)s" % table_name
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, '`k1` > 10', frag_no=3)
    else:
        runner.check_predicate(line, '`k1` > 10', frag_no=2)
    line = "select * from (select k1, k2 from %s where k1 > 10 \
       group by k1, k2 having hex(k1) > 1)s" % table_name
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, '`k1` > 10, hex(`k1`) > 1.0', frag_no=3)
    else:
        runner.check_predicate(line, '`k1` > 10, hex(`k1`) > 1.0', frag_no=2)


def test_windowns_function():
    """
    {
    "title": "test_query_predicate_pushdown.test_windowns_function",
    "describe": "测试支持的窗口函数",
    "tag": "system,p1"
    }
    """
    """测试支持的窗口函数"""
    func_list = ["sum", 'avg', 'max', 'min', 'FIRST_VALUE']
    not_support = ['cume_dist', 'DENSE_RANK', 'NTH_VALUE', 
                   'PERCENT_RANK', 'RANK', 'ROW_NUMBER']
    ##lag lead
    excepetd_err = "No matching function"
    for func in func_list:
        line = "select k1, k2, %s(k2) over (partition by k1) as ss from " \
               "%s where k1>10" % (func, table_name)
        runner.check_predicate(line, '`k1` > 10', frag_no=3)
        
    for func in not_support:
        try:
            line = "select k1, k2, %s(k2) over (partition by k1) as ss " \
                   "from %s where k1>10" % (func, table_name)
            runner.check_predicate(line, '`k1` > 10')
            assert 0 == 1
        except Exception as err:
            assert excepetd_err in str(err), "excepet %s, actual res %s" \
                % (excepetd_err, str(err))

    line = "select k1, k2, LAG(k2,2,1) over (partition by k1) as ss " \
           "from %s where k1>10" % table_name
    runner.check_predicate(line, '`k1` > 10', frag_no=3)
    line = "select k1, k2, LEAD(k2,2,1) over (partition by k1) as ss " \
           "from %s where k1>10" % table_name
    runner.check_predicate(line, '`k1` > 10', frag_no=3)
    line = "select k1, k2, NTILE(3) over (partition by k1) as ss " \
           "from %s where k1>10" % table_name
    runner.check_predicate(line, '`k1` > 10', frag_no=3)


###多join会有多个scannode
def test_predicate_multi_join():
    """
    {
    "title": "test_query_predicate_pushdown.test_predicate_multi_join",
    "describe": "测试多join下的谓词下推",
    "tag": "system,p1"
    }
    """
    """测试多join下的谓词下推
    join：两个表可下推
    left join：右表可下推
    right join：左表可下推
    """
    right_frag_no = 3
    left_frag_no = 4
    line = 'show data'
    print(runner.query_palo.do_sql(line))
    line1 = "select * from (select k1, k2, sum(k2) over (partition by k1, k2) as ss\
       from %s where k1 > 10)s order by k1, k2" % join_name
    line2 = "select * from (select k1, k2, sum(k2) over (partition by k1, k2) as ss\
       from %s)s  where s.k1 > 10 order by k1, k2" % join_name
    runner.check2_palo(line1, line2)
    runner.check_predicate(line2, '`k1` > 10', frag_no=right_frag_no)
    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b\
       on a.k1 = b.k1)s where s.ak1 > 1 and s.bk1 < 2 \
       order by ak1, bk1" % (table_name, table_name)
    runner.check(line)
    # runner.check_predicate(line, '`k1` > 1', frag_no=2)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, '`k1` < 2', frag_no=3)
    else:
        runner.check_predicate(line, '`k1` < 2', frag_no=2)
    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b\
       on a.k1 = b.k1 where a.k1 > 1)s order by ak1, bk1" % (table_name, table_name)
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, '`k1` > 1', frag_no=4)
    elif db_name == 'test_query_qa_unique':
        runner.check_predicate(line, '`k1` > 1', frag_no=2, scannode_no=2)
    else:
        runner.check_predicate(line, '`k1` > 1', frag_no=2)
    ##issue 2094，todo，后续优化，详见https://github.com/apache/incubator-doris/issues/2094
    ##runner.check_predicate(line, '`k1` > 1', frag_no=right_frag_no)
    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b\
       on a.k1 = b.k1)s where s.bk1 > 1 order by ak1, bk1" % (table_name, table_name)
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, '`k1` > 1', frag_no=3)
    else:
        runner.check_predicate(line, '`k1` > 1', frag_no=2)
    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b\
       on a.k1 = b.k1 where b.k1 > 1)s order by ak1, bk1" % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, '`k1` > 1', frag_no=2)

    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b\
       on a.k1 = b.k1)s where s.ak1 > 1 order by ak1, bk1" % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, '`k1` > 1', frag_no=3)
    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b\
       on a.k1 = b.k1 where a.k1 > 1)s order by ak1, bk1" % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, '`k1` > 1', frag_no=3)

    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b\
           on a.k1 = b.k1 where a.k1 > 1 or a.k1 in (2, 10))s \
           order by ak1, bk1" % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        check_value_list = ['`a`.`k1` > 1 OR `a`.`k1` IN (2, 10)',
                            '`a`.`k1` IN (2, 10) OR `a`.`k1`']
        runner.check_predicate(line, check_value_list, frag_no=3)

    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b\
       on a.k1 > b.k1)s where s.ak1 > 1 order by ak1, bk1" % (table_name, table_name)
    runner.check(line)
    runner.check_predicate(line, '`k1` > 1', frag_no=2)

    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b\
       on a.k1 > b.k1 where a.k1 > 1)s order by ak1, bk1" % (table_name, table_name)
    runner.check(line)
    runner.check_predicate(line, '`k1` > 1', frag_no=2)

    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b\
       on a.k1 < b.k1)s where s.ak1 > 1 order by ak1, bk1" % (table_name, table_name)
    runner.check(line)
    runner.check_predicate(line, '`k1` > 1', frag_no=2)
    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b\
       on a.k1 < b.k1 where a.k1 > 1)s order by ak1, bk1" % (table_name, table_name)
    runner.check(line)
    runner.check_predicate(line, '`k1` > 1', frag_no=2)
    line = "select a.k1 ak1, b.k1 bk1 from %s a join %s b \
       on a.k1 = b.k1 where a.k1 + a.k2>10 order by ak1, bk1" % (table_name, table_name)
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, '`a`.`k1` + `a`.`k2` > 10', frag_no=4)
    elif db_name == 'test_query_qa_unique':
        runner.check_predicate(line, '`a`.`k1` + `a`.`k2` > 10', frag_no=2, scannode_no=2)
    else:
        runner.check_predicate(line, '`a`.`k1` + `a`.`k2` > 10', frag_no=2)

    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b\
       on a.k1 = b.k1)s where s.bk1 > s.ak1 order by ak1, bk1" % (table_name, join_name)
    runner.check(line)
    runner.check_predicate(line, '`b`.`k1` > `a`.`k1`', verify=False)

    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b\
       on a.k1 = b.k1 and b.k1 > a.k1)s order by ak1, bk1" % (table_name, join_name)
    runner.check_predicate(line, '`b`.`k1` > `a`.`k1`', verify=False)
    runner.check(line)
    ##left join,右表谓词可下推
    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a left join %s b on\
       a.k1 = b.k1 and b.k1>10)s order by ak1, bk1" % (join_name, table_name)
    runner.check(line)
    runner.check_predicate(line, '`b`.`k1` > 10', frag_no=3)
    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a left join %s b on\
           a.k1 = b.k1 and a.k1>10)s order by ak1, bk1" % (join_name, table_name)
    runner.check(line)
    runner.check_predicate(line, '`a`.`k1` > 10', verify=False)
    ##right join,左表谓词可下推
    line = "select * from (select a.k1 ak1 from %s a right join %s b on\
           a.k1 = b.k1 and b.k1>10)s order by ak1 nulls first" % (table_name, table_name)
    line2 = "select * from (select a.k1 ak1 from %s a right join %s b on\
           a.k1 = b.k1 and b.k1>10)s order by ak1" % (table_name, table_name)
    runner.check2(line, line2)
    runner.check_predicate(line, '`b`.`k1` > 10', verify=False)
    line = "select * from (select a.k1 ak1 from %s a right join %s b on\
               a.k1 = b.k1 and a.k1>10)s order by ak1 nulls first" % (table_name, table_name)
    line2 = "select * from (select a.k1 ak1 from %s a right join %s b on\
               a.k1 = b.k1 and a.k1>10)s order by ak1" % (table_name, table_name)
    runner.check2(line, line2)
    runner.check_predicate(line, '`a`.`k1` > 10', verify=False)

    ##multi join
    line = "select * from (select s.bk1 from (select b.k1 bk1 from %s a\
       join %s b on a.k1 = b.k1)s join baseall bb on s.bk1=bb.k1) hh\
        order by hh.bk1 nulls first" % (table_name, table_name)
    line2 = "select * from (select s.bk1 from (select b.k1 bk1 from %s a\
       join %s b on a.k1 = b.k1)s join baseall bb on s.bk1=bb.k1) hh\
        order by hh.bk1" % (table_name, table_name)
    runner.check2(line, line2)
    runner.check_predicate(line, '`b`.`k1` = `bb`.`k1`', verify=False)


def test_multi_windows_func():
    """
    {
    "title": "test_query_predicate_pushdown.test_multi_windows_func",
    "describe": "多个窗口函数并存,谓词可以下推",
    "tag": "system,p1,fuzz"
    }
    """
    """多个窗口函数并存,谓词可以下推"""
    line = "select s.ss from (select k1, sum(k2) over (partition by k2), sum(k2) over \
       (partition by k1) as ss from %s)s  where s.k1 > 10 order by k1" % table_name
    line_2 = "select s.ss from (select k1, sum(k2) over (partition by k2), sum(k2) over \
           (partition by k1) as ss from %s  where k1 > 10 )s order by k1" % table_name
    runner.check2_palo(line, line_2)
    runner.check_predicate(line, '> 10', frag_no=2, ScanNode_predicate=False)
    runner.check_predicate(line, '> 10', frag_no=3, verify=False)

    line = "select s.ss from (select k1, sum(k2) over (partition by k2), sum(k2) over \
               (partition by k1) as ss from %s)s where s.k1 in (10, 2) order by k1" % table_name
    line_2 = "select s.ss from (select k1, sum(k2) over (partition by k2), sum(k2) over \
               (partition by k1) as ss from %s where k1 in (10, 2))s order by k1" % table_name
    runner.check2_palo(line, line_2)
    runner.check_predicate(line, 'IN (10, 2)', frag_no=2, ScanNode_predicate=False)
    line = "select s.k1 from (select k1, sum(k2) over (partition by k2), sum(k3) over \
                   (partition by k1), sum(k8) over (partition by k2),\
                   sum(k4) over (partition by k3) as ss from %s)s where s.k1>10 order by s.k1" % table_name
    line_2 = "select s.k1 from (select k1, sum(k2) over (partition by k2), sum(k3) over \
               (partition by k1), sum(k8) over (partition by k2),\
               sum(k4) over (partition by k3) as ss from %s where k1>10)s order by s.k1" % table_name
    runner.check2_palo(line, line_2)
    runner.check_predicate(line, '`k1` > 10', frag_no=2, ScanNode_predicate=False)

    line = "select * from (select k1, sum(k2) over (partition by k2),\
       sum(k2) over (partition by k3),\
       sum(k8) over (partition by k2) as ss from %s)s where s.k1>10" % join_name
    runner.checkwrong(line)


def test_predicate_order_by():
    """
    {
    "title": "test_query_predicate_pushdown.test_predicate_order_by",
    "describe": "order by：分为窗口函数里和非窗口函数,子查询里有order by的不可下推",
    "tag": "system,p1,fuzz"
    }
    """
    """order by：分为窗口函数里和非窗口函数,子查询里有order by的不可下推"""
    line = "select * from (select k1, k2, sum(k2) over (partition by k1) as ss from %s\
        order by k1, k2)s  where s.k1 > 10" % table_name
    line2 = "select * from (select k1, k2, sum(k2) over (partition by k1) as ss from %s\
        where k1 > 10 order by k1, k2)s" % table_name
    runner.check2_palo(line, line2, True)
    runner.check_predicate(line, '> 10', verify=False)
    line = "select * from (select k1, k2, sum(k2) as ss from %s \
       group by k1, k2 order by k1, k2)s  where s.k1 in (10, 2) or s.k1 = 4" % table_name
    runner.check(line, True)
    runner.check_predicate(line, '`k1` > 10', verify=False)

    line = "select k1, k2, sum(k2) over (partition by k1) as ss from %s\
       where k1 in (10, 2) or k1 = 20 order by k1, k2" % table_name
    runner.check_predicate(line, '`k1` IN (10, 2, 20)', frag_no=3)

    line = "select * from (select k1, k2 from %s order by k1, k2)s \
       where s.k1 > 10" % table_name
    runner.check(line, True)
    runner.check_predicate(line, '`k1` > 10', verify=False)
    line = "select * from (select k1, k2 from %s)s  where s.k1 > 10 order by k1,k2" % table_name
    runner.check(line)
    runner.check_predicate(line, '`k1` > 10', frag_no=2)

    line = "select * from (select k1, k2, sum(k2) over (partition by k1) as ss \
       from %s order by k1, k2)s  where s.k1 > 10" % table_name
    line2 = "select * from (select k1, k2, sum(k2) over (partition by k1) as ss \
       from %s where k1 > 10 order by k1, k2 )s" % table_name
    runner.check2_palo(line, line2, True)
    runner.check_predicate(line, '`k1` > 10', verify=False)
    line = "select * from (select k1, k2, sum(k2) over (partition by k1) as ss\
       from %s)s  where s.k1 = 10" % table_name
    line2 = "select * from (select k1, k2, sum(k2) over (partition by k1) as ss\
       from %s where k1 = 10)s" % table_name
    runner.check2_palo(line, line2, True)
    if db_name == 'test_query_qa_list':
        runner.check_predicate(line, '`k1` = 10', frag_no=3)
    else:
        runner.check_predicate(line, '`k1` = 10', frag_no=2)
    line = "select * from (select k1, k2, sum(k2) over (partition by k1) as ss\
       from %s order by k1, k2)s where s.k1 >10" % table_name
    line2 = "select * from (select k1, k2, sum(k2) over (partition by k1) as ss\
       from %s  where k1 >10 order by k1, k2)s" % table_name
    runner.check2_palo(line, line2, True)
    runner.check_predicate(line, '`k1` > 10', verify=False)

    line = "select k1, k2 from %s where k1>10 order by k1, k2" % join_name
    runner.check(line)
    runner.check_predicate(line, '`k1` > 10', frag_no=2)

    ##order by的影响
    line = "select * from (select k1, k2 from %s )s  where s.k1 > 10 order by k1, k2" % table_name
    runner.check(line)
    runner.check_predicate(line, '`k1` > 10', frag_no=2)
    line = "select * from (select k1, k2 from %s order by k1, k2)s \
       where s.k1 > 10 order by k1, k2" % table_name
    runner.check(line)
    runner.check_predicate(line, '`k1` > 10', verify=False)

    line = "select * from (select k1, k2, sum(k2) over (partition by k1) as ss " \
           "from %s order by k1, k2 where ss > 10)s;" % table_name
    runner.checkwrong(line)


def test_predicate_group_by():
    """
    {
    "title": "test_query_predicate_pushdown.test_predicate_group_by",
    "describe": "聚合类谓词下推,测试点:比较结果是否正确；再比较是否进行谓词正确下推",
    "tag": "system,p1"
    }
    """
    """聚合类谓词下推
    测试点:比较结果是否正确；再比较是否进行谓词正确下推
    """
    line = "select k1 ,sum(k2) from %s group by k1 having sum(k1)>100 order by k1" % table_name
    runner.check(line)
    runner.check_predicate(line, '> 10', verify=False)
    line = "select * from (select k1, k2 from %s group by k1, k2)s \
      where hex(1) > 1 order by k1" % table_name
    runner.check(line)
    runner.check_predicate(line, '> 1.0', verify=False)
    line = "select * from (select k1, k2 from %s group by k1, k2 \
       having hex(1) > 1)s order by k1" % table_name
    runner.check(line)
    runner.check_predicate(line, '> 1.0', verify=False)
    line = "select * from (select k1, k2 from %s group by k1, k2)s\
       where hex(k1) > 1 or hex(k1) in (1,3) order by k1" % table_name
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, "hex(`k1`) > 1.0 OR hex(`k1`) IN ('1', '3')", frag_no=3)
    else:
        runner.check_predicate(line, "hex(`k1`) > 1.0 OR hex(`k1`) IN ('1', '3')", frag_no=2)
    line = "select * from (select k1, k2 from %s group by k1, k2 \
      having hex(k1) > 1)s order by k1" % table_name
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, '> 1.0', frag_no=3)
    else:
        runner.check_predicate(line, '> 1.0', frag_no=2)
    line = "select * from (select k1, k2 from %s group by k1, k2)s \
       where s.k1 > 10 order by k1" % table_name
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, '`k1` > 10', frag_no=3)
    else:
        runner.check_predicate(line, '`k1` > 10', frag_no=2)
    line = "select * from (select k1, k2 from %s group by k1, k2)s\
       where s.k2 > s.k1 order by k1" % table_name
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, '`k2` > `k1`', frag_no=3)
    else:
        runner.check_predicate(line, '`k2` > `k1`', frag_no=2)
    line = "select * from (select k1, k2 from %s where k2 > k1\
       group by k1, k2)s order by k1" % table_name
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, '`k2` > `k1`', frag_no=3)
    else:
        runner.check_predicate(line, '`k2` > `k1`', frag_no=2)
    line = "select * from (select k1, k2, k3 as sk3 from %s group by k1, k2,k3)s\
       where s.k1 > 10 and s.sk3 > 10 order by k1" % table_name
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, '`k1` > 10, `k3` > 10', frag_no=3)
    else:
        runner.check_predicate(line, '`k1` > 10, `k3` > 10', frag_no=2)


def test_predicate_subquery_nest():
    """
    {
    "title": "test_query_predicate_pushdown.test_predicate_subquery_nest",
    "describe": "子查询嵌套, 测试点：比对结果；验证谓词是否下推",
    "tag": "system,p1"
    }
    """
    """子查询嵌套
    测试点：比对结果；验证谓词是否下推
    """
    #where 同一个key
    line = "select * from (select s.k1, s.k2 from (select k1, k2, sum(k2) over \
       (partition by k1) as ss from %s\
       where k1>10)s group by s.k1, s.k2)s2 where s2.k1 > 20 order by s2.k1" % table_name
    line_2 = "select * from (select s.k1, s.k2 from (select k1, k2, sum(k2) over \
       (partition by k1) as ss from %s\
       where k1>10)s where s.k1>20  group by s.k1, s.k2)s2 order by s2.k1" % table_name
    runner.check2_palo(line, line_2)
    if db_name == 'test_query_qa_list':
        runner.check_predicate(line, '`k1` > 10, `k1` > 20', frag_no=3)
    else:
        runner.check_predicate(line, '`k1` > 10, `k1` > 20', frag_no=2)
    line = "select * from (select s.k1, s.k2 from (select k1, k2, sum(k2) over (partition by k1)\
      as ss from %s where k1 > 10 and k1 > 20)s \
      group by s.k1, s.k2)s2 order by s2.k1" % table_name
    line_2 = "select * from (select s.k1, s.k2 from (select k1, k2, sum(k2) over (partition by k1)\
          as ss from %s)s group by s.k1, s.k2)s2 \
          where s2.k1 > 10 and s2.k1 > 20 order by s2.k1" % table_name
    runner.check2_palo(line, line_2)
    if db_name == 'test_query_qa_list':
        runner.check_predicate(line, '`k1` > 10, `k1` > 20', frag_no=3)
    else:
        runner.check_predicate(line, '`k1` > 10, `k1` > 20', frag_no=2)
    #where 不同key
    line = "select * from (select s.k1, s.k2 from (select k1, k2, sum(k2) over \
           (partition by k1) as ss from %s\
           where k1>10)s group by s.k1, s.k2, s.ss)d order by d.k1" % table_name
    line_2 = "select * from (select s.k1, s.k2 from (select k1, k2, sum(k2) over \
           (partition by k1) as ss from %s)s\
           where s.k1>10 group by s.k1, s.k2, s.ss)d order by d.k1" % table_name
    runner.check2_palo(line, line_2)
    runner.check_predicate(line, '`k1` > 10', frag_no=3)
    line = "select * from (select s.k1, s.k2 from (select k1, k2, sum(k2) over (partition by k1)\
          as ss from %s where k1 > 10 )s group by s.k1, s.k2)s2 order by k1" % table_name
    line_2 = "select * from (select s.k1, s.k2 from (select k1, k2, sum(k2) over (partition by k1)\
          as ss from %s)s group by s.k1, s.k2)s2 where s2.k1>10 order by k1" % table_name
    runner.check2_palo(line, line_2)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, '`k1` > 10', frag_no=3)
    else:
        runner.check_predicate(line, '`k1` > 10', frag_no=2)

    line_1 = "select k1, k2, sum(k2) over (partition by k1) as ss from %s \
       where k1 + k2 > 10 order by k1, k2" % table_name
    line_2 = "select k1, k2, sum(k2) over (partition by k1) as ss from %s\
       where k2 + k1 > 8 + 2 order by k1, k2" % table_name
    runner.check2_palo(line_1, line_2)
    runner.check_predicate(line_1, '`k1` + `k2` > 10', frag_no=3)
    line_1  = "select s.k1, s.k2, s.ss from (select k1, k2, sum(k2) over (partition by k1) as ss \
       from %s)s  where s.k1 + s.k2 > 10 group by s.k1, s.k2, s.ss order by s.k1, s.k2" % table_name
    line_2 = "select s.k1, s.k2, s.ss from (select k1, k2, sum(k2) over (partition by k1) as ss \
           from %s where k1 + k2 > 10)s group by s.k1, s.k2, s.ss order by s.k1, s.k2" % table_name
    runner.check2_palo(line_1, line_2)
    runner.check_predicate(line_1, '`k1` + `k2` > 10', verify=False)
    line = "select * from (select s.ak1, s.ak2 from (select a.k1 ak1, a.k2 ak2 \
     from %s a join %s b on a.k1=b.k1)s )s2 where s2.ak1 \
     between 20 and 50 order by s2.ak1" % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, '`a`.`k1` >= 20, `a`.`k1` <= 50', frag_no=3)

    ##scan 和select 层次的下推
    line_1 = "select * from (select s.k1, s.k2, s.ss from (select k1, k2, sum(k2) over \
       (partition by k1) as ss from %s)s  where s.k2 > 10 group by s.k1, s.k2, s.ss)s2\
        where s2.k1 > 10 and s2.k2 > 9 order by k1, k2" % table_name
    line_2 = "select * from (select s.k1, s.k2, s.ss from (select k1, k2, sum(k2) over \
           (partition by k1) as ss from %s  where k2 > 10)s  where s.k1 > 10 and s.k2 > 9\
            group by s.k1, s.k2, s.ss)s2 order by k1, k2" % table_name
    runner.check2_palo(line_1, line_2)
    runner.check_predicate(line_1, '`k1` > 10', frag_no=3)
    line_1 = "select * from (select s.k2, s.k1, s.ss from (select k2, k1, sum(k2) over \
       (partition by k1) as ss from %s)s  where s.k1 > 10 group by s.k1, s.k2, s.ss)s2 \
       where s2.k2 > 10 and s2.k1 > 9 order by k1" % table_name
    line_2 = "select * from (select s.k2, s.k1, s.ss from (select k2, k1, sum(k2) over \
           (partition by k1) as ss from %s where k1 > 10 )s group by s.k1, s.k2, s.ss)s2 \
           where s2.k2 > 10 and s2.k1 > 9 order by k1" % table_name
    runner.check2_palo(line_1, line_2)
    # todo: if set disable_colocate_plan = true then frag_no=4
    runner.check_predicate(line_1, '`k1` > 10, `k1` > 9', frag_no=3)
    line_1 = "select * from (select s.k1, s.k3, s.ss from (select k1, k3, sum(k2) over\
       (partition by k1) as ss from %s)s  where s.k3 > 1 group by s.k1, s.k3, s.ss)s2\
        where s2.k1 > 10 and s2.k3 > 8 order by k1" % table_name
    line_2 = "select * from (select s.k1, s.k3, s.ss from (select k1, k3, sum(k2) over\
       (partition by k1) as ss from %s  where k3 > 1)s group by s.k1, s.k3, s.ss)s2\
        where s2.k1 > 10 and s2.k3 > 8 order by k1" % table_name
    runner.check2_palo(line_1, line_2)
    # todo: if set disable_colocate_plan = true then frag_no=4
    runner.check_predicate(line_1, '`k3` > 1, `k3` > 8', frag_no=3, verify=False)
     

def test_predicate_like():
    """
    {
    "title": "test_query_predicate_pushdown.test_predicate_like",
    "describe": "测试谓词like的下推",
    "tag": "system,p1"
    }
    """
    """测试谓词like的下推"""
    line = "select * from (select k1, k2 from %s where k6 like 'tr')s order by k1" % table_name
    runner.check(line)
    runner.check_predicate(line, "`k6` LIKE 'tr'", frag_no=2)
    line = "select * from (select k1, k2 from %s where k6 like 'tr'\
       group by k1, k2)s order by k1" % table_name
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, "`k6` LIKE 'tr'", frag_no=3)
    else:
        runner.check_predicate(line, "`k6` LIKE 'tr'", frag_no=2)
    line = "select * from (select k1, k6 from %s order by k1)s \
       where s.k6 like 'tr' order by k6" % table_name
    runner.check(line)
    runner.check_predicate(line, "`k6` LIKE 'tr'", verify=False)
    line = "select * from (select k1, k6 from %s where k6 like 'tr'\
       order by k1 )s  where s.k6 like 't' order by k6" % table_name
    runner.check(line)
    runner.check_predicate(line, "`k6` LIKE 'tr'", frag_no=2)
    line = "select * from (select a.k6 ak6, b.k6 bk6 from %s a\
      join %s b on a.k6 = b.k6)s where s.ak6 like 'tr' order by ak6" % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, "`k6` LIKE 'tr'", frag_no=3)
    line = "select * from (select s.ak1, s.ak2 from (select a.k1 ak1, a.k2 ak2\
       from %s a join %s b on a.k1=b.k1)s )s2 \
       where s2.ak1>20 order by s2.ak1" % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, "`a`.`k1` > 20", frag_no=3)
    line = "select * from (select a.k6 ak6, b.k6 bk6 from %s a \
       join %s b on a.k6 = b.k6)s where s.ak6 not like 'tr' order by ak6"\
        % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, "NOT `a`.`k6` LIKE 'tr'", frag_no=3)
    line = "select * from (select a.k6 ak6, b.k6 bk6 from %s a \
      join %s b on a.k6 = b.k6)s where s.bk6 like 'tr' order by ak6"\
       % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, "`k6` LIKE 'tr'", frag_no=2)
    line = "select * from (select a.k6 ak6, b.k6 bk6 from %s a \
       join %s b on a.k6 = b.k6 group by a.k6, b.k6)s where s.ak6 like 'tr' \
       order by ak6" % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, "`k6` LIKE 'tr'", frag_no=4)


def test_predicate_or():
    """
    {
    "title": "test_query_predicate_pushdown.test_predicate_or",
    "describe": "测试谓词or的下推",
    "tag": "system,p1"
    }
    """
    """测试谓词or的下推"""
    line = "select * from (select k1, k2 from %s where k1 = 10 or k1 = 30)s order by k1" % table_name
    runner.check(line)
    runner.check_predicate(line, "`k1` IN (10, 30)", frag_no=2)
    line = "select * from (select k1, k2 from %s where k1 = 10 \
      or k1 = 30 group by k1, k2)s order by k1" % table_name
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, "`k1` IN (10, 30)", frag_no=3)
    else:
        runner.check_predicate(line, "`k1` IN (10, 30)", frag_no=2)
    line = "select * from (select k1, k6 from %s order by k1)s \
       where s.k1 = 10 or s.k1 =30 order by k1" % table_name
    runner.check(line)
    runner.check_predicate(line, "`k1` <= 30", verify=False)
    line = "select * from (select k1, k6 from %s where k1 = 10 \
      or k1 = 30 order by k1 )s  where s.k6 like 't' order by k6" % table_name
    runner.check(line)
    runner.check_predicate(line, "`k1` IN (10, 30)", frag_no=2)
    line = "select * from (select s.ak1, s.ak2 from (select a.k1 ak1, a.k2 ak2\
           from %s a join %s b on a.k1=b.k1)s )s2 \
           where s2.ak1>20  or s2.ak1<50 order by s2.ak1" % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, "`a`.`k1` > 20 OR `a`.`k1` < 50", frag_no=3)

    line = "select * from (select a.k1 ak1, b.k6 bk6 from %s a\
       join %s b on a.k1 = b.k1)s where s.ak1 = 10 or s.ak1 = 30 \
       order by s.ak1" % (table_name, table_name)
    runner.check(line)
    if db_name == 'test_query_qa_list':
        runner.check_predicate(line, "`k1` IN (10, 30)", frag_no=4)
    elif db_name == 'test_query_qa_unique':
        runner.check_predicate(line, "`k1` IN (10, 30)", frag_no=2, scannode_no=2)
    else:
        runner.check_predicate(line, "`k1` IN (10, 30)", frag_no=2)
    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a \
      join %s b on a.k1 = b.k1)s where s.bk1 = 10 or s.bk1 = 30 \
      order by ak1" % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, "`k1` IN (10, 30)", frag_no=2)
    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a \
       join %s b on a.k1 = b.k1 group by a.k1, b.k1)s \
       where s.ak1 = 10 or s.ak1 =30 order by s.ak1" % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, "`k1` IN (10, 30)", frag_no=4)

    line = "select * from (select k1, k6 from %s where k1 = 10 or \
       k1 in (30,40) order by k1 )s  where s.k6 like 't' order by k6" % table_name
    runner.check(line)
    expect_predicate = ["`k1` IN (30, 40, 10)", "`k1` IN (10, 30, 40)"]
    runner.check_predicate(line, expect_predicate, frag_no=2)
    line = "select * from (select a.k1 ak1, b.k6 bk6 from %s a\
      join %s b on a.k1 = b.k1)s where s.ak1 in (10, 40) \
    or s.ak1 in (30, 90) order by s.ak1" % (table_name, table_name)
    runner.check(line)
    if db_name == 'test_query_qa_list':
        runner.check_predicate(line, "`a`.`k1` IN (10, 40, 30, 90)", frag_no=4)
    elif db_name == 'test_query_qa_unique':
        runner.check_predicate(line, "`a`.`k1` IN (10, 40, 30, 90)", frag_no=2, scannode_no=2)
    else:
        runner.check_predicate(line, "`k1` IN (10, 40, 30, 90)", frag_no=2)


def test_predicate_between():
    """
    {
    "title": "test_query_predicate_pushdown.test_predicate_between",
    "describe": "测试谓词between的下推",
    "tag": "system,p1"
    }
    """
    """测试谓词between的下推"""
    line = "select * from (select k1, k2 from %s where k1 between 10 and 30)s\
       order by k1" % table_name
    runner.check(line)
    runner.check_predicate(line, "`k1` >= 10, `k1` <= 30", frag_no=2)
    line = "select * from (select k1, k2 from %s where k1 between 10 and 30\
      group by k1, k2)s order by k1" % table_name
    runner.check(line)
    if db_name in ['test_query_qa_list', 'test_query_qa_multi']:
        runner.check_predicate(line, "`k1` >= 10, `k1` <= 30", frag_no=3)
    else:
        runner.check_predicate(line, "`k1` >= 10, `k1` <= 30", frag_no=2)
    line = "select * from (select k1, k6 from %s order by k1)s  where s.k1 \
      between 10 and 30 order by k1" % table_name
    runner.check(line)
    runner.check_predicate(line, "`k1` >= 10, `k1` <= 30", verify=False)
    line = "select * from (select k1, k6 from %s where k1 between 10 and 30\
       order by k1 )s  where s.k6 like 't' order by k6" % table_name
    runner.check(line)
    runner.check_predicate(line, "`k1` >= 10, `k1` <= 30", frag_no=2)
    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b on a.k1 = b.k1)s\
       where s.ak1 between 10 and 30 order by ak1" % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, "`a`.`k1` >= 10, `a`.`k1` <= 30", frag_no=3)
    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b on a.k1 = b.k1)s\
       where s.bk1 between 10 and 30 order by ak1" % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, "`b`.`k1` >= 10, `b`.`k1` <= 30", frag_no=2)
    line = "select * from (select a.k1 ak1, b.k1 bk1 from %s a join %s b on a.k1 = b.k1\
       group by a.k1, b.k1)s where s.ak1 between 10 and 30 order by ak1"\
        % (table_name, join_name)
    runner.check(line)
    if msg_flag:
        runner.check_predicate(line, "`a`.`k1` >= 10, `a`.`k1` <= 30", frag_no=4)
    

def teardown_module():
    """
    todo
    """
    pass
    # mysql_cursor.close()
    # mysql_con.close()

if __name__ == "__main__":
    setup_module()

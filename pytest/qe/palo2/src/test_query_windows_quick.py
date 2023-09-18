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
test_query_windows_quick.py
"""

import sys
import time
import random
sys.path.append("../lib/")
from palo_qe_client import QueryBase
import query_util as util

sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage

table_name = "baseall"

fields = ["k1", "k2", "k3", "k4", "k5", "k6", "k10", "k11",
          "k7", "k8", "k9"]


def setup_module():
    """
    setup 
    """
    global runner
    runner = QueryExecuter()


class QueryExecuter(QueryBase):
    def __init__(self):
#        super(QueryBase, self).__init__()
        self.get_clients()

    def checkwrong(self, line):
        """
        check the error
        """
        print(line)
        flag = 0
        #query_palo.do_sql_without_con('set FORCE_PRE_AGGREGATION=0', con)
        times = 0 
        while (times <= 5):
            try:
                LOG.info(L('palo sql', palo_sql=line))
                palo_result = self.query_palo.do_sql(line)
                LOG.info(L('palo result', palo_result=palo_result))
            except:
                flag += 1
                time.sleep(1)
            finally:
                times += 1
        assert flag == 6 
        print("hello world")

    def check2(self, line1, line2):
        """check """
        print(line1)
        print(line2)
        times = 0
        flag = 0
        while (times <= 10 and flag == 0):
            try:
                LOG.info(L('palo sql 1', palo_sql_1=line1))
                palo_result_1 = self.query_palo.do_sql(line1)
                LOG.info(L('palo sql 2', palo_sql_2=line2))
                palo_result_2 = self.query_palo.do_sql(line2)
                util.check_same(palo_result_1, palo_result_2)
                flag = 1
            except Exception as e:
                print(Exception, ":", e)
                print("hello")
                LOG.error(L('err', error=e))
                time.sleep(1)
                times += 1
                if (times == 3):
                    assert 0 == 1


def test_query_normal_aggression():
    """
    {
    "title": "test_query_windows_quick.test_query_normal_aggression",
    "describe": "normal count, max, min, sum",
    "tag": "function,p0"
    }
    """
    """
    normal count, max, min, sum
    """
    i = random.randint(0, 3)
    j = random.randint(0, 3)
    while (i == j):
        i = random.randint(0, 3)
        j = random.randint(0, 3)
    k1 = fields[i]
    k2 = fields[j]
    print("-------------------------------------")
    print(i, j)
    print("-------------------------------------")
    line1 = "select %s, sum(%s) over (partition by %s) as wj \
             from baseall order by %s, wj" \
             % (k1, k2, k1, k1)
    line2 = "select t1.%s, t2.mysum from baseall t1,\
             (select %s, sum(%s) as mysum from baseall \
             group by %s) t2 where t1.%s=t2.%s \
             order by t1.%s, t2.mysum" % (k1, k1, k2, k1, k1, k1, k1)
    runner.check2(line1, line2)

    line1 = "select * from (select %s, sum(%s) over (partition by %s) as wj \
             from baseall) b order by %s, wj" \
             % (k1, k2, k1, k1)
    runner.check2(line1, line2)

    i = random.randint(0, 10)
    j = random.randint(0, 10)
    while (i == j or i == 9):
        i = random.randint(0, 10)
        j = random.randint(0, 10)
    print("----------------------------")
    print(i, j)
    k1 = fields[i]
    k2 = fields[j]
    line1 = "select %s, min(%s) over (partition by %s) as wj \
             from baseall order by %s, wj" \
             % (k1, k2, k1, k1) 
    line2 = "select t1.%s, t2.mysum from baseall t1,\
             (select %s, min(%s) as mysum from baseall \
              group by %s) t2 where t1.%s=t2.%s \
              order by t1.%s, t2.mysum" % (k1, k1, k2, k1, k1, k1, k1)
    runner.check2(line1, line2)
    
    i = random.randint(0, 10)
    j = random.randint(0, 10)
    while (i == j or i == 9):
        i = random.randint(0, 10)
        j = random.randint(0, 10)
    print("----------------------------")
    print(i, j)
    print("----------------------------")
    k1 = fields[i]
    k2 = fields[j]
    line1 = "select %s, max(%s) over (partition by %s) as wj \
             from baseall order by %s, wj" % (k1, k2, k1, k1) 
    line2 = "select t1.%s, t2.mysum from baseall t1,\
             (select %s, max(%s) as mysum from baseall \
             group by %s) t2 where t1.%s=t2.%s \
             order by t1.%s, t2.mysum" % (k1, k1, k2, k1, k1, k1, k1)
    runner.check2(line1, line2)
    
    i = random.randint(0, 10)
    j = random.randint(0, 10)
    while (i == j or i == 9):
        i = random.randint(0, 10)
        j = random.randint(0, 10)
    print("------------------------------------")
    print(i, j)
    k1 = fields[i]
    k2 = fields[j]
    line1 = "select %s, count(%s) over (partition by %s) as wj \
             from baseall order by %s, wj" % (k1, k2, k1, k1) 
    line2 = "select t1.%s, t2.mysum from baseall t1,\
             (select %s, count(%s) as mysum from baseall \
             group by %s) t2 where t1.%s=t2.%s \
             order by t1.%s, t2.mysum" % (k1, k1, k2, k1, k1, k1, k1)
    runner.check2(line1, line2)


def test_query_normal_order_aggression():
    """
    {
    "title": "test_query_windows_quick.test_query_normal_order_aggression",
    "describe": "normal query add normal order by",
    "tag": "function,p0"
    }
    """
    """
    normal query add normal order by
    """
    i = random.randint(0, 10)
    j = random.randint(0, 10)
    k = random.randint(0, 10)
    while (i == j or i == k or j == k or k == 9 or i == 9):
        i = random.randint(0, 10)
        j = random.randint(0, 10)
        k = random.randint(0, 10) 
    print("---------------------------------")
    print(i, j, k)
    k1 = fields[i]
    k2 = fields[j]
    k3 = fields[k]
    print(i, j, k)
    print("--------------------------------------------------")
    line1 = "select %s, %s, count(%s) over (partition by %s, %s order by %s)\
             as wj from baseall order by %s, %s, wj" \
             % (k1, k3, k2, k1, k3, k3, k1, k3) 
    line2 = "select %s, count(%s) over (partition by %s order by %s \
             range between unbounded preceding and unbounded following) \
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1) 
    line3 = "select %s, count(%s) over (partition by %s order by %s \
             rows between unbounded preceding and unbounded following) \
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1) 
    line4 = "select t1.%s, t2.mysum from baseall t1,\
             (select %s, count(%s) as mysum from baseall \
             group by %s) t2 where t1.%s=t2.%s \
             order by t1.%s, t2.mysum" % (k1, k1, k2, k1, k1, k1, k1)
    line5 = "select t1.%s, t1.%s, t2.mysum from baseall t1,\
             (select %s, %s, count(%s) as mysum from baseall \
             group by %s, %s) t2 where t1.%s=t2.%s and t1.%s=t2.%s\
             order by t1.%s, t1.%s, t2.mysum" \
             % (k1, k3, k1, k3, k2, k1, k3, k1, k1, k3, k3, k1, k3)
               
    runner.check2(line1, line5)
    runner.check2(line2, line4)
    runner.check2(line3, line4)

    line1 = "select %s, %s, max(%s) over (partition by %s, %s order by %s)\
             as wj from baseall order by %s, %s, wj" \
             % (k1, k3, k2, k1, k3, k3, k1, k3) 
    line2 = "select %s, max(%s) over (partition by %s order by %s \
             range between unbounded preceding and unbounded following) \
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1) 
    line3 = "select %s, max(%s) over (partition by %s order by %s \
             rows between unbounded preceding and unbounded following) \
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1) 
    line4 = "select t1.%s, t2.mysum from baseall t1,\
             (select %s, max(%s) as mysum from baseall \
             group by %s) t2 where t1.%s=t2.%s \
             order by t1.%s, t2.mysum" % (k1, k1, k2, k1, k1, k1, k1)
    line5 = "select t1.%s, t1.%s, t2.mysum from baseall t1,\
             (select %s, %s, max(%s) as mysum from baseall \
             group by %s, %s) t2 where t1.%s=t2.%s and t1.%s=t2.%s\
             order by t1.%s, t1.%s, t2.mysum" \
             % (k1, k3, k1, k3, k2, k1, k3, k1, k1, k3, k3, k1, k3)
      
    runner.check2(line1, line5)
    runner.check2(line2, line4)
    runner.check2(line3, line4)

    line1 = "select %s, %s, min(%s) over (partition by %s, %s order by %s)\
             as wj from baseall order by %s, %s, wj" \
             % (k1, k3, k2, k1, k3, k3, k1, k3) 
    line2 = "select %s, min(%s) over (partition by %s order by %s \
             range between unbounded preceding and unbounded following) \
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1) 
    line3 = "select %s, min(%s) over (partition by %s order by %s \
             rows between unbounded preceding and unbounded following) \
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1) 
    line4 = "select t1.%s, t2.mysum from baseall t1,\
             (select %s, min(%s) as mysum from baseall \
             group by %s) t2 where t1.%s=t2.%s \
             order by t1.%s, t2.mysum" % (k1, k1, k2, k1, k1, k1, k1)
    line5 = "select t1.%s, t1.%s, t2.mysum from baseall t1,\
             (select %s, %s, min(%s) as mysum from baseall \
             group by %s, %s) t2 where t1.%s=t2.%s and t1.%s=t2.%s\
             order by t1.%s, t1.%s, t2.mysum" \
             % (k1, k3, k1, k3, k2, k1, k3, k1, k1, k3, k3, k1, k3)
      
    runner.check2(line1, line5)
    runner.check2(line2, line4)
    runner.check2(line3, line4)

    i = random.randint(0, 3)
    j = random.randint(0, 3)
    k = random.randint(0, 3)
    while (i == j or i == k or j == k):
        i = random.randint(0, 3)
        j = random.randint(0, 3)
        k = random.randint(0, 3)
    k1 = fields[i]
    k2 = fields[j]
    k3 = fields[k]
    print(i, j, k)
    print("--------------------------------------------------")

    line1 = "select %s, %s, sum(%s) over (partition by %s, %s order by %s)\
             as wj from baseall order by %s, %s, wj" \
             % (k1, k3, k2, k1, k3, k3, k1, k3) 
    line2 = "select %s, sum(%s) over (partition by %s order by %s \
             range between unbounded preceding and unbounded following) \
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1) 
    line3 = "select %s, sum(%s) over (partition by %s order by %s \
             rows between unbounded preceding and unbounded following) \
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1) 
    line4 = "select t1.%s, t2.mysum from baseall t1,\
             (select %s, sum(%s) as mysum from baseall \
             group by %s) t2 where t1.%s=t2.%s \
             order by t1.%s, t2.mysum" % (k1, k1, k2, k1, k1, k1, k1)
    line5 = "select t1.%s, t1.%s, t2.mysum from baseall t1,\
             (select %s, %s, sum(%s) as mysum from baseall \
             group by %s, %s) t2 where t1.%s=t2.%s and t1.%s=t2.%s\
             order by t1.%s, t1.%s, t2.mysum" \
             % (k1, k3, k1, k3, k2, k1, k3, k1, k1, k3, k3, k1, k3)
     
    runner.check2(line1, line5)
    runner.check2(line2, line4)
    runner.check2(line3, line4)


def test_query_preceding_current_aggression():
    """
    {
    "title": "test_query_windows_quick.test_query_preceding_current_aggression",
    "describe": "unbounded preceding and current row",
    "tag": "function,p0"
    }
    """
    """
    unbounded preceding and current row
    """
    i = random.randint(0, 10)
    j = random.randint(0, 10)
    k = random.randint(0, 10)
    while (i == j or i == k or j == k or i == 9):
        i = random.randint(0, 10)
        j = random.randint(0, 10)
        k = random.randint(0, 10)
    print("-----------------------------")
    print(i, j, k)

    k1 = fields[i]
    k2 = fields[j]
    k3 = fields[k]
    print(i, j, k)
    print("--------------------------------------------------")
    line1 = "select %s, count(%s) over (partition by %s order by %s\
             range between unbounded preceding and current row)\
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line2 = "select %s, count(%s) over (partition by %s order by %s)\
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line4 = "select a, count(c) from \
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,\
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,\
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,\
             t1.k10 as k10, t1.k11 as k11, \
             t1.%s as a, t1.%s as b, t2.%s as c \
             from baseall t1 join baseall  t2 \
             where t1.%s=t2.%s and t1.%s>=t2.%s) T \
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b \
             order by a, count(c)" \
             % (k1, k2, k2, k1, k1, k3, k3)
    runner.check2(line1, line4)
    runner.check2(line2, line4)
    
    line1 = "select %s, max(%s) over (partition by %s order by %s\
             range between unbounded preceding and current row)\
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line2 = "select %s, max(%s) over (partition by %s order by %s)\
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line4 = "select a, max(c) from \
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,\
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,\
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,\
             t1.k10 as k10, t1.k11 as k11, \
             t1.%s as a, t1.%s as b, t2.%s as c \
             from baseall t1 join baseall  t2 \
             where t1.%s=t2.%s and t1.%s>=t2.%s) T \
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b\
             order by a, max(c)" \
             % (k1, k2, k2, k1, k1, k3, k3)
    runner.check2(line1, line4)
    runner.check2(line2, line4)

    line1 = "select %s, min(%s) over (partition by %s order by %s\
             range between unbounded preceding and current row)\
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line2 = "select %s, min(%s) over (partition by %s order by %s)\
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line4 = "select a, min(c) from \
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,\
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,\
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,\
             t1.k10 as k10, t1.k11 as k11, \
             t1.%s as a, t1.%s as b, t2.%s as c \
             from baseall t1 join baseall  t2 \
             where t1.%s=t2.%s and t1.%s>=t2.%s) T \
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b\
             order by a, min(c)" \
             % (k1, k2, k2, k1, k1, k3, k3)
    runner.check2(line1, line4)
    runner.check2(line2, line4)
 
    i = random.randint(0, 3)
    j = random.randint(0, 3)
    k = random.randint(0, 3)
    while (i == j or i == k or j == k):
        i = random.randint(0, 3)
        j = random.randint(0, 3)
        k = random.randint(0, 3)
    print("---------------------------------")
    print(i, j, k)
    k1 = fields[i]
    k2 = fields[j]
    k3 = fields[k]
    print(i, j, k)
    print("--------------------------------------------------")
    line1 = "select %s, sum(%s) over (partition by %s order by %s\
             range between unbounded preceding and current row)\
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line2 = "select %s, sum(%s) over (partition by %s order by %s)\
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line4 = "select a, sum(c) from \
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,\
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,\
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,\
             t1.k10 as k10, t1.k11 as k11, \
             t1.%s as a, t1.%s as b, t2.%s as c \
             from baseall t1 join baseall  t2 \
             where t1.%s=t2.%s and t1.%s>=t2.%s) T \
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b\
             order by a, sum(c)" \
             % (k1, k2, k2, k1, k1, k3, k3)
    runner.check2(line1, line4)
    runner.check2(line2, line4)


def test_query_current_following_aggression():
    """
    {
    "title": "test_query_windows_quick.test_query_current_following_aggression",
    "describe": "unbounded current row and unbounded following",
    "tag": "function,p0"
    }
    """
    """
    unbounded current row and unbounded following
    """
    i = random.randint(0, 10)
    j = random.randint(0, 10)
    k = random.randint(0, 10)
    while (i == j or i == k or j == k or i == 9):
        i = random.randint(0, 10)
        j = random.randint(0, 10)
        k = random.randint(0, 10) 
    print("----------------------------------")
    print(i, j, k)
    k1 = fields[i]
    k2 = fields[j]
    k3 = fields[k]
    print(i, j, k)
    print("--------------------------------------------------")
    line1 = "select %s, count(%s) over (partition by %s order by %s\
             range between current row and unbounded following)\
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line4 = "select a, count(c) from \
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,\
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,\
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,\
             t1.k10 as k10, t1.k11 as k11, \
             t1.%s as a, t1.%s as b, t2.%s as c \
             from baseall t1 join baseall  t2 \
             where t1.%s=t2.%s and t1.%s<=t2.%s) T \
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b\
             order by a, count(c)" \
             % (k1, k2, k2, k1, k1, k3, k3)
    runner.check2(line1, line4)
    
    line1 = "select %s, max(%s) over (partition by %s order by %s\
             range between current row and unbounded following)\
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line4 = "select a, max(c) from \
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,\
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,\
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,\
             t1.k10 as k10, t1.k11 as k11, \
             t1.%s as a, t1.%s as b, t2.%s as c \
             from baseall t1 join baseall  t2 \
             where t1.%s=t2.%s and t1.%s<=t2.%s) T \
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b\
             order by a, max(c)" \
             % (k1, k2, k2, k1, k1, k3, k3)
    runner.check2(line1, line4)

    line1 = "select %s, min(%s) over (partition by %s order by %s\
             range between current row and unbounded following)\
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line4 = "select a, min(c) from \
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,\
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,\
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,\
             t1.k10 as k10, t1.k11 as k11, \
             t1.%s as a, t1.%s as b, t2.%s as c \
             from baseall t1 join baseall  t2 \
             where t1.%s=t2.%s and t1.%s<=t2.%s) T \
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b \
             order by a, min(c)" \
             % (k1, k2, k2, k1, k1, k3, k3)
    runner.check2(line1, line4)

    i = random.randint(0, 4)
    j = random.randint(0, 4)
    k = random.randint(0, 4)
    while (i == j or i == k or j == k):
        i = random.randint(0, 4)
        j = random.randint(0, 4)
        k = random.randint(0, 4)
    print("--------------------------------------------")
    print(i, j, k)
    k1 = fields[i]
    k2 = fields[j]
    k3 = fields[k]
    print(i, j, k)
    print("--------------------------------------------------")
    line1 = "select %s, sum(%s) over (partition by %s order by %s\
             range between current row and unbounded following)\
             as wj from baseall order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line4 = "select a, sum(c) from \
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,\
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,\
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,\
             t1.k10 as k10, t1.k11 as k11, \
             t1.%s as a, t1.%s as b, t2.%s as c \
             from baseall t1 join baseall  t2 \
             where t1.%s=t2.%s and t1.%s<=t2.%s) T \
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b\
             order by a, sum(c)" \
             % (k1, k2, k2, k1, k1, k3, k3)
    runner.check2(line1, line4)


def test_query_m_n_aggression():
    """
    {
    "title": "test_query_windows_quick.test_query_m_n_aggression",
    "describe": " unbounded current row and unbounded following",
    "tag": "function,p0"
    }
    """
    """
    unbounded current row and unbounded following
    """
    i = random.randint(0, 10)
    k = random.randint(0, 10)
    while (i == k or i == 5 or k == 5 or i == 9 or k == 8):
        i = random.randint(0, 10)
        k = random.randint(0, 10) 
    for j in (0, 1, 2, 3, 9, 10):
        if i != j and j != k:
            print("-----------------------------------")
            print(i, j, k)
            k1 = fields[i]
            k2 = fields[j]
            k3 = fields[k]
            print(i, j, k)
            print("--------------------------------------------------")
            line1 = "select A.%s, sum(A.%s) over (partition by A.%s order by A.%s\
                     rows between 1 preceding and 1 following) as wj from \
                     (select * from baseall order by %s, %s limit 10) as A\
                     order by A.%s, wj" \
                     % (k1, k2, k1, k3, k1, k3, k1)
            line2 = "select A.%s, A.wj from (" % (k1)
            for p in range(0, 10): 
                if p - 1 < 0:
                    k4 = 0
                else:
                    k4 = p - 1
                if p == 9:
                    pos = 2
                else: 
                    pos = 3
                cur = "(select t1.%s, sum(t2.%s) as wj from\
                       (select * from baseall order by %s, %s limit %s,1) as t1 join\
                       (select * from baseall order by %s, %s limit %s,%s) as t2\
                       where t1.%s=t2.%s group by t1.%s)"\
                       % (k1, k2, k1, k3, p, k1, k3, k4, pos, k1, k1, k1)
                if p < 9:
                    line2 = line2 + cur + " union all "
                else:
                    line2 = line2 + cur + ") as A order by A.%s, A.wj" % (k1)
            runner.check2(line1, line2)
            print("ok")


def test_query_first_value():
    """
    {
    "title": "test_query_windows_quick.test_query_first_value",
    "describe": "test_query_windows_quick.test_query_first_value",
    "tag": "function,p0"
    }
    """
    """
    first_value
    """
    i = random.randint(0, 10)
    j = random.randint(0, 10)
    while (i == j or i == 9):
        i = random.randint(0, 10)
        j = random.randint(0, 10)
    k = j
    k1 = fields[i]
    k2 = fields[j]
    k3 = fields[k]
    print(i, j, k)
    print("--------------------------------------------------")
    line1 = "select %s, first_value(%s) over (partition by %s order by %s)\
             as wj from baseall  order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line2 = "select %s, first_value(%s) over (partition by %s order by %s \
             range between unbounded preceding and current row)\
             as wj from baseall  order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line3 = "select %s, first_value(%s) over (partition by %s order by %s \
             rows between unbounded preceding and current row)\
             as wj from baseall  order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line4 = "select B.a, A.w from (select min(%s) as w, %s, %s from baseall \
             group by %s, %s having count(*)=1) as A join\
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,\
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,\
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,\
             t1.k10 as k10, t1.k11 as k11, \
             t1.%s as a, t1.%s as b, t2.%s as c, t2.%s as d \
             from baseall t1 join baseall  t2 \
             where t1.%s=t2.%s and t1.%s>=t2.%s) T \
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b\
             order by a, wjj) as B\
             where A.%s=B.wjj and B.a=A.%s order by B.a, A.w"\
             % (k2, k1, k3, k1, k3, k1, k2, k2, k3, k1, k1, k3, k3, k3, k1)
                    #runner.check(line1, line4)
            
    line4 = "select a, min(d) as wjj from \
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,\
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,\
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,\
             t1.k10 as k10, t1.k11 as k11, \
             t1.%s as a, t1.%s as b, t2.%s as c, t2.%s as d \
             from baseall t1 join baseall  t2 \
             where t1.%s=t2.%s and t1.%s>=t2.%s) T \
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b\
             order by a, wjj"\
             % (k1, k2, k2, k3, k1, k1, k3, k3)
    runner.check2(line2, line4)
    runner.check2(line3, line4)
    runner.check2(line1, line4)


def test_query_last_value():
    """
    {
    "title": "test_query_windows_quick.test_query_last_value",
    "describe": "test_query_windows_quick.test_query_last_value",
    "tag": "function,p0"
    }
    """
    """
    last_value
    """
    i = random.randint(0, 10)
    j = random.randint(0, 10)
    while (i == j or i == 9):
        i = random.randint(0, 10)
        j = random.randint(0, 10)
    k = j
    k1 = fields[i]
    k2 = fields[j]
    k3 = fields[k]
    print(i, j, k)
    print("--------------------------------------------------")
    line1 = "select %s, last_value(%s) over (partition by %s order by %s)\
             as wj from baseall  order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line2 = "select %s, last_value(%s) over (partition by %s order by %s \
             range between unbounded preceding and current row)\
             as wj from baseall  order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line3 = "select %s, last_value(%s) over (partition by %s order by %s \
             rows between unbounded preceding and current row)\
             as wj from baseall  order by %s, wj" \
             % (k1, k2, k1, k3, k1)
    line4 = "select B.a, A.w from (select min(%s) as w, %s, %s from baseall \
             group by %s, %s having count(*)=1) as A join\
             (select a, max(d) as wjj from \
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,\
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,\
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,\
             t1.k10 as k10, t1.k11 as k11, \
             t1.%s as a, t1.%s as b, t2.%s as c, t2.%s as d \
             from baseall t1 join baseall  t2 \
             where t1.%s=t2.%s and t1.%s>=t2.%s) T \
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b\
             order by a, wjj) as B\
             where A.%s=B.wjj and B.a=A.%s order by B.a, A.w"\
             % (k2, k1, k3, k1, k3, k1, k2, k2, k3, k1, k1, k3, k3, k3, k1)
    #runner.check(line1, line4)
            
    line4 = "select a, max(d) as wjj from \
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,\
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,\
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,\
             t1.k10 as k10, t1.k11 as k11, \
             t1.%s as a, t1.%s as b, t2.%s as c, t2.%s as d \
             from baseall t1 join baseall  t2 \
             where t1.%s=t2.%s and t1.%s>=t2.%s) T \
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, b \
             order by a, wjj"\
             % (k1, k2, k2, k3, k1, k1, k3, k3)
    runner.check2(line1, line4)


def test_query_row_number():
    """
    {
    "title": "test_query_windows_quick.test_query_row_number",
    "describe": "test_query_windows_quick.test_query_row_number",
    "tag": "function,p0"
    }
    """
    """
    row_number
    """
    i = random.randint(0, 10)
    k = random.randint(0, 10)
    while (i == k or i == 9):
        i = random.randint(0, 10)
        k = random.randint(0, 10) 
    k1 = fields[i]
    k3 = fields[k]
    print(i, k)
    print("--------------------------------------------------")
    line1 = "select %s, row_number() over (partition by %s order by %s) as wj from baseall \
             order by %s, wj" \
             % (k1, k1, k3, k1)
    line2 = "select %s, count(k1) over (partition by %s order by %s\
             rows between unbounded preceding and current row)\
             as wj from baseall order by %s, wj" \
             % (k1, k1, k3, k1)
    runner.check2(line2, line1)


def test_query_error():
    """
    {
    "title": "test_query_windows_quick.test_query_error",
    "describe": "test_query_windows_quick.test_query_error",
    "tag": "function,p0,fuzz"
    }
    """
    """
    not support
    """
    i = random.randint(0, 10)
    j = random.randint(0, 10)
    k = random.randint(0, 10)
    while (i == j or i == k or j == k):
        i = random.randint(0, 10)
        j = random.randint(0, 10)
        k = random.randint(0, 10) 
    k1 = fields[i]
    k2 = fields[j]
    k3 = fields[k]
    print(i, j, k)
    print("--------------------------------------------------")
    line = "select %s, lag(%s) over (partition by %s order by %s) \
            from baseall" % (k1, k2, k1, k3)
    runner.checkwrong(line)
    line = "select %s, lag(%s, -1, 1) over (partition by %s order by %s) \
            from baseall" % (k1, k2, k1, k3)
    runner.checkwrong(line)
    line = "select %s, lag(%s, 1) over (partition by %s order by %s) \
            from baseall" % (k1, k2, k1, k3)
    runner.checkwrong(line)
    line = "select %s, lead(%s) over (partition by %s order by %s) \
            from baseall" % (k1, k2, k1, k3)
    runner.checkwrong(line)
    line = "select %s, lead(%s, -1, 1) over (partition by %s order by %s) \
            from baseall" % (k1, k2, k1, k3)
    runner.checkwrong(line)
    line = "select %s, lead(%s, 1) over (partition by %s order by %s) \
            from baseall" % (k1, k2, k1, k3)
    runner.checkwrong(line)
    line = "select %s, first_value(%s) over (partition by %s) \
            from baseall" % (k1, k2, k1)
    # runner.checkwrong(line)
    line = "select %s, first_value(%s) over (order by %s) \
            from baseall" % (k1, k2, k3)
    runner.checkok(line)
    line = "select %s, max(%s) over (order by %s) from baseall"\
            %(k1, k2, k3)
    runner.checkok(line)
    line = "select %s, sum(%s) over (partition by %s order by %s rows \
            between current row and unbounded preceding) as wj \
            from baseall order by %s, wj"\
            % (k1, k2, k1, k3, k1)
    runner.checkwrong(line)
    line = "select %s, sum(%s) over (partition by %s order by %s rows \
            between 0 preceding and 1 following) as wj \
            from baseall order by %s, wj"\
            % (k1, k2, k1, k3, k1)
    runner.checkwrong(line)
    line = "select %s, sum(%s) over (partition by %s order by %s rows \
            between unbounded following and current row) as wj \
            from baseall order by %s, wj"\
            % (k1, k2, k1, k3, k1)
    runner.checkwrong(line)
    line = "select %s, rank(%s) over (partition by %s order by %s) as wj\
            from baseall order by %s, wj"\
            % (k1, k2, k1, k3, k1)
    runner.checkwrong(line)
    line = "select %s, max() over (partition by %s order by %s) as wj\
            from baseall order by %s, wj"\
            % (k1, k1, k3, k1)
    runner.checkwrong(line)
    line = "select %s, count(*) over (partition by %s order by %s) as wj\
            from baseall order by %s, wj"\
            % (k1, k1, k3, k1)
    runner.checkwrong(line)
    line = "select %s, count(%s) over (order by %s rows partition by %s) as wj\
            from baseall order by %s, wj"\
            % (k1, k2, k1, k3, k1)
    runner.checkwrong(line)

            
def test_query_rank():
    """
    {
    "title": "test_query_windows_quick.test_query_rank",
    "describe": "test_query_windows_quick.test_query_rank",
    "tag": "function,p0"
    }
    """
    """
    rank
    """
    i = random.randint(0, 10)
    k = random.randint(0, 10)
    while (i == k or i == 9 or k == 9):
        i = random.randint(0, 10)
        k = random.randint(0, 10) 
    #if i == 5 and k == 9:
    #    continue
    #if i == 9 and k == 4:
    #    continue
    k1 = fields[i]
    k3 = fields[k]
    print(i, k)
    print("--------------------------------------------------")
    line1 = "select %s, rank() over (partition by %s order by %s) as wj \
             from baseall order by %s, wj" \
             %(k1, k1, k3, k1)
    line4 = "select F2.%s, (F1.wj - F2.basewj + 1) as wj from\
             (select a, c, count(*) as wj from \
             (select t1.k1 as k1, t1.k2 as k2, t1.k3 as k3,\
             t1.k4 as k4, t1.k5 as k5,t1.k6 as k6,\
             t1.k7 as k7, t1.k8 as k8, t1.k9 as k9,\
             t1.k10 as k10, t1.k11 as k11, \
             t1.%s as a,  t1.%s as c \
             from baseall t1 join baseall  t2 \
             where t1.%s=t2.%s and t1.%s>=t2.%s) T \
             group by k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, a, c) as F1 join\
             (select %s, %s, count(*) as basewj from baseall group by %s, %s) as F2\
             where F1.a=F2.%s and F1.c = F2.%s order by F2.%s, wj" \
             % (k1, k1, k3, k1, k1, k3, k3, k1, k3, k1, k3, k1, k3, k1)
    runner.check2(line1, line4)


def test_hang():
    """
    {
    "title": "test_query_windows_quick.test_hang",
    "describe": "test_query_windows_quick.test_hang",
    "tag": "function,p0"
    }
    """
    """
    test_hang
    """
    i = random.randint(0, 10)
    k = random.randint(0, 10)
    while (i == k):
        i = random.randint(0, 10)
        k = random.randint(0, 10) 
    k1 = fields[i]
    k3 = fields[k]
    print(i, k)
    print("--------------------------------------------------")
    line1 = "select %s, row_number() over (partition by %s order by %s) as wj from \
             baseall order by %s, wj" \
             % (k1, k1, k3, k1)
    line = "("
    for p in range(0, 829):
        if p == 0:
            cur = "(select %s, 1 as wj from baseall order by %s, %s limit 1)" \
                   %(k1, k1, k3)
        else:
            cur = "(select %s, %s as wj from baseall order by %s, %s limit %s, 1)" \
                   % (k1, p + 1, k1, k3, p)
        if p < 828:
            line = line + cur + " union all "
        else:
            line = line + cur + ")"
    line2 = "select A.%s, A.wj - B.dyk + 1 as num from \
             (select %s, wj from %s as W1) as A join \
             (select %s, min(wj) as dyk from %s as W2 group by %s) as B \
             where A.%s=B.%s  order by A.%s, num" \
             % (k1, k1, line, k1, line, k1, k1, k1, k1)
    runner.check2(line2, line1)


def test_hujie():
    """
    {
    "title": "test_query_windows_quick.test_hujie",
    "describe": "test_query_windows_quick.test_hujie",
    "tag": "function,p0"
    }
    """
    """
    test_hujie
    """
    line = "("
    for p in range(0, 829):
        if p == 0:
            cur = "(select * from baseall order by k1, k6 limit 1)" 
        else:
            cur = "(select * from baseall order by k1, k6 limit %s, 1)"\
                  % (p)
        if p < 828:
            line = line + cur + " union all "
        else:
            line = line + cur + ")"
    line2 = "select T.k1, T.k6 from %s as T \
             order by T.k1, T.k6"\
            % (line)
    line1 = "select k1, k6 from baseall order by k1, k6" 

    runner.check2(line2, line1)


def test_bug():
    """
    {
    "title": "test_query_windows_quick.test_bug",
    "describe": "for bugs",
    "tag": "function,p0"
    }
    """
    """for bugs"""
    line = 'SELECT wj FROM (SELECT row_number() over (PARTITION BY k6 ORDER BY k1) AS wj ' \
           'FROM baseall ) AS A where wj = 2'
    runner.checkok(line)
    line = 'SELECT A.k2 AS a, A.k1 as b, B.k1 as c, B.k2 as d FROM ' \
           '( SELECT k2, k1, row_number () over (PARTITION BY k2 ORDER BY k3) AS wj ' \
           'FROM baseall ) AS A JOIN ( SELECT k2, k1, row_number () over ' \
           '(PARTITION BY k2 ORDER BY k3) AS wj FROM baseall ) AS B WHERE A.k2=B.k2'
    runner.checkok(line)


def teardown_module():
    """
    todo
    """
    print('End')
    # mysql_cursor.close()
    # mysql_con.close()



#!/bin/env pyth
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
#   @file win_function.py
#   @date 2017-07-26 14:16:42
#   @brief This file is for window analytic test. execute time < 1min
#
#############################################################################
"""
This file is for window analytic test. execute time < 1min
"""
import sys
sys.path.append("../lib/")
from palo_qe_client import QueryBase
import query_util as util
import win_function

sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage

table_name = "baseall"


def setup_module():
    """
    setup
    """
    global runner
    runner = QueryExecuter() 
    ret = runner.query_palo.do_sql('show variables like "enable_vectorized_engine"')
    global enable_vectorized_engine
    if len(ret) != 0 and ret[0][1] != 'true':
        enable_vectorized_engine = False
    else:
        enable_vectorized_engine = True


class QueryExecuter(QueryBase):
    def __init__(self):
#        super(QueryBase, self).__init__()
        self.get_clients()

    def check(self, line1, line2, func, preceding, following, partition_id, order_id, value_id, 
              win_type):
        """
        check for two query
        """
        print(line1)
        ret1 = ''
        ret2 = ''
        try:
            LOG.info(L('palo line1', line_1=line1))
            ret1 = self.query_palo.do_sql(line1)
            ret2 = self.query_palo.do_sql(line2)
            r = func(ret2, preceding, following, partition_id, order_id, value_id, win_type)
            assert r is not False, 'check win_func work wrong'
            util.check_same(ret1, r)
        except Exception as e:
            print(str(e))
            LOG.error(L('err', sql_1=line1, error=e))
            assert 0 == 1, 'check wrong'

    def check2(self, win_sql, compare_sql):
        """check two sql with same result"""
        print(win_sql)
        print(compare_sql)
        try:
            LOG.info(L('palo line1', line_1=win_sql))
            ret1 = runner.query_palo.do_sql(win_sql)
            LOG.info(L('palo line2', line_2=compare_sql))
            ret2 = runner.query_palo.do_sql(compare_sql)
            util.check_same(ret1, ret2)
        except Exception as e:
            print(str(e))
            assert 0 == 1, 'check wrong'


def test_query_sum():
    """
    {
    "title": "test_query_window_basic.test_query_sum",
    "describe": "test win function sum",
    "tag": "function,p1,fuzz"
    }
    """
    """test win function sum"""
    line = 'select k6, k3, sum(k3) over (partition by k6) from baseall order by k6, k3'
    compare = 'select k6, k3 from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_sum, 'unbounded', 'unbounded', 0, 1, 1, 'range')
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_sum, 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 range \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 range \
            between 1 preceding and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 range \
            between 1 preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 range \
            between current row and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 range \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_sum, 'current', 'unbounded', 0, 1, 1, 'range')
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 range \
            between unbounded preceding and current row ) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_sum, 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 range \
            between current row and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 range \
            between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_sum, 'unbounded', 'unbounded', 0, 1, 1, 'range')
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 range \
            between unbounded preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    # note order
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 rows \
            between current row and unbounded following) wj from baseall order by k6, k3, wj desc'
    runner.check(line, compare, win_function.win_sum, 'current', 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 rows\
            between current row and 3 following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_sum, 'current', 3, 0, 1, 1, 'rows')
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 rows\
            between current row and current row) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_sum, 'current', 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 rows\
            between unbounded preceding and current row) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_sum, 'unbounded', 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 rows\
            between unbounded preceding and unbounded following) wj from baseall \
            order by k6, k3, wj'
    runner.check(line, compare, win_function.win_sum, 'unbounded', 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 rows\
            between unbounded preceding and 1 following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_sum, 'unbounded', 1, 0, 1, 1, 'rows')
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 rows\
            between 1 preceding and current row) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_sum, 1, 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 rows\
            between 1 preceding and unbounded following) wj from baseall order by k6, k3, wj desc'
    runner.check(line, compare, win_function.win_sum, 1, 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 rows\
            between 1 preceding and 1 following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_sum, 1, 1, 0, 1, 1, 'rows')


def test_query_min():
    """
    {
    "title": "test_query_window_basic.test_query_min",
    "describe": "test win function min",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test win function min
    """
    line = 'select k6, k3, min(k3) over (partition by k6) from baseall order by k6, k3'
    compare = 'select k6, k3 from baseall order by k6, k3'
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_min, 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3 range \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3 range \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_min, 'current', 'unbounded', 0, 1, 1, 'range')
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3 range \
            between unbounded preceding and current row ) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_min, 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3 range \
            between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_min, 'unbounded', 'unbounded', 0, 1, 1, 'range')
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3 rows \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_min, 'current', 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3 rows\
            between current row and 3 following) from baseall order by k6, k3'
    if enable_vectorized_engine:
        runner.check(line, compare, win_function.win_min, 'current', 3, 0, 1, 1, 'rows')
    else:
        runner.checkwrong(line)
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3 rows\
            between current row and current row) from baseall order by k6, k3'
    if enable_vectorized_engine:
        runner.check(line, compare, win_function.win_min, 'current', 'current', 0, 1, 1, 'rows')
    else:
        runner.checkwrong(line)
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3 rows\
            between unbounded preceding and current row) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_min, 'unbounded', 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3 rows\
            between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_min, 'unbounded', 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3 rows\
            between unbounded preceding and 1 following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_min, 'unbounded', 1, 0, 1, 1, 'rows')
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3 rows\
            between 1 preceding and current row) from baseall order by k6, k3'
    if enable_vectorized_engine:
        runner.check(line, compare, win_function.win_min, 1, 'current', 0, 1, 1, 'rows')
    else:
        runner.checkwrong(line)
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3 rows\
            between 1 preceding and unbounded following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_min, 1, 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, min(k3) over (partition by k6 order by k3 rows\
            between 1 preceding and 1 following) from baseall order by k6, k3'
    if enable_vectorized_engine:
        runner.check(line, compare, win_function.win_min, 1, 1, 0, 1, 1, 'rows')
    else:
        runner.checkwrong(line)


def test_query_max():
    """
    {
    "title": "test_query_window_basic.test_query_max",
    "describe": "test win function max",
    "tag": "function,p1,fuzz"
    }
    """
    """test win function max"""
    line = 'select k6, k3, max(k3) over (partition by k6) from baseall order by k6, k3'
    compare = 'select k6, k3 from baseall order by k6, k3'
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_max, 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3 range \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3 range \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_max, 'current', 'unbounded', 0, 1, 1, 'range')
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3 range \
            between unbounded preceding and current row ) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_max, 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3 range \
            between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_max, 'unbounded', 'unbounded', 0, 1, 1, 'range')
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3 rows \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_max, 'current', 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3 rows \
            between current row and 3 following) from baseall order by k6, k3'
    if enable_vectorized_engine:
        runner.check(line, compare, win_function.win_max, 'current', 3, 0, 1, 1, 'rows')
    else:
        runner.checkwrong(line)
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3 rows \
            between current row and current row) from baseall order by k6, k3'
    if enable_vectorized_engine:
        runner.check(line, compare, win_function.win_max, 'current', 'current', 0, 1, 1, 'rows')
    else:
        runner.checkwrong(line)
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3 rows \
            between unbounded preceding and current row) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_max, 'unbounded', 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3 rows \
            between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_max, 'unbounded', 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3 rows \
            between unbounded preceding and 1 following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_max, 'unbounded', 1, 0, 1, 1, 'rows')
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3 rows \
            between 1 preceding and current row) from baseall order by k6, k3'
    if enable_vectorized_engine:
        runner.check(line, compare, win_function.win_max, 1, 'current', 0, 1, 1, 'rows')
    else:
        runner.checkwrong(line)
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3 rows \
            between 1 preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_max, 1, 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, max(k3) over (partition by k6 order by k3 rows \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    if enable_vectorized_engine:
        runner.check(line, compare, win_function.win_max, 1, 1, 0, 1, 1, 'rows')
    else:
        runner.checkwrong(line)


def test_query_count():
    """
    {
    "title": "test_query_window_basic.test_query_count",
    "describe": "test win function count",
    "tag": "function,p1,fuzz"
    }
    """
    """test win function count"""
    line = 'select k6, k3, count(k3) over (partition by k6) from baseall order by k6, k3'
    compare = 'select k6, k3 from baseall order by k6, k3'
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3) from baseall \
            order by k6, k3'
    runner.check(line, compare, win_function.win_count, 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3 range \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_count, 'current', 'unbounded', 0, 1, 1, 'range')
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3 range \
            between unbounded preceding and current row ) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_count, 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3 range \
            between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_count, 'unbounded', 'unbounded', 0, 1, 1, 'range')
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3 range \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3 rows \
            between current row and unbounded following) wj from baseall order by k6, k3, wj desc '
    runner.check(line, compare, win_function.win_count, 'current', 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3 rows \
            between current row and 3 following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_count, 'current', 3, 0, 1, 1, 'rows')
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3 rows \
            between current row and current row) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_count, 'current', 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3 rows \
            between unbounded preceding and current row) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_count, 'unbounded', 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3 rows \
            between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_count, 'unbounded', 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3 rows \
            between unbounded preceding and 1 following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_count, 'unbounded', 1, 0, 1, 1, 'rows')
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3 rows \
            between 1 preceding and current row) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_count, 1, 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3 rows \
            between 1 preceding and unbounded following) wj from baseall order by k6, k3, wj desc'
    runner.check(line, compare, win_function.win_count, 1, 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, count(k3) over (partition by k6 order by k3 rows \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_count, 1, 1, 0, 1, 1, 'rows')


def test_query_avg():
    """
    {
    "title": "test_query_window_basic.test_query_avg",
    "describe": "test win function avg",
    "tag": "function,p1,fuzz"
    }
    """
    """test win function avg"""
    line = 'select k6, k3, avg(k3) over (partition by k6) from baseall order by k6, k3'
    compare = 'select k6, k3 from baseall order by k6, k3'
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_avg, 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3 range \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3 range \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_avg, 'current', 'unbounded', 0, 1, 1, 'range')
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3 range \
                    between unbounded preceding and current row ) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_avg, 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3 range \
                between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_avg, 'unbounded', 'unbounded', 0, 1, 1, 'range')
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3 rows \
            between current row and unbounded following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_avg, 'current', 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3 rows \
            between current row and 3 following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_avg, 'current', 3, 0, 1, 1, 'rows')
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3 rows \
            between current row and current row) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_avg, 'current', 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3 rows \
            between unbounded preceding and current row) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_avg, 'unbounded', 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3 rows \
            between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_avg, 'unbounded', 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3 rows \
            between unbounded preceding and 1 following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_avg, 'unbounded', 1, 0, 1, 1, 'rows')
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3 rows \
            between 1 preceding and current row) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_avg, 1, 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3 rows \
            between 1 preceding and unbounded following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_avg, 1, 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, avg(k3) over (partition by k6 order by k3 rows \
            between 1 preceding and 1 following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_avg, 1, 1, 0, 1, 1, 'rows')


def test_query_first_value():
    """
    {
    "title": "test_query_window_basic.test_query_first_value",
    "describe": "win function first_value()",
    "tag": "function,p1,fuzz"
    }
    """
    """
    win function first_value()
    """
    line = 'select k6, k3, first_value(k3) over (partition by k6) from baseall order by k6, k3'
    compare = 'select k6, k3 from baseall order by k6, k3'
    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3) from baseall \
            order by k6, k3'
    runner.check(line, compare, win_function.win_first_value, 
                 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3 range \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3 range \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_first_value, 
                 'current', 'unbounded', 0, 1, 1, 'range')

    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3 range \
                    between unbounded preceding and current row ) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_first_value, 
                 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3 range \
                between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_first_value, 
                 'unbounded', 'unbounded', 0, 1, 1, 'range')
    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3 rows \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_first_value, 
                 'current', 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3 \
            rows between current row and 3 following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_first_value, 'current', 3, 0, 1, 1, 'rows')
    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3 \
            rows between current row and current row) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_first_value, 
                 'current', 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3 \
            rows between unbounded preceding and current row) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_first_value, 
                 'unbounded', 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3 \
            rows between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_first_value, 
                 'unbounded', 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3 \
            rows between unbounded preceding and 1 following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_first_value, 'unbounded', 1, 0, 1, 1, 'rows')
    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3 \
            rows between 1 preceding and current row) from baseall order by k6, k3'
    # bug
    # runner.check(line, compare, win_function.win_first_value, 1, 'current', 0, 1, 1, 'rows')

    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3 \
            rows between 1 preceding and unbounded following) from baseall order by k6, k3'
    # bug
    # runner.check(line, compare, win_function.win_first_value, 1, 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, first_value(k3) over (partition by k6 order by k3 \
            rows between 1 preceding and 1 following) from baseall order by k6, k3'
    # bug
    # runner.check(line, compare, win_function.win_first_value, 1, 1, 0, 1, 1, 'rows')


def test_query_last_value():
    """
    {
    "title": "test_query_window_basic.test_query_last_value",
    "describe": "win function last_value()",
    "tag": "function,p1,fuzz"
    }
    """
    """
    win function last_value()
    """
    line = 'select k6, k3, last_value(k3) over (partition by k6) from baseall order by k6, k3'
    compare = 'select k6, k3 from baseall order by k6, k3'
    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3) from baseall \
            order by k6, k3'
    runner.check(line, compare, win_function.win_last_value, 
                 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3 range \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3 range \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_last_value, 
                 'current', 'unbounded', 0, 1, 1, 'range')

    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3 range \
                    between unbounded preceding and current row ) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_last_value, 
                 'unbounded', 'current', 0, 1, 1, 'range')
    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3 range \
                between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_last_value, 
                 'unbounded', 'unbounded', 0, 1, 1, 'range')
    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3 rows \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_last_value, 
                 'current', 'unbounded', 0, 1, 1, 'rows')

    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3 \
            rows between current row and 3 following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_last_value, 'current', 3, 0, 1, 1, 'rows')
    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3 \
            rows between current row and current row) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_last_value, 'current', 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3 \
            rows between unbounded preceding and current row) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_last_value, 
                 'unbounded', 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3 \
            rows between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_last_value, 
                 'unbounded', 'unbounded', 0, 1, 1, 'rows')
    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3 \
            rows between unbounded preceding and 1 following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_last_value, 'unbounded', 1, 0, 1, 1, 'rows')
    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3 \
            rows between 1 preceding and current row) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_last_value, 1, 'current', 0, 1, 1, 'rows')
    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3 \
            rows between 1 preceding and unbounded following) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_last_value, 1, 'unbounded', 0, 1, 1, 'rows')

    line = 'select k6, k3, last_value(k3) over (partition by k6 order by k3 \
            rows between 1 preceding and 1 following) wj from baseall order by k6, k3, wj'
    runner.check(line, compare, win_function.win_last_value, 1, 1, 0, 1, 1, 'rows')


def test_query_row_number():
    """
    {
    "title": "test_query_window_basic.test_query_row_number",
    "describe": "row_number function has no param",
    "tag": "function,p1,fuzz"
    }
    """
    """
    row number
    row_number function has no param
    """
    line = 'select k6, k3, row_number() over (partition by k6) from baseall order by k6, k3'
    compare = 'select k6, k3 from baseall order by k6, k3'
    line = 'select k6, k3, row_number() over (partition by k6 order by k3) wj from baseall \
            order by k6, k3, wj'
    runner.check(line, compare, win_function.win_row_number, None, None, 0, 1, None, None)
    line = 'select k6, k3, row_number() over (partition by k6 order by k3 range \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, row_number() over (partition by k6 order by k3 range \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, row_number() over (partition by k6 order by k3 range \
                    between unbounded preceding and current row ) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, row_number() over (partition by k6 order by k3 range \
                between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, row_number() over (partition by k6 order by k3 rows \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, row_number() over (partition by k6 order by k3 \
            rows between current row and 3 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, row_number() over (partition by k6 order by k3 \
            rows between current row and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, row_number() over (partition by k6 order by k3 \
            rows between unbounded preceding and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, row_number() over (partition by k6 order by k3 \
            rows between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, row_number() over (partition by k6 order by k3 \
            rows between unbounded preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, row_number() over (partition by k6 order by k3 \
            rows between 1 preceding and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, row_number() over (partition by k6 order by k3 \
            rows between 1 preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, row_number() over (partition by k6 order by k3 \
            rows between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)


def test_query_lead():
    """
    {
    "title": "test_query_window_basic.test_query_lead",
    "describe": "lead",
    "tag": "function,p1,fuzz"
    }
    """
    """
    lead
    """
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6) from baseall order by k6, k3'
    compare = 'select k6, k3 from baseall order by k6, k3'
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3) wj from baseall \
            order by k6, k3, wj'
    runner.check(line, compare, win_function.win_lead, 1, None, 0, 1, None, None)
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3 range \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3 range \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3 range \
                        between unbounded preceding and current row ) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3 range \
            between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3 rows \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3 \
            rows between current row and 3 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3 \
            rows between current row and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3 \
            rows between unbounded preceding and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3 \
            rows between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3 \
            rows between unbounded preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3 \
            rows between 1 preceding and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3 \
            rows between 1 preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lead(k3, 1, null) over (partition by k6 order by k3 \
            rows between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)


def test_query_lag():
    """
    {
    "title": "test_query_window_basic.test_query_lag",
    "describe": "test win function lag",
    "tag": "function,p1,fuzz"
    }
    """
    """test win function lag"""
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6) from baseall order by k6, k3'
    compare = 'select k6, k3 from baseall order by k6, k3'
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3) wj from baseall \
            order by k6, k3, wj'
    runner.check(line, compare, win_function.win_lag, 1, None, 0, 1, None, None)
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3 range \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3 range \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3 range \
            between unbounded preceding and current row ) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3 range \
            between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3 rows \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3 \
            rows between current row and 3 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3 \
            rows between current row and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3 \
            rows between unbounded preceding and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3 \
            rows between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3 \
            rows between unbounded preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3 \
            rows between 1 preceding and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3 \
            rows between 1 preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, lag(k3, 1, null) over (partition by k6 order by k3 \
            rows between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)


def test_query_rank():
    """
    {
    "title": "test_query_window_basic.test_query_rank",
    "describe": "rank function has no param",
    "tag": "function,p1,fuzz"
    }
    """
    """
    rank
    rank function has no param
    """
    line1 = 'select k6, k3, rank() over (partition by k6) from baseall order by k6, k3'
    line2 = 'select k6, k3, 1 from baseall order by k6, k3'
    runner.check2(line1, line2)
    compare = 'select k6, k3 from baseall order by k6, k3'
    line = 'select k6, k3, rank() over (partition by k6 order by k3) from baseall order by k6, k3'
    runner.check(line, compare, win_function.win_rank, None, None, 0, 1, None, None)
    line = 'select k6, k3, rank() over (partition by k6 order by k3 range \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, rank() over (partition by k6 order by k3 range \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, rank() over (partition by k6 order by k3 range \
            between unbounded preceding and current row ) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, rank() over (partition by k6 order by k3 range \
            between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, rank() over (partition by k6 order by k3 rows \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, rank() over (partition by k6 order by k3 \
            rows between current row and 3 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, rank() over (partition by k6 order by k3 \
            rows between current row and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, rank() over (partition by k6 order by k3 \
            rows between unbounded preceding and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, rank() over (partition by k6 order by k3 \
            rows between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, rank() over (partition by k6 order by k3 \
            rows between unbounded preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, rank() over (partition by k6 order by k3 \
            rows between 1 preceding and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, rank() over (partition by k6 order by k3 \
            rows between 1 preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, rank() over (partition by k6 order by k3 \
            rows between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)


def test_query_dense_rank():
    """
    {
    "title": "test_query_window_basic.test_query_dense_rank",
    "describe": "dense_rank function has no param",
    "tag": "function,p1,fuzz"
    }
    """
    """
    dense_rank
    dense_rank function has no param
    """
    line1 = 'select k6, k3, dense_rank() over (partition by k6) from baseall order by k6, k3'
    line2 = 'select k6, k3, 1 from baseall order by k6, k3'
    runner.check2(line1, line2)
    compare = 'select k6, k3 from baseall order by k6, k3'
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3) from baseall \
            order by k6, k3'
    runner.check(line, compare, win_function.win_dense_rank, None, None, 0, 1, None, None)
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3 range \
            between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3 range \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3 range \
            between unbounded preceding and current row ) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3 range \
            between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3 rows \
            between current row and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3 \
            rows between current row and 3 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3 \
            rows between current row and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3 \
            rows between unbounded preceding and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3 \
            rows between unbounded preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3 \
            rows between unbounded preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3 \
            rows between 1 preceding and current row) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3 \
            rows between 1 preceding and unbounded following) from baseall order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, dense_rank() over (partition by k6 order by k3 \
            rows between 1 preceding and 1 following) from baseall order by k6, k3'
    runner.checkwrong(line)


def test_query_mix():
    """
    {
    "title": "test_query_window_basic.test_query_mix",
    "describe": "for bug",
    "tag": "function,p1"
    }
    """
    """
    todo
    """
    line = 'select * from ((select k6, k3 as v, sum(k3) over (partition by k6 order by k3) wj \
            from baseall) union all (select k6, k2 as v, sum(k2) over \
            (partition by k6 order by k3) wj from baseall) ) a order by k6, v, wj'
    # todo check result
    print(line)
    runner.query_palo.do_sql(line)
    print("hello world")


def test_query_win_not_support():
    """
    {
    "title": "test_query_window_basic.test_query_win_not_support",
    "describe": "todo",
    "tag": "autotest"
    }
    """
    """
    RANGE is only supported with both the lower and upper bounds UNBOUNDED or one UNBOUNDED and the other CURRENT ROW.
    'min(`k3`)' is only supported with an UNBOUNDED PRECEDING start bound
    'max(`k3`)' is only supported with an UNBOUNDED PRECEDING start bound.
    Windowing clause not allowed with 'dense_rank()'
    Windowing clause not allowed with 'rank()'
    Windowing clause not allowed with 'lag(`k3`, 1, NULL)'
    Windowing clause not allowed with 'lead(`k3`, 1, NULL)'
    Windowing clause not allowed with 'row_number()'
    """
    pass


def teardown_module():
    """
    todo
    """
    print('End')


if __name__ == '__main__':
    setup_module()
    test_query_avg()
    test_query_count()
    test_query_sum()
    test_query_min()
    test_query_max()
    test_query_dense_rank()
    test_query_rank()
    test_query_lag()
    test_query_lead()
    test_query_sum()
    # test_query_first_value()
    test_query_last_value()
    test_query_row_number()
    test_query_mix()
    test_query_win_not_support()
    # test()


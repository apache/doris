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
case about agg query
"""
import pymysql
import sys
import random
sys.path.append("../lib/")
from palo_qe_client import QueryBase
import query_util as util
from warnings import filterwarnings
filterwarnings('ignore', category=pymysql.Warning)

table_name = "test"
join_name = "baseall"


def setup_module():
    """
    init config
    """
    global runner
    runner = QueryBase()


def test_agg_no_group():
    """
    {
    "title": "test_query_agg.test_agg_no_group",
    "describe": "test aggregation without group",
    "tag": "p1,function,fuzz"
    }
    """
    """
    test aggregation without group
    """
    line = 'select max(k1), max(k2), max(k3), max(k4), max(k5), max(upper(k6)), max(upper(k7)), ' \
           'max(k8), max(k9), max(k10), max(k11) from %s' % table_name
    runner.check(line)
    line = 'select min(k1), min(k2), min(k3), min(k4), min(k5), min(upper(k6)), min(upper(k7)), \
            min(k8), min(k9), min(k10), min(k11) from %s' % table_name
    runner.check(line)
    line1 = 'select avg(k1), avg(k2), avg(k3), avg(cast(k4 as largeint)), avg(k5), \
            avg(k8), avg(k9) from %s' % table_name
    line2 = 'select avg(k1), avg(k2), avg(k3), avg(k4), avg(k5), \
            avg(k8), avg(k9) from %s' % table_name
    runner.check2(line1, line2)
    line1 = 'select sum(k1), sum(k2), sum(k3), sum(cast(k4 as largeint)), sum(k5), \
            sum(k8), sum(k9) from %s' % table_name
    line2 = 'select sum(k1), sum(k2), sum(k3), sum(k4), sum(k5), \
            sum(k8), sum(k9) from %s' % table_name
    runner.check2(line1, line2)
    line = 'select count(*), count(k1), count(k2), count(k3), count(k4), count(k5), count(k6), \
            count(k7), count(k8), count(k9), count(k10), count(k11) from %s' % table_name
    runner.check(line)
    line1 = 'select stddev(k1), stddev(k2), stddev(k3), stddev(cast(k4 as largeint)), \
            stddev(cast(k5 as double)), stddev(k8), stddev(k9) from %s' % table_name
    line2 = 'select stddev(k1), stddev(k2), stddev(k3), stddev(k4), \
            stddev(k5), stddev(k8), stddev(k9) from %s' % table_name
    runner.check2(line1, line2)
    line1 = 'select variance(k1), variance(k2), variance(k3), variance(cast(k4 as largeint)),  \
            variance(cast(k5 as double)), variance(k8), variance(k9) from %s' % table_name
    line2 = 'select variance(k1), variance(k2), variance(k3), variance(k4), variance(k5), \
            variance(k8), variance(k9) from %s' % table_name
    runner.check2(line1, line2)

    line = 'select min(distinct k1), min(distinct k2), min(distinct k3), min(distinct k4), \
            min(distinct k5), min(distinct upper(k6)), min(distinct upper(k7)), min(distinct k8), \
            min(distinct k9), min(distinct k10), min(distinct k11) from %s' % table_name
    runner.check(line)
    line1 = 'select avg(distinct k1), avg(distinct k2), avg(distinct k3), \
            avg(distinct cast(k4 as largeint)), avg(distinct cast(k5 as double)), \
            avg(distinct k8), avg(distinct k9) from %s' % table_name
    line2 = 'select avg(distinct k1), avg(distinct k2), avg(distinct k3), avg(distinct k4), \
            avg(distinct k5), avg(distinct k8), avg(distinct k9) from %s' % table_name
    runner.check2(line1, line2)
    line1 = 'select sum(distinct k1), sum(distinct k2), sum(distinct k3), \
            sum(distinct cast(k4 as largeint)), \
            sum(distinct k5), sum(distinct k8), sum(distinct k9) from %s' % table_name
    line2 = 'select sum(distinct k1), sum(distinct k2), sum(distinct k3), sum(distinct k4), \
            sum(distinct k5), sum(distinct k8), sum(distinct k9) from %s' % table_name
    runner.check2(line1, line2)
    line = 'select count(distinct k1), count(distinct k2), count(distinct k3), count(distinct k4), \
            count(distinct k5), count(distinct k8), \
            count(distinct k9), count(distinct k10), count(distinct k11) from %s' % table_name
    runner.check(line)
    line1 = 'select stddev(distinct k1), stddev(distinct k2), stddev(distinct k3), \
            stddev(distinct cast(k4 as largeint)), stddev(distinct cast(k5 as double)), \
            stddev(distinct k8), stddev(distinct k9) from %s' % table_name
    # can't support multi distinct
    runner.checkwrong(line1)
    line1 = 'select variance(distinct k1), variance(distinct k2), variance(distinct k3), \
            variance(distinct cast(k4 as largeint)), variance(distinct cast(k5 as double)), \
            variance(distinct k8), variance(distinct k9) \
            from %s' % table_name
    # can't support multi distinct
    runner.checkwrong(line1)
    line1 = 'select stddev(distinct k1) from %s' % table_name
    line2 = 'select stddev(distinct k1) from %s' % table_name
    runner.checkok(line1)
    line1 = 'select variance(distinct k1) from %s' % table_name
    line2 = 'select variance(distinct k1) from %s' % table_name
    runner.checkok(line1) 


def test_agg_with_group():
    """
    {
    "title": "test_query_agg.test_agg_with_group",
    "describe": "test aggregation with group clause",
    "tag": "p1,function,fuzz"
    }
    """
    """test aggregation with group clause"""
    # 无k6, k7 对于空格的不同处理 distinct count的数量不同
    group_column = ['k1', 'k2', 'k3', 'k4', 'k5', 'k10', 'k11']
    for col in group_column:
        line = 'select max(k1), max(k2), max(k3), max(k4), max(k5), max(upper(k6)), max(upper(k7)), \
                max(k8), max(k9), max(k10), max(k11) from %s group by %s order by %s' \
                % (table_name, col, col)
        runner.check(line)
        line = 'select min(k1), min(k2), min(k3), min(k4), min(k5), min(upper(k6)), min(upper(k7)), \
                min(k8), min(k9), min(k10), min(k11) from %s group by %s order by %s' \
               % (table_name, col, col)
        runner.check(line)
        line1 = 'select avg(k1), avg(k2), avg(k3), avg(cast(k4 as largeint)), avg(k5), \
                avg(k8), avg(k9) from %s group by %s order by %s' % (table_name, col, col)
        line2 = 'select avg(k1), avg(k2), avg(k3), avg(k4), avg(k5), \
                avg(k8), avg(k9) from %s group by %s order by %s' % (table_name, col, col)
        runner.check2(line1, line2)
        line1 = 'select sum(k1), sum(k2), sum(k3), sum(cast(k4 as largeint)), sum(k5), \
                sum(k8), sum(k9) from %s group by %s order by %s' % (table_name, col, col)
        line2 = 'select sum(k1), sum(k2), sum(k3), sum(k4), sum(k5), \
                sum(k8), sum(k9) from %s group by %s order by %s' % (table_name, col, col)
        runner.check2(line1, line2)
        line = 'select count(*), count(k1), count(k2), count(k3), count(k4), count(k5), count(k6),\
                count(k7), count(k8), count(k9), count(k10), count(k11) from %s group by %s \
                order by %s' % (table_name, col, col)
        runner.check(line)
        line1 = 'select stddev(k1), stddev(k2), stddev(k3), stddev(k4), stddev(cast(k5 as double)), \
                stddev(k8), stddev(k9) from %s group by %s order by %s' % (table_name, col, col)
        line2 = 'select stddev(k1), stddev(k2), stddev(k3), stddev(k4), \
                stddev(k5), stddev(k8), stddev(k9) from %s group by %s order by %s' \
                % (table_name, col, col)
        runner.check2(line1, line2)
        line1 = 'select variance(k1), variance(k2), variance(k3), variance(k4), \
                variance(cast(k5 as double)), variance(k8), variance(k9) \
                from %s group by %s order by %s' % (table_name, col, col)
        line2 = 'select variance(k1), variance(k2), variance(k3), variance(k4), variance(k5), \
                variance(k8), variance(k9) \
                from %s group by %s order by %s' % (table_name, col, col)
        runner.check2(line1, line2)

        line = 'select min(distinct k1), min(distinct k2), min(distinct k3), min(distinct k4), \
                min(distinct k5), min(distinct upper(k6)), min(distinct upper(k7)), min(distinct k8), \
                min(distinct k9), min(distinct k10), min(distinct k11) from %s group by %s \
                order by %s' % (table_name, col, col)
        runner.check(line)
        line1 = 'select avg(distinct k1), avg(distinct k2), avg(distinct k3), \
                avg(distinct cast(k4 as largeint)), avg(distinct cast(k5 as double)), \
                avg(distinct k8), avg(distinct k9) from %s group by %s order by %s' \
               % (table_name, col, col)
        line2 = 'select avg(distinct k1), avg(distinct k2), avg(distinct k3), avg(distinct k4), \
                avg(distinct k5), avg(distinct k8), avg(distinct k9) from %s group by %s \
                order by %s' % (table_name, col, col)
        runner.check2(line1, line2)
        line1 = 'select sum(distinct k1), sum(distinct k2), sum(distinct k3), \
                sum(distinct cast(k4 as largeint)), sum(distinct k5), sum(distinct k8), \
                sum(distinct k9) from %s group by %s order by %s' \
                % (table_name, col, col)
        line2 = 'select sum(distinct k1), sum(distinct k2), sum(distinct k3), sum(distinct k4), \
                sum(distinct k5), sum(distinct k8), sum(distinct k9) from %s group by %s \
                order by %s' % (table_name, col, col)
        runner.check(line)
        line = 'select count(distinct k1), count(distinct k2), count(distinct k3), \
                count(distinct k4), count(distinct k5), \
                count(distinct k8), count(distinct k9), count(distinct k10), count(distinct k11) \
                from %s group by %s order by %s' % (table_name, col, col)
        runner.check(line)
        line1 = 'select stddev(distinct k1), stddev(distinct k2), stddev(distinct k3), \
                stddev(distinct k4), stddev(distinct k5), stddev(distinct k6), stddev(distinct k7),\
                stddev(distinct k8), stddev(distinct k9) from %s group by %s order by %s' \
                % (table_name, col, col)
        # stddev can't support multi distinct.
        runner.checkwrong(line1)
        line1 = 'select variance(distinct k1), variance(distinct k2), variance(distinct k3), \
                variance(distinct k4), variance(distinct k5), variance(distinct k6), \
                variance(distinct k7), variance(distinct k8), variance(distinct k9) \
                from %s group by %s order by %s' % (table_name, col, col)
        # stddev can't support multi distinct.
        runner.checkwrong(line1)
        line1 = 'select variance(distinct k1) from %s group by %s order by %s' \
                % (table_name, col, col)
        line2 = 'select variance(distinct k1) from %s group by %s order by %s' \
                % (table_name, col, col)
        runner.checkok(line1)
        line1 = 'select stddev(distinct k1) from %s group by %s order by %s' \
                % (table_name, col, col)
        line2 = 'select stddev(distinct k1) from %s group by %s order by %s' \
                % (table_name, col, col)
        runner.checkok(line1)


def test_agg_impala_1():
    """
    {
    "title": "test_query_agg.test_agg_impala_1",
    "describe": "impala aggregation tests",
    "tag": "p1,function"
    }
    """
    """impala aggregation tests"""
    line1 = 'select abs(cast(variance(k1) as double) - 6.667) < 0.01, \
            abs(cast(variance(k8) as double) -84.123) < 0.001 from %s order by 1, 2' % table_name
    line2 = 'select abs(cast(var_samp(k1) as decimal) - 6.667) < 0.01, \
            abs(cast(var_samp(k8) as decimal) -84.123) < 0.001 from %s order by 1, 2' % table_name
    runner.check2(line1, line2)
    line1 = 'select variance(k1), stddev(k2), variance_pop(k3), stddev_pop(k4) from %s \
            where k2 = 32757 order by 1, 2, 3, 4' % table_name
    line2 = 'select variance(k1), stddev(k2), var_pop(k3), stddev_pop(k4) from %s \
         where k2 = 32757 order by 1, 2, 3, 4' % table_name
    runner.check2(line1, line2)
    line1 = 'SELECT variance(k1), variance(k2), variance(k3),variance(k4), variance(k9), \
            variance(k8),variance_samp(k8), variance_samp(k8) from %s \
            WHERE k2 >= 1000 AND k2 < 1006' % table_name
    line2 = 'SELECT variance(k1), variance(k2), variance(k3),variance(k4), variance(k9), \
            variance(k8),var_samp(k8), var_samp(k8) from %s \
            WHERE k2 >= 1000 AND k2 < 1006' % table_name
    runner.check2(line1, line2)
    line1 = 'SELECT variance_pop(k1), variance_pop(k2), variance_pop(k3), variance_pop(k4), \
            variance_pop(k9), variance_pop(k8), var_pop(k8) from %s \
            WHERE k2 >= 1000 AND k2 < 1006 order by 1, 2, 3, 4' % table_name
    line2 = 'SELECT var_pop(k1), var_pop(k2), var_pop(k3), var_pop(k4), \
            var_pop(k9), var_pop(k8), var_pop(k8) from %s \
            WHERE k2 >= 1000 AND k2 < 1006 order by 1, 2, 3, 4' % table_name
    runner.check2(line1, line2)
    line1 = 'SELECT round(stddev(k1), 5), round(stddev(k2), 5), round(stddev(k3), 5), \
            round(stddev(k4), 5), round(stddev(k9), 5), round(stddev(k8), 5), \
            round(stddev_samp(k8), 5) from %s WHERE k2 >= 1000 AND k2 < 1006' % table_name
    line2 = 'SELECT round(stddev(k1), 5), round(stddev(k2), 5), round(stddev(k3), 5), \
            round(stddev(k4), 5), round(stddev(k9), 5), round(stddev(k8), 5), \
            round(stddev_samp(k8), 5) from %s WHERE k2 >= 1000 AND k2 < 1006' % table_name
    runner.check2(line1, line2)
    line = 'SELECT round(stddev_pop(k1), 5), round(stddev_pop(k2), 5), \
            round(stddev_pop(k3), 5), round(stddev_pop(k4), 5), \
            round(stddev_pop(k9), 5), round(stddev_pop(k8), 5) \
            from %s WHERE k2 >= 1000 AND k2 < 1006' % table_name
    runner.check(line)
    line = 'select count(*), count(k1), min(k1), max(k1), sum(k1), avg(k1) from %s \
            where k1 is not null' % table_name
    runner.check(line)
    line = 'select count(*), count(k2), min(k2), max(k2), sum(k2), avg(k2) from %s \
            where k1 is not null' % table_name
    runner.check(line)
    line = 'select count(*), count(k3), min(k3), max(k3), sum(k3), avg(k3) from %s \
            where k1 is not null' % table_name
    runner.check(line)
    line1 = 'select count(*), count(k4), min(k4), max(k4), sum(cast(k4 as largeint)), \
            avg(cast(k4 as largeint)) from %s \
            where k1 is not null' % table_name
    line2 = 'select count(*), count(k4), min(k4), max(k4), sum(k4), avg(k4) from %s \
            where k1 is not null' % table_name
    runner.check2(line1, line2)
    line = 'select count(*), count(k9), min(k9), max(k9), sum(k9), avg(k9) from %s \
            where k1 is not null' % table_name
    runner.check(line)
    line = 'select count(*), count(k8), min(k8), max(k8), round(sum(k8), 0), \
            round(avg(k8), 0) from %s where k1 is not null' % table_name
    runner.check(line)
    line = 'select count(*), min(k11), max(k11) from %s where k1 is not null' \
            % table_name
    runner.check(line)


def test_agg_impala_2():
    """
    {
    "title": "test_query_agg.test_agg_impala_2",
    "describe": "impala aggregation tests",
    "tag": "p1,function"
    }
    """
    """impala aggregation tests"""
    line = 'select k1, count(*) from %s where k1 is not null group by 1 order by 1' % table_name
    runner.check(line)
    line = 'select count(*) from %s where k1 is not null group by k1 order by k1' % table_name
    runner.check(line)
    line = 'select k2 %% 10, count(*) from %s where k1 is not null group by 1 order by 1' \
           % table_name
    runner.check(line)
    line = 'select count(*) from %s where k1 is not null group by k2 %% 10 order by 1' % table_name
    runner.check(line)
    line = 'select k3 %% 10, count(*) from %s where k1 is not null group by 1 order by 1' \
           % table_name
    runner.check(line)
    line = 'select count(*) from %s where k1 is not null group by k3 %% 10 order by 1' % table_name
    runner.check(line)
    line = 'select count(ALL *) from %s where k1 is not null group by k3 %% 10 order by 1' \
           % table_name
    runner.check(line)
    line = 'select k4 %% 100, count(*) from %s where k1 is not null group by 1 order by 1' \
           % table_name
    runner.check(line)
    line = 'select count(*) from %s where k1 is not null group by k4 %% 100 order by 1' % table_name
    runner.check(line)
    line = 'select k9, k9 * 2, count(*) from %s group by 1, 2 order by 1, 2' % table_name
    runner.check(line)
    line = 'select count(*) from %s group by k9 order by k9' % table_name
    runner.check(line)
    line = 'select k9, count(*) from %s where k9 is null and k1 is not null group by 1 order by 1' \
           % table_name
    runner.check(line)
    line = 'select k8, k8 * 2, count(*) from %s group by 1, 2 order by 1, 2, 3' % table_name
    runner.check(line)
    line = 'select k8, count(*) a from %s group by k8 order by k8, a' % table_name
    runner.check(line)
    line = 'select k8, count(*) from %s where k8 is null and k1 is not null group by 1 order by 1' \
           % table_name
    runner.check(line)
    line = 'select k11, count(*) from %s where k1 is not null group by 1 order by 1' % table_name
    runner.check(line)
    line = 'select count(*) from %s where k1 is not null group by k11 order by k11' % table_name
    runner.check(line)
    line = 'select k1 %% 3, k2 %% 3, count(*) from %s where k1 = 1 group by 1, 2 order by 1, 2' \
           % table_name
    runner.check(line)
    line = 'select count(*) from %s where k1 = 1 group by k1 %% 3, k2 %% 3 order by 1' % table_name
    runner.check(line)
    line = 'select k1 %% 3, k2 %% 3, count(*) from %s where k1 = 1 group by 2, 1 order by 2, 1' \
           % table_name
    runner.check(line)
    line = 'select k1 %% 2, k2 %% 2, k3 %% 2, k4 %% 2, k11, count(*) from %s \
            where (k11 = "01/01/10" or k11 = "01/02/10") and k1 is not null \
            group by 1, 2, 3, 4, 5 order by 1, 2, 3, 4, 5' % table_name
    runner.check(line)
    line = 'select count(*) from %s where (k11 = "01/01/10" or k11 = "01/02/10") \
            and k1 is not null group by k1 %% 2, k2 %% 2, k3 %% 2, k4 %% 2, k11 order by 1' \
            % table_name
    runner.check(line)
    line = 'select count(*), min(k1), max(k1), sum(k1), avg(k1) from %s \
            where k1 = -1 and k1 is not null ' % table_name
    runner.check(line)
    line = 'select count(*), min(k2), max(k2), sum(k2), avg(k2) from %s \
            where k2 = -1 and k1 is not null' % table_name
    runner.check(line)
    line = 'select count(*), min(k3), max(k3), sum(k3), avg(k3) from %s \
            where k3 = -1 and k1 is not null' % table_name
    runner.check(line)
    line = 'select count(*), min(k4), max(k4), sum(k4), avg(k4) from %s \
            where k4 = -1 and k1 is not null' % table_name
    runner.check(line)
    line = 'select count(*), min(k9), max(k9), sum(k9), avg(k9) from %s \
            where k9 < -1.0 and k1 is not null' % table_name
    runner.check(line)
    line = 'select count(*), min(k8), max(k8), sum(k8), avg(k8) from %s \
            where k8 < -1.0 and k1 is not null' % table_name
    runner.check(line)


def test_agg_impala_3():
    """
    {
    "title": "test_query_agg.test_agg_impala_3",
    "describe": "impala aggregation tests",
    "tag": "p1,function"
    }
    """
    """impala aggregation tests"""
    line = 'select k3 %% 7, count(*), max(k3) from %s where k1 is not null \
            group by 1 order by 1' % table_name
    runner.check(line)
    line = 'select k3 %% 7, count(*) from %s where k1 is not null group by 1 \
            having max(k3) > 991 order by 1' % table_name
    runner.check(line)
    line = 'select k3 %% 7, count(*) from %s where k1 is not null group by 1 \
            having max(k3) > 991 and count(*) > 1420 order by 1' % table_name
    runner.check(line)
    line = 'select k3 %% 7, count(*) from %s where k1 is not null group by 1 \
            having min(k3) < 7 order by 1' % table_name
    runner.check(line)
    line = 'select k3 %% 7, count(*) from %s where k1 is not null group by 1 \
            having min(k3) < 7 and count(*) > 1420 order by 1' % table_name
    runner.check(line)
    line = 'select k3 %% 7, count(*), sum(k3) from %s where k1 is not null group by 1 order by 1' \
            % table_name
    runner.check(line)
    line = 'select k3 %% 7, count(*), sum(k3) from %s where k1 is not null group by 1 \
            having sum(k3) >= 715000 order by 1' % table_name
    runner.check(line)
    line = 'select k3 %% 7, count(*), sum(k3) from %s where k1 is not null group by 1 \
            having sum(k3) >= 715000 or count(*) > 1420 order by 1' % table_name
    runner.check(line)
    line = 'select k3 %% 7, count(*), sum(k3) from %s where k1 is not null group by 1 \
            having sum(k3) is null order by 1' % table_name
    runner.check(line)
    line = 'select k3 %% 7, count(*), avg(k3) from %s where k1 is not null group by 1 order by 1' \
           % table_name
    runner.check(line)
    line = 'select k3 %% 7, count(*), avg(k3) from %s where k1 is not null group by 1 \
            having avg(k3) > 500 order by 1' % table_name
    runner.check(line)
    line = 'select k3 %% 7, count(*), avg(k3) from %s where k1 is not null group by 1 \
            having avg(k3) > 500 or count(*) = 10 order by 1' % table_name
    runner.check(line)
    line = 'select k11, count(*) from %s where k1 is not null group by k11 \
            having k11 < cast("2010-01-01 01:05:20" as datetime) order by k11' % table_name
    runner.check(line)
    line = 'select count(NULL), min(NULL), max(NULL), sum(NULL), avg(NULL) from %s \
            where k1 is not null' % table_name
    runner.check(line)
    line = 'select min(distinct NULL), max(distinct NULL) from %s' % table_name
    runner.check(line)
    line = 'select k3 * k3, k3 + k3 a from %s group by k3 * k3, k3 + k3, k3 * k3 \
            having a < 5 order by 1 limit 10' % table_name
    runner.check(line)
    line = 'select 1 from (select count(k4) c from %s having min(k3) is not null) as t \
            where c is not null' % table_name
    runner.check(line)
    line = 'select count(k1), sum(k1 * k1) from %s' % table_name
    runner.check(line)
    line = 'select count(k3), sum(k3), avg(k3) from %s where k3 is NULL' % table_name
    runner.check(line)
    line = 'select k2 %% 2, k3 > 1, k2 from %s where k2 < 2 group by 1,2,3 order by 1, 2, 3' \
           % table_name
    runner.check(line)
    line1 = 'select min(cast(-1.0 as float)), max(cast(-1.0 as float)) from %s' % join_name
    line2 = 'select min(cast(-1.0 as decimal)), max(cast(-1.0 as decimal)) from %s' % join_name
    runner.check2(line1, line2)
    line = 'select count(null * 1) from %s' % join_name
    runner.check(line)
    line = 'select extract(year from k11) as k11, extract(month from k11) as month, sum(k1) \
            from %s group by 1, 2 order by 1, 2;' % table_name
    runner.check(line)


def test_agg_impala_4():
    """
    {
    "title": "test_query_agg.test_agg_impala_4",
    "describe": "test impala case",
    "tag": "p1,function"
    }
    """
    """test impala case"""
    line1 = 'select k1, group_concat(k7) from (select * from %s where k2 %% 100 = k1 \
            order by k3 limit 99999) a group by k1 order by k1' % table_name
    line2 = 'select k1, group_concat(k7 order by k3 separator ", ") from (select * from %s  \
            where k2 %% 100 = k1 order by k3 limit 99999) a group by k1 order by k1' % table_name
    runner.check2(line1, line2)
    line1 = 'select k1, group_concat(k7, NULL) from (select * from %s where k2 %% 100 = k1 \
            order by k3 limit 99999) a group by k1 order by k1' % table_name
    runner.check(line1)
    line = 'select k1, group_concat(NULL, NULL) from (select * from %s where k2 %% 100 = k1 \
            order by k3 limit 99999) a group by k1 order by k1' % table_name
    runner.check(line)
    line1 = 'select k1, group_concat(k7, "->") from \
            (select * from %s where k2 %% 100 = k1 order by k3 limit 99999) \
            a group by k1 order by k1' % table_name
    line2 = 'select k1, group_concat(k7 order by k3 separator "->") from \
            (select * from %s where k2 %% 100 = k1 order by k3 limit 99999) \
            a group by k1 order by k1' % table_name
    runner.check2(line1, line2)
    line1 = 'select k1, group_concat(trim(k7), trim(k7)) from (select * from %s \
            where k2 %% 200 = k1 order by k3 limit 99999) a group by k1 order by k1' % table_name
    runner.checkok(line1)
    line1 = 'select k1, group_concat(k7, "->"), group_concat(cast(k11 as string)) from \
            (select * from %s where k2 %% 250 = k1 order by k3 limit 99999) a group by k1 \
            order by k1' % table_name
    line2 = 'select k1, group_concat(k7 order by k3 separator "->"), \
            group_concat(k11 order by k3 separator ", ") from \
            (select * from %s where k2 %% 250 = k1 order by k3 limit 99999) a group by k1 \
            order by k1' % table_name
    runner.check2(line1, line2)
    line1 = 'select k1, group_concat(k7, "->"), group_concat(cast(k11 as string)) \
            from (select * from %s where k2 %% 250 = k1 order by k3 limit 99999) a \
            group by k1 order by k1' % table_name
    line2 = 'select k1, group_concat(k7 order by k3 separator "->"), \
            group_concat(k11 order by k3 separator ", ") from (select * from %s  \
            where k2 %% 250 = k1 order by k3 limit 99999) a group by k1 order by k1' % table_name
    runner.check2(line1, line2)
    line1 = 'select group_concat(k7) from %s where k7 = NULL' % table_name
    line2 = 'select group_concat(k7 separator ", ") from %s where k7 = NULL' % table_name
    runner.check2(line1, line2)
    line1 = 'select group_concat(k7) from %s where k3 = 1' % table_name
    line2 = 'select group_concat(k7 separator ", ") from %s where k3 = 1' % table_name
    runner.check2(line1, line2)
    line1 = 'select group_concat("abc", "xy") from %s where k2 %% 1000 = k1' % table_name
    line2 = 'select group_concat("abc" separator "xy") from %s where k2 %% 1000 = k1' % table_name
    runner.check2(line1, line2)


def test_agg_distinct_count():
    """
    {
    "title": "test_query_agg.test_agg_distinct_count",
    "describe": "count distinct multi columns",
    "tag": "p1,function,fuzz"
    }
    """
    """
    count distinct multi columns
    """
    line = 'select count(distinct k1, k2) from %s' % table_name
    runner.check(line)
    line = 'select count(distinct k2, k3) from %s' % table_name
    runner.check(line)
    line = 'select count(distinct k4, k5) from %s' % table_name
    runner.check(line)
    line = 'select count(distinct k6, k7, k10, k11, k8, k9) from %s' % table_name
    runner.check(line)
    line = 'select count(distinct k4, k5), count(distinct k4, k5) from %s' % table_name
    runner.check(line)
    line = 'select count(distinct k1, k2), count(distinct k2, k3), count(distinct k4, k5), \
            count(distinct k6, k7, k10, k11, k8, k9) from %s' % table_name
    runner.checkwrong(line)
    
    # MySQL的字符串排序或distinct的时候不区分大小写，与palo不一样
    # k7中含有空格的数据与MySQL不一致
    # fields = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9', 'k10', 'k11']
    fields = ['k1', 'k2', 'k3', 'k4', 'k5', 'upper(k6)', 'k8', 'k9', 'k10', 'k11']
    sql = 'select count(distinct %s) a, count(distinct %s) b, count(distinct %s) c from %s'
    retry = 20
    while retry > 0:
        num = 3
        columns = list()
        while num > 0: 
            # 每个count distinct里面只能有一个列，除非只有一个count distinct，
            # 或者多个countdistinct完全一样
            # length = random.randint(2, len(fields))
            # sub = random.sample(fields, length)
            # s = ', '.join(sub)
            # columns.append(s)
            index = random.randint(0, len(fields) - 1)
            columns.append(fields[index])
            num = num - 1
        line = sql % (columns[0], columns[1], columns[2], table_name)
        runner.check(line)
        retry = retry - 1
    line = 'select count(distinct k1, k2), count(distinct k3) from %s' % table_name
    runner.checkwrong(line)


def test_agg_ndv():
    """
    {
    "title": "test_query_agg.test_agg_ndv",
    "describe": " 近似值聚合函数。",
    "tag": "p1,function,fuzz"
    }
    """
    """
    近似值聚合函数。
    """
    # 各个类型的字段都适合这个函数
    count = runner.query_palo.do_sql('select count(*) from %s' % table_name)
    table_null = 'test_query_agg_null'
    allow_diff = 0.033
    res = ('',)
    try:
        runner.init("drop table if exists %s" % table_null)
    except Exception as e:
        pass
    sql = 'create table %s(k1 tinyint, k2 smallint NULL, k3 int NULL, k4 bigint NULL,\
    k5 decimal(9,3) NULL, k6 char(5) NULL, k8 date NULL, k9 datetime NULL, \
    k7 varchar(20) NULL, k10 double sum, k11 float sum) engine=olap \
    distributed by hash(k1) buckets 5 properties("storage_type"="column")' % table_null
    msql = 'create table %s(k1 tinyint, k2 smallint, k3 int,  k4 bigint NULL,\
    k5 decimal(9,3), k6 char(5), k8 date, k9 datetime, k7 varchar(20),\
     k10 double, k11 float)' % table_null
    runner.init(sql, msql)
    # insert NULL data
    sql = "insert into %s values (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL,\
      NULL, 8.9, 9.8)" % table_null
    runner.init(sql)
    runner.check("select count(*) from %s" % table_null)
    for index in range(11):
        if index in [6, 7]:
            continue
        line1 = 'select k1, ndv(k%s) from %s group by k1 order by k1' % (index + 1, table_name)
        line2 = 'select k1, count(distinct k%s) from %s group by k1 order by k1' \
            % (index + 1, table_name)
        if index == 1:
            # percent is more than 0.03,Special treat
            # palo line 225, mysql line 232, gap 0.0301724
            runner.check2_diff(line1, line2, 0.04)
            continue
        runner.check2_diff(line1, line2, allow_diff)
        # 带group where
        line1 = 'select k1, ndv(k%s) from %s where k1>80 group by k1 order by k1' \
            % (index + 1, table_name)
        line2 = 'select k1, count(distinct k%s) from %s where k1>80 group by k1 order by k1'\
             % (index + 1, table_name)
        runner.check2_diff(line1, line2, allow_diff)
        # NULL 值
        line1 = 'select k1, ndv(k%s) from %s group by k1 order by k1'\
             % (index + 1, table_null)
        line2 = 'select k1, count(distinct k%s) from %s group by k1 order by k1'\
               % (index + 1, table_null)
        runner.check2(line1, line2)
        line1 = 'select k%s, ndv(k1) from %s group by k%s order by k%s'\
             % (index + 1, table_null, index + 1, index + 1)
        line2 = 'select k%s, count(distinct k1) from %s group by k%s order by k%s'\
               % (index + 1, table_null, index + 1, index + 1)
        runner.check2(line1, line2)
    # 精算+估算
    line1= 'select ndv(k1), k11,ndv(k5), ndv(k6), ndv(k1) from %s group by k11 \
            order by k11' % table_name
    line2= 'select  count(distinct k1), k11, count(distinct k5), count(distinct k6), count(distinct k1)\
          from %s group by k11 order by k11' % table_name
    runner.check2_diff(line1, line2, 0.03, [0, 1, 0, 0, 0])

    # char,varchar
    line1 = 'select ndv(k6), ndv(k7) from %s' % table_name
    print(line1)
    res = runner.query_palo.do_sql(line1)
    print(res)
    excepted = ((19958, 19955),)
    util.check_same(res, excepted)
  
    # where
    line1 = "select ndv(k1) from %s where k1>80" % table_name
    line2 = "select count(distinct k1) from %s where k1>80" % table_name
    runner.check2_diff(line1, line2)
    # 函数 k1+k2有估算
    line1 = 'select ndv(k1+k2), ndv(4+78), ndv(abs(-2)) from %s' % table_name
    line2 = 'select count(distinct k1+k2),count(distinct 4+78), \
           count(distinct(abs(-2))) from %s' % table_name
    runner.check2_diff(line1, line2)
    # checkwrong
    sql = 'select k1, ndv(k1, k2) from %s group by k1 order by k1' % table_name
    runner.checkwrong(sql)
    sql = 'select ndv(1)'
    runner.checkwrong(sql)
    sql = 'select k10, ndv(k11) from %s group by k11  order by k10' % table_name
    runner.checkwrong(sql)


def test_agg_approx_count_distinct():
    """
    {
    "title": "test_query_agg.test_agg_distinct_count",
    "describe": "approx count distinct",
    "tag": "p1,function,fuzz"
    }
    """
    line1 = 'select approx_count_distinct(k1) from %s' % table_name
    line2 = 'select count(distinct k1) from %s' % table_name
    runner.check2_diff(line1, line2)
    line1 = 'select approx_count_distinct(k2) from %s' % table_name
    line2 = 'select count(distinct k2) from %s' % table_name
    runner.check2_diff(line1, line2)
    line1 = 'select approx_count_distinct(k5) from %s' % table_name
    line2 = 'select count(distinct k5) from %s' % table_name
    runner.check2_diff(line1, line2)
    line1 = 'select approx_count_distinct(k6) from %s' % table_name
    line2 = 'select count(distinct k6) from %s' % table_name
    runner.check2_diff(line1, line2)
    line1 = 'select approx_count_distinct(k10) from %s' % table_name
    line2 = 'select count(distinct k10) from %s' % table_name
    runner.check2_diff(line1, line2)
    line1 = 'select approx_count_distinct(k11) from %s' % table_name
    line2 = 'select count(distinct k11) from %s' % table_name
    runner.check2_diff(line1, line2)


def teardown_module():
    """
    todo
    """
    pass


if __name__ == "__main__":
    print("test")
    setup_module()
    test_agg_ndv()

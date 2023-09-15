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
import os
import pymysql
import sys
import time
import pytest

sys.path.append("../lib/")
from palo_qe_client import PaloQE
from palo_qe_client import QueryBase
import query_util as util
from warnings import filterwarnings
filterwarnings('ignore', category=pymysql.Warning)
table_name = "test"
join_name = "baseall"

sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage

def setup_module():
    """
    init config
    """
    global runner
    runner = QueryExecuter()


class QueryExecuter(QueryBase):
    """query executer"""
    def __init__(self):
        self.get_clients()

    def get_clients(self):
        """get palo query client and mysql connection"""
        self.query_host = ''
        self.query_port = 9030
        self.query_db = "test_query_qa"
        self.query_user = "root"
        self.query_passwd = ""

        if 'FE_HOST' in os.environ.keys():
            self.query_host = os.environ["FE_HOST"]
        if 'FE_QUERY_PORT' in os.environ.keys():
            self.query_port = int(os.environ["FE_QUERY_PORT"])
        if 'FE_USER' in os.environ.keys():
            self.query_user = os.environ["FE_USER"]
        if 'FE_PASSWORD' in os.environ.keys():
            self.query_passwd = os.environ["FE_PASSWORD"]
        if 'FE_DB' in os.environ.keys():
            self.query_db = os.environ["FE_DB"]
        print("--------------------------")
        print(self.query_host, self.query_port, self.query_user, self.query_passwd, self.query_db)
        print("--------------------------")

        self.mysql_host = ''
        self.mysql_port = 3306
        self.mysql_db = "win_test"
        self.mysql_user = "root"
        self.mysql_passwd = "root"

        if 'MYSQL_HOST' in os.environ.keys():
            self.mysql_host = os.environ["MYSQL_HOST"]
        if 'MYSQL_PORT' in os.environ.keys():
            self.mysql_port = int(os.environ["MYSQL_PORT"])
        if 'MYSQL_USER' in os.environ.keys():
            self.mysql_user = os.environ["MYSQL_USER"]
        if 'MYSQL_PASSWORD' in os.environ.keys():
            self.mysql_passwd = os.environ["MYSQL_PASSWORD"]
        if 'MYSQL_DB' in os.environ.keys():
            self.mysql_db = os.environ["MYSQL_DB"]

        print("--------------------------")
        print(self.mysql_host, self.mysql_port, self.mysql_user, self.mysql_passwd, self.mysql_db)
        print("--------------------------")
        self.query_palo = PaloQE(self.query_host, self.query_port, self.query_user,
                                 self.query_passwd, self.query_db)
        mysql_con = pymysql.connect(host=self.mysql_host, user=self.mysql_user,
                                    passwd=self.mysql_passwd,
                                    port=self.mysql_port, db=self.mysql_db)
        self.mysql_cursor = mysql_con.cursor()
        self.mysql_cursor.execute("select version()")
        ret = self.mysql_cursor.fetchall()
        version = ret[0][0].split('.')
        if int(version[0]) < 8:
            raise pytest.skip('mysql version is too low, expect version 8+')

    def check(self, line1, line2=None, force_order=True):
        """
        check if result of mysql and palo is same
        line1 is for palo, line2 is for mysql
        """
        print(line1)
        if line2 is None:
            line2 = line1
        else:
            print(line2)
        times = 0
        flag = 0
        while times <= 10 and flag == 0:
            try:
                palo_result = self.query_palo.do_sql(line1)
                self.mysql_cursor.execute(line2)
                LOG.info(L('palo line, mysql line', palo_line=line1, mysql_line=line2))
                mysql_result = self.mysql_cursor.fetchall()
                LOG.info(L('palo result, mysql result', palo_result=palo_result, mysql_result=mysql_result))
                util.check_same(palo_result, mysql_result, force_order)
                flag = 1
            except Exception as e:
                print(Exception, ":", e)
                time.sleep(1)
                times += 1
                if times == 3:
                    assert 0 == 1


def test_win_1():
    """
    {
    "title": "test_win_1",
    "describe": "test win",
    "tag": "autotest"
    }
    """
    """test win"""
    line1 = 'SELECT FIRST_VALUE(-2605.952148) OVER ' \
            '(PARTITION BY k1 ORDER BY k1 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) wj ' \
            'FROM baseall '
    line2 = 'SELECT FIRST_VALUE(-2605.952148) OVER ' \
            '(PARTITION BY k1 ORDER BY k1 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) wj ' \
            'FROM baseall'
    runner.check(line1, line2)

    line1 = 'SELECT LAST_VALUE(-2605.952148) OVER ' \
            '(PARTITION BY k1 ORDER BY k1 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) wj ' \
            'FROM baseall'
    line2 = 'SELECT LAST_VALUE(-2605.952148) OVER ' \
            '(PARTITION BY k1 ORDER BY k1 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) wj ' \
            'FROM baseall'
    runner.check(line1, line2)

    # todo 
    line1 = 'SELECT max(-2605.952148) OVER ' \
            '(PARTITION BY k1 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) wj ' \
            'from baseall order by wj'
    line2 = 'SELECT MAX(-2605.952148) OVER ' \
            '(PARTITION BY k1 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) wj ' \
            'FROM baseall WINDOW w1 AS (PARTITION BY k1)'
    # runner.check(line1, line2)

    line1 = 'SELECT k1, LAST_VALUE(k1) OVER ' \
            '(ORDER BY k1 rows BETWEEN 7 preceding AND 5 PRECEDING) wj FROM baseall'
    line2 = 'SELECT k1, LAST_VALUE(k1) OVER w FROM baseall ' \
            'WINDOW w AS (ORDER BY k1 RANGE BETWEEN 7 PRECEDING AND 5 PRECEDING)'
    runner.check(line1, line2)

    line1 = 'SELECT k1, ' \
            'FIRST_VALUE(k1) OVER (ORDER BY k1 ROWS BETWEEN 3 FOLLOWING AND 5 FOLLOWING) fv, ' \
            'LAST_VALUE(k1) OVER (ORDER BY k1 ROWS BETWEEN 3 FOLLOWING AND 5 FOLLOWING) lv ' \
            'from baseall'
    line2 = 'SELECT k1, FIRST_VALUE(k1) OVER w fv, ' \
            'LAST_VALUE(k1) OVER w lv FROM baseall ' \
            'WINDOW w AS (ORDER BY k1 ROWS BETWEEN 3 FOLLOWING AND 5 FOLLOWING)'
    runner.check(line1, line2)

    line = 'select k1, lag(k1, 1, 1) over (order by k1) wj from baseall group by k1'
    runner.check(line)

    line = 'select k1, lead(k1, 1, 1) over (order by k1) wj from baseall group by k1'
    runner.check(line)

    line = 'select k1, lag(k1 + 3/2 -1 + 5, 1, 1) over (order by k1) wj from baseall group by k1'
    runner.check(line)

    line1 = 'SELECT k1, ' \
            'avg(k1) over (ORDER BY k1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as avg, ' \
            'sum(k1) over (ORDER BY k1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) cnt, k1, ' \
            'COUNT(k1) over (ORDER BY k1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) count from baseall'
    line2 = 'SELECT k1, AVG(k1) OVER w `avg`, SUM(k1) OVER w `sum`, k1, COUNT(k1) OVER w count ' \
            'FROM baseall WINDOW w as (ORDER BY k1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)'
    runner.check(line1, line2)

    line = 'SELECT * FROM (SELECT IF(1, round(null), ROW_NUMBER() OVER (PARTITION BY k1)) ' \
           'FROM baseall) AS a'
    runner.check(line)

    line = 'SELECT SUM(k1) OVER (ORDER BY k1 ' \
           'ROWS BETWEEN 9223372036854775806 FOLLOWING AND 9223372036854775807 FOLLOWING) as `sum` ' \
           'FROM baseall'
    runner.check(line)

    line = 'SELECT SUM(k1) OVER(ORDER BY k1 ' \
           'ROWS BETWEEN 9223372036854775807 FOLLOWING AND 9223372036854775807 FOLLOWING) as `sum` ' \
           'FROM baseall'
    runner.check(line)

    line = 'SELECT SUM(k1) OVER(ORDER BY k1 ' \
           'ROWS BETWEEN 9223372036854775805 FOLLOWING AND 9223372036854775807 FOLLOWING) as `sum` ' \
           'FROM baseall'
    runner.check(line)

    line = 'SELECT SUM(k1) OVER(ORDER BY k1 ' \
           'ROWS BETWEEN 9223372036854775807 FOLLOWING AND 9223372036854775805 FOLLOWING) as `sum` ' \
           'FROM baseall'
    runner.check(line)

    line = 'WITH der AS (SELECT CASE WHEN k1 IN ("0") THEN k1 END AS a FROM baseall), ' \
           'der1 AS (SELECT ROW_NUMBER() OVER (ORDER BY a), a FROM der) SELECT * FROM der1'
    runner.check(line)

    line = 'WITH der AS (SELECT CASE WHEN k1 IN ("0") THEN k1 END AS a FROM baseall), ' \
           'der1 AS (SELECT ROW_NUMBER() OVER (ORDER BY a) FROM der) SELECT * FROM der1'
    runner.check(line)

    line = 'WITH der AS (SELECT k1 AS a FROM baseall), ' \
           'der1 AS (SELECT ROW_NUMBER() OVER (ORDER BY a) FROM der) SELECT * FROM der1'
    runner.check(line)


def test_win_33():
    """
    {
    "title": "test_win_33",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    line = 'SELECT k1 AS b, ROW_NUMBER() OVER (ORDER BY k1) FROM baseall'
    runner.check(line)

    line = 'SELECT k1 + 3 AS b, ROW_NUMBER() OVER (ORDER BY k1) FROM baseall'
    runner.check(line)

    line = 'SELECT 3 AS k1, ROW_NUMBER() OVER (ORDER BY k1) FROM baseall'
    runner.check(line)

    line1 = 'SELECT SUM(k1) over (ORDER BY k2,k11 ' \
            'RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) w from baseall order by 1'
    line2 = 'SELECT SUM(k1) OVER w as win FROM baseall ' \
            'WINDOW w AS(ORDER BY k2,k11 RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) ' \
            'order by 1'
    runner.check(line1, line2)

    line = 'select k1, k2, sum(k1 + k2) over(order by k1 rows unbounded preceding) foo ' \
           'from baseall'
    runner.check(line)

    line = 'select k1, k2, sum(k1 + k2) ' \
           'over(order by k1 rows between unbounded preceding and current row) foo from baseall'
    runner.check(line)

    line = 'select k1, k2, sum(k1 + k2) ' \
           'over(rows unbounded preceding) foo from baseall order by k1'
    runner.checkwrong(line)

    line = 'select k1, k2, sum(k1 + k2) over(order by k1 rows unbounded preceding) foo ' \
           'from baseall limit 10'
    runner.check(line)


def test_win_2():
    """
    {
    "title": "test_win_2",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    line = 'SELECT k1, k2, SUM(k1+k2) OVER (ORDER BY k2, k1 ROWS UNBOUNDED PRECEDING) foo ' \
           'FROM baseall'
    runner.check(line)

    line = 'SELECT k1, k2, SUM(k1+k2) OVER (ORDER BY k2, k1 ROWS UNBOUNDED PRECEDING) foo ' \
           'FROM baseall ORDER BY foo'
    runner.check(line)

    line = 'SELECT k1, k2, SUM(k1+k2) OVER (ORDER BY k2, k1 ROWS UNBOUNDED PRECEDING) foo ' \
           'FROM baseall ORDER BY foo DESC'
    runner.check(line)

    line = 'SELECT k1, k2, SUM(k1+k2) OVER (ORDER BY k2, k1 DESC ROWS UNBOUNDED PRECEDING) foo ' \
           'FROM baseall'
    runner.check(line)

    line = 'SELECT k2, k2, SUM(k1+k2) OVER (ORDER BY v3 DESC ROWS UNBOUNDED PRECEDING) foo ' \
           'FROM baseall'
    runner.checkwrong(line)

    line = 'drop view if exists win_v'
    runner.init(line)
    line = 'CREATE VIEW win_v AS SELECT k1, k2, ' \
           'SUM(k1+k2) OVER (ORDER BY k2, k1 DESC ROWS UNBOUNDED PRECEDING) foo FROM baseall'
    runner.init(line)
    line = 'SELECT * FROM win_v'
    runner.check(line)
    line = 'DROP VIEW win_v;'
    runner.init(line)

    line = 'SELECT k1, k2, SUM(k1+k2) OVER (ORDER BY k2, k1 DESC ROWS UNBOUNDED PRECEDING) foo ' \
           'FROM baseall ORDER BY foo'
    runner.check(line)

    line = 'SELECT k1, k2, SUM(k1+k2) OVER (ORDER BY k2, k1 DESC ROWS UNBOUNDED PRECEDING) foo ' \
           'FROM baseall ORDER BY foo DESC'
    runner.check(line)

    line = 'SELECT k1, SUM(k2) OVER () FROM baseall GROUP BY k1;'
    runner.checkwrong(line)

    line = 'SELECT k1, SUM(k1) OVER (order by k1 ROWS UNBOUNDED PRECEDING) wf FROM baseall'
    runner.check(line)

    line = 'SELECT k1, MIN(k8), SUM(k9), SUM(k1) OVER (order by k1 ROWS UNBOUNDED PRECEDING) wf ' \
           'FROM baseall GROUP BY (k1)'
    runner.check(line)

    line = 'SELECT k1, MIN(k9), SUM(k8), SUM(k1) OVER (order by k1 ROWS UNBOUNDED PRECEDING) wf ' \
           'FROM baseall GROUP BY k1 ORDER BY wf DESC'
    runner.check(line)

    line = 'SELECT k1, MIN(k9), SUM(k8), SUM(k1) OVER (order by k1 ROWS UNBOUNDED PRECEDING) wf ' \
           'FROM baseall GROUP BY k1, k9 ORDER BY wf DESC'
    runner.check(line)

    line = 'SELECT k1, SUM(k1) OVER (order by k1 RANGE UNBOUNDED PRECEDING) foo ' \
           'FROM baseall GROUP BY k1'
    runner.check(line)

    line = 'SELECT k1, AVG(DISTINCT k2), SUM(k1) OVER (order by k1 ROWS UNBOUNDED PRECEDING) foo ' \
           'FROM baseall GROUP BY (k1)'
    runner.check(line)

    line = 'SELECT k2, SUM(k2+1) OVER (order by k2 ROWS UNBOUNDED PRECEDING) foo ' \
           'FROM baseall GROUP BY (k2);'
    runner.check(line)

    line = 'SELECT k1, SUM(k1+1) OVER (ORDER BY k1 DESC ROWS UNBOUNDED PRECEDING) foo ' \
           'FROM baseall GROUP BY (k1)'
    runner.check(line)

    line = 'SELECT k1/SUM(k2) OVER (PARTITION BY k3) AS x FROM baseall GROUP BY k4'
    runner.checkwrong(line)

    line = 'SELECT k1/SUM(k2) OVER (PARTITION BY kk) AS x FROM baseall;'
    runner.checkwrong(line)

    line = 'SELECT k1/SUM(k2) OVER (PARTITION BY 1) AS x FROM baseall;'
    runner.checkwrong(line)


def test_win_3():
    """
    {
    "title": "test_win_3",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    line1 = 'SELECT k6, AVG(k5), ROW_NUMBER() OVER () ' \
            'from baseall group by k6 order by k6 desc'
    line2 = 'SELECT k6, AVG(k5), ROW_NUMBER() OVER w FROM baseall ' \
            'GROUP BY k6 WINDOW w AS () ORDER BY k6 DESC;'
    runner.check(line1, line2)

    line1 = 'SELECT k6, AVG(k5), SUM(AVG(k5)) OVER (order by k6 ROWS UNBOUNDED PRECEDING) ' \
            'FROM baseall GROUP BY k6 ORDER BY k6 desc'
    line2 = 'SELECT k6, AVG(k5), SUM(AVG(k5)) OVER w FROM baseall GROUP BY k6 ' \
            'WINDOW w AS (order by k6 ROWS UNBOUNDED PRECEDING) ORDER BY k6 DESC'
    runner.check(line1, line2)

    line1 = 'SELECT k6, AVG(k3), ROW_NUMBER() OVER () wj FROM baseall GROUP BY k6 ' \
            'HAVING k6="true" OR k6 IS NULL ORDER BY k6 DESC'
    line2 = 'SELECT k6, AVG(k3), ROW_NUMBER() OVER w wj FROM baseall GROUP BY k6 ' \
            'HAVING k6="true" OR k6 IS NULL WINDOW w AS () ORDER BY k6 DESC'
    runner.check(line1, line2)

    line1 = 'SELECT k6, AVG(k3), SUM(AVG(k3)) OVER (order by k6 ROWS UNBOUNDED PRECEDING) wj ' \
            'FROM baseall GROUP BY k6 HAVING k6="false" OR k6="true" OR k6 IS NULL ' \
            'ORDER BY k6 DESC'
    line2 = 'SELECT k6, AVG(k3), SUM(AVG(k3)) OVER w wj FROM baseall GROUP BY k6 HAVING k6="false" ' \
            'OR k6="true" OR k6 IS NULL WINDOW w AS (order by k6 ROWS UNBOUNDED PRECEDING) ' \
            'ORDER BY k6 DESC'
    runner.check(line1, line2)

    line1 = 'SELECT k3, FIRST_VALUE(k3) OVER (PARTITION BY k6 ORDER BY k3' \
            ' ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) frist, ' \
            'LAST_VALUE(k3) OVER (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) last, k6 FROM baseall'
    line2 = 'SELECT k3, FIRST_VALUE(k3) OVER w first, LAST_VALUE(k3) OVER w last, k6 FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6 ORDER BY k3 ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)'
    runner.check(line1, line2)

    line = 'SELECT k6, COUNT(k3) OVER (ORDER BY k3 rows -1 PRECEDING) FROM baseall;'
    runner.checkwrong(line)

    line = 'SELECT k6, COUNT(k3) OVER (ORDER BY k3 rows BETWEEN -1 PRECEDING and 2 PRECEDING) ' \
           'FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT k6, COUNT(k3) OVER (ORDER BY k3 rows BETWEEN 1 PRECEDING and -1 PRECEDING) ' \
           'FROM baseall'
    runner.checkwrong(line)

    # todo checkok -> checkwrong?
    line = 'SELECT k6, COUNT(k3) OVER (ORDER BY k3 rows BETWEEN 1 PRECEDING and 2 PRECEDING) ' \
           'FROM baseall'
    runner.checkwrong(line)


def test_win_4():
    """
    {
    "title": "test_win_4",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    line = 'SELECT k10, FIRST_VALUE(k10) OVER (ORDER BY k10 ) FROM baseall'
    runner.check(line)

    line = 'SELECT RANK() OVER (ORDER BY k4) r FROM baseall'
    runner.check(line)

    line = 'SELECT DENSE_RANK() OVER (ORDER BY k4) r FROM baseall'
    runner.check(line)

    line = 'SELECT u.k6, SUM(DISTINCT u.k3) AS uids FROM baseall u, test v ' \
           'WHERE v.k3 = u.k3 GROUP BY u.k6 ORDER BY uids'
    runner.check(line)

    line = 'SELECT k3, k6, RANK() OVER (ORDER BY k6) FROM baseall ORDER BY k3, k6'
    runner.check(line)

    line = 'SELECT k3, k6, DENSE_RANK() OVER (ORDER BY k6) FROM baseall ORDER BY k3, k6'
    runner.check(line)

    line = 'SELECT u.k6, RANK() OVER (ORDER BY u.k6 DESC) `rank`, AVG(DISTINCT u.k3) AS uids ' \
           'FROM baseall u, test v WHERE v.k3 = u.k3 GROUP BY u.k6 ORDER BY u.k6'
    runner.check(line)

    line1 = 'SELECT u.k6, AVG(u.k3) AS uids, RANK() OVER (ORDER BY AVG(u.k3)) wj ' \
            'FROM baseall u, test v WHERE v.k3 = u.k3 GROUP BY u.k6'
    line2 = 'SELECT u.k6, AVG(u.k3) AS uids, RANK() OVER w `rank` FROM baseall u, test v ' \
            'WHERE v.k3 = u.k3 GROUP BY u.k6 WINDOW w AS (ORDER BY AVG(u.k3))'
    runner.check(line1, line2)

    line1 = 'SELECT u.k6, AVG(DISTINCT u.k3) AS uids,' \
            ' RANK() OVER (ORDER BY AVG(DISTINCT u.k3) DESC) rank ' \
            'FROM baseall u, test t2 WHERE t2.k3 = u.k3 GROUP BY u.k6 ORDER BY u.k6'
    line2 = 'SELECT u.k6, AVG(DISTINCT u.k3) AS uids, RANK() OVER w `rank` FROM baseall u, test t2 ' \
            'WHERE t2.k3 = u.k3 GROUP BY u.k6 WINDOW w AS (ORDER BY AVG(DISTINCT u.k3) DESC) ' \
            'ORDER BY u.k6'
    runner.check(line1, line2)

    line1 = 'SELECT u.k6, AVG(u.k3) AS uids, RANK() OVER (ORDER BY AVG(u.k3) DESC) rank ' \
            'FROM baseall u, test t2 WHERE t2.k3 = u.k3 GROUP BY u.k6 ORDER BY `rank` DESC'
    line2 = 'SELECT u.k6, AVG(u.k3) AS uids, RANK() OVER w `rank` FROM baseall u, test t2 ' \
            'WHERE t2.k3 = u.k3 GROUP BY u.k6 WINDOW w AS (ORDER BY AVG(u.k3) DESC) ' \
            'ORDER BY `rank` DESC'
    runner.check(line1, line2)

    line1 = 'SELECT k3, k6, RANK() OVER (ORDER BY k6) r1, DENSE_RANK() OVER (ORDER BY k6) ' \
            'FROM baseall ORDER BY k3'
    line2 = 'SELECT k3, k6, RANK() OVER w, DENSE_RANK() OVER w FROM baseall ' \
            'WINDOW w AS (ORDER BY k6) ORDER BY k3'
    runner.check(line1, line2)

    line = 'SELECT k3, k6, RANK() OVER (ORDER BY k6 DESC) FROM baseall ORDER BY k3'
    runner.check(line)

    line = 'SELECT u.k3 value, SUM(u.k3) OVER (order by u.k3 ROWS UNBOUNDED PRECEDING) ' \
           'FROM baseall u, test t2 WHERE t2.k3 = u.k3'
    runner.check(line)

    line = 'SELECT AVG(u.k3) average, SUM(AVG(u.k3)) OVER (order by u.k3 ROWS UNBOUNDED PRECEDING) ' \
           'FROM baseall u, test t2 WHERE t2.k3 = u.k3 GROUP BY u.k6'
    runner.checkwrong(line)

    line = 'SELECT k6, AVG(k3), RANK() OVER (ORDER BY AVG(k3) DESC) FROM baseall ' \
           'GROUP BY k6 ORDER BY k6'
    runner.check(line)

    line = 'SELECT k6, RANK() OVER (ORDER BY AVG(k3) DESC) FROM baseall GROUP BY k6 ORDER BY k6'
    runner.check(line)

    line = 'SELECT          RANK() OVER (ORDER BY AVG(k3)) FROM baseall'
    runner.check(line)

    line = 'SELECT AVG(k3), RANK() OVER (ORDER BY AVG(k3)) FROM baseall'
    runner.check(line)

    line = 'SELECT AVG(k3), SUM(AVG(k3)) OVER (ORDER BY AVG(k3) ROWS UNBOUNDED PRECEDING) ' \
           'FROM baseall'
    runner.check(line)

    line = 'SELECT k6, k3, RANK() OVER (PARTITION BY k6 ORDER BY k3 DESC) FROM baseall'
    runner.check(line)

    line = 'SELECT k6, k3, RANK() OVER (PARTITION BY k6 ORDER BY k3 ASC) FROM baseall'
    runner.check(line)


def test_win_5():
    """
    {
    "title": "test_win_5",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    line1 = 'SELECT k6, k3, ' \
            'SUM(k3) OVER (PARTITION BY k6 ORDER BY k3 ASC ROWS UNBOUNDED PRECEDING) summ, ' \
            'RANK() OVER (PARTITION BY k6 ORDER BY k3 ASC) rank ' \
            'FROM baseall'
    line2 = 'SELECT k6, k3, SUM(k3) OVER w summ, RANK() OVER w `rank` FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6 ORDER BY k3 ASC ROWS UNBOUNDED PRECEDING)'
    runner.check(line1, line2)

    line1 = 'SELECT k6, k3, ' \
            'SUM(k3) OVER (PARTITION BY k6 ORDER BY k3 ASC ROWS UNBOUNDED PRECEDING) summ, ' \
            'RANK() OVER (PARTITION BY k6 ORDER BY k3 ASC) rank ' \
            'FROM baseall ORDER BY summ'
    line2 = 'SELECT k6, k3, SUM(k3) OVER w summ, RANK() OVER w `rank` FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6 ORDER BY k3 ASC ROWS UNBOUNDED PRECEDING) ORDER BY summ'
    runner.check(line1, line2)

    line = 'SELECT * FROM ' \
           '(SELECT  RANK() OVER (ORDER BY k2) AS `rank`, k10, k11 FROM baseall) alias ' \
           'ORDER BY `rank`, k10, k11'
    runner.check(line)

    line = 'SELECT * FROM ' \
           '(SELECT RANK() OVER (ORDER BY k10) AS `rank`, k10, k11 FROM baseall) alias ' \
           'ORDER BY `rank`, k11 DESC'
    runner.check(line)

    line = 'SELECT k1, k2, ' \
           'SUM(k1) OVER (PARTITION BY k1  ORDER BY k2 ROWS UNBOUNDED PRECEDING) FROM baseall'
    runner.check(line)

    line = 'SELECT SUM(u.k1), ' \
           'SUM(SUM(u.k1)) OVER (ORDER BY u.k6 ROWS UNBOUNDED PRECEDING) ' \
           'FROM baseall u,test t2 WHERE u.k3=t2.k3 GROUP BY u.k6'
    runner.check(line)

    line = 'SELECT u.k1, SUM(SUM(u.k1)) OVER (ORDER BY u.k6 ROWS UNBOUNDED PRECEDING) ' \
           'FROM baseall u,test t2 WHERE u.k3=t2.k3 GROUP BY u.k6'
    runner.checkwrong(line)

    line = 'SELECT SUM(u.k1) OVER (ORDER BY u.k6 ROWS UNBOUNDED PRECEDING) ' \
           'FROM baseall u,test t2 WHERE u.k3=t2.k3 GROUP BY u.k6'
    runner.checkwrong(line)

    line1 = 'SELECT RANK() OVER (PARTITION BY t1.k3 ORDER BY t1.k6) FROM baseall t1,test t2 ' \
            'WHERE t1.k3=t2.k3 order by 1'
    line2 = 'SELECT RANK() OVER w FROM baseall t1,test t2 WHERE t1.k3=t2.k3 ' \
            'WINDOW w AS (PARTITION BY t1.k3 ORDER BY t1.k6) order by 1'
    runner.check(line1, line2)

    line = 'SELECT RANK() OVER (PARTITION BY t.k3 ORDER BY t.k6) wj ' \
           'FROM (SELECT * FROM baseall) t order by 1'
    runner.check(line)

    line = 'SELECT  SUM(k3) OVER (PARTITION BY k6 ORDER BY k3 ROWS UNBOUNDED PRECEDING) summ, ' \
           'k6 FROM baseall'
    runner.check(line)

    line1 = 'SELECT k2, SUM(k2) OVER (PARTITION BY k1 ORDER BY k2) wj, ' \
            'LEAD(k2, 1, 2) OVER (PARTITION BY k1 ORDER BY k2) wj2, k1 FROM baseall order by k1'
    line2 = 'SELECT k2, SUM(k2) OVER w, LEAD(k2, 1, 2) OVER w `lead2`, k1 FROM baseall ' \
            'WINDOW w AS (PARTITION BY k1 ORDER BY k2) order by k1'
    runner.check(line1, line2)

    line1 = 'SELECT k2, SUM(k2) OVER (PARTITION BY k1 ORDER BY k2 RANGE UNBOUNDED PRECEDING) wj, ' \
            'LEAD(k2, 1, 2) OVER (PARTITION BY k1 ORDER BY k2) wj2, ' \
            'k1 FROM baseall order by k1'
    line2 = 'SELECT k2, SUM(k2) OVER w, LEAD(k2, 1, 2) OVER w `lead2`, k1 FROM baseall ' \
            'WINDOW w AS (PARTITION BY k1 ORDER BY k2 RANGE UNBOUNDED PRECEDING) order by k1'
    runner.check(line1, line2)

    line1 = 'SELECT k4, ' \
            'LAST_VALUE(k4) OVER (PARTITION BY k1 ORDER BY k4 RANGE UNBOUNDED PRECEDING) wj , ' \
            'k1 FROM baseall order by k1'
    line2 = 'SELECT k4, LAST_VALUE(k4) OVER w wj, k1 FROM baseall ' \
            'WINDOW w AS (PARTITION BY k1 ORDER BY k4 RANGE UNBOUNDED PRECEDING) order by k1'
    runner.check(line1, line2)

    line = 'SELECT k4, LAST_VALUE(k4) OVER (PARTITION BY k1 ORDER BY k4 ROWS 2 PRECEDING) wj, ' \
            'k1 FROM baseall order by k1'
    runner.check(line)

    line = 'SELECT t1.k2, ROW_NUMBER() OVER (PARTITION BY t1.k2) FROM baseall t1 order by 1, 2'
    runner.check(line)


def test_win_6():
    """
    {
    "title": "test_win_6",
    "describe": "test win",
    "tag": "autotest"
    }
    """
    """test win"""
    line1 = 'SELECT u.k6, u.k1, u.k10, ROW_NUMBER() OVER (PARTITION BY u.k3 ORDER BY u.k6, u.k1) row_no, ' \
            'RANK() OVER (PARTITION BY u.k3 ORDER BY u.k6, u.k1) AS rank ' \
            'FROM baseall u, test t2 WHERE u.k3=t2.k3 order by 1, 2, 3, 4'
    line2 = 'SELECT u.k6, u.k1, u.k10, ROW_NUMBER() OVER w AS row_no, RANK() OVER w AS `rank` ' \
            'FROM baseall u, test t2 WHERE u.k3=t2.k3 WINDOW w AS (PARTITION BY u.k3 ORDER BY u.k6, u.k1)' \
            'order by 1, 2, 3, 4'
    runner.check(line1, line2)

    line1 = 'SELECT u.k6, u.k3, u.k11, ' \
            'ROW_NUMBER() OVER (PARTITION BY u.k10 ORDER BY u.k3, u.k1) row_no, ' \
            'RANK() OVER (PARTITION BY u.k10 ORDER BY u.k3, u.k1) rank ' \
            'FROM baseall u, test t2 WHERE u.k3=t2.k3'
    line2 = 'SELECT u.k6, u.k3, u.k11, ROW_NUMBER() OVER w AS row_no, RANK() OVER w AS `rank` ' \
            'FROM baseall u, test t2 WHERE u.k3=t2.k3 ' \
            'WINDOW w AS (PARTITION BY u.k10 ORDER BY u.k3, u.k1)'
    runner.check(line1, line2)

    line1 = 'SELECT  u.k10, t2.k3, RANK() OVER (PARTITION BY u.k10  ORDER BY u.k3) rank ' \
            'FROM baseall u, test t2 order by 1, 2, 3 limit 100'
    line2 = 'SELECT  u.k10, t2.k3, RANK() OVER w AS `rank` FROM baseall u, test t2 ' \
            'WINDOW w AS (PARTITION BY u.k10  ORDER BY u.k3) order by 1, 2, 3 limit 100'
    runner.check(line1, line2)

    line1 = 'SELECT * FROM ' \
            '(SELECT  u.k10, t2.k3, RANK() OVER (PARTITION BY u.k10  ORDER BY u.k3) rank ' \
            'FROM baseall u, test t2 order by 1, 2, 3 limit 100) a'
    line2 = 'SELECT * FROM (SELECT  u.k10, t2.k3, RANK() OVER w AS `rank` ' \
            'FROM baseall u, test t2 ' \
            'WINDOW w AS (PARTITION BY u.k10  ORDER BY u.k3) order by 1, 2, 3 limit 100) a'
    runner.check(line1, line2)


def test_win_7():
    """
    {
    "title": "test_win_7",
    "describe": "test win",
    "tag": "autotest"
    }
    """
    """test win"""
    line1 = 'SELECT t.*, SUM(t.`rank`) OVER (order by t.id, t.sex, t.date, t.row_no ROWS UNBOUNDED PRECEDING) wj ' \
            'FROM (SELECT u.k6 sex, u.k3 id, u.k10 date, ' \
                'ROW_NUMBER() OVER (PARTITION BY u.k10, u.k1 ORDER BY u.k3, u.k1) row_no, ' \
                'RANK() OVER (PARTITION BY u.k10, u.k1 ORDER BY u.k3, u.k1) rank ' \
                'FROM baseall u, test t2 WHERE u.k3=t2.k3 order by 1, 2, 3, 4, 5) as t'
    line2 = 'SELECT t.*, SUM(t.`rank`) OVER (order by t.id, t.sex, t.date, t.row_no ROWS UNBOUNDED PRECEDING) wj ' \
            'FROM (SELECT u.k6 sex, u.k3 id, u.k10 date, ROW_NUMBER() OVER w AS row_no, ' \
                'RANK() OVER w AS `rank` FROM baseall u, test t2 WHERE u.k3=t2.k3 ' \
                'WINDOW w AS (PARTITION BY u.k10, u.k1 ORDER BY u.k3, u.k1) order by 1, 2, 3, 4, 5) AS t'
    runner.check(line1, line2)


def test_win_10():
    """
    {
    "title": "test_win_10",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    line = 'SELECT t1.*, RANK() OVER (ORDER BY k6), ' \
           'SUM(k3) OVER (ORDER BY k6,k3,k1 ROWS UNBOUNDED PRECEDING) FROM baseall t1 order by k1'
    runner.check(line)

    line = 'SELECT * from (SELECT *, SUM(k1) OVER (order by k3,k2 ROWS UNBOUNDED PRECEDING), ' \
           'RANK() OVER (ORDER BY k6, k1) FROM baseall) alias ORDER BY k1'
    runner.check(line)

    line = 'SELECT t1.*, SUM(k3) OVER (ORDER BY k3, k1 ROWS UNBOUNDED PRECEDING),' \
           'RANK() OVER (ORDER BY k6,k3), ROW_NUMBER() OVER (ORDER BY k6,k3) FROM baseall t1'
    runner.check(line)
    
    line = 'SELECT t.k3, SUM(r00 + r01) OVER (ORDER BY k3 ROWS UNBOUNDED PRECEDING) AS s ' \
           'FROM (SELECT t1.*, RANK() OVER (ORDER BY k6, k3) AS r00, ' \
           'RANK() OVER (ORDER BY k6, k3 DESC) AS r01, ' \
           'RANK() OVER (ORDER BY k6, k3 DESC) AS r02, ' \
           'RANK() OVER (PARTITION BY k3 ORDER BY k6) AS r03, ' \
           'RANK() OVER (ORDER BY k6,k3) AS r04, ' \
           'RANK() OVER (ORDER BY k6,k3) AS r05, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r06, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r07, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r08, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r09, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r10, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r11, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r12, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r13, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r14 FROM baseall t1) t'
    runner.check(line)

    line = 'SELECT t.k3, SUM(r00 + r01) OVER (ORDER BY k3 ROWS UNBOUNDED PRECEDING) AS s FROM ' \
           '(SELECT t1.*, RANK() OVER (ORDER BY k6, k3) AS r00, ' \
           'RANK() OVER (ORDER BY k6, k3 DESC) AS r01, ' \
           'RANK() OVER (ORDER BY k6, k3 DESC) AS r02, ' \
           'RANK() OVER (PARTITION BY k3 ORDER BY k6) AS r03, ' \
           'RANK() OVER (ORDER BY k6,k3) AS r04, ' \
           'RANK() OVER (ORDER BY k6,k3) AS r05, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r06, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r07, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r08, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r09, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r10, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r11, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r12, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r13, ' \
           'RANK() OVER (ORDER BY k6, k3) AS r14 FROM baseall t1 limit 4) t;'
    runner.check(line)

    line1 = 'SELECT SUM(k3 * 2)  OVER (PARTITION BY k6), AVG(k3) OVER (PARTITION BY k6), ' \
            'count(k3) over (PARTITION BY k6) from baseall'
    line2 = 'SELECT SUM(k3) OVER w * 2, AVG(k3) OVER w, COUNT(k3) OVER w FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6)'
    runner.check(line1, line2)

    line1 = 'SELECT * FROM (SELECT k3, SUM(k3) OVER (PARTITION BY k6), ' \
            'COUNT(1) OVER (PARTITION BY k6), k6 from baseall) alias ORDER BY 1, 2, 3'
    line2 = 'SELECT * FROM (SELECT k3, SUM(k3) OVER w, COUNT(*) OVER w, k6 FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6)) alias ORDER BY 1, 2, 3'
    runner.check(line1, line2)

    line = 'SELECT * FROM (SELECT k3, SUM(k3) OVER (PARTITION BY k6), ' \
           'COUNT(*) OVER (PARTITION BY k6), k6 from baseall) alias ORDER BY 1, 2, 3'
    runner.checkwrong(line)

    line1 = 'SELECT SUM(k3) OVER (PARTITION BY k6) FROM baseall;'
    line2 = 'SELECT SUM(k3) OVER w FROM baseall WINDOW w AS (PARTITION BY k6)'
    runner.check(line1, line2)

    line1 = 'SELECT k3, SUM(k3) OVER (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) wj, k6 FROM baseall'
    line2 = 'SELECT k3, SUM(k3) OVER w, k6 FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6 ORDER BY k3 ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)'
    runner.check(line1, line2)

    line = 'drop view if exists v_win'
    runner.init(line)
    line1 = 'CREATE VIEW v_win AS SELECT k3, SUM(k3) OVER (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) wj, k6 FROM baseall'
    line2 = 'CREATE VIEW v_win AS SELECT k3, SUM(k3) OVER w, k6 FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6 ORDER BY k3 ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)'
    runner.init(line1, line2)
    line = 'SELECT * FROM v_win'
    runner.check(line)
    line = 'DROP VIEW v_win'
    runner.init(line)


def test_win_11():
    """
    {
    "title": "test_win_11",
    "describe": "test win",
    "tag": "autotest"
    }
    """
    """test win"""
    line1 = 'SELECT SUM(k3) OVER (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) wj FROM baseall'
    line2 = 'SELECT SUM(k3) OVER w FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6 ORDER BY k3 ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)'
    runner.check(line1, line2)

    line1 = 'SELECT k3, SUM(k3) OVER (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) wj, k6 FROM baseall'
    line2 = 'SELECT k3, SUM(k3) OVER w, k6 FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6 ORDER BY k3 ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)'
    runner.check(line1, line2)

    line1 = 'SELECT SUM(k3) OVER (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) wj1, ' \
            'COUNT(1) OVER (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) wj FROM baseall'
    line2 = 'SELECT SUM(k3) OVER w, COUNT(1) OVER w FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6 ORDER BY k3 ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)'
    runner.check(line1, line2)

    line = 'SELECT k3, AVG(k3) OVER (order by k3 ROWS UNBOUNDED PRECEDING) FROM k3'
    runner.check(line)

    line1 = 'SELECT k3, AVG(k3) OVER (ORDER BY k3 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) avg, ' \
            'COUNT(k3) OVER (ORDER BY k3 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) cnt ' \
            'FROM baseall'
    line2 = 'SELECT k3, AVG(k3) OVER w, COUNT(k3) OVER w FROM baseall ' \
            'WINDOW w AS (ORDER BY k3 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)'
    runner.check(line1, line2)

    line = 'SELECT k9, SUM(k9) OVER (ORDER BY k9), AVG(k9) OVER (ORDER BY k9) FROM baseall'
    runner.check(line)

    line = 'SELECT k8, SUM(k8) OVER (ORDER BY k8), AVG(k8) OVER (ORDER BY k8) FROM baseall'
    runner.check(line)

    line = 'SELECT k8, SUM(k8) OVER (ORDER BY k8), ' \
           'AVG(k8) OVER (ORDER BY k8 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM baseall'
    runner.check(line)
    
    line1 = 'SELECT k8, SUM(k8) OVER (ORDER BY k8 RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) a, ' \
            'AVG(k8) OVER (ORDER BY k8 RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) b FROM baseall'
    line2 = 'SELECT k8, SUM(k8) OVER w, AVG(k8) OVER w FROM baseall ' \
            'WINDOW w AS (ORDER BY k8 RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)'
    runner.check(line1, line2)

    line1 = 'SELECT k8, SUM(k8) OVER (ORDER BY k8 RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) a, ' \
            'AVG(k8) OVER (ORDER BY k8 RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) b FROM baseall'
    line2 = 'SELECT k8, SUM(k8) OVER w, AVG(k8) OVER w FROM baseall ' \
            'WINDOW w AS (ORDER BY k8 RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING)'
    runner.check(line1, line2)

    line1 = 'SELECT k8, SUM(k8) OVER (ORDER BY k8 RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) a, ' \
            'AVG(k8) OVER (ORDER BY k8 RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM baseall'
    line2 = 'SELECT k8, SUM(k8) OVER w, AVG(k8) OVER w FROM baseall ' \
            'WINDOW w AS (ORDER BY k8 RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING)'
    runner.check(line1, line2)

    line1 = 'SELECT ROW_NUMBER() OVER (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) wj, k3, ' \
            'SUM(k3) OVER (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) wj1, k6 FROM baseall'
    line2 = 'SELECT ROW_NUMBER() OVER w, k3, SUM(k3) OVER w, k6 FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)'
    runner.check(line1, line2)
    
    line1 = 'SELECT ROW_NUMBER() OVER (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING), ' \
            'SUM(k3) over (PARTITION BY k6 ORDER BY k3 ROWS ' \
            'BETWEEN 1 FOLLOWING AND 2 FOLLOWING) from baseall'
    line2 = 'SELECT ROW_NUMBER() OVER w, SUM(k3) OVER w FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6 ORDER BY k3 ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)'
    runner.check(line1, line2)
    
    line1 = 'SELECT RANK() OVER (PARTITION BY k6 ORDER BY k3) wj, k3, ' \
            'SUM(k3) OVER (PARTITION BY k6 ORDER BY k3) wj1 from baseall'
    line2 = 'SELECT RANK() OVER w, k3, SUM(k3) OVER w, k6 FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6 ORDER BY k3)'
    runner.check(line1, line2)

    line1 = 'SELECT RANK() OVER (PARTITION BY k6 ORDER BY k3 ROWS ' \
            'BETWEEN 1 FOLLOWING AND 2 FOLLOWING) wj, ' \
            'SUM(k3) OVER (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) wj1 from baseall'
    line2 = 'SELECT RANK() OVER w, SUM(k3) OVER w FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6 ORDER BY k3 ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)'
    runner.check(line1, line2)

    line1 = 'SELECT k3, k6, SUM(k3) OVER (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) wj, ' \
            'ROW_NUMBER() OVER (PARTITION BY k6 ORDER BY k3 ' \
            'ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) wj1 from baseall'
    line2 = 'SELECT k3, k6, SUM(k3) OVER w, ROW_NUMBER() OVER w, RANK() OVER w FROM baseall ' \
            'WINDOW w AS (PARTITION BY k6 ORDER BY k3 ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)'
    runner.check(line1, line2)


def test_first_value_type():
    """
    {
    "title": "test_first_value_type",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    cols = ['k3', 'k8', 'k9', 'k1', 'k4', 'k5', 'k10', 'k7']
    for col in cols:
        line = 'SELECT {col}, ' \
               'FIRST_VALUE(k6) OVER (ORDER BY k6, k1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ' \
               'FROM baseall'
        runner.check(line.format(col=col))
        line = 'SELECT {col}, FIRST_VALUE({col}) OVER (ORDER BY {col}) FROM baseall'
        runner.check(line.format(col=col))
        line = 'select {col}, FIRST_VALUE({col}) OVER (ROWS UNBOUNDED PRECEDING) FROM baseall'
        runner.checkwrong(line.format(col=col))
        line = 'SELECT {col}, FIRST_VALUE({col}) OVER (PARTITION BY k6 ORDER BY {col}) ' \
               'FROM baseall'
        runner.check(line.format(col=col))
        line = 'SELECT {col}, FIRST_VALUE({col}) OVER (ORDER BY {col} DESC) FROM baseall'
        runner.check(line.format(col=col))
        line = 'SELECT {col}, FIRST_VALUE({col}) OVER (ORDER BY {col} ROWS  2 PRECEDING) ' \
               'FROM baseall'
        runner.check(line.format(col=col))
        line = 'SELECT {col}, FIRST_VALUE({col}) OVER (ORDER BY {col} RANGE 2 PRECEDING) ' \
               'FROM baseall'
        runner.checkwrong(line.format(col=col))
        line = 'SELECT {col}, FIRST_VALUE({col}) OVER (ORDER BY {col} ' \
               'ROWS  BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM baseall'
        runner.check(line.format(col=col))
        line = 'SELECT {col}, FIRST_VALUE({col}) OVER (ORDER BY {col} ' \
               'ROWS  BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM baseall'
        runner.check(line.format(col=col))
        line = 'SELECT {col}, FIRST_VALUE({col}) OVER (ORDER BY {col} ' \
               'RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM baseall'
        runner.check(line.format(col=col))
        line1 = 'CREATE VIEW v AS SELECT k3, FIRST_VALUE(k3) OVER (ORDER BY k3 ' \
                'RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) w FROM baseall'
        line2 = 'CREATE VIEW v AS SELECT k3, FIRST_VALUE(k3) OVER w FROM baseall ' \
                'WINDOW w AS (ORDER BY k3 RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)'
        runner.init(line1, line2)
        line = 'select * from v'
        runner.check(line)
        line = 'drop view v'
        runner.init(line)
        line = 'SELECT {col}, FIRST_VALUE({col}) OVER ' \
               '(ORDER BY {col} ROWS  BETWEEN 2 FOLLOWING AND 3 FOLLOWING) FROM baseall'
        runner.check(line.format(col=col))
        

def test_win_35():
    """
    {
    "title": "test_win_35",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    line = 'SELECT k3, SUM(k3) OVER (ORDER BY k3 ROWS 2 PRECEDING) FROM baseall ORDER BY k3'
    runner.check(line)

    line = 'SELECT k3, SUM(k3) OVER (ORDER BY k3 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ' \
           'FROM baseall ORDER BY k3'
    runner.check(line)

    line = 'SELECT k3, SUM(k3) OVER (ORDER BY k3 RANGE UNBOUNDED PRECEDING) ' \
           'FROM baseall ORDER BY k3'
    runner.check(line)

    line = 'SELECT k10, k3, SUM(k3) OVER (PARTITION BY k10 ORDER BY k3 ROWS 2 PRECEDING) ' \
           'FROM baseall ORDER BY k10,k3'
    runner.check(line)

    line = 'SELECT k10, k3, SUM(k3) OVER ' \
           '(PARTITION BY k10 ORDER BY k3 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ' \
           'FROM baseall ORDER BY k10,k3'
    runner.check(line)

    line = 'SELECT k10, k3, SUM(k3) OVER ' \
           '(PARTITION BY k10 ORDER BY k3 RANGE UNBOUNDED PRECEDING) FROM baseall ORDER BY k10,k3'
    runner.check(line)
    
    line1 = 'SELECT k3, SUM(k3) OVER (ORDER BY k3) wj, AVG(k3) OVER (ORDER BY k3) wj1 FROM baseall'
    line2 = 'SELECT k3, SUM(k3) OVER w, AVG(k3) OVER w FROM baseall WINDOW w AS (ORDER BY k3)'
    runner.check(line1, line2)
    
    line1 = 'SELECT k10, k3, SUM(k3) OVER (PARTITION BY k10 ORDER BY k3) wj, ' \
            'AVG(k3) OVER (PARTITION BY k10 ORDER BY k3) wj1 FROM baseall ORDER BY k10'
    line2 = 'SELECT k10, k3, SUM(k3) OVER w, AVG(k3) OVER w FROM baseall ' \
            'WINDOW w AS (PARTITION BY k10 ORDER BY k3) ORDER BY k10'
    runner.check(line1, line2)

    line1 = 'SELECT k3, SUM(k3) OVER (ORDER BY k3) w, AVG(k3) OVER (ORDER BY k3) w1 FROM baseall'
    line2 = 'SELECT k3, SUM(k3) OVER w, AVG(k3) OVER w1 FROM baseall ' \
            'WINDOW w AS (ORDER BY k3), w1 AS (ORDER BY k3)'
    runner.check(line1, line2)

    line = 'SELECT k3, k10, COUNT(*) OVER (ORDER BY k10 RANGE INTERVAL 1 DAY PRECEDING) FROM baseall'
    runner.checkwrong(line)
    
    line = 'SELECT k3, ' \
            'SUM(k2) OVER (ORDER BY k2 ROWS BETWEEN 2.1 PRECEDING AND 1.1 FOLLOWING) w, ' \
            'COUNT(*) OVER (ORDER BY k2 ROWS BETWEEN 2.1 PRECEDING AND 1.1 FOLLOWING) w1 ' \
            'FROM baseall '
    runner.checkwrong(line)
    
    line = 'SELECT COUNT(*) OVER (ORDER BY k7 ROWS 3 PRECEDING) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k2,k4 ROWS 3 PRECEDING) FROM baseall'
    runner.check(line)

    line = 'SELECT DISTINCT k3,COUNT(1) OVER (order by k2) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT COUNT(1) OVER (order by k3), MOD(SUM(k3),2) FROM baseall GROUP BY k3'
    runner.check(line)

    line = 'SELECT DISTINCT COUNT(k2) OVER (order by k3), MOD(SUM(k3),2) FROM baseall GROUP BY k3'
    runner.checkwrong(line)

    line = 'SELECT k3, SUM(k3) OVER (order by k3), MOD(SUM(k3),2) FROM baseall GROUP BY k3'
    runner.check(line)

    line = 'SELECT k3, SUM(SUM(k3)) OVER (order by k3), SUM(k3) OVER (ORDER BY k3), ' \
           'MOD(SUM(k3),2), SUM(k3) FROM baseall GROUP BY k3'
    runner.check(line)


def test_win_12():
    """
    {
    "title": "test_win_12",
    "describe": "test win",
    "tag": "autotest"
    }
    """
    """test win"""
    line1 = 'SELECT LAST_VALUE(k3) OVER (PARTITION BY k2 ORDER BY k3 ROWS UNBOUNDED PRECEDING) w, ' \
            'FIRST_VALUE(k3) OVER (PARTITION BY k2 ORDER BY k3 ROWS UNBOUNDED PRECEDING) w1 ' \
            'FROM baseall '
    line2 = 'SELECT LAST_VALUE(k3) OVER w, FIRST_VALUE(k3) OVER w FROM baseall ' \
            'WINDOW w AS (PARTITION BY k2 ORDER BY k3 ROWS UNBOUNDED PRECEDING)'
    runner.check(line1, line2)
    
    line1 = 'SELECT k2, LAST_VALUE((CAST(k3 AS BIGINT))) OVER ' \
            '(PARTITION BY k2 ORDER BY CAST(k3 AS BIGINT) RANGE UNBOUNDED PRECEDING) w, ' \
            'FIRST_VALUE(CAST(k3 AS BIGINT)) OVER ' \
            '(PARTITION BY k2 ORDER BY CAST(k3 AS BIGINT) RANGE UNBOUNDED PRECEDING) w1 ' \
            'FROM baseall'
    line2 = 'SELECT k2, LAST_VALUE((CAST(k3 AS SIGNED))) OVER w, ' \
            'FIRST_VALUE(CAST(k3 AS SIGNED)) OVER w FROM baseall ' \
            'WINDOW w AS (PARTITION BY k2 ORDER BY CAST(k3 AS SIGNED) RANGE UNBOUNDED PRECEDING)'
    runner.check(line1, line2)
    
    line1 = 'SELECT k3,SUM(k3) OVER (PARTITION BY k2), SUM(k3) OVER () FROM baseall'
    line2 = 'SELECT k3,SUM(k3) OVER (PARTITION BY k2), SUM(k3) OVER () FROM baseall'
    runner.check(line1, line2)
    
    line = '(SELECT k2, ROW_NUMBER() OVER () FROM baseall order by k2) UNION ALL ' \
           '(SELECT k2, ROW_NUMBER() OVER () FROM baseall order by k2)'
    runner.checkok(line)

    line1 = '(SELECT k2, ROW_NUMBER() OVER (order by k2) FROM baseall order by k2) UNION ALL ' \
            '(SELECT k2, ROW_NUMBER() OVER (order by k2) FROM baseall order by k2)'
    line2 = '(SELECT k2, ROW_NUMBER() OVER (order by k2) FROM baseall order by k2) UNION ALL ' \
            '(SELECT k2, ROW_NUMBER() OVER (order by k2) FROM baseall order by k2)'
    runner.check(line1, line2)
    
    line = 'SELECT * FROM (SELECT k2, k3, ROW_NUMBER() OVER (ORDER BY k3, k2) ' \
           'FROM baseall UNION SELECT k2, k3, ROW_NUMBER() OVER (ORDER BY k3, k2) FROM baseall) alias'
    runner.check(line)
    
    line = 'SELECT  k3, RANK() OVER (ORDER BY  k3) FROM  baseall UNION ALL ' \
           'SELECT  k3, RANK() OVER (ORDER BY  k3) FROM  baseall'
    runner.check(line)

    line = 'SELECT  k3, DENSE_RANK() OVER (ORDER BY  k3) FROM  baseall UNION ALL ' \
           'SELECT  k3, DENSE_RANK() OVER (ORDER BY  k3) FROM  baseall'
    runner.check(line)

    line = 'SELECT  k3, SUM( k3) OVER (ORDER BY  k3) FROM  baseall UNION ALL ' \
           'SELECT  k3, SUM( k3) OVER (ORDER BY  k3) FROM  baseall'
    runner.check(line)

    line = 'SELECT  k3, LEAD(k3,1,3) OVER (ORDER BY  k3) FROM  baseall UNION ALL ' \
           'SELECT  k3, LEAD( k3,1,3) OVER (ORDER BY  k3) FROM  baseall'
    runner.check(line)

    line = 'SELECT  k3, FIRST_VALUE( k3) OVER (ORDER BY  k3) FROM  baseall UNION ALL ' \
           'SELECT  k3, FIRST_VALUE( k3) OVER (ORDER BY  k3) FROM  baseall'
    runner.check(line)

    line = 'SELECT  k3, LAST_VALUE( k3) OVER (ORDER BY  k3) FROM  baseall UNION ALL ' \
           'SELECT  k3, LAST_VALUE( k3) OVER (ORDER BY  k3) FROM  baseall'
    runner.check(line)

    line = 'SELECT k4, COUNT(k4) OVER (ORDER BY k4 ROWS BETWEEN 1 FOLLOWING AND 100 FOLLOWING) bb ' \
           'FROM baseall'
    runner.check(line)

    line = 'SELECT  k3, AVG( k3) OVER (PARTITION BY  k3) summ FROM  baseall'
    runner.check(line)

    line = 'SELECT     AVG( k3) OVER (PARTITION BY  k3) summ FROM  baseall'
    runner.check(line)

    line = 'SELECT  k3, AVG( k3) OVER (PARTITION BY  k3) summ, ' \
           'AVG( k3) OVER (PARTITION BY  k3) summ2 FROM  baseall'
    runner.check(line)

    line = 'SELECT     AVG( k3) OVER (PARTITION BY  k3) summ, ' \
           'AVG( k3) OVER (PARTITION BY  k3) summ2 FROM  baseall'
    runner.check(line)


def test_win_13():
    """
    {
    "title": "test_win_13",
    "describe": "test win",
    "tag": "autotest"
    }
    """
    """test win"""
    line1 = 'SELECT COUNT(k3) OVER (ORDER BY k3 ROWS 1 PRECEDING) w, k3, ' \
            'AVG(k3) OVER (ORDER BY k3 ROWS 1 PRECEDING) w1, ' \
            'SUM(k3) OVER (ORDER BY k3 ROWS 1 PRECEDING) w2, ' \
            'FIRST_VALUE(k3) OVER (ORDER BY k3 ROWS 1 PRECEDING) w3 ' \
            'FROM baseall'
    line2 = 'SELECT COUNT(k3) OVER w, k3, AVG(k3) OVER w, SUM(k3) OVER w, ' \
            'FIRST_VALUE(k3) OVER w FROM baseall WINDOW w AS (ORDER BY k3 ROWS 1 PRECEDING)'
    runner.check(line1, line2)
    
    line1 = 'SELECT COUNT(k1) OVER (ORDER BY k1 ROWS 1 PRECEDING) w, k1, ' \
            'AVG(k1) OVER (ORDER BY k1 ROWS 1 PRECEDING) w1, ' \
            'SUM(k1) OVER (ORDER BY k1 ROWS 1 PRECEDING) w2, ' \
            'FIRST_VALUE(k1) OVER (ORDER BY k1 ROWS 1 PRECEDING) w3 FROM baseall'
    line2 = 'SELECT COUNT(k1) OVER w, k1, AVG(k1) OVER w, SUM(k1) OVER w, FIRST_VALUE(k1) OVER w ' \
            'FROM baseall WINDOW w AS (ORDER BY k1 RANGE 1 PRECEDING)'
    runner.check(line1, line2)
    
    line1 = 'SELECT k1, ' \
            'count(k1) over (ORDER BY k1 ASC  ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) w, ' \
            'count(1) over (ORDER BY k1 ASC  ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) w1, ' \
            'FIRST_VALUE(k1) OVER (ORDER BY k1 ASC  ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) w2 ' \
            'FROM baseall'
    line2 = 'SELECT k1, count(k1) over w, count(*) over w, FIRST_VALUE(k1) OVER w FROM baseall ' \
            'WINDOW w AS (ORDER BY k1 ASC  RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING)'
    runner.check(line1, line2)
    
    line1 = 'SELECT k1, ' \
            'count(k1) over (ORDER BY k1 DESC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) w, ' \
            'count(1) over (ORDER BY k1 DESC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) w1, ' \
            'FIRST_VALUE(k1) OVER (ORDER BY k1 DESC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) w2 ' \
            'FROM baseall'
    line2 = 'SELECT k1, count(k1) over w, count(*) over w, FIRST_VALUE(k1) OVER w FROM baseall ' \
            'WINDOW w AS (ORDER BY k1 DESC RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING)'
    runner.check(line1, line2)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 ' \
           'ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) FROM baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 ' \
           'ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 ' \
           'ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 ' \
           'ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) FROM  baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 ' \
           'ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM  baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 ' \
           'RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM  baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 ' \
           'ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) FROM  baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 ' \
           'ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) FROM  baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 ' \
           'DESC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) FROM baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 DESC ' \
           'ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 DESC ' \
           'ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 DESC ' \
           'ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) FROM  baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 DESC ' \
           'ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM  baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 DESC ' \
           'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM  baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 DESC ' \
           'ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) FROM  baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3 DESC ' \
           'ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) FROM  baseall'
    runner.check(line)

    line = 'SELECT *, RANK() OVER (ORDER BY k1,k2,k3) AS O_IJK, ' \
           'RANK() OVER (ORDER BY k2) AS O_J, RANK() OVER (ORDER BY k3,k2) AS O_KJ ' \
           'FROM baseall ORDER BY k1, k2, k3'
    runner.check(line)

    line = 'SELECT RANK() OVER (ORDER BY AVG(k3)) FROM baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k2) FROM baseall WHERE k2 IS NULL'
    runner.check(line)


def test_win_15():
    """
    {
    "title": "test_win_15",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    # this may be a bug
    line = 'SELECT * FROM (SELECT *,DENSE_RANK() OVER (ORDER BY k2 DESC), ' \
           'DENSE_RANK() OVER (ORDER BY k2) FROM baseall) alias ORDER BY k3,k2'
    # runner.check(line)

    line = 'SELECT * FROM (SELECT *, SUM(k3) OVER (ORDER BY k3) FROM baseall ' \
           'ORDER BY k3) alias ORDER BY k3,k2'
    runner.check(line)

    line = 'SELECT AVG(k3), RANK() OVER (ORDER BY k3) FROM baseall group by k3, k1'
    runner.check(line)

    line = 'SELECT *, AVG(k3) OVER (ORDER BY k3) FROM baseall'
    runner.check(line)

    line = 'SELECT *, AVG(k3) OVER (order by k3, k1 ROWS UNBOUNDED PRECEDING) FROM baseall'
    runner.check(line)

    line = 'SELECT * FROM baseall ORDER BY RANK() OVER (ORDER BY k1 DESC, k2, k3)'
    runner.check(line)

    line = 'SELECT *, RANK() OVER (ORDER BY k1 DESC, k2, k3) AS r FROM baseall ORDER BY r'
    runner.check(line)

    line = 'SELECT * FROM baseall WHERE 1 = RANK() OVER (ORDER BY k1)'
    runner.checkwrong(line)

    line = 'SELECT * FROM baseall HAVING 1 = rank() OVER (ORDER BY k1)'
    runner.checkwrong(line)

    line = '(select k3 from baseall) union (select k3 from baseall) order by ' \
           '(row_number() over (order by k3))'
    runner.checkwrong(line)

    line = '(select k3 from baseall) union (select k3 from baseall) order by ' \
           '(1+row_number() over (order by k3))'
    runner.checkwrong(line)


def test_win_16():
    """
    {
    "title": "test_win_16",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    line = '(select k3 from baseall) union (select k3 from baseall order by ' \
           '(row_number() over (order by k3)))'
    runner.check(line)

    line = 'SELECT k1 AS foo, SUM(k1) OVER (ORDER BY k1 ROWS foo PRECEDING)  FROM  baseall'
    runner.checkwrong(line)

    line = 'SELECT k1, SUM(k1) OVER (ORDER BY k1 ROWS k1 PRECEDING)  FROM  baseall'
    runner.checkwrong(line)

    line = 'SELECT RANK() OVER (ORDER BY 1) FROM  baseall'
    runner.checkwrong(line)

    line = 'SELECT * FROM (SELECT k1, k2, k3, RANK() OVER (ORDER BY 1*1) FROM  baseall ) alias ' \
           'ORDER BY k1, k2, k3'
    runner.checkwrong(line)

    line = 'SELECT * FROM (SELECT count(1) OVER (order by k1), ' \
           'sum(k2) OVER (order by k2) AS sum1, k3 from  baseall) as alias'
    runner.check(line)

    line = 'SELECT * FROM (SELECT count(1) OVER (order by k1) + sum(k2) OVER (order by k2) sum1, ' \
           'k3 from  baseall) as alias'
    runner.check(line)

    line = 'SELECT * FROM (SELECT SUM(k1) OVER (order by k1), k3 FROM  baseall) AS alias'
    runner.check(line)

    line = 'SELECT * FROM (SELECT SUM(k2) OVER (order by k2), k4 FROM  baseall) AS alias'
    runner.check(line)
    
    line = 'SELECT  k1,  k2, k3, RANK() OVER ()  FROM baseall'
    runner.check(line)

    line = 'SELECT  k1,  k2, k3, SUM(k1) OVER ()  FROM baseall'
    runner.check(line)

    line = 'SELECT  k1,  k2, k3, COUNT(1) OVER ()  FROM baseall'
    runner.check(line)

    line = 'SELECT  k1,  k2, k3, MAX(k8) OVER ()  FROM baseall'
    runner.check(line)

    line = 'SELECT  k1,  k2, k3, MIN(k10) OVER ()  FROM baseall'
    runner.check(line)

    line = 'SELECT  k1,  k2, k3, LAG(k2, 1, 2) OVER ()  FROM baseall'
    runner.checkok(line)

    line = 'SELECT  k1,  k2, k3, LEAD(k2, 1, 2) OVER ()  FROM baseall'
    runner.checkok(line)

    line = 'SELECT  k1,  k2, k3, FIRST_VALUE(k1) OVER ()  FROM baseall'
    runner.checkok(line)

    line = 'SELECT  k1,  k2, k3, LAST_VALUE(k1) OVER ()  FROM baseall'
    runner.checkok(line)

    line = 'SELECT SUM(DISTINCT  k2) OVER () FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT SUM(k3) OVER (ORDER BY k2 ROWS 2 PRECEDING EXCLUDE CURRENT ROW) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT k2, k3, FIRST_VALUE(SUM(k2+k3) OVER()) OVER () AS sum FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT k2, k3, FIRST_VALUE(1+SUM(k2+k3) OVER()) OVER () AS sum FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT k2, k3, SUM(1+SUM(k2+k3) OVER()) OVER () AS sum FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT k2, k3, FIRST_VALUE(k2) OVER (PARTITION BY ROW_NUMBER() OVER ()) AS sum FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT k2, k3, FIRST_VALUE(k2) OVER (PARTITION BY 1+ROW_NUMBER() OVER ()) AS sum FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT k2, k3, FIRST_VALUE(k2) OVER (ORDER BY ROW_NUMBER() OVER ()) AS sum FROM baseall'
    runner.checkwrong(line)


def test_win_31():
    """
    {
    "title": "test_win_31",
    "describe": "test win",
    "tag": "autotest"
    }
    """
    """test win"""
    line = 'SELECT SUM(k2) OVER (ORDER BY k3) FROM baseall'
    runner.check(line)

    line = 'SELECT COUNT(1) OVER (ORDER BY k3) FROM baseall'
    runner.check(line)

    line = 'SELECT AVG(k3) OVER (ORDER BY k3) FROM baseall'
    runner.check(line)

    line = 'SELECT k2,k3,LAST_VALUE(k2) OVER (ORDER BY k3,k2) FROM baseall'
    runner.check(line)

    line1 = 'SELECT k2, k3, COUNT(k2) OVER (PARTITION BY k2 ORDER BY k3 ' \
            'ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) count, ' \
            'SUM(k2) OVER (PARTITION BY k2 ORDER BY k3 ' \
            'ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) sum, ' \
            'AVG(k2) over (PARTITION BY k2 ORDER BY k3 ' \
            'ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) average, ' \
            'LAST_VALUE(k2) OVER (PARTITION BY k2 ORDER BY k3 ' \
            'ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) lastval FROM baseall'
    line2 = 'SELECT k2, k3, COUNT(k2) OVER w count, SUM(k2) OVER w sum, ' \
            'AVG(k2) over w average, LAST_VALUE(k2) OVER w lastval FROM baseall ' \
            'WINDOW w as (PARTITION BY k2 ORDER BY k3 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)'
    runner.check(line1, line2)

    line1 = 'SELECT k2, k3, COUNT(k2) OVER (PARTITION BY k2 ORDER BY k3 ' \
            'ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) count, ' \
            'SUM(k2) OVER (PARTITION BY k2 ORDER BY k3 ' \
            'ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) sum, ' \
            'AVG(k2) OVER (PARTITION BY k2 ORDER BY k3 ' \
            'ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) average, ' \
            'LAST_VALUE(k2) OVER (PARTITION BY k2 ORDER BY k3 ' \
            'ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) lastval FROM baseall'
    line2 = 'SELECT k2, k3, COUNT(k2) OVER w count, SUM(k2) OVER w sum, ' \
            'AVG(k2) OVER w average, LAST_VALUE(k2) OVER w lastval FROM baseall ' \
            'WINDOW w as (PARTITION BY k2 ORDER BY k3 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)'
    runner.check(line1, line2)

    line1 = 'SELECT k2, k3, COUNT(k2) OVER (PARTITION BY k2 ORDER BY k3 ' \
            'ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) count, ' \
            'SUM(k2) OVER (PARTITION BY k2 ORDER BY k3 ' \
            'ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) sum, ' \
            'AVG(k2) OVER (PARTITION BY k2 ORDER BY k3 ' \
            'ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) average, ' \
            'LAST_VALUE(k2) OVER (PARTITION BY k2 ORDER BY k3 ' \
            'ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) lastval FROM baseall'
    line2 = 'SELECT k2, k3, COUNT(k2) OVER w count, SUM(k2) OVER w sum, ' \
            'AVG(k2) OVER w average, LAST_VALUE(k2) OVER w lastval FROM baseall ' \
            'WINDOW w as (PARTITION BY k2 ORDER BY k3 ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)'
    runner.check(line1, line2)


def test_win_17():
    """
    {
    "title": "test_win_17",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    line = 'SELECT last_value(k1) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) ' \
           'FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT SUM(k3) OVER (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) w1, ' \
           'COUNT(1) OVER (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) w FROM baseall'
    runner.checkwrong(line)

    line1 = 'SELECT k2, k3, SUM(k3) OVER (PARTITION BY k2 ORDER BY k3 ' \
            'ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) w FROM baseall '
    line2 = 'SELECT k2, k3, SUM(k3) OVER w FROM baseall WINDOW w AS ' \
            '(PARTITION BY k2 ORDER BY k3 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)'
    runner.check(line1, line2)

    line = ' SELECT SUM(1) OVER ()'
    runner.checkwrong(line)

    line = 'SELECT k2 FROM baseall WHERE k2 IN ( ' \
           'SELECT CAST(SUM(k2) OVER (ROWS CURRENT ROW) AS BIGINT) FROM baseall)'
    runner.checkwrong(line)

    line = 'SELECT FIRST_VALUE(k1) IGNORE NULLS OVER () FROM baseall'
    runner.checkwrong(line)

    line1 = 'SELECT SUM(k3) OVER (PARTITION BY k3 ORDER BY k2 ' \
            'ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) w FROM baseall'
    line2 = 'SELECT SUM(k3) OVER w FROM baseall WINDOW w AS ' \
            '(PARTITION BY k3 ORDER BY k2 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)'
    runner.check(line1, line2)

    line = 'SELECT LAST_VALUE(1) OVER (PARTITION BY t1.k3), t2.k2 ' \
           'FROM baseall t1 LEFT JOIN baseall t2 ON t1.k3 = t2.k2 WHERE t1.k1=1'
    runner.check(line)

    line = 'SELECT k2, RANK() OVER (ORDER BY k2) rank_asc, ' \
           'RANK() OVER (ORDER BY k2 desc) rank_desc, ' \
           'RANK() OVER (ORDER BY k2) + RANK() OVER (ORDER BY k2 desc) rank_asc_desc FROM baseall'
    runner.check(line)

    line = 'SELECT ROW_NUMBER () OVER (), COUNT(*) FROM baseall WHERE k1 < 5'
    runner.check(line)

    line1 = 'SELECT SUM(k2) OVER (PARTITION BY k3 ORDER BY k2) W ' \
            'FROM baseall ORDER BY SUM(k2) OVER (PARTITION BY k3 ORDER BY k2)'
    line2 = 'SELECT SUM(k2) OVER W FROM baseall ' \
            'WINDOW w AS (PARTITION BY k3 ORDER BY k2) ORDER BY SUM(k2) OVER w'
    runner.check(line1, line2)

    line1 = 'SELECT SUM(k2) OVER (PARTITION BY k3 ORDER BY k2) W ' \
            'FROM baseall ORDER BY 1+SUM(k2) OVER (PARTITION BY k3 ORDER BY k2)'
    line2 = 'SELECT SUM(k2) OVER W FROM baseall ' \
            'WINDOW w AS (PARTITION BY k3 ORDER BY k2) ORDER BY 1+SUM(k2) OVER w'
    runner.check(line1, line2)

    line1 = 'SELECT SUM(SUM(k2)) OVER (PARTITION BY k2 ORDER BY k2) W ' \
            'FROM baseall GROUP BY k2 ORDER BY SUM(SUM(k2)) OVER (PARTITION BY k2 ORDER BY k2)'
    line2 = 'SELECT SUM(SUM(k2)) OVER W FROM baseall GROUP BY k2 ' \
            'WINDOW w AS (PARTITION BY k2 ORDER BY k2) ORDER BY SUM(SUM(k2)) OVER w'
    runner.check(line1, line2)

    line1 = 'SELECT 1+SUM(SUM(k2)) OVER (PARTITION BY k2 ORDER BY k2) W ' \
            'FROM baseall GROUP BY k2 ORDER BY 1+SUM(SUM(k2)) OVER (PARTITION BY k2 ORDER BY k2)'
    line2 = 'SELECT 1+SUM(SUM(k2)) OVER W FROM baseall GROUP BY k2 ' \
            'WINDOW w AS (PARTITION BY k2 ORDER BY k2) ORDER BY 1+SUM(SUM(k2)) OVER w'
    runner.check(line1, line2)

    line1 = 'SELECT 1+SUM(k2) OVER (PARTITION BY k3 ORDER BY k2) W ' \
            'FROM baseall ORDER BY SUM(k2) OVER (PARTITION BY k3 ORDER BY k2)'
    line2 = 'SELECT 1+SUM(k2) OVER W FROM baseall ' \
            'WINDOW w AS (PARTITION BY k3 ORDER BY k2) ORDER BY SUM(k2) OVER w'
    runner.check(line1, line2)

    line1 = 'SELECT SUM(2+SUM(k2)) OVER (PARTITION BY k3 ORDER BY k3) W ' \
            'FROM baseall GROUP BY k3 ' \
            'ORDER BY SUM(2+SUM(k2)) OVER (PARTITION BY k3 ORDER BY k3) DESC'
    line2 = 'SELECT SUM(2+SUM(k2)) OVER W FROM baseall GROUP BY k3 ' \
            'WINDOW w AS (PARTITION BY k3 ORDER BY k3) ORDER BY SUM(2+SUM(k2)) OVER w DESC'
    runner.check(line1, line2)


    line = 'SELECT ROW_NUMBER() OVER () AS num FROM baseall HAVING (num = "2")'
    runner.checkwrong(line)

    line  ='SELECT ROW_NUMBER() OVER () FROM baseall HAVING ( ROW_NUMBER() OVER () = "2")'
    runner.checkwrong(line)

    line = 'SELECT k1,k2, RANK() OVER (ORDER BY k1), RANK() OVER (order BY k1) FROM baseall'
    runner.check(line)

    line = 'SELECT * FROM baseall WHERE k1 IN (SELECT ROW_NUMBER() OVER () FROM baseall)'
    runner.check(line)

    line = 'SELECT * FROM baseall WHERE k1 IN (SELECT ROW_NUMBER() OVER () + 1 FROM baseall)'
    runner.check(line)

    line = 'SELECT * from baseall WHERE EXISTS(SELECT ROW_NUMBER() OVER () FROM baseall)'
    runner.check(line)


def test_win_30():
    """
    {
    "title": "test_win_30",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    line = 'SELECT * FROM baseall upper WHERE upper.k1 IN ' \
           '(SELECT ROW_NUMBER() OVER () FROM baseall WHERE baseall.k1 > upper.k1)'
    runner.checkwrong(line)

    #todo
    line = 'SELECT * FROM baseall AS upper WHERE ' \
           '(SELECT FIRST_VALUE(upper.k1) OVER (ORDER BY upper.k1) FROM baseall LIMIT 1) = 1'
    runner.checkwrong(line)

    line = 'SELECT k1, SUBSTR(k6,1,2), SUM(k1) OVER (PARTITION BY SUBSTR(k6,1,2)) `sum` ' \
           'FROM baseall'
    runner.check(line)

    line = 'SELECT k3 AS Having_same_sum_of_i, SUM(k2), ' \
           'SUM(SUM(k2)) OVER (PARTITION BY SUM(k2)) AS sum_sum FROM baseall GROUP BY k3'
    runner.check(line)

    line = 'SELECT AVG(SUM(k1) OVER ()) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT SUM(k1) OVER () AS k2, SUM(k2)  FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT SUM(k1) OVER () AS c FROM baseall ORDER BY c'
    runner.check(line)

    line = 'SELECT RANK() OVER (PARTITION BY k3 ORDER BY k3) FROM baseall GROUP BY k1,k2'
    runner.checkwrong(line)

    line = 'SELECT RANK() OVER (PARTITION BY k3 ORDER BY k3) FROM baseall'
    runner.check(line)

    line = 'SELECT RANK() OVER (PARTITION BY k1 ORDER BY k2) FROM baseall GROUP BY k1,k2'
    runner.check(line)

    line = 'SELECT RANK() OVER (PARTITION BY k1 ORDER BY k2) FROM baseall'
    runner.check(line)

    line = 'SELECT RANK() OVER (PARTITION BY (k1+k2) ORDER BY (k2+k1)) FROM baseall GROUP BY k1,k2'
    runner.check(line)

    line = 'SELECT AVG(k1), RANK() OVER (ORDER BY k1) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT AVG(k1), SUM(AVG(k1)) OVER (PARTITION BY k1) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT AVG(k1), SUM(k1) OVER () FROM baseall'
    runner.checkwrong(line)

    line1 = 'SELECT k2, SUM(k2), 1+SUM(k2), SUM(SUM(k2)) OVER (PARTITION BY k2) w, ' \
            '1+SUM(SUM(k2)) OVER (PARTITION BY k2) w1 FROM baseall GROUP BY k2'
    line2 = 'SELECT k2, SUM(k2), 1+SUM(k2), SUM(SUM(k2)) OVER w, 1+SUM(SUM(k2)) OVER w ' \
            'FROM baseall GROUP BY k2 WINDOW w AS (PARTITION BY k2)'
    runner.check(line1, line2)

    line = 'SELECT SUM(k4/k3), LAST_VALUE(SUM(k4/k3)) OVER (ORDER BY k1)  ' \
           'FROM baseall GROUP BY k1,k2'
    runner.check(line)

    line = 'SELECT LAST_VALUE(SUM(k4/k3)) OVER (ORDER BY k1)  FROM baseall GROUP BY k1,k2'
    runner.check(line)

    line = 'SELECT 1+FIRST_VALUE(SUM(k4/k3)) OVER (ORDER BY k1)  FROM baseall GROUP BY k1,k2'
    runner.check(line)

    line = 'SELECT ROW_NUMBER() OVER () rn, 1+FIRST_VALUE(SUM(k4/k3)) OVER (ORDER BY k1) plus_fv, ' \
           '1+LAST_VALUE(SUM(k4/k3)) OVER (ORDER BY k1) plus_lv FROM baseall GROUP BY k1,k2'
    runner.check(line)
    

def test_win_18():
    """
    {
    "title": "test_win_18",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    line = 'SELECT k1, SUM(k2) FROM baseall GROUP BY k1'
    runner.check(line)

    line = 'SELECT k1, SUM(k2) FROM baseall GROUP BY k1 HAVING k1=1'
    runner.check(line)

    line = 'SELECT k1, SUM(SUM(k2)) OVER () FROM baseall GROUP BY k1 HAVING k1=1'
    runner.check(line)

    line  = 'SELECT COUNT(*), ROW_NUMBER() OVER (ORDER BY k1) AS rownum FROM baseall ' \
            'ORDER BY rownum'
    runner.checkwrong(line)

    line = 'SELECT ROW_NUMBER() OVER () FROM baseall AS alias1, baseall AS alias2 ' \
           'WHERE alias1.k1 = 1 ORDER BY alias2.k1'
    runner.check(line)

    line = 'SELECT FIRST_VALUE(alias1.k10) OVER (order by alias1.k10) FV ' \
           'FROM baseall AS alias1, baseall AS alias2'
    runner.check(line)

    line = 'SELECT t1.k1, FIRST_VALUE(t2.k1) OVER () ' \
           'FROM baseall t1 LEFT JOIN baseall t2 ON t1.k1 = t2.k2 order by 1'
    runner.check(line)

    line = 'SELECT SUM(MAX(k3)) OVER (ORDER BY MAX(k3)) FROM baseall'
    runner.check(line)

    line = 'SELECT SUM(MAX(k3)) OVER (ORDER BY MAX(k3)) AS ss FROM baseall'
    runner.check(line)

    line = 'SELECT RANK() OVER () FROM baseall'
    runner.check(line)

    line = 'SELECT DENSE_RANK() OVER () FROM baseall'
    runner.check(line)

    line = 'SELECT RANK() OVER () FROM (SELECT 1) baseall'
    runner.check(line)

    line = 'SELECT DENSE_RANK() OVER () FROM (SELECT 1) baseall'
    runner.check(line)

    line = 'SELECT ROW_NUMBER() OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) ' \
           'FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT ROW_NUMBER() OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) ' \
           'FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT ROW_NUMBER() OVER ' \
           '(ORDER BY a RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT ROW_NUMBER() OVER ' \
           '(ROWS BETWEEN INTERVAL 2 DAY PRECEDING AND UNBOUNDED FOLLOWING) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT ROW_NUMBER() OVER ' \
           '(ROWS BETWEEN UNBOUNDED PRECEDING AND INTERVAL 2 DAY FOLLOWING) FROM baseall'
    runner.checkwrong(line)


def test_win_20():
    """
    {
    "title": "test_win_20",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    line = 'SELECT COUNT(*) AS count, ROW_NUMBER() OVER (ORDER BY k2) AS rn ' \
           'FROM baseall ORDER BY k2'
    runner.checkwrong(line)

    line = 'SELECT 1 UNION ' \
           '(SELECT ROW_NUMBER() OVER (ORDER BY k2) AS rn FROM baseall ORDER BY k2)'
    runner.check(line)

    line = 'SELECT MAX(row_number() OVER ()) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT MIN(rank() OVER (ORDER BY k1)) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT BIT_AND(rank() OVER (ORDER BY k1)) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT MIN(k1),SUM(rank() OVER (ORDER BY k1)) FROM baseall GROUP BY k1'
    runner.checkwrong(line)

    line = 'SELECT  ROW_NUMBER() OVER ( ORDER BY k1 ) AS f1, ' \
           'RANK() OVER ( ORDER BY k2 ) AS f2, ' \
           'LEAD(k3, 1, 3) OVER ( ORDER BY k1) AS f7 FROM baseall'
    runner.check(line)

    line1 = 'SELECT RANK() OVER (ORDER BY AVG(k1)) AS rnk FROM baseall ORDER BY k2'
    runner.checkwrong(line1)

    line = 'SELECT ROW_NUMBER() OVER ( ORDER BY t1.k1 ) AS rn ' \
           'FROM baseall t1, bigtable t2 WHERE t1.k1 = 1 GROUP BY t1.k2'
    runner.checkwrong(line)

    line = 'SELECT ROW_NUMBER() OVER ( ORDER BY t1.k1 ) AS rn ' \
           'FROM baseall t1, bigtable t2 WHERE t1.k1 > 5 GROUP BY t1.k2'
    runner.checkwrong(line)

    line = 'SELECT k1 AS field1, ROW_NUMBER() OVER (order by k1) AS field2 FROM baseall HAVING field1 >= 2'
    runner.check(line)

    line = 'SELECT k1 AS field1, ROW_NUMBER() OVER (order by k1) AS field2 FROM baseall ' \
           'HAVING field1 >= 2 ORDER BY field1'
    runner.check(line)

    line = 'SELECT ROW_NUMBER() OVER (ORDER BY a.k1) ' \
           'FROM baseall a LEFT JOIN bigtable b ON a.k2 = b.k2 ' \
           'WHERE (b.k3 IS NULL AND a.k1 IN (6))'
    runner.check(line)

    line = 'SELECT MAX( table2.`k7` ) AS max1 , ' \
           'MIN( table1.`k10` ) AS min1 , AVG( table2.`k3` ) AS avg1 , ' \
           'MAX( table1.`k6` ) AS max2 , table2.`k7`, ' \
           'FIRST_VALUE( table1. `k7` ) OVER (ORDER BY MAX( table2.`k1` ), ' \
           'MIN( table1.`k10` ), AVG( table2.`k3` ), ' \
           'MAX( table1.`k6` ),table2.`k7` ) AS 1st_val ' \
           'FROM  baseall AS table1 LEFT JOIN bigtable AS table2 ' \
           'ON table1.`k3` = table2.`k3` GROUP BY  table2.`k7`,  table1.`k7`'
    runner.check(line)

    # this may be a bug
    line = 'SELECT LAG(k7, 1, 3) OVER () AS wf_lag, ' \
           'ROW_NUMBER() OVER () AS wf_rn FROM baseall WHERE k7 LIKE ("%1%") AND k1=2'
    # runner.check(line)


def test_win_22():
    """
    {
    "title": "test_win_22",
    "describe": "test win",
    "tag": "autotest,fuzz"
    }
    """
    """test win"""
    line = 'SELECT LEAD(t2.k1, 2, 5) OVER ( ORDER BY t1.k1, t2.k3 DESC ) as lead1, ' \
           't1.k4, LEAD(t1.k2, 4, 5) OVER ( PARTITION BY t1.k3 ORDER BY t1.k1, t2.k3) as lead2 ' \
           'FROM  baseall t1 RIGHT JOIN bigtable t2 ON t1.k4=t2.k4 WHERE  t2.k4 IS NOT NULL'
    runner.check(line)

    line = 'SELECT  RANK() OVER ( ORDER BY baseall.k1 ) + 1 AS rank_expr FROM bigtable, baseall'
    runner.check(line)

    line = 'SELECT k1, k2, LEAD (k2, 1, 4) OVER (ORDER BY k1,k2 ASC) AS k3 ' \
           'FROM baseall k1 ORDER BY k1, k2, k3'
    runner.check(line)

    line = 'SELECT k1, k2, LEAD (k2, 1, 3) OVER (PARTITION BY NULL ORDER BY k1,k2 ASC) AS k3 ' \
           'FROM baseall k1 ORDER BY k1, k2, k3'
    runner.checkwrong(line)

    #this is a bug
    line = 'SELECT  MAX( k6  )  AS field1  FROM baseall  AS alias1 HAVING field1 <>  5'
    # runner.check(line)

    # bug same with up
    line = 'drop view if exists v1'
    runner.init(line)
    line = 'CREATE VIEW v1 AS SELECT MAX(k7) AS field1 FROM baseall AS alias1 HAVING field1 <> 5'
    runner.init(line)
    line = 'SELECT * FROM v1'
    # runner.check(line)
    line = 'drop view v1'
    runner.init(line)

    # if this need order by??
    line = 'SELECT k1, k2, k3, SUM(k1) ' \
           'OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM baseall'
    # runner.check(line)

    line = 'SELECT k1, k2, k3, SUM(k1) ' \
           'OVER (ORDER BY k2 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM baseall'
    runner.check(line)

    line = 'SELECT k1, k2, k3, SUM(k1) OVER (ORDER BY k2 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ' \
           'FROM baseall'
    runner.check(line)

    line = 'SELECT k1, k2, k3, k6, SUM(k1) OVER (ORDER BY k6 ' \
           'RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM baseall'
    runner.check(line)

    line = 'SELECT k1, k2, k3, SUM(k1) OVER (RANGE UNBOUNDED PRECEDING) FROM baseall'
    # runner.check(line)

    line = 'SELECT k1, k2, k3, SUM(k1) OVER ' \
           '(RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT k1, k2, k3, SUM(k1) OVER ' \
           '(ORDER BY k2 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM baseall'
    runner.checkok(line)

    line = 'SELECT k1, k2, k3, SUM(k1) OVER ' \
           '(ORDER BY k1 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM baseall'
    runner.check(line)

    line = 'SELECT k1, k2, k3, SUM(k1) OVER ' \
           '(ORDER BY k3 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM baseall'
    runner.checkok(line)

    line = 'SELECT k1, k2, k3, SUM(k1) OVER ' \
           '(ORDER BY k3 ROWS BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT ROW_NUMBER() OVER (ORDER BY t2.k3) AS rn, ' \
           'COUNT(DISTINCT t1.k3) AS cnt, SUM(DISTINCT t1.k3) AS `sum` ' \
           'FROM baseall t1 LEFT JOIN bigtable t2 ON t1.k1 = t2.k1 ' \
           'WHERE t1.k1 IN (1) GROUP BY t1.k1'
    runner.checkwrong(line)

    line = 'SELECT ROW_NUMBER() OVER () AS rn, COUNT(DISTINCT t1.k3) AS cnt, ' \
           'SUM(DISTINCT t1.k3) AS `sum` ' \
           'FROM baseall t1 LEFT JOIN bigtable t2 ON t1.k1 = t2.k1 ' \
           'WHERE t1.k1 IN (1) GROUP BY t1.k1'
    runner.check(line)

    line = 'SELECT ROW_NUMBER() OVER (), ' \
           'FIRST_VALUE(SUM(DISTINCT t1.k3)) OVER (ORDER BY t1.k1), ' \
           'FIRST_VALUE(SUM(DISTINCT t1.k3) + 1) OVER (ORDER BY t1.k1), ' \
           'SUM(DISTINCT t1.k3), RANK() OVER (ORDER BY t1.k1) ' \
           'FROM baseall t1 LEFT JOIN bigtable t2 ON t1.k1 = t2.k1 ' \
           'WHERE t1.k1 IN (1) GROUP BY t1.k1'
    runner.check(line)

    line = 'SELECT RANK() OVER ( ORDER BY MIN( t2.k1 ) + MAX( t1.k2 )  ) AS rnk ' \
           'FROM baseall t1 RIGHT OUTER JOIN bigtable t2 ON t1.k1 = t2.k1 ORDER BY rnk'
    runner.check(line)


def test_win_23():
    """
    {
    "title": "test_win_23",
    "describe": "test win",
    "tag": "autotest"
    }
    """
    """test win"""
    line = 'SELECT RANK() OVER (PARTITION BY alias1.`k3`, alias2.`k3`, alias1.`k1`, alias1.`k6` ' \
           'ORDER BY alias1.`k7`, alias1.`k6`, alias2.`k7`, alias1.`k1`) pr ' \
           'FROM  baseall AS alias1 RIGHT OUTER JOIN bigtable AS alias2 ON  alias1.`k1` =  alias2.`k1` LEFT JOIN test AS alias3 ' \
           'ON  alias2.`k3` =  alias3.`k1` ' \
           'WHERE  alias2.`k1` > 5 AND alias2.`k1` <= ( 5 + 5 ) AND alias2.`k1` > 5 AND ' \
           'alias2.`k1` <= ( 5 + 2 ) AND alias3.`k1`  IN (5, 0) AND alias3.`k1` >= 5 AND ' \
           'alias3.`k1` < ( 8 + 4 ) AND alias1.`k3` >= 5 AND alias1.`k3` <= ( 4 + 0 ) OR  ' \
           'alias2.`k3` != 5 OR  alias3.`k3` <> 7 OR  alias3.`k3` >= 5 ' \
           'AND alias3.`k3` < ( 5 + 2 )'
    runner.check(line)

    line = 'SELECT SUM(k1) OVER (ORDER BY SUM(k1)) FROM baseall GROUP BY k1'
    runner.check(line)

    line = 'SELECT SUM(k1) OVER (ORDER BY 1+SUM(k1)) FROM baseall GROUP BY k1'
    runner.check(line)

    line = 'SELECT SUM(k1) OVER (PARTITION BY 1+SUM(k1)) FROM baseall GROUP BY k1'
    runner.check(line)

    line = 'SELECT SUM(k1) OVER (ORDER BY AVG(k1)) FROM baseall GROUP BY k1,k3'
    runner.check(line)

    line = 'SELECT SUM(k1) OVER (ORDER BY SUM(k3)) FROM baseall GROUP BY k1,k3'
    runner.check(line)

    line = 'SELECT 0 & (SUM(1) OVER ()) FROM (select 1) as dt'
    runner.check(line)

    line = 'SELECT 1 & (SUM(1) OVER ()) FROM (select 1) as dt'
    runner.check(line)
    
    line = 'SELECT RANK() OVER ( ORDER BY k1 ) AS rnk FROM bigtable GROUP BY k1'
    runner.check(line)

    # bug
    line = 'SELECT * FROM baseall WHERE k1 IN (SELECT RANK() OVER ( ORDER BY k1 ) AS rnk ' \
           'FROM bigtable  GROUP BY k1)'
    # runner.check(line)


def test_win_24():
    """
    {
    "title": "test_win_24",
    "describe": "test win",
    "tag": "autotest, fuzz"
    }
    """
    """test win"""
    # bug
    line = 'SELECT * FROM baseall WHERE k1 NOT IN (SELECT RANK() OVER ( ORDER BY k1 ) AS rnk ' \
           'FROM bigtable  GROUP BY k1)'
    # runner.check(line)

    line = 'SELECT MIN(table2.k1) + table2.k3 AS part_expr, ' \
           'DENSE_RANK() OVER (PARTITION BY MIN(table2.k1) + table2.k3 ORDER BY table1.k3) field1 ' \
           'FROM  baseall AS table1 LEFT JOIN bigtable AS table2 ON table1.k1 = table2.k1 ' \
           'GROUP BY table2.k3, table1.k3'
    runner.check(line)

    line = 'drop view if exists view_E'
    runner.init(line)
    line = 'CREATE VIEW view_E AS SELECT * FROM baseall;'
    runner.init(line)
    line = 'SELECT MAX( alias1.k3 )  AS field1 ' \
           'FROM view_E AS alias1 LEFT JOIN bigtable AS alias2 ON alias1.k3  = alias2.k3 ' \
           'WHERE alias1.k1 IN (  5  )  HAVING field1  <=  6'
    runner.check(line)
    #line = 'drop view if exists view_E'
    #runner.init(line)

    line = 'SELECT * FROM (SELECT MAX( alias1.k3) AS field1 ' \
           'FROM view_E AS alias1 LEFT JOIN test AS alias2 ON alias1.k3 = alias2.k3 ' \
           'WHERE alias1.k1 IN (5)  HAVING field1  <=  6) s1'
    runner.check(line)

    line = 'SELECT * FROM (SELECT MAX( alias1.k3) AS field1 ' \
           'FROM view_E AS alias1 LEFT JOIN test AS alias2 ON alias1.k3  = alias2.k3 ' \
           'WHERE alias1.k1 IN (5)  HAVING MAX(alias1.k3) <=  6) s1'
    runner.check(line)

    line = 'drop view if exists view_E'
    runner.init(line)

    line = 'SELECT k10,k5,LAST_VALUE(k5) OVER ' \
           '(ORDER BY k10 ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) FROM baseall;'
    runner.check(line)

    line = 'SELECT k5, SUM(k5) OVER (ROWS 3.14 PRECEDING) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT k8, SUM(k8) OVER (ROWS BETWEEN 3 PRECEDING AND 3.4 FOLLOWING) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT k9, SUM(k9) OVER (ROWS BETWEEN CURRENT ROW AND 3.4 FOLLOWING) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT LEAD(1,1,1) OVER(PARTITION BY k6) FROM baseall'
    runner.check(line)

    line = 'SELECT * FROM (SELECT LEAD(k1, 1, 2) OVER (ORDER BY k1 ROWS CURRENT ROW) AS a, ' \
           'k1 AS b FROM baseall WHERE k1 = k2 - 1988) z'
    runner.checkwrong(line)

    line = 'SELECT FIRST_VALUE( alias1.k4 ) OVER ' \
           '(ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS field1 ' \
           'FROM baseall AS alias1 RIGHT JOIN bigtable AS alias2 ON  alias1.k4 = alias2.k4'
    runner.checkwrong(line)
    
    line = 'SELECT AVG(k1), RANK() OVER (ORDER BY k1) FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT AVG(k1) FROM baseall ORDER BY RANK() OVER (PARTITION BY AVG(k1) ORDER BY k1)'
    runner.checkwrong(line)

    line = 'SELECT AVG(k1), RANK() OVER (ORDER BY k1) w FROM baseall'
    runner.checkwrong(line)

    line = 'SELECT k1, k3, LAST_VALUE(k1) OVER (ORDER BY k3,k1) AS `last` FROM baseall'
    runner.check(line)

    line = 'SELECT k1, k3, LAST_VALUE(k1) OVER ' \
           '(ORDER BY k3,k1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `last` ' \
           'FROM baseall'
    runner.check(line)


def test_win_25():
    """
    {
    "title": "test_win_25",
    "describe": "test win",
    "tag": "autotest"
    }
    """
    """test win"""
    line1 = 'SELECT k1, k3, COUNT(k1) OVER (ORDER BY k3,k1 DESC) AS cnt, ' \
            'COUNT(1) OVER (ORDER BY k3,k1 DESC) AS `cnt(*)`, ' \
            'FIRST_VALUE(k1) OVER (ORDER BY k3,k1 DESC) AS first, ' \
            'LAST_VALUE (k1) OVER (ORDER BY k3,k1 DESC) AS last FROM baseall'
    line2 = 'SELECT k1, k3, COUNT(k1) OVER w AS cnt, COUNT(1) OVER w AS `cnt(*)`, ' \
            'FIRST_VALUE(k1) OVER w AS first, LAST_VALUE (k1) OVER w AS last ' \
            'FROM baseall WINDOW w AS (ORDER BY k3,k1 DESC)'
    runner.check(line1, line2)

    line1 = 'SELECT k1, k3, COUNT(k1) OVER (ORDER BY k3,k1 DESC ' \
            'RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cnt, ' \
            'COUNT(1) OVER (ORDER BY k3,k1 DESC ' \
            'RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `cnt(*)`, ' \
            'FIRST_VALUE(k1) OVER (ORDER BY k3,k1 DESC ' \
            'RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first, ' \
            'LAST_VALUE (k1) OVER (ORDER BY k3,k1 DESC ' \
            'RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last FROM baseall'
    line2 = 'SELECT k1, k3, COUNT(k1) OVER w AS cnt, COUNT(*) OVER w AS `cnt(*)`, ' \
            'FIRST_VALUE(k1) OVER w AS first, LAST_VALUE (k1) OVER w AS last ' \
            'FROM baseall WINDOW w AS (ORDER BY k3,k1 DESC ' \
            'RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)'
    runner.check(line1, line2)

    line1 = 'SELECT k1, k3, COUNT(k1) OVER (ORDER BY k3,k1 DESC ' \
            'RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS cnt, ' \
            'COUNT(1) OVER (ORDER BY k3,k1 DESC ' \
            'RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS `cnt(*)`, ' \
            'FIRST_VALUE(k1) OVER (ORDER BY k3,k1 DESC ' \
            'RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS first, ' \
            'LAST_VALUE (k1) OVER (ORDER BY k3,k1 DESC ' \
            'RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS last FROM baseall'
    line2 = 'SELECT k1, k3, COUNT(k1) OVER w AS cnt, COUNT(*) OVER w AS `cnt(*)`, ' \
            'FIRST_VALUE(k1) OVER w AS first, LAST_VALUE (k1) OVER w AS last ' \
            'FROM baseall WINDOW w AS (ORDER BY k3,k1 DESC ' \
            'RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)'
    runner.check(line1, line2)


if __name__ == '__main__':
    setup_module()
    #test_win_1()
    #test_win_2()
    #test_win_3()
    #test_win_4()
    #test_win_5()
    #test_win_6()
    #test_win_7()
    #test_win_10()
    #test_win_33()
    #test_win_11()
    #test_win_12()
    #test_win_13()
    #test_win_15()
    #test_win_16()
    #test_win_17()
    #test_win_18()
    #test_win_30()
    #test_win_31()
    #test_win_20()
    #test_win_22()
    #test_win_23()
    #test_win_24()
    #test_win_25()
    #test_first_value_type()









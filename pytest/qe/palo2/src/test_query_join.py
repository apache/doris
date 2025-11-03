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
                       on等值连接|on不等值连接|on仅含过滤|on等值连接和过滤|on等值连接和不等值连接|无on条件|on中含有or条件的
join
inner join
left (outer) join
rigth (outer) join
full (outer) join
cross join
merge join
left(right) semi join
left (right)anti join
a, b

"""
import os
import sys
import time
from operator import eq
sys.path.append("../lib/")
from palo_qe_client import QueryBase
import query_util as util

sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage

table_name = "test"
join_name = "baseall"
tmp_name = "bigtable"

if 'FE_DB' in os.environ.keys():
    db = os.environ["FE_DB"]
else:
    db = "test_query_qa"


def setup_module():
    """
    init config
    """
    global runner
    runner = QueryExecuter()


class QueryExecuter(QueryBase):
    def __init__(self):
        self.get_clients()

    def check2(self, line1, line2):
        """
        check for two query
        """
        print(line1)
        print(line2)
        times = 0
        flag = 0
        ret = True
        try:
            LOG.info(L('mysql sql', mysql_sql=line2))
            self.mysql_cursor.execute(line2)
            mysql_result = self.mysql_cursor.fetchall()
        except Exception as e:
            print(Exception, ":", e)
            LOG.error(L('err', error=e))
            assert 0 == 1, 'mysql execute error'

        while (times <= 10 and flag == 0):
            try:
                LOG.info(L('palo sql', palo_sql=line1))
                palo_result = self.query_palo.do_sql(line1)
                util.check_same(palo_result, mysql_result)
                if times == 5:
                    flag = 1
            except Exception as e:
                print(Exception, ":", e)
                LOG.error(L('err', error=e))
                time.sleep(1)
                ret = False
            finally:
                times += 1
        assert ret, 'check error'

    def check(self, line):
        """
        check if result of mysql and palo is same
        """
        print(line)
        times = 0
        flag = 0
        ret = True
        try:
            LOG.info(L('mysql sql', mysql_sql=line))
            self.mysql_cursor.execute(line)
            mysql_result = self.mysql_cursor.fetchall()
        except Exception as e:
            print(Exception, ":", e)
            LOG.error(L('mysql execute err', error=e))
            assert 0 == 1, 'mysql execute error'
        while (times <= 10 and flag == 0):
            try:
                LOG.info(L('palo sql', palo_sql=line))
                palo_result = self.query_palo.do_sql(line)
                util.check_same(palo_result, mysql_result)
                if (times == 3):
                    flag = 1
            except Exception as e:
                print(Exception, ":", e)
                LOG.error(L('palo execute err', error=e))
                time.sleep(1)
                ret = False
            finally:
                times += 1
        assert ret, 'check_error'

    def init(self, sql):
        """init palo and mysql view"""
        print(sql)
        try:
            LOG.info(L('palo sql', palo_sql=sql))
            palo_result = self.query_palo.do_sql(sql)
            LOG.info(L('mysql sql', mysql_sql=sql))
            self.mysql_cursor.execute(sql)
            if sql.lower().startswith("insert"):
                self.mysql_con.commit()

            mysql_result = self.mysql_cursor.fetchall()
            return True
        except Exception as e:
            print(Exception, ":", e)
            LOG.error(L('err', error=e))
            return False

    def create_insert_null_to_table(self, table_name):
        """create table, and insert_null_to_table"""
        sql = 'drop table if exists {table_name}'
        super(QueryExecuter, runner).init(sql.format(table_name=table_name))
        sql = 'create table {table_name}(k1 tinyint, k2 decimal(9,3) NULL, k3 char(5) NULL,\
                    k4 date NULL, k5 datetime NULL, \
                    k6 double sum) engine=olap \
                    distributed by hash(k1) buckets 2 properties("storage_type"="column")'
        msql = 'create table {table_name}(k1 tinyint, k2 decimal(9,3), k3 char(5), k4 date,\
                     k5 datetime, k6 double)'
        super(QueryExecuter, runner).init(sql.format(table_name=table_name), msql.format(table_name=table_name))
        # insert data
        line = "insert into {table_name} values (1, NULL,'null', NULL, NULL, 8.9)"
        runner.init(line.format(table_name=table_name))
        line = "insert into {table_name} values (2, NULL,'2', NULL, NULL, 8.9)"
        runner.init(line.format(table_name=table_name))
        line = "insert into {table_name} values (3, NULL,'null', '2019-09-09', NULL, 8.9)"
        runner.init(line.format(table_name=table_name))

    def execute_sql(self, sql):
        """execute sql and return result"""
        print(sql)
        try:
            LOG.info(L('palo sql', palo_sql=sql))
            palo_result = self.query_palo.do_sql(sql)
            return  palo_result
        except Exception as e:
            print(e)
            LOG.error(L('err', error=e))
            return False


def test_join():
    """
    {
    "title": "test_query_join.test_join",
    "describe": "join",
    "tag": "function,p1"
    }
    """
    """join"""
    selected = list()
    selected.append('a.k1, b.k1, a.k2, b.k2, a.k3, b.k3')
    selected.append('count(a.k1), count(b.k1), count(a.k2), count(b.k2), count(*)')

    for i in selected:
        line = 'select %s from {left_table} a join {right_table} b \
                on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a join {right_table} b \
                on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a join {right_table} b \
                on a.k1 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a join {right_table} b \
                on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a join {right_table} b \
                on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a join {right_table} b \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a join {right_table} b \
                on a.k1 = b.k1 or a.k2 = b.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a join {right_table} b \
                on a.k1 < b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a join {right_table} b \
                on a.k1 = b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a join {right_table} b \
                on a.k1 = b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a join {right_table} b \
                on a.k1 < b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a join {right_table} b on a.k1 = b.k1 \
                join {third_table} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))
        line = 'select %s from {left_table} a join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
                join {third_table} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))


def test_inner_join():
    """
    {
    "title": "test_query_join.test_inner_join",
    "describe": "inner join",
    "tag": "function,p1"
    }
    """
    """inner join"""
    selected = list()
    selected.append('a.k1, b.k1, a.k2, b.k2, a.k3, b.k3')
    selected.append('count(a.k1), count(b.k1), count(a.k2), count(b.k2), count(*)')

    for i in selected:
        line = 'select %s from {left_table} a inner join {right_table} b \
                on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a inner join {right_table} b \
                on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a inner join {right_table} b \
                on a.k1 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a inner join {right_table} b \
                on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a inner join {right_table} b \
                on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a inner join {right_table} b \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a inner join {right_table} b \
                on a.k1 = b.k1 or a.k2 = b.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a inner join {right_table} b \
                on a.k1 < b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a inner join {right_table} b \
                on a.k1 = b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a inner join {right_table} b \
                on a.k1 = b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a inner join {right_table} b \
                on a.k1 < b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        line = 'select %s from {left_table} a inner join {right_table} b on a.k1 = b.k1 \
                inner join {third_table} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))
        line = 'select %s from {left_table} a inner join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
                inner join {third_table} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))


def test_left_join():
    """
    {
    "title": "test_query_join.test_left_join",
    "describe": "left outer join",
    "tag": "function,p1,fuzz"
    }
    """
    """left outer join"""
    selected = list()
    selected.append('a.k1, b.k1, a.k2, b.k2, a.k3, b.k3')
    selected.append('count(a.k1), count(b.k1), count(a.k2),  count(b.k2), count(*)')

    join_type = list()
    join_type.append('')
    join_type.append(' outer ')

    i = 'a.k1, b.k1, a.k2, b.k2, a.k3, b.k3'
    print('4')
    line = 'select %s from {left_table} a left join {right_table} b \
            on a.k1 = b.k1 and a.k2 > 0 order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535' % i
    runner.check(line.format(left_table=table_name, right_table=join_name))
    return
    print('5')
    line = 'select %s from {left_table} a left join {right_table} b \
            on a.k1 = b.k1 and a.k2 > b.k2 order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535' % i
    runner.check(line.format(left_table=table_name, right_table=join_name))
    print('13')
    line = 'select %s from {left_table} a left join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
            left join {third_table} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 \
            order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535' % i
    runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))

    for i in selected:
        print('1')
        line = 'select %s from {left_table} a left join {right_table} b \
                    on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('2')
        line = 'select %s from {left_table} a left join {right_table} b \
                on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('3')
        line = 'select %s from {left_table} a left join {right_table} b \
                on a.k1 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('6')
        line = 'select %s from {left_table} a left join {right_table} b \
               order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('7')
        line = 'select %s from {left_table} a left join {right_table} b \
               on a.k1 = b.k1 or a.k2 = b.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('8')
        line = 'select %s from {left_table} a left join {right_table} b \
               on a.k1 < b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('9')
        line = 'select %s from {left_table} a left join {right_table} b \
               on a.k1 = b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('10')
        line = 'select %s from {left_table} a left join {right_table} b \
               on a.k1 = b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('11')
        line = 'select %s from {left_table} a left join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('12')
        line = 'select %s from {left_table} a left join {right_table} b on a.k1 = b.k1 \
                left join {third_table} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))
        print('***************************************************************************')


def test_left_outer_join():
    """
    {
    "title": "test_query_join.test_left_outer_join",
    "describe": "left outer join",
    "tag": "function,p1,fuzz"
    }
    """
    """left outer join"""
    selected = list()
    selected.append('a.k1, b.k1, a.k2, b.k2, a.k3, b.k3')
    selected.append('count(a.k1), count(b.k1), count(a.k2),  count(b.k2), count(*)')
    i = 'a.k1, b.k1, a.k2, b.k2, a.k3, b.k3'
    print('4')
    line = 'select %s from {left_table} a left join {right_table} b \
            on a.k1 = b.k1 and a.k2 > 0 order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535' % i
    runner.check(line.format(left_table=table_name, right_table=join_name))
    print('5')
    line = 'select %s from {left_table} a left join {right_table} b \
            on a.k1 = b.k1 and a.k2 > b.k2 order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535' % i
    runner.check(line.format(left_table=table_name, right_table=join_name))
    print('13')
    line = 'select %s from {left_table} a left join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
            left join {third_table} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 \
            order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535' % i
    runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))

    for i in selected:
        print('1')
        line = 'select %s from {left_table} a left outer join {right_table} b \
                on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('2')
        line = 'select %s from {left_table} a left outer join {right_table} b \
                on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('3')
        line = 'select %s from {left_table} a left outer join {right_table} b \
                on a.k1 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('6')
        line = 'select %s from {left_table} a left outer join {right_table} b \
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('7')
        line = 'select %s from {left_table} a left outer join {right_table} b \
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('8')
        line = 'select %s from {left_table} a left outer join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('9')
        line = 'select %s from {left_table} a left outer join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('10')
        line = 'select %s from {left_table} a left outer join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('11')
        line = 'select %s from {left_table} a left outer join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('12')
        line = 'select %s from {left_table} a left outer join {right_table} b on a.k1 = b.k1 \
                left outer join {third_table} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535' \
                % i
        runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))


def test_right_join():
    """
    {
    "title": "test_query_join.test_right_join",
    "describe": "right outer join",
    "tag": "function,p1,fuzz"
    }
    """
    """right outer join"""
    selected = list()
    selected.append('a.k1, b.k1, a.k2, b.k2, a.k3, b.k3')
    selected.append('count(a.k1), count(b.k1), count(a.k2),  count(b.k2), count(*)')
    i = 'a.k1, b.k1, a.k2, b.k2, a.k3, b.k3'
    print('5')
    line = 'select %s from {left_table} a right join {right_table} b \
               on a.k1 = b.k1 and a.k2 > b.k2 order by isnull(a.k1), 1, 2, 3, 4, 5 limit 65535' % i
    runner.check(line.format(left_table=table_name, right_table=join_name))
    for i in selected:
        print('1')
        line = 'select %s from {left_table} a right join {right_table} b \
               on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('2')
        line = 'select %s from {left_table} a right join {right_table} b \
               on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('3')
        line = 'select %s from {left_table} a right join {right_table} b \
                    on a.k1 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('4')
        line = 'select %s from {left_table} a right join {right_table} b \
                on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('6')
        line = 'select %s from {left_table} a right join {right_table} b \
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('7')
        line = 'select %s from {left_table} a right join {right_table} b \
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('8')
        line = 'select %s from {left_table} a right join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('9')
        line = 'select %s from {left_table} a right join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('10')
        line = 'select %s from {left_table} a right join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('11')
        line = 'select %s from {left_table} a right join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('12')
        line = 'select %s from {left_table} a right join {right_table} b on a.k1 = b.k1 \
                right join {third_table} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))
    print('13')
    i = 'a.k1, b.k1, a.k2, b.k2, a.k3, b.k3'
    line = 'select %s from {left_table} a right join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
            right join {third_table} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 \
            order by isnull(a.k1), 1, 2, 3, 4, 5 limit 65535' % i
    runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))


def test_right_outer_join():
    """
    {
    "title": "test_query_join.test_right_outer_join",
    "describe": "right outer join",
    "tag": "function,p1,fuzz"
    }
    """
    """right outer join"""
    selected = list()
    selected.append('a.k1, b.k1, a.k2, b.k2, a.k3, b.k3')
    selected.append('count(a.k1), count(b.k1), count(a.k2), count(b.k2), count(*)')
    i = 'a.k1, b.k1, a.k2, b.k2, a.k3, b.k3'
    print('5')
    line = 'select %s from {left_table} a right join {right_table} b \
               on a.k1 = b.k1 and a.k2 > b.k2 order by isnull(a.k1), 1, 2, 3, 4, 5 limit 65535' % i
    runner.check(line.format(left_table=table_name, right_table=join_name))

    for i in selected:
        print('1')
        line = 'select %s from {left_table} a right  outer join {right_table} b \
               on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('2')
        line = 'select %s from {left_table} a right  outer join {right_table} b \
                    on a.k1 > b.k1 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('3')
        line = 'select %s from {left_table} a right  outer join {right_table} b \
                    on a.k1 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('4')
        line = 'select %s from {left_table} a right  outer join {right_table} b \
               on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('6')
        line = 'select %s from {left_table} a right  outer join {right_table} b \
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('7')
        line = 'select %s from {left_table} a right  outer join {right_table} b \
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('8')
        line = 'select %s from {left_table} a right  outer join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('9')
        line = 'select %s from {left_table} a right  outer join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('10')
        line = 'select %s from {left_table} a right  outer join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('11')
        line = 'select %s from {left_table} a right  outer join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('12')
        line = 'select %s from {left_table} a right outer join {right_table} b on a.k1 = b.k1 \
                right outer join {third_table} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535' \
                % i
        runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))
    print('13')
    i = 'a.k1, b.k1, a.k2, b.k2, a.k3, b.k3'
    line = 'select %s from {left_table} a right outer join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
            right outer join {third_table} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 \
            order by isnull(a.k1), 1, 2, 3, 4, 5 limit 65535' % i
    runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))


def test_full_outer_join():
    """
    {
    "title": "test_query_join.test_full_outer_join",
    "describe": "full outer join",
    "tag": "function,p1,fuzz"
    }
    """
    """full outer join"""
    selected = list()
    selected.append('a.k1, b.k1, a.k2, b.k2, a.k3, b.k3')
    selected.append('count(a.k1), count(b.k1), count(a.k2), count(b.k2), count(*)')
    for i in selected:
        print('1')
        line1 = 'select %s from {left_table} a full outer join {right_table} b on a.k1 = b.k1 \
                 order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b on a.k1 = b.k1 \
                 order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('2')
        line1 = 'select %s from {left_table} a full outer join {right_table} b on a.k1 > b.k1 \
                where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' \
                % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b on a.k1 > b.k1 \
                where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' \
                % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('3')
        line1 = 'select %s from {left_table} a full outer join {right_table} b on a.k1 > 0 \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b on a.k1 > 0 \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('6')
        line = 'select %s from {left_table} a full outer join {right_table} b \
                where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' \
                % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('7')
        line1 = 'select %s from {left_table} a full outer join {right_table} b \
                on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left  outer join {right_table} b \
                on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('8')
        line1 = 'select %s from {left_table} a full outer join {right_table} b \
                on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b \
                on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('9')
        line1 = 'select %s from {left_table} a full outer join {right_table} b \
                on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b \
                on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('10')
        line1 = 'select %s from {left_table} a full outer join {right_table} b \
                on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b \
                on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('11')
        line1 = 'select %s from {left_table} a full outer join {right_table} b \
                on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b \
                on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('12')
        line1 = 'select %s from {left_table} a full outer join {right_table} b on a.k1 = b.k1 \
                full outer join {third_table} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535' \
                % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b on a.k1 = b.k1 \
                 left outer join {third_table} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535' \
                 % i
        line1 = line1.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        line2 = line2.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        runner.check2(line1, line2)
    print('5')
    line1 = 'select a.k1 k1, a.k2, a.k3, b.k1, b.k2, b.k3 from {left_table} a full outer join {right_table} b \
             on a.k1 = b.k1 and a.k2 > b.k2 order by isnull(k1), 1, 2, 3, 4, 5 limit 65535'
    line2 = 'select a.k1 k1, a.k2, a.k3, b.k1, b.k2, b.k3 from {left_table} a left outer join {right_table} b \
             on a.k1 = b.k1 and a.k2 > b.k2 union (select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 \
             from {left_table} a right outer join {right_table} b on a.k1 = b.k1 and a.k2 > b.k2) \
             order by isnull(k1), 1, 2, 3, 4, 5 limit 65535'
    line1 = line1.format(left_table=table_name, right_table=join_name)
    line2 = line2.format(left_table=table_name, right_table=join_name)
    runner.check2(line1, line2)
    print('13')
    line1 = 'select count(*) from {left_table} a full outer join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
            full outer join {third_table} c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0'
    line2 = 'select count(*) from ((select a.k1 as k1, b.k1 as k2, a.k2 as k3, b.k2 as k4, a.k3 as k5, b.k3 as k6, c.k1 as k7, c.k2 as k8, c.k3 as k9 from {left_table} a \
            left outer join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
            left outer join {third_table} c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0) union \
            (select a.k1, b.k1, a.k2, b.k2, a.k3, b.k3, c.k1, c.k2, c.k3 from {left_table} a \
            left outer join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
            right outer join {third_table} c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0) union \
            (select a.k1, b.k1, a.k2, b.k2, a.k3, b.k3, c.k1, c.k2, c.k3 from {left_table} a \
            right outer join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
            left outer join {third_table} c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0) \
            union (select a.k1, b.k1, a.k2, b.k2, a.k3, b.k3, c.k1, c.k2, c.k3 from {left_table} a \
            right outer join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
            right outer join {third_table} c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0))a'
    line1 = line1.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
    line2 = line2.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
    runner.check2(line1, line2)
    i = 'a.k1, b.k1, a.k2, b.k2, a.k3, b.k3'
    print('4')
    line1 = 'select %s from {left_table} a full outer join {right_table} b on a.k1 = b.k1 \
             and a.k2 > 0 order by 1, isnull(b.k1), 2, 3, 4, 5  limit 65535' % i
    line2 = 'select %s from {left_table} a left outer join {right_table} b on a.k1 = b.k1 and a.k2 > 0 \
            order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535' % i
    line1 = line1.format(left_table=table_name, right_table=join_name)
    line2 = line2.format(left_table=table_name, right_table=join_name)
    runner.check2(line1, line2)


def test_cross_join():
    """
    {
    "title": "test_query_join.test_cross_join",
    "describe": "cross join",
    "tag": "function,p1,fuzz"
    }
    """
    """cross join"""
    selected = list()
    selected.append('a.k1, b.k1, a.k2, b.k2, a.k3, b.k3')
    selected.append('count(a.k1), count(b.k1), count(a.k2), count(b.k2), count(*)')

    for i in selected:
        print('1')
        line = 'select %s from {left_table} a cross join {right_table} b \
                    on a.k1 = b.k1 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('2')
        line = 'select %s from {left_table} a cross join {right_table} b \
                    on a.k1 > b.k1 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('3')
        line = 'select %s from {left_table} a cross join {right_table} b \
                    on a.k1 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('4')
        line = 'select %s from {left_table} a cross join {right_table} b \
                    on a.k1 = b.k1 and a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('5')
        line = 'select %s from {left_table} a cross join {right_table} b \
                    on a.k1 = b.k1 and a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('6')
        line = 'select %s from {left_table} a cross join {right_table} b \
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('7')
        line = 'select %s from {left_table} a cross join {right_table} b \
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('8')
        line = 'select %s from {left_table} a cross join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('9')
        line = 'select %s from {left_table} a cross join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('10')
        line = 'select %s from {left_table} a cross join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('11')
        line = 'select %s from {left_table} a cross join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('12')
        line = 'select %s from {left_table} a cross join {right_table} b on a.k1 = b.k1 \
                cross join {third_table} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))
        print('13')
        line = 'select %s from {left_table} a cross join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
                cross join {third_table} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))
        print('***************************************************************************')


def test_left_semi_join():
    """
    {
    "title": "test_query_join.test_left_semi_join",
    "describe": "left semi join",
    "tag": "function,p1,fuzz"
    }
    """
    """left semi join"""
    selected = list()
    selected.append('a.k1, a.k2, a.k3, a.k4, a.k5')
    selected.append('count(a.k1), count(a.k2), count(a.k4), count(a.k3), count(*)')

    for i in selected:
        print('1')
        line1 = 'select %s from {left_table} a left semi join {right_table} b on a.k1 = b.k1 \
                 order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b on a.k1 = b.k1 \
                 where b.k3 is not null order by 1, 2, 3, 4, 5 \
                 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('2')
        line1 = 'select %s from {left_table} a left semi join {right_table} b on a.k1 > b.k1 \
                where a.k2 > 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from (select distinct a.* from {left_table} a left outer join ' \
                '{right_table} b on a.k1 > b.k1 where b.k3 is not null and a.k2 > 0 and a.k6 > "000") a ' \
                'order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('3')
        line1 = 'select %s from {left_table} a left semi join {right_table} b on a.k1 > 0 \
                where a.k2 > 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b on a.k1 > 0 ' \
                'where b.k3 is not null and a.k2 > 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 ' \
                'limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('4')
        line1 = 'select %s from {left_table} a left semi join {right_table} b \
                on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b \
                on a.k1 = b.k1 and a.k2 > 0 where b.k3 is not null \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('5')
        line1 = 'select %s from {left_table} a left semi join {right_table} b \
                    on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b \
                 on a.k1 = b.k1 and a.k2 > b.k2 where b.k3 is not null \
                 order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('6')
        line = 'select %s from {left_table} a left semi join {right_table} b \
                where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' \
                % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('7')
        line1 = 'select %s from {left_table} a left semi join {right_table} b \
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from( select distinct a.* from {left_table} a left outer join {right_table} b ' \
                'on a.k1 = b.k1 or a.k2 = b.k2 where b.k3 is not null and  a.k2 > 0 and a.k6 > "000") a' \
                ' order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('8')
        line1 = 'select %s from {left_table} a left semi join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from( select distinct a.* from {left_table} a left outer join {right_table} b ' \
                'on a.k1 < b.k1 or a.k2 > b.k2 where b.k3 is not null and  a.k2 > 0 and a.k6 > "000") a' \
                ' order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('9')
        line1 = 'select %s from {left_table} a left semi join {right_table} b \
                on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k6 > "000" \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from( select distinct a.* from {left_table} a left outer join {right_table} b ' \
                'on a.k1 = b.k1 or a.k2 > b.k2 where b.k3 is not null and  a.k2 > 0 and a.k6 > "000") a' \
                ' order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('10')
        line1 = 'select %s from {left_table} a left semi join {right_table} b \
                on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and a.k6 > "000" \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from( select distinct a.* from {left_table} a left outer join {right_table} b ' \
                'on a.k1 = b.k1 or a.k2 > 0 where b.k3 is not null and  a.k2 > 0 and a.k6 > "000") a' \
                ' order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('11')
        line1 = 'select %s from {left_table} a left semi join {right_table} b \
                on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and a.k6 > "000" \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from( select distinct a.* from {left_table} a left outer join {right_table} b ' \
                'on a.k1 < b.k1 or a.k2 > 0 where b.k3 is not null and  a.k2 > 0 and a.k6 > "000") a' \
                ' order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('12')
        line1 = 'select %s from {left_table} a left semi join {right_table} b on a.k1 = b.k1 \
                left semi join {third_table} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535' \
                % i
        line2 = 'select %s from (select distinct a.* from {left_table} a left outer join {right_table} b on a.k1 = b.k1 \
                left outer join {third_table} c on a.k2 = c.k2 where a.k1 is not null \
                and b.k1 is not null and c.k1 is not null) a order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        line2 = line2.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        runner.check2(line1, line2)
        print('13')
        line1 = 'select %s from {left_table} a left semi join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
                left semi join {third_table} c on a.k3 = c.k3 and a.k1 = c.k1 + 1 and c.k3 > 0 \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from (select distinct a.* from {left_table} a left outer join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
                left outer join {third_table} c on a.k3 = c.k3 and a.k1 = c.k1 + 1 and c.k3 > 0 \
                where a.k1 is not null and b.k1 is not null and c.k1 is not null and a.k1 > 0 and c.k3 > 0) a\
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        line2 = line2.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        runner.check2(line1, line2)


def test_right_semi_join():
    """
    {
    "title": "test_query_join.test_right_semi_join",
    "describe": "right semi join",
    "tag": "function,p1,fuzz"
    }
    """
    """right semi join, note 6"""
    selected = list()
    selected.append('b.k1, b.k2, b.k3, b.k4, b.k5')
    selected.append('count(b.k1), count(b.k2), count(b.k4), count(b.k3), count(*)')

    for i in selected:
        print('1')
        line1 = 'select %s from {left_table} a right semi join {right_table} b \
                on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b \
                on a.k1 = b.k1 where a.k2 is not null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('2')
        line1 = 'select %s from {left_table} a right semi join {right_table} b \
                    on a.k1 > b.k1 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from(select distinct b.* from  {left_table} a right outer join {right_table} b ' \
                'on a.k1 > b.k1 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" ' \
                'and a.k2 is not null) b order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('3')
        line1 = 'select %s from {left_table} a right semi join {right_table} b \
                    on a.k1 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b ' \
                'on a.k1 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" and a.k2 is not null ' \
                'order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('4')
        line1 = 'select %s from {left_table} a right semi join {right_table} b \
                 on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b on a.k1 = b.k1 and \
                 a.k2 > 0 where a.k2 is not null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('5')
        line1 = 'select %s from {left_table} a right semi join {right_table} b \
                    on a.k1 = b.k1 and a.k2 > b.k2  order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b \
                    on a.k1 = b.k1 and a.k2 > b.k2 where a.k2 is not null \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('6')
        line = 'select %s from {left_table} a right semi join {right_table} b \
               where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('7')
        line1 = 'select %s from {left_table} a right semi join {right_table} b \
                    on a.k1 = b.k1 or a.k2 = b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from (select distinct b.* from  {left_table} a right outer join ' \
                '{right_table} b on a.k1 = b.k1 or a.k2 = b.k2 where b.k2 > 0 and b.k3 != 0 ' \
                'and b.k6 > "000" and a.k2 is not null) b order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('8')
        line1 = 'select %s from {left_table} a right semi join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from (select distinct b.* from  {left_table} a right outer join ' \
                '{right_table} b on a.k1 < b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 ' \
                'and b.k6 > "000" and a.k2 is not null) b order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('9')
        line1 = 'select %s from {left_table} a right semi join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from (select distinct b.* from  {left_table} a right outer join ' \
                '{right_table} b on a.k1 = b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 ' \
                'and b.k6 > "000" and a.k2 is not null) b order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('10')
        line1 = 'select %s from {left_table} a right semi join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from (select distinct b.* from  {left_table} a right outer join ' \
                '{right_table} b on a.k1 = b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 ' \
                'and b.k6 > "000" and a.k2 is not null) b order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('11')
        line1 = 'select %s from {left_table} a right semi join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from (select distinct b.* from  {left_table} a right outer join ' \
                '{right_table} b on a.k1 < b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 ' \
                'and b.k6 > "000" and a.k2 is not null) b order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('12')
        line1 = 'select %s from {left_table} a right semi join {right_table} c on a.k1 = c.k1 \
                right semi join {third_table} b on b.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535' \
                % i
        line2 = 'select %s from (select distinct b.* from {left_table} a right outer join {right_table} c on a.k1 = c.k1 \
                right outer join {third_table} b on b.k2 = c.k2 where a.k1 is not null \
                and b.k1 is not null and c.k1 is not null) b order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=tmp_name, right_table=table_name, third_table=join_name)
        line2 = line2.format(left_table=tmp_name, right_table=table_name, third_table=join_name)
        runner.check2(line1, line2)
        print('13')
        line1 = 'select %s from {left_table} c right semi join {right_table} a on c.k2 = a.k2 and c.k1 > 0 \
                right semi join {third_table} b on a.k3 = b.k3 and b.k1 = a.k1 + 1 and a.k3 > 0 \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from (select distinct a.* from {left_table} c right outer join {right_table} b1 on c.k2 = b1.k2 and c.k1 > 0 \
                right outer join {third_table} a on c.k3 = a.k3 and a.k1 = c.k1 + 1 and a.k3 > 0 \
                where a.k1 is not null and b1.k1 is not null and a.k1 is not null and a.k1 > 0 and c.k3 > 0) b\
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name, third_table=tmp_name)
        line2 = line2.format(left_table=join_name, right_table=table_name, third_table=tmp_name)
        runner.check2(line1, line2)


def test_left_anti_join():
    """
    {
    "title": "test_query_join.test_left_anti_join",
    "describe": "left anti join",
    "tag": "function,p1,fuzz"
    }
    """
    """left anti join"""
    selected = list()
    selected.append('a.k1, a.k2, a.k3, a.k4, a.k5')
    selected.append('count(a.k1), count(a.k2), count(a.k4), count(a.k3), count(*)')

    for i in selected:
        print('1')
        line1 = 'select %s from {left_table} a left anti join {right_table} b \
                    on a.k1 = b.k1  order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b \
                    on a.k1 = b.k1 where b.k3 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('2')
        line1 = 'select %s from {left_table} a left anti join {right_table} b \
                    on a.k1 > b.k1 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b ' \
                'on a.k1 > b.k1 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" and b.k3 is null ' \
                'order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('3')
        line1 = 'select %s from {left_table} a left anti join {right_table} b \
                    on a.k1 > 0 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b ' \
                'on a.k1 > 0 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" and b.k3 is null ' \
                'order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('4')
        line1 = 'select %s from {left_table} a left anti join {right_table} b \
                    on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 50000' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b \
                    on a.k1 = b.k1 and a.k2 > 0 where b.k3 is null \
                    order by 1, 2, 3, 4, 5 limit 50000' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('5')
        line1 = 'select %s from {left_table} a left anti join {right_table} b \
                 on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b \
                 on a.k1 = b.k1 and a.k2 > b.k2 where b.k3 is null \
                 order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('6')
        line = 'select %s from {left_table} a left anti join {right_table} b \
               where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('7')
        line1 = 'select %s from {left_table} a left anti join {right_table} b \
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b ' \
                'on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" ' \
                'and b.k3 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('8')
        line1 = 'select %s from {left_table} a left anti join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b ' \
                'on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" ' \
                'and b.k3 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('9')
        line1 = 'select %s from {left_table} a left anti join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b ' \
                'on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" ' \
                'and b.k3 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('10')
        line1 = 'select %s from {left_table} a left anti join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b ' \
                'on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" ' \
                'and b.k3 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('11')
        line1 = 'select %s from {left_table} a left anti join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b ' \
                'on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" ' \
                'and b.k3 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name)
        line2 = line2.format(left_table=table_name, right_table=join_name)
        runner.check2(line1, line2)
        print('12')
        line1 = 'select %s from {left_table} a left anti join {right_table} b on a.k1 = b.k1 \
                left anti join {third_table} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535' \
                % i
        line2 = 'select %s from {left_table} a left outer join {right_table} b on a.k1 = b.k1 \
                left outer join {third_table} c on a.k2 = c.k2 where \
                b.k1 is null and c.k1 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        line2 = line2.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        runner.check2(line1, line2)
        print('13')
        line1 = 'select %s from {left_table} a left anti join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
                left anti join {third_table} c on a.k3 = c.k3 and a.k1 = c.k1 + 1 and c.k3 > 0 \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from (select distinct a.* from {left_table} a left outer join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
                left outer join {third_table} c on a.k3 = c.k3 and a.k1 = c.k1 + 1 and c.k3 > 0 \
                where b.k1 is null and c.k1 is null) a\
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        line2 = line2.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        runner.check2(line1, line2)


def test_right_anti_join():
    """
    {
    "title": "test_query_join.test_right_anti_join",
    "describe": "right anti join",
    "tag": "function,p1,fuzz"
    }
    """
    """right anti join"""
    selected = list()
    selected.append('b.k1, b.k2, b.k3, b.k4, b.k5')
    selected.append('count(b.k1), count(b.k2), count(b.k3), count(b.k4), count(*)')

    for i in selected:
        print('1')
        line1 = 'select %s from {left_table} a right anti join {right_table} b \
                    on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b \
                on a.k1 = b.k1 where a.k2 is null \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('2')
        line1 = 'select %s from {left_table} a right anti join {right_table} b \
                    on a.k1 > b.k1 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b ' \
                'on a.k1 > b.k1 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" ' \
                'and a.k2 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('3')
        line1 = 'select %s from {left_table} a right anti join {right_table} b \
                    on a.k1 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b ' \
                'on a.k1 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" ' \
                'and a.k2 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('4')
        line1 = 'select %s from {left_table} a right anti join {right_table} b \
                    on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b \
                    on a.k1 = b.k1 and a.k2 > 0 where a.k2 is null \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('5')
        line1 = 'select %s from {left_table} a right anti join {right_table} b \
                    on a.k1 = b.k1 and a.k2 > b.k2  order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b \
                    on a.k1 = b.k1 and a.k2 > b.k2 where a.k2 is null \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('6')
        line = 'select %s from {left_table} a right anti join {right_table} b \
               where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.checkwrong(line.format(left_table=table_name, right_table=join_name))
        print('7')
        line1 = 'select %s from {left_table} a right anti join {right_table} b \
                    on a.k1 = b.k1 or a.k2 = b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b ' \
                'on a.k1 = b.k1 or a.k2 = b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" ' \
                'and a.k2 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('8')
        line1 = 'select %s from {left_table} a right anti join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b ' \
                'on a.k1 < b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" ' \
                'and a.k2 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('9')
        line1 = 'select %s from {left_table} a right anti join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b ' \
                'on a.k1 = b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" ' \
                'and a.k2 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('10')
        line1 = 'select %s from {left_table} a right anti join {right_table} b \
                    on a.k1 = b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b ' \
                'on a.k1 = b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" ' \
                'and a.k2 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('11')
        line1 = 'select %s from {left_table} a right anti join {right_table} b \
                    on a.k1 < b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from {left_table} a right outer join {right_table} b ' \
                'on a.k1 < b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" ' \
                'and a.k2 is null order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)
        print('12')
        line1 = 'select %s from {left_table} a right anti join {right_table} c on a.k1 = c.k1 \
                right anti join {third_table} b on c.k2 = b.k2 order by 1, 2, 3, 4, 5 limit 65535'\
                % i
        line2 = 'select %s from (select distinct b.k1, b.k2, b.k3, b.k4, b.k5 from \
                {left_table} a right outer join {right_table} c on a.k1 = c.k1 right outer join \
                {third_table} b on c.k2=b.k2) b order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        line2 = line2.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        runner.check2(line1, line2)
        print('13')
        line1 = 'select %s from {left_table} a right anti join {right_table} c on a.k2 = c.k2 and a.k1 > 0 \
                right anti join {third_table} b on c.k3 = b.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from (select distinct c.* from {left_table} a right outer join {right_table} b on a.k2 = b.k2 and a.k1 > 0 \
                right outer join {third_table} c on b.k3 = c.k3 and c.k1 = b.k1 + 1 and c.k3 > 0 \
                where b.k1 is null and a.k1 is null and a.k1 > 0) b\
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line2 = 'select %s from (select distinct c.k1 k1, c.k2 k2, c.k3 k3, c.k4 k4, c.k5 k5 from \
                (select b2.* from {left_table} a right outer join {right_table} b2 on a.k2 = b2.k2 and a.k1 > 0 \
                where a.k1 is null and a.k1 > 0) b1 right outer join {third_table} c \
                on b1.k3 = c.k3 and c.k1 = b1.k1 + 1 and c.k3 > 0 where b1.k1 is null) b \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        line1 = line1.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        line2 = line2.format(left_table=table_name, right_table=join_name, third_table=tmp_name)
        runner.check2(line1, line2)


def test_no_join():
    """
    {
    "title": "test_query_join.test_no_join",
    "describe": "join with no join word",
    "tag": "function,p1"
    }
    """
    """join with no join word"""
    selected = list()
    selected.append('a.k1, b.k1, a.k2, b.k2, a.k3, b.k3')
    selected.append('count(a.k1), count(b.k1), count(a.k2), count(b.k2), count(*)')

    for i in selected:
        print('1')
        line = 'select %s from {left_table} a , {right_table} b \
                    where a.k1 = b.k1 and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('2')
        line = 'select %s from {left_table} a , {right_table} b \
                    where a.k1 > b.k1 and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('3')
        line = 'select %s from {left_table} a , {right_table} b \
                    where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('4')
        line = 'select %s from {left_table} a , {right_table} b \
                    where a.k1 = b.k1 and a.k2 > 0 and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('5')
        line = 'select %s from {left_table} a , {right_table} b \
                    where a.k1 = b.k1 and a.k2 > b.k2 and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('6')
        line = 'select %s from {left_table} a , {right_table} b \
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('7')
        line = 'select %s from {left_table} a , {right_table} b \
                    where (a.k1 = b.k1 or a.k2 = b.k2) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('8')
        line = 'select %s from {left_table} a , {right_table} b \
                    where (a.k1 < b.k1 or a.k2 > b.k2) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('9')
        line = 'select %s from {left_table} a , {right_table} b \
                    where (a.k1 = b.k1 or a.k2 > b.k2) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('10')
        line = 'select %s from {left_table} a , {right_table} b \
                    where (a.k1 = b.k1 or a.k2 > 0) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('11')
        line = 'select %s from {left_table} a , {right_table} b \
                    where (a.k1 < b.k1 or a.k2 > 0) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" \
                    order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name))
        print('12')
        line = 'select %s from {left_table} a, {right_table} b, {third_table} c where a.k1 = b.k1 \
                and a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))
        print('13')
        line = 'select %s from {left_table} a, {right_table} b, {third_table} c where a.k2 = b.k2 and a.k1 > 0 \
                and a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 \
                order by 1, 2, 3, 4, 5 limit 65535' % i
        runner.check(line.format(left_table=table_name, right_table=join_name, third_table=tmp_name))


def test_empty_join():
    """
    {
    "title": "test_query_join.test_empty_join",
    "describe": "test join with empty table",
    "tag": "function,p1"
    }
    """
    """test join with empty table"""
    line = 'drop view if exists empty'
    assert runner.init(line)
    line = 'create view empty as select * from baseall where k1 = 0'
    assert runner.init(line)
    empty_name = 'empty'
    line = 'select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from %s a join %s b on a.k1 = b.k1 \
            order by 1, 2, 3, 4, 5' % (join_name, empty_name)
    runner.check(line)
    line = 'select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from %s a inner join %s b on a.k1 = b.k1 \
            order by 1, 2, 3, 4, 5' % (join_name, empty_name)
    runner.check(line)
    line = 'select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from %s a left join %s b on a.k1 = b.k1 \
            order by 1, 2, 3, 4, 5' % (join_name, empty_name)
    runner.check(line)
    line = 'select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from %s a right join %s b on a.k1 = b.k1 \
            order by 1, 2, 3, 4, 5' % (join_name, empty_name)
    runner.check(line)
    line1 = 'select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from %s a full outer join %s b on a.k1 = b.k1 \
            order by 1, 2, 3, 4, 5' % (join_name, empty_name)
    line2 = 'select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from %s a left join %s b on a.k1 = b.k1 \
            order by 1, 2, 3, 4, 5' % (join_name, empty_name)
    runner.check2(line1, line2)
    line = 'select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from %s a cross join %s b on a.k1 = b.k1 \
            order by 1, 2, 3, 4, 5' % (join_name, empty_name)
    # ret = execute_sql(line)
    # assert ret == ()
    line = 'select a.k1, a.k2, a.k3 from %s a left semi join %s b on a.k1 = b.k1 \
            order by 1, 2, 3' % (join_name, empty_name)
    ret = runner.execute_sql(line)
    assert ret == ()
    line = 'select b.k1, b.k2, b.k3 from %s a right semi join %s b on a.k1 = b.k1 \
            order by 1, 2, 3' % (join_name, empty_name)
    ret = runner.execute_sql(line)
    assert ret == ()
    line1 = 'select a.k1, a.k2, a.k3 from %s a left anti join %s b on a.k1 = b.k1 \
            order by 1, 2, 3' % (join_name, empty_name)
    line2 = 'select k1, k2, k3 from %s order by 1, 2, 3' % join_name
    runner.check2(line1, line2)
    line = 'select b.k1, b.k2, b.k3 from %s a right anti join %s b on a.k1 = b.k1 \
            order by 1, 2, 3' % (join_name, empty_name)
    ret = runner.execute_sql(line)
    assert ret == ()


def test_join_other():
    """
    {
    "title": "test_query_join.test_join_other",
    "describe": "join bug",
    "tag": "function,p1"
    }
    """
    """join bug"""
    line1 = 'select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a \
           right semi join test b on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4 limit 65535'
    ret1 = runner.execute_sql(line1)
    line2 = 'select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a \
           right semi join test b on a.k1 = b.k1 and a.k2 < b.k2 order by 1, 2, 3, 4 limit 65535'
    ret2 = runner.execute_sql(line2)
    assert util.check(line1, line2)

    line = 'select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a \
           right semi join test b on a.k1 = b.k1 order by 1, 2, 3, 4 limit 65535'
    c_r1 = runner.execute_sql(line)
    line = 'select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a \
          right semi join test b on a.k1 = b.k1 order by 1, 2, 3, 4 limit 65535'
    c_r2 = runner.execute_sql(line)
    for i in range(0, 100):
        line = 'select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a \
               right semi join test b on a.k1 = b.k1 order by 1, 2, 3, 4 limit 65535'
        r1 = runner.execute_sql(line)
        line = 'select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a \
              right semi join test b on a.k1 = b.k1 order by 1, 2, 3, 4 limit 65535'
        r2 = runner.execute_sql(line)
        print(r1)
        print(r2)
        util.check_same(r1, c_r1)
        util.check_same(r2, c_r2)
    line = 'select count(*) from test a full outer join baseall b on a.k2 = b.k2 and a.k1 > 0 \
            full outer join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 and a.k3 > 0 \
            order by 1 limit 65535'
    r1 = runner.execute_sql(line)
    line = 'select count(*) from test a full outer join baseall b on a.k2 = b.k2 and a.k1 > 0 \
           full outer join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0 \
           order by 1 limit 65535'
    r2 = runner.execute_sql(line)
    util.check_same(r1, r2)

    line = 'drop view if exists nullable'
    assert runner.init(line)
    line = 'create view nullable(n1, n2) as select a.k1, b.k2 from baseall \
            a left join bigtable b on a.k1 = b.k1 + 10 where b.k2 is null'
    assert runner.init(line)
    line = 'select k1, n1 from baseall a right outer join nullable b on a.k1 % 2 = b.n1 % 2 \
           order by a.k1, b.n1'
    runner.check(line)
    line = 'select n.k1, m.k1, m.k2, n.k2 from (select a.k1, a.k2, a.k3 from \
           baseall a join baseall b on (a.k1 = b.k1 and a.k2 = b.k2 and a.k3 = b.k3)) m \
           left join test n on m.k1 = n.k1 order by 1, 2, 3, 4'
    runner.check(line)
    # https://github.com/apache/incubator-doris/issues/4210
    line = 'select * from baseall t1 where k1 = (select min(k1) from test t2 where t2.k1 = t1.k1 and t2.k2=t1.k2)' \
           ' order by k1'
    runner.check(line)


def test_join_basic():
    """
    {
    "title": "test_query_join.test_join_basic",
    "describe": "join on diff type equal",
    "tag": "function,p1"
    }
    """
    """join on diff type equal"""
    columns = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k10', 'k11']
    # join_type = ['inner', 'left outer', 'right outer', 'cross', '']
    join_type = ['inner', 'left outer', 'right outer', '']
    for type in join_type:
        for c in columns:
            line = 'select * from {left_table} a %s join {right_table} b on (a.%s = b.%s) \
                   order by isnull(a.k1), a.k1, a.k2, a.k3, isnull(b.k1), b.k1, b.k2, b.k3 \
                   limit 60015' % (type, c, c)
            runner.check(line.format(left_table=join_name, right_table=table_name))

    for c in columns:
        line1 = 'select * from {left_table} a full outer join {right_table} b on (a.%s = b.%s) \
                order by isnull(a.k1), a.k1, a.k2, a.k3, a.k4, isnull(b.k1), b.k1, b.k2, b.k3, \
                b.k4 limit 65535' % (c, c)
        line2 = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak7, a.k10 ak10, a.k11 ak11, \
                 a.k7 ak7, a.k8 ak8, a.k9 ak9, b.k1 bk1, b.k2 bk2, b.k3 bk3, b.k4 bk4, b.k5 bk5, \
                 b.k6 bk6, b.k10 bk10, b.k11 bk11, b.k7 bk7, b.k8 bk8, b.k9 bk9 \
                 from {left_table} a left outer join {right_table} b on (a.%s = b.%s) \
                 union select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, a.k10 ak10, \
                 a.k11 ak11, a.k7 ak7, a.k8 ak8, a.k9 ak9, b.k1 bk1, b.k2 bk2, b.k3 bk3, b.k4 bk4, \
                 b.k5 bk5, b.k6 bk6, b.k10 bk10, b.k11 bk11, b.k7 bk7, b.k8 bk8, b.k9 bk9 from \
                 {left_table} a right outer join {right_table} b on (a.%s = b.%s) order by \
                 isnull(ak1), 1, 2, 3, 4, isnull(bk1), 12, 13, 14, 15 limit 65535' % (c, c, c, c)
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)

    for c in columns:
        line1 = 'select * from {left_table} a left semi join {right_table} b on (a.%s = b.%s) \
                order by a.k1, a.k2, a.k3' % (c, c)
        line2 = 'select distinct a.* from {left_table} a left outer join {right_table} b on (a.%s = b.%s) \
                where b.k1 is not null order by a.k1, a.k2, a.k3' % (c, c)
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)

    for c in columns:
        line1 = 'select * from {left_table} a right semi join {right_table} b on (a.%s = b.%s) \
                order by b.k1, b.k2, b.k3' % (c, c)
        line2 = 'select distinct b.* from {left_table} a right outer join {right_table} b on (a.%s = b.%s) \
                where a.k1 is not null order by b.k1, b.k2, b.k3' % (c, c)
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)

    for c in columns:
        line1 = 'select * from {left_table} a left anti join {right_table} b on (a.%s = b.%s) \
                order by a.k1, a.k2, a.k3' % (c, c)
        line2 = 'select distinct a.* from {left_table} a left outer join {right_table} b on (a.%s = b.%s) \
                where b.k1 is null order by a.k1, a.k2, a.k3' % (c, c)
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)

    for c in columns:
        line1 = 'select * from {left_table} a right anti join {right_table} b on (a.%s = b.%s) \
                order by b.k1, b.k2, b.k3' % (c, c)
        line2 = 'select distinct b.* from {left_table} a right outer join {right_table} b on (a.%s = b.%s) \
                where a.k1 is null order by b.k1, b.k2, b.k3' % (c, c)
        line1 = line1.format(left_table=join_name, right_table=table_name)
        line2 = line2.format(left_table=join_name, right_table=table_name)
        runner.check2(line1, line2)


def test_join_complex():
    """
    {
    "title": "test_query_join.test_join_complex",
    "describe": "join commplex",
    "tag": "function,p1"
    }
    """
    """join commplex"""
    columns = ['k1']
    # join_type = ['inner', 'left outer', 'right outer', 'cross', '']
    join_type = ['inner', 'left outer', 'right outer', '']

    for type in join_type:
        for c in columns:
            line = 'select count(a.k1), count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from {left_table} where k1 < 5 \
                    order by k1) a %s join (select k1, k2, k6 from {right_table} where k1 < 5 \
                    order by k1) b on (a.%s = b.%s)' % (type, c, c)
            runner.check(line.format(left_table=join_name, right_table=join_name))

    line1 = 'select count(a.k1), count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from {left_table} where k1 < 5 \
            order by k1) a full outer join (select k1, k2, k6 from {right_table} where k1 < 5 \
            order by k1) b on (a.k1 = b.k1)'
    line2 = 'select count(c.k1), count(c.m1), count(*) from \
            (select distinct a.*, b.* from (select k1 + 2 as m1, k2 + 1000 as m2, k6 as m6 \
            from {left_table} where k1 < 5 order by k1) a left outer join \
            (select k1, k2, k6 from {right_table} where k1 < 5 order by k1) b on (a.m1 = b.k1) \
            union (select distinct a.*, b.* from \
            (select k1 + 2 as m1, k2 + 1000 as m2, k6 as m6 from {left_table} where k1 < 5 \
            order by k1) a right outer join (select k1, k2, k6 from {right_table} where k1 < 5 \
            order by k1) b on (a.m1 = b.k1))) c'
    line1 = line1.format(left_table=join_name, right_table=join_name)
    line2 = line2.format(left_table=join_name, right_table=join_name)
    runner.check2(line1, line2)

    line1 = 'select count(a.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from {left_table} where k1 < 5 \
            order by k1) a left semi join (select k1, k2, k6 from {right_table} where k1 < 5 \
            order by k1) b on (a.k1 = b.k1)'
    line2 = 'select count(a.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from {left_table} where k1 < 5 \
            order by k1) a left outer join (select k1, k2, k6 from {right_table} where k1 < 5 \
            order by k1) b on (a.k1 = b.k1) where b.k1 is not null '
    line1 = line1.format(left_table=join_name, right_table=join_name)
    line2 = line2.format(left_table=join_name, right_table=join_name)
    runner.check2(line1, line2)

    line1 = 'select count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from {left_table} where k1 < 5 \
            order by k1) a right semi join (select k1, k2, k6 from {right_table} where k1 < 5 \
            order by k1) b on (a.k1 = b.k1) '
    line2 = 'select count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from {left_table} where k1 < 5 \
            order by k1) a right outer join (select k1, k2, k6 from {right_table} where k1 < 5 \
            order by k1) b on (a.k1 = b.k1) where a.k1 is not null'
    line1 = line1.format(left_table=join_name, right_table=join_name)
    line2 = line2.format(left_table=join_name, right_table=join_name)
    runner.check2(line1, line2)

    line1 = 'select count(a.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from {left_table} where k1 < 5 \
            order by k1) a left anti join (select k1, k2, k6 from {right_table} where k1 < 5 \
            order by k1) b on (a.k1 = b.k1)'
    line2 = 'select count(a.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from {left_table} where k1 < 5 \
            order by k1) a left outer join (select k1, k2, k6 from {right_table} where k1 < 5 \
            order by k1) b on (a.k1 = b.k1) where b.k1 is null'
    line1 = line1.format(left_table=join_name, right_table=join_name)
    line2 = line2.format(left_table=join_name, right_table=join_name)
    runner.check2(line1, line2)

    line1 = 'select count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from {left_table} where k1 < 5 \
            order by k1) a right anti join (select k1, k2, k6 from {right_table} where k1 < 5 \
            order by k1) b on (a.k1 = b.k1) '
    line2 = 'select count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from {left_table} where k1 < 5 \
            order by k1) a right outer join (select k1, k2, k6 from {right_table} where k1 < 5 \
            order by k1) b on (a.k1 = b.k1) where a.k1 is null'
    line1 = line1.format(left_table=join_name, right_table=join_name)
    line2 = line2.format(left_table=join_name, right_table=join_name)
    runner.check2(line1, line2)


def test_join_null_table():
    """
    {
    "title": "test_query_join.test_join_null_table",
    "describe": "test join multi table",
    "tag": "function,p1"
    }
    """
    """test join multi table"""
    line = 'drop view if exists nullable'
    assert runner.init(line)
    line = 'create view nullable(n1, n2) as select a.k1, b.k2 from baseall \
            a left join bigtable b on a.k1 = b.k1 + 10 where b.k2 is null'
    assert runner.init(line)
    null_name = 'nullable'

    join_type = ['inner', 'left outer', 'right outer', '']
    # join_type = ['inner', 'left outer', 'cross', '']
    for type in join_type:
        line = 'select * from {left_table} a %s join {null_table} b on a.k1 = b.n1 order by \
                a.k1, b.n1' % type
        runner.check(line.format(left_table=join_name, null_table=null_name))
        line = 'select * from {left_table} a %s join {null_table} b on a.k1 = b.n2 order by \
                a.k1, b.n1' % type
        runner.check(line.format(left_table=join_name, null_table=null_name))
    print('-----------------------------------------------------------')
    line = 'select a.k1, a.k2 from {left_table} a left semi join {null_table} b on a.k1 = b.n2 \
            order by a.k1'
    ret = runner.execute_sql(line.format(left_table=join_name, null_table=null_name))
    assert ret == ()
    line = 'select b.n1, b.n2 from {left_table} a right semi join {null_table} b on a.k1 = b.n2 \
           order by b.n1'
    ret = runner.execute_sql(line.format(left_table=join_name, null_table=null_name))
    assert ret == ()
    line = 'select b.k1, b.k2 from {null_table} a right semi join {right_table} b on b.k1 = a.n2 \
           order by b.k1'
    ret = runner.execute_sql(line.format(right_table=join_name, null_table=null_name))
    assert ret == ()
    line = 'select a.n1, a.n2 from {null_table} a left semi join {right_table} b on b.k1 = a.n2 \
           order by 1, 2'
    ret = runner.execute_sql(line.format(right_table=join_name, null_table=null_name))
    assert ret == ()

    line = 'select a.k1, a.k2 from {left_table} a left anti join {null_table} b on a.k1 = b.n2 \
           order by 1, 2'
    ret = runner.execute_sql(line.format(left_table=join_name, null_table=null_name))
    line = 'select k1, k2 from %s order by k1, k2' % join_name
    ret1 = runner.execute_sql(line)
    print(ret)
    util.check_same(ret, ret1)
    line = 'select b.n1, b.n2 from {left_table} a right anti join {null_table} b on a.k1 = b.n2 \
            order by 1, 2'
    ret = runner.execute_sql(line.format(left_table=join_name, null_table=null_name))
    line = 'select n1, n2 from %s order by n1, n2' % null_name
    ret1 = runner.execute_sql(line)
    print(ret)
    util.check_same(ret, ret1)
    # assert ret == () 应该是全值
    line = 'select b.k1, b.k2 from {null_table} a right anti join {right_table} b on b.k1 = a.n2 \
           order by 1, 2'
    ret = runner.execute_sql(line.format(right_table=join_name, null_table=null_name))
    print(ret)
    line = 'select k1, k2 from %s order by k1, k2' % join_name
    ret1 = runner.execute_sql(line)
    util.check_same(ret, ret1)
    line = 'select a.n1, a.n2 from {null_table} a left anti join {right_table} b on b.k1 = a.n2 \
           order by 1, 2'
    ret = runner.execute_sql(line.format(right_table=join_name, null_table=null_name))
    print(ret)
    line = 'select n1, n2 from %s order by n1, n2' % null_name
    ret1 = runner.execute_sql(line)
    util.check_same(ret, ret1)

    '''
    select * from nullable a left anti join baseall b on b.k1 = a.n2;
    select * from baseall a left anti join nullable b on a.k1 = b.n2;
    select * from nullable a right anti join baseall b on a.n2 = b.k1;
    select * from baseall a right anti join nullable b on b.n2 = a.k1;
    '''


def test_join_on_predicate():
    """
    {
    "title": "test_query_join.test_join_on_predicate",
    "describe": "add test case for join bug",
    "tag": "function,p1"
    }
    """
    """add test case for join bug"""
    line = 'select c.k1 from {join_table} a join {right_table} b on a.k2 between 0 and 1000 ' \
           'join {test_table} c on a.k10 = c.k10 order by k1 limit 65535'
    runner.check(line.format(join_table=join_name, right_table=table_name, test_table=tmp_name))
    line = 'select a.k1 from baseall a join test b on b.k2 between 0 and 1000 and a.k1 = b.k1 order by k1;'
    runner.check(line)
    line = 'select a.k1 from baseall a join test b on b.k2 between 0 and 1000 order by k1;'
    runner.check(line)
    line = 'select a.k1 from baseall a join test b on b.k2 in (49, 60, 85) order by k1;'
    runner.check(line)
    line = 'select a.k1 from baseall a join test b on b.k2 in (49, 60, 85) and a.k1 = b.k1 order by k1'
    runner.check(line)
    line = 'select count(a.k1) from baseall a join test b on a.k1 < 10 and a.k1 = b.k1'
    runner.check(line)
    line = 'SELECT t2.k1,t2.k2,t3.k1,t3.k2 FROM baseall t2 LEFT JOIN test t3 ON t2.k2=t3.k2 ' \
           'WHERE t2.k1 = 4 OR (t2.k1 > 4 AND t3.k1 IS NULL) order by 1, 2, 3, 4'
    runner.check(line)


def test_query_null_safe_equal():
    """
    {
    "title": "test_query_join.test_query_null_safe_equal",
    "describe": "<=> test case",
    "tag": "function,p1"
    }
    """
    """<=> test case"""
    line = "select 1 <=> 2, 1 <=> 1, 'a'= 'a'"
    runner.check(line)
    line = "select 1 <=> null, null <=> null,  not('1' <=> NULL)"
    runner.check(line)
    line = "select 'null' <=> 'null', 'null' <=> 'NULL'"
    # runner.check(line)
    # todo, 暂不支持，issue：https://github.com/apache/incubator-doris/issues/2258
    line = "select 'a' <=> NULL, '2019-09-09' <=> NULL, 'true' <=> NULL, 78.367 <=> NULL"
    # runner.check(line)

    # 其他操作
    line1 = "select  cast('2019-09-09' as int) <=> NULL, cast('2019' as int) <=> NULL"
    line2 = "select  NULL <=> NULL, 2019 <=> NULL "
    runner.check2_palo(line1, line2)

    line1 = "select (2019+10) <=> NULL, not (2019+10) <=> NULL, ('1'+'2') <=> NULL"
    line2 = "select  2029 <=> NULL, not 2029 <=> NULL, 3 <=> NULL"
    runner.check2_palo(line1, line2)

    line = "select 2019 <=> NULL and NULL <=> NULL, NULL <=> NULL and NULL <=> NULL, \
       2019 <=> NULL or NULL <=> NULL"
    runner.check(line)


def test_query_join_null_safe_equal():
    """
    {
    "title": "test_query_join.test_query_join_null_safe_equal",
    "describe": "<=> in join test case",
    "tag": "function,p1"
    }
    """
    """<=> in join test case"""
    null_table_1 = "join_null_safe_equal_1"
    null_table_2 = "join_null_safe_equal_2"
    runner.create_insert_null_to_table(null_table_1)
    runner.create_insert_null_to_table(null_table_2)
    line = "insert into {table_name} values (5, NULL,'null', NULL, '2019-09-09 00:00:00', 8.9)"
    runner.init(line.format(table_name=null_table_1))
    line = "select k1<=>NULL, k2<=>NULL, k4<=>NULL, k5<=>NULL, k6<=>NULL\
      from {table_name} order by k1, k2, k4, k5, k6"
    runner.check(line.format(table_name=null_table_1))
    for index in range(1, 7):
        left_join = "select * from %s a left join %s b on  a.k%s<=>b.k%s \
            order by a.k1, b.k1" % (null_table_1, null_table_1, index, index)
        runner.check(left_join)
        right_join = "select * from %s a right join %s b on  a.k%s<=>b.k%s\
            order by a.k1, b.k1" % (null_table_1, null_table_1, index, index)
        runner.check(right_join)
        hash_join = "select * from %s a right join %s b on  a.k%s<=>b.k%s and a.k2=b.k2\
            order by a.k1, b.k1" % (null_table_1, null_table_1, index, index)
        runner.check(hash_join)
        cross_join = "select * from %s a right join %s b on  a.k%s<=>b.k%s and a.k2 !=b.k2\
            order by a.k1, b.k1" % (null_table_1, null_table_1, index, index)
        runner.check(right_join)
        cross_join = "select * from %s a right join %s b on  a.k%s<=>b.k%s and a.k1 > b.k1\
            order by a.k1, b.k1" % (null_table_1, null_table_1, index, index)
        runner.check(right_join)
    # windows
    line1 = "select * from (select k1, k2, sum(k2) over (partition by k1) as ss from %s)a\
       left join %s b on  a.k2=b.k2 and a.k1 >b.k1 order by a.k1, b.k1"  % (null_table_2, null_table_1)
    line2 = "select * from (select k1, k2, k5 from %s) a left join %s b\
      on  a.k2=b.k2 and a.k1 >b.k1 order by a.k1, b.k1" % (null_table_2, null_table_1)
    runner.check2(line1, line2)
    line = "drop table {table_name}"
    runner.init(line.format(table_name=null_table_1))
    runner.init(line.format(table_name=null_table_2))


def test_join_null_value():
    """
    {
    "title": "test_query_join.test_join_null_value",
    "describe": "test join null_value",
    "tag": "function,p1"
    }
    """
    """test join null_value"""
    table_1 = "join_null_value_left_table"
    table_2 = "join_null_value_right_table"
    runner.create_insert_null_to_table(table_1)
    runner.create_insert_null_to_table(table_2)
    line = "insert into {table_name} values (5, 2.2,'null', NULL, '2019-09-09 00:00:00', 8.9)"
    runner.init(line.format(table_name=table_1))
    join_type = ['inner', 'left outer', 'right outer', '']
    for type in join_type:
        for index in range(1, 7):
            line = 'select * from {left_table} a %s join {null_table} b on a.k%s = b.k%s and \
                a.k2 = b.k2 and a.k%s != b.k2 order by \
                a.k1, b.k1' % (type, index, index, index)
            runner.check(line.format(left_table=table_1, null_table=table_2))
            line = 'select * from {left_table} a %s join {null_table} b on a.k%s = b.k%s and \
                a.k2 = b.k2 and a.k%s != b.k2 order by \
                a.k1, b.k1' % (type, index, index, index)
            runner.check(line.format(left_table=table_1, null_table=table_2))
    # <=>, =, is NULL, ifnull
    line = 'select * from {left_table} a  left join {null_table} b on a.k2 <=> b.k2 and \
        a.k3 is NULL order by a.k1, b.k1'
    runner.check(line.format(left_table=table_1, null_table=table_2))
    line = "select * from {left_table} a join {null_table} b on a.k2<=> b.k2 and \
        a.k4<=>NULL order by a.k1,b.k1"
    runner.check(line.format(left_table=table_1, null_table=table_2))
    line = "select * from {left_table} a join {null_table} b on a.k2<=> b.k2\
        and a.k4<=>NULL  and b.k4 is not NULL order by a.k1,b.k1"
    runner.check(line.format(left_table=table_1, null_table=table_2))
    line = "select * from {left_table} a join {null_table} b on a.k2<=> b.k2 and \
       a.k4<=>NULL  and b.k4 is not NULL and a.k3=2 order by a.k1,b.k1"
    runner.check(line.format(left_table=table_1, null_table=table_2))
    line = "select * from {left_table} a join {null_table} b on ifnull(a.k4,null)\
       <=> ifnull(b.k5,null) order by a.k1, a.k2, a.k3, b.k1, b.k2"
    runner.check(line.format(left_table=table_1, null_table=table_2))

    line = "drop table {table_name}"
    runner.init(line.format(table_name=table_1))
    runner.init(line.format(table_name=table_2))


def test_join_null_string():
    """
    {
    "title": "test_query_join.test_join_null_value",
    "describe": "test join null_value, github issue #6305",
    "tag": "function,p1"
    }
    """
    table_1 = 'table_join_null_string_1'
    table_2 = 'table_join_null_string_2'
    sql = "drop table %s if exists" % table_1
    runner.init(sql)
    sql = "drop table %s if exists" % table_2
    runner.init(sql)
    sql = "create table %s (a int, b varchar(11)) distributed by hash(a) buckets 3" % table_1
    runner.init(sql)
    sql = "create table %s (a int, b varchar(11)) distributed by hash(a) buckets 3" % table_2
    runner.init(sql)
    sql = "insert into %s values (1,'a'),(2,'b'),(3,'c'),(4,NULL)" % table_1
    runner.init(sql)
    sql = "insert into %s values (1,'a'),(2,'b'),(3,'c'),(4,NULL)" % table_2
    runner.init(sql)
    sql_1 = "select count(*) from %s join %s where %s.b = %s.b" % (table_1, table_2, table_1, table_2)
    sql_2 = "select 3"
    runner.check2(sql_1, sql_2)
    sql = "drop table %s" % table_1
    runner.init(sql)
    sql = "drop table %s" % table_2
    runner.init(sql)


def test_issue_6171():
    """
    {
    "title": "test_query_join.test_issue_6171",
    "describe": "test bucket shuffle join, github issue #6171",
    "tag": "function,p1"
    }
    """
    sql = 'create database if not exists test_issue_6171'
    runner.checkok(sql)
    table_list = ['T_DORIS_A', 'T_DORIS_B', 'T_DORIS_C', 'T_DORIS_D', 'T_DORIS_E']
    column_list = [',APPLY_CRCL bigint(19)',
                    ',FACTOR_FIN_VALUE decimal(19,2),PRJT_ID bigint(19)',
                    '',
                    ',LIMIT_ID bigint(19),CORE_ID bigint(19)',
                    ',SHARE_ID bigint,SPONSOR_ID bigint']
    for i in range(5):
        sql = "drop table if exists test_issue_6171.%s" % table_list[i]
        runner.checkok(sql)
        sql = 'create table test_issue_6171.%s (ID bigint not null %s) \
                UNIQUE KEY(`ID`) \
                DISTRIBUTED BY HASH(`ID`) BUCKETS 32 \
                PROPERTIES("replication_num"="1");' % (table_list[i], column_list[i])
        runner.checkok(sql)
    sql = 'desc SELECT B.FACTOR_FIN_VALUE, D.limit_id FROM test_issue_6171.T_DORIS_A A LEFT JOIN ' \
          'test_issue_6171.T_DORIS_B B ON B.PRJT_ID = A.ID LEFT JOIN test_issue_6171.T_DORIS_C C ' \
          'ON A.apply_crcl = C.id JOIN test_issue_6171.T_DORIS_D D ON C.ID = D.CORE_ID order by '\
          'B.FACTOR_FIN_VALUE, D.limit_id desc'
    ret = runner.execute_sql(sql)
    expect = 'join op: INNER JOIN(BROADCAST)'
    flag = False
    for msg in ret:
        if msg[0].find(expect) != -1:
            flag = True
    assert flag, 'Error plan in bucket shuffle join'
    sql = 'drop database test_issue_6171'
    runner.checkok(sql)


"""
没有多表的不同join类型的join，多表的join只有基本的on等值join
"""


def teardown_module():
    """
    end
    """
    print("End")
    # mysql_cursor.close()
    # mysql_con.close()

if __name__ == '__main__':
    setup_module()
    # test_join()
    # test_inner_join()
    test_left_join()
    # test_left_outer_join()
    # test_right_join()
    # test_right_outer_join()
    # test_full_outer_join()
    # test_cross_join()
    # 有问题 12结果check不对，13多次执行可结果不同
    # test_left_semi_join()
    # test_right_semi_join()
    # left anti join和right的有一个结果是不对的，right的结果应该不对
    # test_left_anti_join()
    # test_right_anti_join() 
    # test_no_join()
    # test_empty_join()
    #test_join_other()
    # test_join_basic()
    # test_join_complex()
    #test_join_null_table()
    #test_query_null_safe_equal()
    #test_query_join_null_safe_equal()
    #test_join_null_value()

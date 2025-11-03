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
test_query_complicate.py
"""

import sys
import time
sys.path.append("../lib/")

from palo_qe_client import QueryBase
import query_util as util

sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage

table_name = "test"
join_name = "baseall"


def setup_module():
    """
    init config
    """
    global runner
    runner = QueryExecuter()


class QueryExecuter(QueryBase):
    """query executer"""
    def __init__(self):
        super().__init__()

    def check(self, line, force_order=False, check_view=True):
        """
        check if result of mysql and palo is same
        """
        print(line)
        LOG.info(L('mysql sql', mysql_sql=line))
        times = 0
        flag = 0
        self.mysql_cursor.execute(line)
        mysql_result = self.mysql_cursor.fetchall()
        palo_view = 'create view v_complicate as %s' % line
        view_select = 'select * from v_complicate'
        QUIT_CON = 1
        if check_view:
            QUIT_CON = 2
        while (times <= 10) and (flag < QUIT_CON):
            try:
                LOG.info(L('palo sql', palo_sql=line))
                palo_result = self.query_palo.do_sql(line)
                util.check_same(palo_result, mysql_result, force_order=True)
                flag += 1
            except Exception as e:
                print(e)
                time.sleep(1)
                times += 1
                if times == 3:
                    LOG.error(L('do sql error', sql=line, error=e))
                    assert 0 == 1, 'do sql error: %s' % line
            if check_view:
                assert self.query_palo.do_sql('drop view if exists v_complicate') == (), 'drop view error'
                assert self.query_palo.do_sql(palo_view) == (), 'create view error : %s' % palo_view
                try:
                    LOG.info(L('palo view sql', sql=view_select))
                    palo_view_result = self.query_palo.do_sql(view_select)
                    util.check_same(palo_view_result, mysql_result, force_order=True)
                    flag += 1
                except Exception as e:
                    print(Exception, "2:", e)
                    LOG.error(L('do view sql error', error=e))
                    time.sleep(1)
                    times += 1
                    if times == 3:
                        assert 0 == 1, 'do view sql error'


def test_complicate_join():
    """
    {
    "title": "test_query_complicate.test_complicate_join",
    "describe": "complicate query about join",
    "tag": "p1,function"
    }
    """
    """complicate query about join"""
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, a.k7 ak7, a.k8 ak8,\
            a.k9 ak9, a.k10 ak10, a.k11 ak11, b.k1 bk1, b.k2 bk2, b.k3 bk3, b.k4 bk4, b.k5 bk5, \
            b.k6 bk6, b.k7 bk7, b.k8 bk8, b.k9 bk9, b.k10 bk10, b.k11 bk11 \
            from test a join bigtable b on a.k1 = b.k1 join baseall on a.k2 = b.k2 \
            where a.k2 > 0 and b.k1 > 0 order by a.k1, a.k2 '
    runner.check(line)
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, a.k7 ak7, a.k8 ak8,\
            a.k9 ak9, a.k10 ak10, a.k11 ak11, b.k1 bk1, b.k2 bk2, b.k3 bk3, b.k4 bk4, b.k5 bk5, \
            b.k6 bk6, b.k7 bk7, b.k8 bk8, b.k9 bk9, b.k10 bk10, b.k11 bk11 \
            from test a join bigtable b on a.k1 = b.k1 join baseall on a.k2 = b.k2 \
            having a.k2 > 0 and b.k1 > 0 order by a.k1, a.k2, a.k3, a.k4'
    runner.check(line)
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, a.k7 ak7, a.k8 ak8,\
            a.k9 ak9, a.k10 ak10, a.k11 ak11, b.k1 bk1, b.k2 bk2, b.k3 bk3, b.k4 bk4, b.k5 bk5, \
            b.k6 bk6, b.k7 bk7, b.k8 bk8, b.k9 bk9, b.k10 bk10, b.k11 bk11,  c.k1 ck1, c.k2 ck2,\
            c.k3 ck3, c.k4 ck4, c.k5 ck5, c.k6 ck6, c.k7 ck7, c.k8 ck8, c.k9 ck9, c.k10 ck10, c.k11 ck11\
            from test a left outer join bigtable b on a.k1 = b.k1 left outer join baseall c \
            on a.k2 = c.k2 where a.k2 > 0 and a.k1 > 0 order by a.k1, a.k2, a.k3, a.k4'
    runner.check(line)
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, a.k7 ak7, a.k8 ak8,\
            a.k9 ak9, a.k10 ak10, a.k11 ak11, b.k1 bk1, b.k2 bk2, b.k3 bk3, b.k4 bk4, b.k5 bk5, \
            b.k6 bk6, b.k7 bk7, b.k8 bk8, b.k9 bk9, b.k10 bk10, b.k11 bk11,  c.k1 ck1, c.k2 ck2,\
            c.k3 ck3, c.k4 ck4, c.k5 ck5, c.k6 ck6, c.k7 ck7, c.k8 ck8, c.k9 ck9, c.k10 ck10, \
            c.k11 ck11 from test a left outer join bigtable b on a.k1 = b.k1 left outer join \
            baseall c on a.k2 = c.k2 having a.k2 > 0 and b.k1 > 0 and c.k3 != 0 order by \
            a.k1, a.k2, a.k3, a.k4'
    runner.check(line)
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, a.k7 ak7, a.k8 ak8,\
            a.k9 ak9, a.k10 ak10, a.k11 ak11, c.k1 ck1, c.k2 ck2, c.k3 ck3, c.k4 ck4, c.k5 ck5, \
            c.k6 ck6, c.k7 ck7, c.k8 ck8, c.k9 ck9, c.k10 ck10, c.k11 ck11 \
            from test a join (select * from test where k2 > 0 and k1 = 1) c \
            on a.k1 = c.k1 and a.k2 = c.k2 where c.k2 > 10 order by a.k1, a.k2, a.k3, a.k4'
    runner.check(line)
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, a.k7 ak7, a.k8 ak8,\
            a.k9 ak9, a.k10 ak10, a.k11 ak11, c.k1 ck1, c.k2 ck2, c.k3 ck3, c.k4 ck4, c.k5 ck5, \
            c.k6 ck6, c.k7 ck7, c.k8 ck8, c.k9 ck9, c.k10 ck10, c.k11 ck11 \
            from test a right outer join (select * from test where k2 > 0 and k1 = 1) c \
            on a.k1 = c.k1 and a.k2 = c.k2 having c.k2 > 10 order by a.k1, a.k2, a.k3, a.k4'
    runner.check(line)
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, a.k7 ak7, a.k8 ak8,\
            a.k9 ak9, a.k10 ak10, a.k11 ak11, c.k1 ck1, c.k2 ck2, c.k3 ck3, c.k4 ck4\
            from test a join (select 1 k1, 2 k2, 3 k3, 4 k4 union select k1, k2, k3, k4 from baseall)\
            c on a.k1 = c.k1 where a.k2 = c.k2 or c.k2 = 2 order by a.k1, a.k2, a.k3, a.k4, c.k1'
    runner.check(line)
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, a.k7 ak7, a.k8 ak8,\
            a.k9 ak9, a.k10 ak10, a.k11 ak11, c.k1 ck1, c.k2 ck2, c.k3 ck3, c.k4 ck4\
            from test a left join (select 1 k1, 2 k2, 3 k3, 4 k4 union select k1, k2, k3, k4 from \
            baseall) c on a.k1 = c.k1 having a.k2 = c.k2 or c.k2 = 2 order by \
            a.k1, a.k2, a.k3, a.k4, c.k1'
    runner.check(line)
    line = 'select a.k2 ak2, c.k2 ck2, count(*) from test a left join (select 1 k1, 2 k2, 3 k3, 4 k4 \
            union select k1, k2, k3, k4 from baseall) c on a.k1 = c.k1 group by a.k2, c.k2 \
            having a.k2 = c.k2 or c.k2 = 2 order by ak2, ck2'
    runner.check(line)
    line = 'select sum(a.k2) s1, sum(b.k2) s2 from test a left outer join bigtable b on a.k1 = b.k1\
            left outer join baseall c on a.k2 = c.k2 where a.k2 > 0 and a.k1 > 0 group by a.k1 \
            order by s1, s2'
    runner.check(line)


def test_complicate_subquery():
    """
    {
    "title": "test_query_complicate.test_complicate_subquery",
    "describe": "complicate query about subquery",
    "tag": "p1,function"
    }
    """
    """complicate query about subquery"""
    line = 'select * from (select * from test where k1 % 3 = 0 and k2 % 5 = 0 and k3 % 10 = 0) c \
            where k1 in (select k1 from baseall) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from (select * from test where k1 % 3 = 0 and k2 % 5 = 0 and k3 % 10 = 0 \
            union select * from test where k1 % 3 = 1 and k2 % 5 = 1 and k3 % 10 = 1) c \
            where k1 > 0 order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from (select k6, k3, sum(k1) over(partition by k6 order by k3) s1, \
            count(k2) over(partition by k6 order by k4) c1, \
            rank() over(partition by k6 order by k3) r1 from baseall where k3 > 0) win \
            order by k6, k3, s1, c1, r1'
    runner.checkok(line)
    # win_check(line)
    line = 'select sum(s1) from (select k6, k3, sum(k1) over(partition by k6 order by k3) s1, \
            count(k2) over(partition by k6 order by k4) c1, \
            rank() over(partition by k6 order by k3) r1 from baseall where k3 > 0) win'
    runner.checkok(line)
    # win_check(line)
    line = 'select * from baseall where k1 in (select r1 from (select k6, k3, \
            sum(k1) over(partition by k6 order by k3) s1, \
            count(k2) over(partition by k6 order by k4) c1, \
            rank() over(partition by k6 order by k3) r1 from baseall where k3 > 0) a) order by k1'
    # line = 'select * from baseall where k1 < 8 order by k1'
    runner.checkok(line)
    line = 'select * from (select * from test where k1 % 3 = 0 and k2 % 5 = 0 and k3 % 10 = 0) c \
            where k1 in (select k1 from baseall) having k1 % 3 = 0 order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from (select k2, max(k3) k3 from baseall group by k2) a order by k2'
    runner.check(line)
    line = 'select * from test where k3 in (select max(k3) from baseall group by k2) order by k1'
    runner.check(line)
    line = 'select * from test where k3 in (select max(k3) from baseall group by k2) \
            having k1 % 3 = 0 order by k1'
    runner.check(line)


def test_complicate_union():
    """
    {
    "title": "test_query_complicate.test_complicate_union",
    "describe": "complicate query about union",
    "tag": "p1,function"
    }
    """
    """complicate query about union"""
    line1 = '(select a.k1 k1, a.k2 k2, a.k3 k3, a.k4 k4, b.k1 k5, b.k2 k6, b.k3 k7, b.k4 k8 \
            from test a right join baseall b on a.k2 = b.k2) union (select a.k1 k1, a.k2 k2, \
            a.k3 k3, a.k4 k4, b.k1 k5, b.k2 k6, b.k3 k7, b.k4 k8 from test a \
            left join baseall b on a.k2 = b.k2) order by k1, k2, k3, k4, k5, k6, k7, k8'
    runner.check(line1)
    line2 = 'select k1, k2 from ((select k1, k2 from test) union (select k1, k2 from \
            (select k3 as k1, k4 as k2 from baseall) b) union (select 0, 0)) c order by k1, k2'
    runner.check(line2)
    line3 = 'select k6, k3, rank() over (partition by k6 order by k3) wj from baseall union all \
            (select k6, -1, count(k1) from baseall group by k6) order by k6, k3, wj'
    runner.checkok(line3)
    # win_check(line3)
    line4 = '(select k1, k2 from (select * from baseall where k2 > 0) a) union (select k1, k2 \
            from test where k1 = (select max(k1) from baseall)) union (select a.k1, a.k2 \
            from test a left outer join bigtable b on a.k1 = b.k1 having k1 > 0 and k2 > 0)'
    runner.check(line4)
    line = 'select * from (%s) t'
    runner.check(line % line1)
    runner.check(line % line2)
    runner.checkok(line % line3)
    runner.check(line % line4)


def test_complicate_win():
    """
    {
    "title": "test_query_complicate.test_complicate_win",
    "describe": "complicate query about win function",
    "tag": "p1,function"
    }
    """
    """complicate query about win function"""
    line = 'select a.k6, a.k3, b.k1, sum(b.k1) over (partition by a.k6 order by a.k3) w_sum \
            from baseall a join bigtable b on a.k1 = b.k1 + 5 order by a.k6, a.k3, a.k1, w_sum'
    runner.checkok(line)
    line = 'select a.k6, a.k3, b.k1, sum(b.k1) over (partition by a.k6 order by a.k3) w_sum \
            from baseall a left join bigtable b on a.k1 = b.k1 + 5 order by 1, 2, 3, 4'
    runner.checkok(line)
    line = 'select k6, k3, k1, sum(k1) over (partition by a.k6 order by k3) w_sum\
            from (select "oh" as k6, k3, k1 from baseall union select k6, k3, k1 from baseall) a \
            order by k6, k3, k1, w_sum'
    runner.checkok(line)
    line = 'select k6, k3, k10, first_value(k10) over (partition by k6 order by k3) w_first \
            from (select * from baseall) a order by 1, 2, 3, 4'
    runner.checkok(line)
    line = 'select k6, k3, k10, first_value(k10) over (partition by k6 order by k3) w_first \
            from baseall having k3 > 0 and k3 < 10000 order by 1, 2, 3, 4'
    runner.checkok(line)
    line = 'select k6, k3, k10, first_value(k10) over (partition by k6 order by k3) w_frist \
            from baseall where k3 > 0 and k3 < 10000 order by 1, 2, 3, 4'
    runner.checkok(line)
    line = 'select k6, k3, k10, first_value(k10) over (partition by k6 order by k3) w_first \
            from baseall having k6 = "false" order by 1, 2, 3, 4'
    runner.checkok(line)
    line = 'select * from (select a.k6, a.k3, b.k1, sum(b.k1) over \
            (partition by a.k6 order by a.k3) w_sum from baseall a join bigtable b \
            on a.k1 = b.k1 + 5) w order by k6, k3, k1, w_sum'
    runner.checkok(line)
    line = 'select * from (select a.k6, a.k3, b.k1, sum(b.k1) over (partition by a.k6 \
            order by a.k3) w_sum from baseall a left join bigtable b on a.k1 = b.k1 + 5) w \
            order by k6, k3, k1, w_sum'
    runner.checkok(line)
    line = 'select * from (select k6, k3, k1, sum(k1) over (partition by a.k6 order by k3) w_sum \
            from (select "oh" as k6, k3, k1 from baseall union select k6, k3, k1 from baseall) a) w \
            order by k6, k3, k1, w_sum'
    runner.checkok(line)
    line = 'select * from (select k6, k3, k10, first_value(k10) over (partition by k6 order by k3) \
            w_first from (select * from baseall) a) w order by k6, k3, k10, w_first'
    runner.checkok(line)
    line = 'select * from (select k6, k3, k10, last_value(k10) over (partition by k6 order by k3) \
            w_last from baseall having k3 > 0) w order by k6, k3, k10, w_last'
    runner.checkok(line)


def test_complicate_group():
    """
    {
    "title": "test_query_complicate.test_complicate_group",
    "describe": "complicate query about group",
    "tag": "p1,function"
    }
    """
    """complicate query about group"""
    line = 'select a.k2 ak2, a.k1 ak1, b.k2 bk2, b.k1 bk1 from (select k2, avg(k1) k1 from baseall group by k2) a join \
            (select k2, count(k1) k1 from test group by k2) b on a.k2 = b.k2 order by ak2'
    runner.check(line)
    line = 'select a.k2 ak2, a.k1 ak1, b.k2 bk2, b.k1 bk1 from (select k2, avg(k1) k1 from baseall group by k2) a left join \
            (select k2, count(k1) k1 from test group by k2) b on a.k2 = b.k2 order by ak2'
    runner.check(line)
    line = 'select * from ((select k2, avg(k1) k1 from baseall group by k2) union \
            (select k2, count(k1) k1 from test group by k2) )b order by k2'
    runner.check(line)
    line = 'select k2, count(k1) from ((select k2, avg(k1) k1 from baseall group by k2) \
            union all (select k2, count(k1) k1 from test group by k2) )b group by k2 \
            having k2 > 0 order by k2'
    runner.check(line)
    line = 'select * from (select k1, count(k2) k2 from test group by k1) a order by k1'
    runner.check(line)
    line = 'select a.k1 ak1, b.k1 bk1, sum(b.k2) sumk2, max(a.k2) maxk2 from test a join baseall b on \
            a.k1 = b.k1 group by a.k1, b.k1 order by 1, 2, 3, 4'
    runner.check(line)
    line = 'select k1, count(k2) k2 from ((select k1, k2 from test) union all \
            (select k1, k2 from baseall)) a group by k1 order by k1'
    runner.check(line)
    line = 'select k1, count(k2) k2 from test where k2 in (select k2 from baseall group by k2) \
            group by k1 order by k1'
    runner.check(line)
    line = 'select wj, max(k1) m from (select k1, count(k2) wj from test group by k1) a \
            where wj > 0 group by wj order by 1, 2'
    runner.check(line)


def teardown_module():
    """tearDown"""
    print("End")


if __name__ == "__main__":
    print("test")
    setup_module()
    test_complicate_union()

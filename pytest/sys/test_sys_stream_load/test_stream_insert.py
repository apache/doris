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
file:test_stream_insert.py
测试insert [streaming] select 复杂query
"""
import os
import sys
import pytest

file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

from data import schema as DATA
import palo_config
import palo_client
import util

table_test = "test_query_qa.test"
table_base = "test_query_qa.baseall"
table_big = "test_query_qa.bigtable"
config = palo_config.config


def setup_module():
    """
    init config
    """
    global client
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    client.init()


def check(table_name, select_line):
    """
    check if result of mysql and palo is same
    1. select
    2. view
    3. check
    """
    insert_sql = 'insert into %s %s' % (table_name, select_line)
    ret = client.execute(insert_sql)
    assert ret == ()
    check_sql = 'select * from %s' 
    insert_result = client.execute(check_sql % table_name)
    orgin_restule = client.execute(select_line)
    # util.check(insert_result, orgin_restule, force_order=True)
    delete_sql = 'delete from %s partition %s where k1 is not null'


def test_insert_join():
    """
    {
    "title": "test_stream_insert.test_insert_join",
    "describe": "complicate query about join",
    "tag": "system,p1"
    }
    """
    """complicate query about join"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list, 
                              keys_desc=DATA.baseall_duplicate_key,
                              distribution_info=DATA.baseall_distribution_info, 
                              set_null=True)
    assert ret
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, \
            b.k10 bk10, b.k11 bk11, b.k7 bk7, b.k8 bk8, b.k9 bk9 \
            from {test} a join {big} b on a.k1 = b.k1 join {base} c on c.k2 = b.k2 \
            where a.k2 > 0 and b.k1 > 0 order by a.k1, a.k2'.format(test=table_test, 
                                                                    big=table_big, base=table_base)
    check(table_name, line)
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, \
            b.k10 bk10, b.k11 bk11, b.k7 bk7, b.k8 bk8, b.k9 bk9 \
            from {test} a join {big} b on a.k1 = b.k1 join {base} c on c.k2 = b.k2 \
            having a.k2 > 0 and b.k1 > 0 order by a.k1, a.k2, a.k3, \
            a.k4'.format(test=table_test, big=table_big, base=table_base)
    check(table_name, line)
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, \
            b.k10 bk10, b.k11 bk11, c.k7 ck7, c.k8 ck8, c.k9 ck9 \
            from {test} a left outer join {big} b on a.k1 = b.k1 left outer join {base} c \
            on a.k2 = c.k2 where a.k2 > 0 and a.k1 > 0 order by \
            a.k1, a.k2, a.k3, a.k4'.format(test=table_test, big=table_big, base=table_base)
    check(table_name, line)
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, \
            c.k10 ck10, c.k11 ck11, c.k7 ck7, c.k8 ck8, c.k9 ck9 \
            from {test} a left outer join {big} b on a.k1 = b.k1 left outer join \
            {base} c on a.k2 = c.k2 having a.k2 > 0 and b.k1 > 0 and c.k3 != 0 order by \
            a.k1, a.k2, a.k3, a.k4'.format(test=table_test, big=table_big, base=table_base)
    check(table_name, line)
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, \
            c.k10 ck10, c.k11 ck11, c.k7 ck7, c.k8 ck8, c.k9 ck9 \
            from {test} a join (select * from {test} where k2 > 0 and k1 = 1) c \
            on a.k1 = c.k1 and a.k2 = c.k2 where c.k2 > 10 order by a.k1, a.k2, a.k3, \
            a.k4'.format(test=table_test, big=table_big, base=table_base)
    check(table_name, line)
    line = 'select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, \
            c.k10 ck10, c.k11 ck11, c.k7 ck7, c.k8 ck8, c.k9 ck9 \
            from {test} a right outer join (select * from {test} where k2 > 0 and k1 = 1) c \
            on a.k1 = c.k1 and a.k2 = c.k2 having c.k2 > 10 \
            order by a.k1, a.k2, a.k3, a.k4'.format(test=table_test, big=table_big, 
                                                    base=table_base)
    check(table_name, line)
    line = 'select c.k1 ak1, c.k2 ak2, c.k3 ak3, c.k4 ak4, a.k5 ak5, a.k6 ak6, a.k10 ak10, \
            a.k11 ak11, a.k7 ak7, a.k8 ak8, a.k9 ak9 from {test} a \
            join (select 1 k1, 2 k2, 3 k3, 4 k4 union select k1, k2, k3, k4 from {base}) \
            c on a.k1 = c.k1 where a.k2 = c.k2 or c.k2 = 2 \
            order by a.k1, a.k2, a.k3, a.k4, c.k1'.format(test=table_test, 
                                                   big=table_big, base=table_base)
    check(table_name, line)
    line = 'select c.k1 ak1, c.k2 ak2, c.k3 ak3, c.k4 ak4, a.k5 ak5, a.k6 ak6, a.k10 ak10, a.k11 ak11, \
             a.k7 ak7, a.k8 ak8, a.k9 ak9 \
            from {test} a left join (select 1 k1, 2 k2, 3 k3, 4 k4 union select k1, k2, k3, k4 from \
            {base}) c on a.k1 = c.k1 having a.k2 = c.k2 or c.k2 = 2 order by \
            a.k1, a.k2, a.k3, a.k4, c.k1'.format(test=table_test, big=table_big, base=table_base)
    check(table_name, line)
    # schame not fix
    line = 'select a.k2 ak2, c.k2 ck2, count(*) from {test} a left join (select 1 k1, 2 k2, 3 k3, 4 k4 \
            union select k1, k2, k3, k4 from {base}) c on a.k1 = c.k1 group by a.k2, c.k2 \
            having a.k2 = c.k2 or c.k2 = 2 order by ak2, ck2'
    # check(line)
    line = 'select sum(a.k2) s1, sum(b.k2) s2 from test a left outer join bigtable b on a.k1 = b.k1\
            left outer join baseall c on a.k2 = c.k2 where a.k2 > 0 and a.k1 > 0 group by a.k1 \
            order by s1, s2'
    # check(line)
    client.clean(database_name)


def test_insert_subquery():
    """
    {
    "title": "complicate query about subquery",
    "describe": "describe",
    "tag": "system,p1"
    }
    """
    """complicate query about subquery"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              keys_desc=DATA.baseall_duplicate_key,
                              distribution_info=DATA.baseall_distribution_info,
                              set_null=True)
    assert ret
    line = 'select * from (select * from {test} where k1 % 3 = 0 and k2 % 5 = 0 and k3 % 10 = 0) c \
            where k1 in (select k1 from {base}) order by k1, k2, k3, k4'.format(test=table_test, 
                                                                                base=table_base)
    check(table_name, line)
    line = 'select * from (select * from {test} where k1 % 3 = 0 and k2 % 5 = 0 and k3 % 10 = 0 \
            union select * from {test} where k1 % 3 = 1 and k2 % 5 = 1 and k3 % 10 = 1) c \
            where k1 > 0 order by k1, k2, k3, k4'.format(test=table_test)
    check(table_name, line)
    line = 'select * from {base} where k1 in (select r1 from (select k6, k3, \
            sum(k1) over(partition by k6 order by k3) s1, \
            count(k2) over(partition by k6 order by k4) c1, \
            rank() over(partition by k6 order by k3) r1 from {base} where k3 > 0) a) \
            order by k1'.format(test=table_test, base=table_base)
    check(table_name, line)
    line = 'select * from (select * from {test} where k1 % 3 = 0 and k2 % 5 = 0 and k3 % 10 = 0) c \
            where k1 in (select k1 from {base}) having k1 % 3 = 0 \
            order by k1, k2, k3, k4'.format(test=table_test, base=table_base)
    check(table_name, line)
    line = 'select * from (select min(k1), k2, max(k3), max(k4), sum(k5), max(k6), max(k10), \
            min(k11), min(k7), sum(k8), sum(k9) from {base} group by k2) a \
            order by k2'.format(base=table_base)
    check(table_name, line)

    line = 'select * from {test} where k3 in (select max(k3) from {base} group by k2) \
            order by k1'.format(test=table_test, base=table_base)
    check(table_name, line)
    line = 'select * from {test} where k3 in (select max(k3) from {base} group by k2) \
            having k1 % 3 = 0 order by k1'.format(test=table_test, base=table_base)
    check(table_name, line)
    client.clean(database_name)


def test_insert_union():
    """
    {
    "title": "complicate query about union",
    "describe": "complicate query about union",
    "tag": "system,p1"
    }
    """
    """complicate query about union"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              keys_desc=DATA.baseall_duplicate_key,
                              distribution_info=DATA.baseall_distribution_info,
                              set_null=True)
    line = 'select * from {base} union select 0, null, null, null, null, null, null, null, null, \
            null, null'.format(base=table_base)
    check(table_name, line)
    line = 'select * from {base} union all select * from {base}'.format(base=table_base)
    check(table_name, line)
    line = 'select * from {base} union select 1, null, null, null, null, null, null, null, null, \
            null, null order by k1, k2, k3, k4, k5, k6, k7, k8, k9 \
            limit 10'.format(base=table_base)
    check(table_name, line)
    client.clean(database_name)


def test_insert_win():
    """
    {
    "title": "complicate query about win function",
    "describe": "complicate query about win function",
    "tag": "system,p1"
    }
    """
    """complicate query about win function"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    sql = 'CREATE TABLE %s ( k1 char(5) NULL, k2 int NULL, k3 tinyint NULL, k4 int NULL) \
           DUPLICATE KEY(k1,k2,k3,k4) DISTRIBUTED BY HASH(k1) BUCKETS 5' % table_name
    ret = client.execute(sql)
    assert ret == ()
    line = 'select a.k6, a.k3, b.k1, sum(b.k1) over (partition by a.k6 order by a.k3) w_sum \
            from {base} a join {big} b on a.k1 = b.k1 + 5 \
            order by a.k6, a.k3, a.k1, w_sum'.format(big=table_big, base=table_base)
    check(table_name, line)
    line = 'select a.k6, a.k3, b.k1, sum(b.k1) over (partition by a.k6 order by a.k3) w_sum \
            from {base} a left join {big} b on a.k1 = b.k1 + 5 \
            order by 1, 2, 3, 4'.format(big=table_big, base=table_base)
    check(table_name, line)
    line = 'select k6, k3, k1, sum(k1) over (partition by a.k6 order by k3) w_sum\
            from (select "oh" as k6, k3, k1 from {base} union select k6, k3, k1 from {base}) a \
            order by k6, k3, k1, w_sum'.format(base=table_base)
    check(table_name, line)
    line = 'select k6, k3, k10, first_value(k10) over (partition by k6 order by k3) w_first \
            from (select * from {base}) a order by 1, 2, 3, 4'.format(base=table_base)
    check(table_name, line)
    line = 'select k6, k3, k10, first_value(k10) over (partition by k6 order by k3) w_first \
            from {base} having k3 > 0 and k3 < 10000 order by 1, 2, 3, 4'.format(base=table_base)
    check(table_name, line)
    line = 'select k6, k3, k10, first_value(k10) over (partition by k6 order by k3) w_frist \
            from {base} where k3 > 0 and k3 < 10000 order by 1, 2, 3, 4'.format(base=table_base)
    check(table_name, line)
    line = 'select k6, k3, k10, first_value(k10) over (partition by k6 order by k3) w_first \
            from {base} having k6 = "false" order by 1, 2, 3, 4'.format(base=table_base)
    check(table_name, line)
    line = 'select * from (select a.k6, a.k3, b.k1, sum(b.k1) over \
            (partition by a.k6 order by a.k3) w_sum from {base} a join {big} b \
            on a.k1 = b.k1 + 5) w order by k6,k3,k1,w_sum'.format(big=table_big, base=table_base)
    check(table_name, line)
    line = 'select * from (select a.k6, a.k3, b.k1, sum(b.k1) over (partition by a.k6 \
            order by a.k3) w_sum from {base} a left join {big} b on a.k1 = b.k1 + 5) w \
            order by k6, k3, k1, w_sum'.format(big=table_big, base=table_base)
    check(table_name, line)
    line = 'select * from (select k6, k3, k1, sum(k1) over (partition by a.k6 order by k3) w_sum \
            from (select "oh" as k6, k3, k1 from {base} union select k6, k3, k1 from {base}) a) w \
            order by k6, k3, k1, w_sum'.format(base=table_base)
    check(table_name, line)
    line = 'select * from (select k6, k3, k10, first_value(k10) over (partition by k6 order by k3) \
            w_first from (select * from {base}) a) w \
            order by k6, k3, k10, w_first'.format(base=table_base)
    check(table_name, line)
    line = 'select * from (select k6, k3, k10, last_value(k10) over (partition by k6 order by k3) \
            w_last from {base} having k3 > 0) w \
            order by k6, k3, k10, w_last'.format(base=table_base)
    check(table_name, line)
    client.clean(database_name)


def test_insert_group():
    """
    {
    "title": "test_stream_insert.test_insert_group",
    "describe": "complicate query about group",
    "tag": "system,p1"
    }
    """
    """complicate query about group"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    sql = 'CREATE TABLE %s ( k1 int NULL, k2 decimal(20, 7) NULL) \
           DUPLICATE KEY(k1,k2) DISTRIBUTED BY HASH(k1) BUCKETS 5' % table_name
    client.execute(sql)
    line = 'select a.k2 ak2, b.k1 bk1 from (select k2, avg(k1) k1 from {base} group by k2) a join \
            (select k2, count(k1) k1 from {test} group by k2) b on a.k2 = b.k2 \
            order by ak2'.format(test=table_test, base=table_base)
    check(table_name, line)
    line = 'select a.k2 ak2, b.k1 bk1 from (select k2, avg(k1) k1 from {base} group by k2) a \
            left join (select k2, count(k1) k1 from {test} group by k2) b on a.k2 = b.k2 \
            order by ak2'.format(test=table_test, base=table_base)
    check(table_name, line)
    line = 'select * from ((select k2, avg(k1) k1 from {base} group by k2) union \
            (select k2, count(k1) k1 from {test} group by k2) )b \
            order by k2'.format(test=table_test, base=table_base)
    check(table_name, line)
    line = 'select k2, count(k1) from ((select k2, avg(k1) k1 from {base} group by k2) \
            union all (select k2, count(k1) k1 from {test} group by k2) )b group by k2 \
            having k2 > 0 order by k2'.format(test=table_test, base=table_base)
    check(table_name, line)
    line = 'select * from (select k1, count(k2) k2 from {test} group by k1) a \
            order by k1'.format(test=table_test)
    check(table_name, line)
    line = 'select a.k1 ak1, max(a.k2) maxk2 from {test} a join {base} b on \
            a.k1 = b.k1 group by a.k1, b.k1 order by 1, 2'.format(base=table_base, 
                                                                        test=table_test)
    check(table_name, line)
    line = 'select k1, count(k2) k2 from ((select k1, k2 from {test}) union all \
            (select k1, k2 from {base})) a group by k1 order by k1'.format(base=table_base, 
                                                                           test=table_test)
    check(table_name, line)
    line = 'select k1, count(k2) k2 from {test} where k2 in (select k2 from {base} group by k2) \
            group by k1 order by k1'.format(base=table_base, test=table_test)
    check(table_name, line)
    line = 'select wj, max(k1) m from (select k1, count(k2) wj from {test} group by k1) a \
            where wj > 0 group by wj order by 1, 2'.format(test=table_test)
    check(table_name, line)
    client.clean(database_name)


def teardown_module():
    """tearDown"""
    print("End")


if __name__ == '__main__':
    setup_module()
    test_insert_join()


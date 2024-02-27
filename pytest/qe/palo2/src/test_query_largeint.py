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
file : test_query_largeint.py
test largeint datatype, and no change to mysql comparing data
"""
import sys
import time
sys.path.append("../lib/")
from palo_qe_client import QueryBase

sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage

table_name = "test"
join_name = "baseall"
largeint_name = "largeint_s"


def setup_module():
    """
    init config
    """
    global runner
    runner = QueryExecuter()
    # init largeint table and data
    init()
    time.sleep(10)


def init():
    """init largeint table"""
    schema = 'create table %s.largeint_s(k1 tinyint, k2 smallint, k3 bigint, \
             k4 largeint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), \
             k8 double max, k9 float sum) engine=olap distributed by hash(k1) buckets 5 \
             properties("storage_type"="column")' % runner.query_db
    mysql_schema = 'create table %s.largeint_s(k1 tinyint, k2 smallint, k3 bigint, \
             k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), \
             k8 double, k9 float)' % runner.query_db

    data = 'insert into %s select * from %s' % (largeint_name, table_name)
    try:
        runner.query_palo.do_sql('drop table if exists %s' % largeint_name)
        runner.query_palo.do_sql(schema)
        runner.query_palo.do_sql(data)
        runner.wait_end()
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(str(e))
        LOG.error(L('err', error=e))
        assert 0 == 1, "largeint init error"


class QueryExecuter(QueryBase):
    def __init__(self):
        self.get_clients()

    def wait_end(self):
        """wait for test_query_qa db load"""
        sql = "SHOW PROC '/dbs'"
        LOG.info(L('palo sql', palo_sql=sql))
        database_list = self.query_palo.do_sql(sql)
        database_id = None
        for database in database_list:
            db_name = self.query_db
            if self.query_user.find('@') == -1:
                cluster_name = 'default_cluster'
            else:
                tmp_list = self.query_user.split('@')
                cluster_name = tmp_list[1]
            name = '%s:%s' % (cluster_name, db_name)
            if database[1] == db_name or database[1] == name:
                database_id = database[0]
        ret = True
        state = None
        while ret and database_id:
            sql = 'SHOW PROC "/jobs/%s/load"' % database_id
            LOG.info(L('palo sql', palo_sql=sql))
            job_list = self.query_palo.do_sql(sql)
            LOG.info(L('palo result', palo_result=job_list[0]))
            state = job_list[0][2]
            print(state)
            if state == "FINISHED" or state == "CANCELLED":
                ret = False
            time.sleep(1)
        assert state == "FINISHED"


def test_largeint_datatype():
    """
    {
    "title": "test_query_largeint.test_largeint_datatype",
    "describe": "cast test",
    "tag": "function,p1"
    }
    """
    """cast test"""
    line1 = 'select cast(k1 as largeint), cast(k2 as largeint), cast(k3 as largeint) from %s \
             order by 1, 2, 3' % largeint_name
    line2 = 'select cast(k1 as signed), cast(k2 as signed), cast(k3 as signed) from %s \
             order by 1, 2, 3' % table_name
    runner.check2(line1, line2)
    line1 = 'select cast(k10 as largeint), cast(k11 as largeint) from %s order by 1, 2' \
             % largeint_name
    line2 = 'select cast(k10 as unsigned), cast(k11 as unsigned) from %s order by 1, 2' \
             % table_name
    runner.check2(line1, line2)
    line1 = 'select cast(k4 as bigint), cast(k4 as string) from %s order by 1, 2' % largeint_name
    line2 = 'select cast(k4 as signed), cast(k4 as char) from %s order by 1, 2' % table_name
    runner.check2(line1, line2)


def test_largeint_math_operate():
    """
    {
    "title": "test_query_largeint.test_largeint_math_operate",
    "describe": "+ - * / ^ and numeric function",
    "tag": "function,p1"
    }
    """
    """+ - * / ^ and numeric function"""
    line1 = 'select k4 - 0.0, k4 + 0, k4 * 0.8, k4 / k1 from %s order by 1, 2, 3, 4' % largeint_name
    line2 = 'select k4 - 0.0, k4 + 0, k4 * 0.8, k4 / k1 from %s order by 1, 2, 3, 4' % table_name
    runner.check2(line1, line2)
    line1 = 'select k1, k4 from %s where (k4) > 0 order by k1, k4' % largeint_name
    line2 = 'select k1, k4 from %s where (k4) > 0 order by k1, k4' % table_name
    runner.check2(line1, line2)
    line1 = 'select k1, k4 from %s where (k4) <= 0 order by k1, k4' % largeint_name
    line2 = 'select k1, k4 from %s where (k4) <= 0 order by k1, k4' % table_name
    runner.check2(line1, line2)
    line1 = 'select k1, k4 from %s where (k4) >= 0 order by k1, k4' % largeint_name
    line2 = 'select k1, k4 from %s where (k4) >= 0 order by k1, k4' % table_name
    runner.check2(line1, line2)
    line1 = 'select k1, k4 from %s where (k4 / k3) > 0 order by k1, k4' % largeint_name
    line2 = 'select k1, k4 from %s where (k4 / k3) > 0 order by k1, k4' % table_name
    runner.check2(line1, line2)
    line1 = 'select k1, k4 from %s where (k4 ^ k4) > 0 order by k1, k4' % largeint_name
    line2 = 'select k1, k4 from %s where (k4 ^ k4) > 0 order by k1, k4' % table_name
    runner.check2(line1, line2)
    line1 = 'select k4, bin(k4), hex(k4), least(k1, k2, k3, k4), greatest(k1, k2, k3, k4) from %s \
             order by 1, 2, 3, 4, 5' % largeint_name
    line2 = 'select k4, bin(k4), hex(k4), least(k1, k2, k3, k4), greatest(k1, k2, k3, k4) from %s \
             order by 1, 2, 3, 4, 5' % table_name
    runner.check2(line1, line2)
    line1 = 'select ln(k4), log10(k4) from %s where k4 > 0 order by 1, 2' % largeint_name
    line2 = 'select ln(k4), log10(k4) from %s where k4 > 0 order by 1, 2' % table_name
    runner.check2(line1, line2)
    line1 = 'select k1, k4 from %s where k4 in (-9223372036854775807, 9223372036854775807) \
             order by k1, k4' % largeint_name
    line2 = 'select k1, k4 from %s where k4 in (-9223372036854775807, 9223372036854775807) \
             order by k1, k4' % table_name
    runner.check2(line1, line2)
    line1 = 'select k1, k4 from %s where k4 > k3 and k4 < k1 order by k1, k4' % largeint_name
    line2 = 'select k1, k4 from %s where k4 > k3 and k4 < k1 order by k1, k4' % table_name
    runner.check2(line1, line2)
    line1 = 'select k1, k4 from %s where k4 <= k3 or  k4 >= k1 order by k1, k4' % largeint_name
    line2 = 'select k1, k4 from %s where k4 <= k3 or  k4 >= k1 order by k1, k4' % table_name
    runner.check2(line1, line2)


def test_largeint_basic():
    """
    {
    "title": "test_query_largeint.test_largeint_basic",
    "describe": "basic union/join/order/group/having",
    "tag": "function,p1"
    }
    """
    """basic union/join/order/group/having"""
    line1 = 'select k1, k4 from %s union all (select k2, k4 from %s) order by k1, k4 limit 65536' \
             % (largeint_name, largeint_name)
    line2 = 'select k1, k4 from %s union all (select k2, k4 from %s) order by k1, k4 limit 65536' \
             % (table_name, table_name)
    runner.check2(line1, line2)
    line1 = 'select k2, k4 from %s where k1 = 0 union distinct (select k2, k3 from baseall) \
             order by k2, k4' % largeint_name
    line2 = 'select k2, k4 from %s where k1 = 0 union distinct (select k2, k3 from baseall) \
             order by k2, k4' % table_name
    runner.check2(line1, line2)
    line1 = 'select k2, k4 from %s where k1 = 1 union (select 0, 0) order by k2, k4' \
             % largeint_name
    line2 = 'select k2, k4 from %s where k1 = 1 union (select 0, 0) order by k2, k4' \
             % table_name
    runner.check2(line1, line2)
    line1 = 'select * from %s a join %s b on a.k4 = b.k4 where a.k4 > 0 order by a.k1, \
             a.k2, a.k3, a.k4, b.k1, b.k2, b.k3, b.k4' % (largeint_name, join_name)
    line2 = 'select * from %s a join %s b on a.k4 = b.k4 where a.k4 > 0 order by a.k1, \
             a.k2, a.k3, a.k4, b.k1, b.k2, b.k3, b.k4' % (table_name, join_name)
    runner.check2(line1, line2)
    line1 = 'select * from %s a right outer join %s b on a.k4 = b.k4 where a.k4 > 0 \
             order by a.k1, a.k2, a.k3, a.k4, b.k1, b.k2, b.k3, b.k4' \
             % (largeint_name, largeint_name)
    line2 = 'select * from %s a right outer join %s b on a.k4 = b.k4 where a.k4 > 0 \
             order by a.k1, a.k2, a.k3, a.k4, b.k1, b.k2, b.k3, b.k4' % (table_name, table_name)
    runner.check2(line1, line2)
    line1 = 'select max(k1), k4 from %s group by k4 order by 1, 2' % largeint_name
    line2 = 'select max(k1), k4 from %s group by k4 order by 1, 2' % table_name
    runner.check2(line1, line2)
    line1 = 'select max(k4), k1 from %s group by k1 order by 1, 2' % largeint_name
    line2 = 'select max(k4), k1 from %s group by k1 order by 1, 2' % table_name
    runner.check2(line1, line2)
    line1 = 'select max(k1), k4 from %s group by k4 having k4 > 0 order by 1, 2' % largeint_name
    line2 = 'select max(k1), k4 from %s group by k4 having k4 > 0 order by 1, 2' % table_name
    runner.check2(line1, line2)


def test_largeint_bug():
    """
    {
    "title": "test_query_largeint.test_largeint_bug",
    "describe": "bugs",
    "tag": "function,p1"
    }
    """
    line = 'drop view if exists l_view'
    runner.checkok(line)
    line = 'create view l_view as select k1 x, k4 y from %s' % largeint_name
    runner.checkok(line)
    line = 'select * from l_view t1 full outer join l_view t3 on t1.y = t3.x order by t1.x limit 10'
    runner.checkok(line)
    line = 'select count(*) from %s where 10000 in (k4)' % largeint_name
    runner.checkok(line)
    line = '(select k4 from %s) union distinct (select 4)' % largeint_name
    runner.checkok(line)


def teardown_module():
    """
    end
    """
    print("End")
    # mysql_cursor.close()
    # mysql_con.close()


if __name__ == '__main__':
    setup_module()
    test_largeint_basic()
    teardown_module()


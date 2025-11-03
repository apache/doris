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

############################################################################
#
#   @file test_query_GROUPING_sets.py
#   @date 2019-12-04
#   @brief 测试GROUPING sets的相关功能
#
#############################################################################

"""
测试GROUPING sets的相关功能
"""

import sys
sys.path.append('../lib')
from palo_qe_client import QueryBase

test_tb = "test"
baseall_tb = "baseall"
bigtable_tb = "bigtable"


def setup_module():
    """
    init config
    """
    global runner, db
    runner = QueryBase()
    db = runner.query_db

    
def check_rollup(result, rollup_index):
    for item in result:
        if 'TABLE: ' in item[0]:
            if rollup_index in item[0]:
                return True
            else:
                return False
    return False


def test_grouping_sets_base():
    """
    {
    "title": "test_grouping_sets_base",
    "describe": "验证GROUPING sets的基础功能，与union做diff",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test_grouping_sets_base
    验证GROUPING sets的基础功能，与union做diff
    """
    table_name = "{0}.{1}".format(db, baseall_tb)

    # check GROUPING sets
    line = "SELECT k1, k2, SUM( k3 ) FROM {0} GROUP BY GROUPING SETS ( (k1, k2), (k1), (k2), ( ) )" \
           " order by k1, k2".format(table_name)
    line2 = """SELECT * FROM
                (SELECT k1 as k1, k2 as k2, SUM( k3 ) FROM {0} GROUP BY k1, k2 UNION
                 SELECT k1, null, SUM( k3 ) FROM {0} GROUP BY k1 UNION
                 SELECT null, k2, SUM( k3 ) FROM {0} GROUP BY k2 UNION
                 SELECT null, null, SUM( k3 )FROM {0}
                ) t ORDER BY k1, k2""".format(table_name)
    runner.check2_palo(line, line2)

    line = "SELECT k3, SUM( k1+k2 ) FROM {0} GROUP BY GROUPING SETS ( (k3), ( ) )" \
           " order by k3".format(table_name)
    line2 = """SELECT * FROM
                (SELECT k3 as k3, SUM( k1 + k2 ) as s FROM {0} GROUP BY k3 UNION
                 SELECT null, SUM( k1 + k2 ) FROM {0}
                ) t ORDER BY k3""".format(table_name)
    runner.check2_palo(line, line2)

    # check GROUPING sets：互不相交的两个set的组合
    line = "SELECT k1, k2 FROM {0} GROUP BY GROUPING SETS ( (k1), (k2) )" \
           " order by k1, k2".format(table_name)
    line2 = """SELECT * FROM
                (SELECT k1 as k1, null as k2 FROM {0} GROUP BY k1 UNION
                 SELECT null, k2 FROM {0} GROUP BY k2
                ) t ORDER BY k1, k2
            """.format(table_name)
    runner.check2_palo(line, line2)

    # check rollup
    line = "SELECT k10, k11, MAX( k9 ) FROM {0} GROUP BY ROLLUP(k10, k11) ORDER BY k10, k11".format(table_name)
    line2 = """SELECT k10, k11, MAX( k9 ) FROM {0} GROUP BY
                GROUPING SETS (( k10, k11 ), ( k10 ), ( )) ORDER BY k10, k11;""".format(table_name)
    line3 = """SELECT * FROM
                (SELECT k10, k11, MAX( k9 ) FROM {0} GROUP BY k10, k11 UNION
                 SELECT k10, null, MAX( k9 ) FROM {0} GROUP BY k10 UNION
                 SELECT null, null, MAX( k9 ) FROM {0}
                ) t ORDER BY k10, k11
            """.format(table_name)
    runner.check2_palo(line, line2)
    runner.check2_palo(line, line3)

    # check cube
    line = "SELECT k5, k6, k10, MIN( k8 ) FROM {0} GROUP BY CUBE( k5, k6, k10 ) ORDER BY k5, k6, k10".format(table_name)
    line2 = """SELECT k5, k6, k10, MIN( k8 ) FROM {0} GROUP BY
                GROUPING SETS (( k5, k6, k10 ), ( k5, k6 ), ( k5, k10 ), ( k5 ), ( k6, k10 ), ( k6 ),  ( k10 ), ( ))
                ORDER BY k5, k6, k10;
            """.format(table_name)
    line3 = """SELECT * FROM (
                SELECT k5, k6, k10, MIN( k8 ) FROM {0} GROUP BY k5, k6, k10 UNION
                SELECT k5, k6, null, MIN( k8 ) FROM {0} GROUP BY k5, k6 UNION
                SELECT k5, null, k10, MIN( k8 ) FROM {0} GROUP BY k5, k10 UNION
                SELECT k5, null, null, MIN( k8 ) FROM {0} GROUP BY k5 UNION
                SELECT null, k6, k10, MIN( k8 ) FROM {0} GROUP BY k6, k10 UNION
                SELECT null, k6, null, MIN( k8 ) FROM {0} GROUP BY k6 UNION
                SELECT null, null, k10, MIN( k8 ) FROM {0} GROUP BY k10 UNION
                SELECT null, null, null, MIN( k8 ) FROM {0}
               ) t ORDER BY k5, k6, k10
            """.format(table_name)
    runner.check2_palo(line, line2)
    runner.check2_palo(line, line3)

    # 组合与嵌套暂不支持
    wrong_sqls = []
    wrong_sqls.append("SELECT k1, k3, MAX( k8 ) FROM {0} GROUP BY k1, GROUPING SETS ( (k1, k3), (k1), ( ) ),"
                      " ROLLUP(k1, k3)".format(table_name))
    wrong_sqls.append("SELECT k1, k3, MAX( k8 ) FROM {0} GROUP BY k1, GROUPING SETS ( (k1, k3), (k1), ( ) ),"
                      " CUBE(k1, k2)".format(table_name))
    wrong_sqls.append("SELECT k1, k3, MAX( k8 ) FROM {0} GROUP BY k1, GROUPING SETS ( (k1, k3), (k1), ( ) ),"
                      " CUBE(k1, k4)".format(table_name))
    # 聚合列和grouping sets列不能重复
    wrong_sqls.append("SELECT k1, k3, MAX( k3 ) FROM {0} GROUP BY GROUPING SETS ( (k1, k3), (k1), ( ) )"
                      .format(table_name))
    for wrong_sql in wrong_sqls:
        runner.checkwrong(wrong_sql)

    wrong_sql = "SELECT k20 FROM {0} GROUP BY CUBE (k1) ORDER BY k20".format(table_name)
    runner.checkwrong(wrong_sql)


def test_grouping_sets_base2():
    """
    {
    "title": "test_grouping_sets_base2",
    "describe": "验证GROUPING sets的基础功能，与union做diff",
    "tag": "function,p1"
    }
    """
    """
    test_grouping_sets_base2
    验证GROUPING sets的基础功能，与union做diff
    """
    table_name = "{0}.{1}".format(db, baseall_tb)

    # check GROUPING sets
    line = """SELECT * FROM 
                (SELECT k1,k4,MAX(k3) FROM {0} GROUP BY GROUPING sets ((k1,k4),(k1),(k4),()) 
                UNION SELECT k1,k2,MAX(k3) FROM {0} GROUP BY GROUPING sets ((k1,k2),(k1),(k2),())
                ) t ORDER BY k1, k4""".format(table_name)
    line2 = """SELECT * FROM
                (SELECT k1 as k1, k2 as k2, MAX( k3 ) FROM {0} GROUP BY k1, k2 UNION
                 SELECT k1, null, MAX( k3 ) FROM {0} GROUP BY k1 UNION
                 SELECT null, k2, MAX( k3 ) FROM {0} GROUP BY k2 UNION
                 SELECT null, null, MAX( k3 )FROM {0}
                 UNION 
                SELECT k1 as k1, k4 as k4, MAX( k3 ) FROM {0} GROUP BY k1, k4 UNION
                 SELECT k1, null, MAX( k3 ) FROM {0} GROUP BY k1 UNION
                 SELECT null, k4, MAX( k3 ) FROM {0} GROUP BY k4 UNION
                 SELECT null, null, MAX( k3 )FROM {0}
                ) t1 ORDER BY k1, k2""".format(table_name)
    runner.check2_palo(line, line2)


def test_grouping_sets_id():
    """
    {
    "title": "test_grouping_sets_id",
    "describe": "验证GROUPING id, 区分是没有统计还是值本来就是null",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test_GROUPING_id
    验证GROUPING id, 区分是没有统计还是值本来就是null
    """
    table_name = "{0}.{1}".format(db, baseall_tb)

    # check GROUPING sets: GROUPING & GROUPING_ID
    line = "SELECT k1, k2, GROUPING(k1), GROUPING(k2), GROUPING_ID(k1, k2), SUM( k3 ) FROM {0}" \
           " GROUP BY GROUPING SETS ( (k1, k2), (k1), (k2), ( ) ) ORDER BY k1, k2".format(table_name)
    line2 = """SELECT * FROM (
                SELECT k1, k2, 0 AS a, 0 AS b, 0 AS c, SUM( k3 ) FROM {0} GROUP BY k1, k2 UNION
                SELECT k1, null, 0 AS a, 1 AS b, 1 AS c, SUM( k3 ) FROM {0} GROUP BY k1 UNION
                SELECT null, k2, 1 AS a, 0 AS b, 2 AS c, SUM( k3 ) FROM {0} GROUP BY k2 UNION
                SELECT null, null, 1 AS a, 1 AS b, 3 AS c, SUM( k3 ) FROM {0}
               ) t ORDER BY k1, k2
            """.format(table_name)
    runner.check2_palo(line, line2)

    # check rollup: GROUPING
    line = "SELECT k10, k11, GROUPING(k10), GROUPING(k11), MAX( k9 ) FROM {0}" \
           " GROUP BY ROLLUP(k10, k11) ORDER BY k10, k11".format(table_name)
    line2 = """SELECT k10, k11, GROUPING(k10), GROUPING(k11), MAX( k9 ) FROM {0} GROUP BY
                GROUPING SETS (( k10, k11 ), ( k10 ), ( )) ORDER BY k10, k11;""".format(table_name)
    line3 = """SELECT * FROM
                (SELECT k10, k11, 0 AS a, 0 AS b, MAX( k9 ) FROM {0} GROUP BY k10, k11 UNION
                 SELECT k10, null, 0 AS a, 1 AS b, MAX( k9 ) FROM {0} GROUP BY k10 UNION
                 SELECT null, null, 1 AS a, 1 AS b, MAX( k9 ) FROM {0}
                ) t ORDER BY k10, k11
            """.format(table_name)
    runner.check2_palo(line, line2)
    runner.check2_palo(line, line3)

    # check cube: GROUPING_ID
    line = "SELECT k5, k6, k10, GROUPING_ID(k5), GROUPING_ID(k5, k6), GROUPING_ID(k6, k5, k10), MIN( k8 ) FROM {0}" \
           " GROUP BY CUBE( k5, k6, k10 ) ORDER BY k5, k6, k10".format(table_name)
    line2 = """SELECT k5, k6, k10, GROUPING_ID(k5), GROUPING_ID(k5, k6), GROUPING_ID(k6, k5, k10), MIN( k8 ) FROM {0}
                GROUP BY
                GROUPING SETS (( k5, k6, k10 ), ( k5, k6 ), ( k5, k10 ), ( k5 ), ( k6, k10 ), ( k6 ),  ( k10 ), ( ))
                ORDER BY k5, k6, k10;
            """.format(table_name)
    line3 = """SELECT * FROM (
                SELECT k5, k6, k10, 0 AS a, 0 AS b, 0 AS c, MIN( k8 ) FROM {0} GROUP BY k5, k6, k10 UNION
                SELECT k5, k6, null, 0 AS a, 0 AS b, 1 AS c, MIN( k8 ) FROM {0} GROUP BY k5, k6 UNION
                SELECT k5, null, k10, 0 AS a, 1 AS b, 4 AS c, MIN( k8 ) FROM {0} GROUP BY k5, k10 UNION
                SELECT k5, null, null, 0 AS a, 1 AS b, 5 AS c, MIN( k8 ) FROM {0} GROUP BY k5 UNION
                SELECT null, k6, k10, 1 AS a, 2 AS b, 2 AS c, MIN( k8 ) FROM {0} GROUP BY k6, k10 UNION
                SELECT null, k6, null, 1 AS a, 2 AS b, 3 AS c, MIN( k8 ) FROM {0} GROUP BY k6 UNION
                SELECT null, null, k10, 1 AS a, 3 AS b, 6 AS c, MIN( k8 ) FROM {0} GROUP BY k10 UNION
                SELECT null, null, null, 1 AS a, 3 AS b, 7 AS c, MIN( k8 ) FROM {0}
               ) t ORDER BY k5, k6, k10
            """.format(table_name)
    runner.check2_palo(line, line2)
    runner.check2_palo(line, line3)

    # 测试只有一列的情况
    line = "SELECT k1 ,GROUPING(k1) FROM {0} GROUP BY GROUPING sets ((k1)) ORDER BY k1".format(table_name)
    line2 = """SELECT k1, 0 FROM {0} ORDER BY k1""".format(table_name)
    runner.check2_palo(line, line2)

    line = "SELECT k1 ,GROUPING(k1) FROM {0} GROUP BY GROUPING sets ((k1), ()) ORDER BY k1".format(table_name)
    line2 = """SELECT k1, 0 FROM {0} UNION SELECT null, 1 ORDER BY k1""".format(table_name)
    runner.check2_palo(line, line2)

    line = "SELECT k1 ,GROUPING(k1) FROM {0} GROUP BY ROLLUP (k1) ORDER BY k1".format(table_name)
    line2 = """SELECT k1, 0 FROM {0} UNION SELECT null, 1 ORDER BY k1""".format(table_name)
    runner.check2_palo(line, line2)

    line = "SELECT k1 ,GROUPING(k1) FROM {0} GROUP BY CUBE (k1) ORDER BY k1".format(table_name)
    line2 = """SELECT k1, 0 FROM {0} UNION SELECT null, 1 ORDER BY k1""".format(table_name)
    runner.check2_palo(line, line2)

    wrong_sql = "SELECT k1 ,GROUPING(k2) FROM {0} GROUP BY CUBE (k1) ORDER BY k1".format(table_name)
    runner.checkwrong(wrong_sql)


def test_grouping_sets_id_null():
    """
    {
    "title": "test_grouping_sets_id_null",
    "describe": "验证GROUPING id, 区分是没有统计还是值本来就是null",
    "tag": "function,p1"
    }
    """
    """
    test_grouping_sets_id_null
    验证GROUPING id, 区分是没有统计还是值本来就是null
    """
    table_name = 'test_grouping_sets_id_null'
    gs_tb_name = "{0}.{1}".format(db, table_name)
    base_table_name = "{0}.{1}".format(db, baseall_tb)
    sql = "drop table if exists {}".format(table_name)
    runner.checkok(sql)
    sql = """
    CREATE TABLE `{0}` (
      `k1` tinyint(4) NULL COMMENT "",
      `k2` smallint(6) NULL COMMENT "",
      `k3` int(11) NULL COMMENT "",
      `k4` bigint(20) NULL COMMENT "",
      `k5` decimal(9, 3) NULL COMMENT "",
      `k6` char(5) NULL COMMENT "",
      `k10` date NULL COMMENT "",
      `k11` datetime NULL COMMENT "",
      `k7` varchar(20) NULL COMMENT "",
      `k8` double MAX NULL COMMENT "",
      `k9` float SUM NULL COMMENT ""
    ) ENGINE=OLAP
    AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`k1`) BUCKETS 5
    PROPERTIES (
    "storage_type" = "COLUMN"
    );
    """.format(table_name)
    runner.checkok(sql)

    sql = 'insert into {0} SELECT * FROM {1}'.format(gs_tb_name, base_table_name)
    runner.checkok(sql)
    sql = 'insert into {0} SELECT k1,null,k3,k4,k5,null,k7,k8,k9,null,k11 FROM {1}'.format(gs_tb_name, base_table_name)
    runner.checkok(sql)

    # check GROUPING sets: GROUPING & GROUPING_ID
    line = "SELECT k1,k2,SUM(k3),GROUPING(k1),GROUPING(k2),GROUPING_id(k1,k2) from {0}" \
           " GROUP BY GROUPING sets ((k1),(k2)) order by k1,k2;".format(gs_tb_name)
    line2 = """SELECT * FROM (
                SELECT k1, null as k2, SUM(k3) AS a, 0 as b,1 as c, 1 as d from {0} GROUP BY k1 union
                SELECT null as k1, k2, SUM(k3), 1, 0, 2 from {0} GROUP BY k2) t order by k1,k2
            """.format(gs_tb_name)
    runner.check2_palo(line, line2)

    line = "SELECT k5,k6,MIN(k3),GROUPING(k5),GROUPING(k6),GROUPING_id(k5,k6) from {0}" \
           " GROUP BY GROUPING sets ((k5),(k6)) order by k5,k6;".format(gs_tb_name)
    line2 = """SELECT * FROM (
                SELECT k5, null as k6, MIN(k3) AS a, 0 as b,1 as c, 1 as d from {0} GROUP BY k5 union
                SELECT null as k5, k6, MIN(k3), 1, 0, 2 from {0} GROUP BY k6) t order by k5,k6
            """.format(gs_tb_name)
    runner.check2_palo(line, line2)

    line = "SELECT k10,k11,MAX(k3) as a,GROUPING(k10) as b,GROUPING(k11) as c,GROUPING_id(k10,k11) as d from {0}" \
           " GROUP BY GROUPING sets ((k10),(k11)) order by k10,k11,a,b,c,d;".format(gs_tb_name)
    line2 = """SELECT * FROM (
                SELECT k10, null as k11, MAX(k3) AS a, 0 as b,1 as c, 1 as d from {0} GROUP BY k10 union
                SELECT null as k10, k11, MAX(k3), 1, 0, 2 from {0} GROUP BY k11) t order by k10,k11,a,b,c,d
            """.format(gs_tb_name)
    runner.check2_palo(line, line2)
    sql = "drop table if exists {}".format(table_name)
    runner.checkok(sql)


def test_grouping_sets_rollup():
    """
    {
    "title": "test_grouping_sets_rollup",
    "describe": "测试GROUPING sets是否能命中rollup",
    "tag": "function,p1"
    }
    """
    """
    test_GROUPING_rollup
    测试GROUPING sets是否能命中rollup
    """
    table_name = 'test_grouping_sets_rollup'
    index_name = 'idx'
    index_name_1 = 'idx1'
    runner.checkok('drop table if exists {0}.{1}'.format(db, table_name))
    runner.checkok('create table {0}.{1}(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3),'
                   ' k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float SUM)'
                   ' engine=olap distributed by hash(k1) buckets 5 ROLLUP({2}(k1, k2, k3), {3}(k1, k2, k3, k8))'
                   ' properties("storage_type"="column")'.format(db, table_name, index_name, index_name_1))
    runner.checkok('insert into {0} select * from baseall'.format(table_name))

    # check GROUPING sets: GROUPING & GROUPING_ID
    sql_list = []
    sql_list.append(["SELECT k1, MAX( k8 ) FROM {0}.{1} GROUP BY GROUPING SETS ( (k1), () )"
                    .format(db, table_name),
                     index_name])
    sql_list.append(["SELECT k1, k2, MAX( k8 ) FROM {0}.{1} GROUP BY GROUPING SETS ( (k1, k2), (k1), (k2), ( ) )"
                    .format(db, table_name),
                     index_name])
    sql_list.append(["SELECT k1, k2, MAX( k8 ) FROM {0}.{1} GROUP BY ROLLUP(k1, k2)".format(db, table_name),
                     index_name])
    sql_list.append(["SELECT k1, k2, MAX( k8 ) FROM {0}.{1} GROUP BY CUBE(k1, k2)".format(db, table_name),
                     index_name])
    # 组合与嵌套暂不支持
    # sql_list.append(["SELECT k1, k3, MAX( k8 ) FROM {0}.{1} GROUP BY k1, GROUPING SETS ( (k1, k3), (k1), ( ) ),"
    #                  " ROLLUP(k1, k3)".format(database_name, table_name), index_name_1])
    # sql_list.append(["SELECT k1, k3, MAX( k8 ) FROM {0}.{1} GROUP BY k1, GROUPING SETS ( (k1, k3), (k1), ( ) ),"
    #                  " CUBE(k1, k2)".format(database_name, table_name), index_name_1])
    # sql_list.append(["SELECT k1, k3, MAX( k8 ) FROM {0}.{1} GROUP BY k1, GROUPING SETS ( (k1, k3), (k1), ( ) ),"
    #                  " CUBE(k1, k4)".format(database_name, table_name), 'null'])

    for sql, index in sql_list:
        print('EXPLAIN ' + sql)
        result = runner.get_sql_result('EXPLAIN ' + sql)
        assert check_rollup(result, index)
    runner.checkok('drop table if exists {0}.{1}'.format(db, table_name))


def test_grouping_select():
    """补充5583 case"""
    sql = "select k1, if(grouping(k1)=1, count(k1), 0) from test_query_qa.test group by grouping sets((k1));"
    msg = "`k1` cannot both in select list and aggregate functions when using GROUPING SETS/CUBE/ROLLUP"
    runner.checkwrong(sql, msg)


def teardown_module():
    """
    end
    """
    print("End")
    # mysql_cursor.close()
    # mysql_con.close()


if __name__ == '__main__':
    setup_module()
    # test_grouping_sets_base()
    test_grouping_sets_rollup()

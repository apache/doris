// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Test grouping sets()/cube()/rollup()/grouping_id()/grouping()
suite("test_grouping_sets", "p0") {
    qt_select """
                SELECT k1, k2, SUM(k3) FROM test_query_db.test
                GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ( ) ) order by k1, k2
              """

    qt_select2 """
                 select (k1 + 1) k1_, k2, sum(k3) from test_query_db.test group by
                 rollup(k1_, k2) order by k1_, k2
               """

    qt_select3 "select 1 as k, k3, sum(k1) from test_query_db.test group by cube(k, k3) order by k, k3"

    qt_select4 """
                 select k2, concat(k7, k12) as k_concat, sum(k1) from test_query_db.test group by
                 grouping sets((k2, k_concat),()) order by k2, k_concat
               """

    qt_select5 """
                 select k1_, k2_, sum(k3_) from (select (k1 + 1) k1_, k2 k2_, k3 k3_ from test_query_db.test) as test
                 group by grouping sets((k1_, k2_), (k2_)) order by k1_, k2_
               """

    qt_select6 """
                 select if(k0 = 1, 2, k0) k_if, k1, sum(k2) k2_sum from test_query_db.baseall where k0 is null or k2 = 1991
                 group by grouping sets((k_if, k1),()) order by k_if, k1, k2_sum
               """

    qt_select7 """ select k1,k2,sum(k3) from test_query_db.test where 1 = 2 group by grouping sets((k1), (k1,k2)) """

    qt_select8 """ WITH dt AS 
                    (select 'test' as name,1 as score
                    UNION
                    all 
                    SELECT 'test' AS name,1 AS score
                    UNION
                    all SELECT 'test2' AS name,12 AS score
                    UNION
                    all SELECT 'test2' AS name,12 AS score ) ,result_data AS 
                    (SELECT name,
                        sum(score) AS score
                    FROM dt
                    GROUP BY  CUBE(name))
                SELECT *
                FROM result_data
                WHERE name = 'test';
            """
    // test grouping sets
    qt_select9 """
        SELECT k3, SUM( k1+k2 ) FROM test_query_db.test 
        GROUP BY GROUPING SETS ( (k3), ( ) ) order by k3
        """
    qt_select10 """
        SELECT k1, k2 FROM test_query_db.test
        GROUP BY GROUPING SETS ( (k1), (k2) ) order by k1, k2
        """
    def sql1_1 = """
                SELECT k10, k11, MAX( k9 ) FROM test_query_db.test GROUP BY ROLLUP(k10, k11) ORDER BY k10, k11
                """
    qt_select11 sql1_1
    def sql1_2 = """
                SELECT k10, k11, MAX( k9 ) FROM test_query_db.test 
                GROUP BY GROUPING SETS (( k10, k11 ), ( k10 ), ( )) ORDER BY k10, k11;
                """
    check_sql_equal(sql1_1, sql1_2)

    def sql2_1 = """
        SELECT k5, k6, k10, MIN( k8 ) FROM test_query_db.test 
        GROUP BY CUBE( k5, k6, k10 ) ORDER BY k5, k6, k10
        """
    qt_select12 sql2_1
    def sql2_2 = """
        SELECT k5, k6, k10, MIN( k8 ) FROM test_query_db.test GROUP BY
        GROUPING SETS (( k5, k6, k10 ), ( k5, k6 ), ( k5, k10 ), ( k5 ), ( k6, k10 ), ( k6 ),  ( k10 ), ( ))
        ORDER BY k5, k6, k10;
        """
    check_sql_equal(sql2_1, sql2_2)
    test {
        sql """
            SELECT k1, k3, MAX( k8 ) FROM test_query_db.test 
            GROUP BY k1, GROUPING SETS ( (k1, k3), (k1), ( ) ), ROLLUP(k1, k3)
            """
        exception "Syntax error"
    }

    qt_select13"""
        SELECT * FROM
            (SELECT k1,k4,MAX(k3) FROM test_query_db.test GROUP BY GROUPING sets ((k1,k4),(k1),(k4),())
             UNION SELECT k1,k2,MAX(k3) FROM test_query_db.test GROUP BY GROUPING sets ((k1,k2),(k1),(k2),())
            ) t ORDER BY k1, k4
        """
    // test grouping sets id
    qt_select14 """
        SELECT k1, k2, GROUPING(k1), GROUPING(k2), GROUPING_ID(k1, k2), SUM( k3 ) 
        FROM test_query_db.test 
        GROUP BY GROUPING SETS ( (k1, k2), (k1), (k2), ( ) ) 
        ORDER BY k1, k2
        """
    def sql3_1 =  """
        SELECT k10, k11, GROUPING(k10), GROUPING(k11), MAX( k9 ) 
        FROM test_query_db.test 
        GROUP BY ROLLUP(k10, k11) ORDER BY k10, k11
        """
    qt_select15 sql3_1
    def sql3_2 = """
        SELECT k10, k11, GROUPING(k10), GROUPING(k11), MAX( k9 ) 
        FROM test_query_db.test GROUP BY
        GROUPING SETS (( k10, k11 ), ( k10 ), ( )) ORDER BY k10, k11;
        """
    check_sql_equal(sql3_1, sql3_2)

    def sql4_1 = """
        SELECT k5, k6, k10, GROUPING_ID(k5), GROUPING_ID(k5, k6), GROUPING_ID(k6, k5, k10), MIN( k8 ) 
        FROM test_query_db.test 
        GROUP BY CUBE( k5, k6, k10 ) 
        ORDER BY k5, k6, k10
        """
    qt_select16 sql4_1
    def sql4_2 = """
        SELECT k5, k6, k10, GROUPING_ID(k5), GROUPING_ID(k5, k6), GROUPING_ID(k6, k5, k10), MIN( k8 ) 
        FROM test_query_db.test
        GROUP BY
            GROUPING SETS ((k5, k6, k10), (k5, k6), (k5, k10), (k5), (k6, k10), (k6),  (k10), ())
        ORDER BY k5, k6, k10
        """
    check_sql_equal(sql4_1, sql4_2)

    qt_select17 """SELECT k1 ,GROUPING(k1) FROM test_query_db.test GROUP BY GROUPING sets ((k1)) ORDER BY k1"""
    qt_select18 """SELECT k1 ,GROUPING(k1) FROM test_query_db.test GROUP BY GROUPING sets ((k1), ()) ORDER BY k1"""
    qt_select19 """SELECT k1 ,GROUPING(k1) FROM test_query_db.test GROUP BY ROLLUP (k1) ORDER BY k1"""
    qt_select20 """SELECT k1 ,GROUPING(k1) FROM test_query_db.test GROUP BY CUBE (k1) ORDER BY k1"""
    test {
        sql "SELECT k1 ,GROUPING(k2) FROM test_query_db.test GROUP BY CUBE (k1) ORDER BY k1"
        exception "Column in Grouping does not exist in GROUP BY clause"
    }

    // test grouping sets id contain null data
    sql """drop table if exists test_query_db.test_grouping_sets_id_null"""
    sql """create table if not exists test_query_db.test_grouping_sets_id_null like test_query_db.test"""
    sql """insert into test_query_db.test_grouping_sets_id_null SELECT * FROM test_query_db.test"""
    sql """
        insert into test_query_db.test_grouping_sets_id_null 
        SELECT k0,k1,null,k3,k4,k5,null,null,k11,k7,k8,k9,k12,k13 FROM test_query_db.test
        """
    qt_select21 """
        SELECT k1,k2,SUM(k3),GROUPING(k1),GROUPING(k2),GROUPING_id(k1,k2) 
        FROM test_query_db.test_grouping_sets_id_null
        GROUP BY GROUPING sets ((k1),(k2)) order by k1,k2
        """
    qt_select22 """
        SELECT k5,k6,MIN(k3),GROUPING(k5),GROUPING(k6),GROUPING_id(k5,k6) 
        FROM test_query_db.test_grouping_sets_id_null 
        GROUP BY GROUPING sets ((k5),(k6)) order by k5,k6
        """
    qt_select23 """
        SELECT k10,k11,MAX(k3) as a,GROUPING(k10) as b,GROUPING(k11) as c,GROUPING_id(k10,k11) as d 
        FROM test_query_db.test_grouping_sets_id_null 
        GROUP BY GROUPING sets ((k10),(k11)) order by k10,k11,a,b,c,d
        """
    sql """drop table if exists test_query_db.test_grouping_sets_id_null"""
    // test grouping sets shoot rollup
    sql "drop table if exists test_query_db.test_grouping_sets_rollup"
    sql """
        create table if not exists test_query_db.test_grouping_sets_rollup(
            k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), 
            k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float SUM) 
        engine=olap distributed by hash(k1) buckets 5 
        ROLLUP(idx(k1, k2, k3), idx1(k1, k2, k3, k8)) 
        properties("replication_num"="1")
        """
    sql """insert into test_query_db.test_grouping_sets_rollup 
        select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9 from test_query_db.test
        """
    explain {
        sql("SELECT k1, MAX( k8 ) FROM test_query_db.test_grouping_sets_rollup GROUP BY GROUPING SETS( (k1), ())")
        contains "(idx1)"
    }
    explain {
        sql("""SELECT k1, k2, MAX( k8 ) 
                FROM test_query_db.test_grouping_sets_rollup 
                GROUP BY GROUPING SETS ( (k1, k2), (k1), (k2), ( ) )
                """)
        contains "(idx1)"
    }
    explain {
        sql("""SELECT k1, k2, MAX( k8 ) FROM test_query_db.test_grouping_sets_rollup GROUP BY ROLLUP(k1, k2)""")
        contains "(idx1)"
    }
    explain {
        sql("""SELECT k1, k2, MAX( k8 ) FROM test_query_db.test_grouping_sets_rollup GROUP BY ROLLUP(k1, k2)""")
        contains "(idx1)"
    }
    explain {
        sql("SELECT k1, k2, MAX( k8 ) FROM test_query_db.test_grouping_sets_rollup GROUP BY CUBE(k1, k2)")
        contains "(idx1)"
    }
    sql "drop table if exists test_query_db.test_grouping_sets_rollup"

    qt_select24 """
        select k1, if(grouping(k1)=1, count(k1), 0) from test_query_db.test group by grouping sets((k1))
        order by 1,2
        """
}

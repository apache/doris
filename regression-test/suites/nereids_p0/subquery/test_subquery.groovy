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

suite("test_subquery") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    qt_sql1 """
        select c1, c3, m2 from 
            (select c1, c3, max(c2) m2 from 
                (select c1, c2, c3 from 
                    (select k3 c1, k2 c2, max(k1) c3 from nereids_test_query_db.test 
                     group by 1, 2 order by 1 desc, 2 desc limit 5) x 
                ) x2 group by c1, c3 limit 10
            ) t 
        where c1>0 order by 2 , 1 limit 3
    """

    qt_sql2 """
        with base as (select k1, k2 from nereids_test_query_db.test as t where k1 in (select k1 from nereids_test_query_db.baseall
        where k7 = 'wangjuoo4' group by 1 having count(distinct k7) > 0)) select * from base limit 10;
    """

    qt_sql3 """
        SELECT k1 FROM nereids_test_query_db.test GROUP BY k1 HAVING k1 IN (SELECT k1 FROM nereids_test_query_db.baseall WHERE
        k2 >= (SELECT min(k3) FROM nereids_test_query_db.bigtable WHERE k2 = baseall.k2)) order by k1;
    """

    qt_sql4 """
        select count() from (select k2, k1 from nereids_test_query_db.baseall order by k1 limit 1) a;
    """

    qt_uncorrelated_exists_with_limit_0 """
        select * from nereids_test_query_db.baseall where exists (select * from nereids_test_query_db.baseall limit 0)
    """

    // test uncorrelated scalar subquery with limit <= 1
    sql """
        select * from nereids_test_query_db.baseall where k1 = (select k1 from nereids_test_query_db.baseall limit 1)
    """

    // test uncorrelated scalar subquery with more than one return rows
    test {
        sql """
            select * from nereids_test_query_db.baseall where k1 = (select k1 from nereids_test_query_db.baseall limit 2)
        """
        exception("Expected LE 1 to be returned by expression")
    }

    // test uncorrelated scalar subquery with order by and limit
    qt_uncorrelated_scalar_with_sort_and_limit """
            select * from nereids_test_query_db.baseall where k1 = (select k1 from nereids_test_query_db.baseall order by k1 desc limit 1)
        """

    sql """drop table if exists test_one_row_relation;"""
    sql """
        CREATE TABLE `test_one_row_relation` (
        `user_id` int(11) NULL 
        )
        UNIQUE KEY(`user_id`)
        COMMENT 'test'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ set enable_nereids_dml=true; """
    
    sql """insert into test_one_row_relation select (select 1);"""

    qt_sql_subquery_one_row_relation """select * from test_one_row_relation;"""

    sql """drop table if exists test_one_row_relation;"""
}

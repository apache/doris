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

suite("test_nereids_grouping_sets") {

    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"

    sql "DROP TABLE IF EXISTS groupingSetsTable"
    sql "DROP TABLE IF EXISTS groupingSetsTableNotNullable"

    sql """
        CREATE TABLE `groupingSetsTable` (
        `k1` bigint(20) NULL,
        `k2` bigint(20) NULL,
        `k3` bigint(20) NULL,
        `k4` bigint(20) not null,
        `k5` varchar(10),
        `k6` varchar(10)
        ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k2`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """

    sql """
        CREATE TABLE `groupingSetsTableNotNullable` (
         `k1` bigint(20) NOT NULL,
         `k2` bigint(20) NOT NULL,
         `k3` bigint(20) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k2`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        INSERT INTO groupingSetsTable VALUES
            (1, 1, 1, 3, 'a', 'b'),
            (1, 1, 2, 3, 'a', 'c'),
            (1, 1, 3, 4, 'a' , 'd'),
            (1, 0, null, 4, 'b' , 'b'),
            (2, 2, 2, 5, 'b', 'c'),
            (2, 2, 4, 5, 'b' , 'd'),
            (2, 2, 6, 4, 'c', 'b'),
            (2, 2, null, 4, 'c', 'c'),
            (3, 3, 3, 3, 'c', 'd'),
            (3, 3, 6, 3, 'd', 'b'),
            (3, 3, 9, 4, 'd', 'c'),
            (3, 0, null, 5, 'd', 'd')
    """

    sql """
        insert into groupingSetsTableNotNullable values
        (1, 0, 0),
        (1, 1, 3), 
        (1, 1, 2), 
        (1, 1, 1), 
        (2, 2, 0), 
        (2, 2, 6), 
        (2, 2, 4), 
        (2, 2, 2), 
        (3, 0, 0), 
        (3, 3, 9), 
        (3, 3, 6), 
        (3, 3, 3);
    """

    sql "SET enable_fallback_to_original_planner=false"

    // grouping
    order_qt_select "select k1+1, grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1));";
    order_qt_select "select k1+1, grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1), (k1), (k2));";
    order_qt_select "select k1+1, grouping(k1+1) from groupingSetsTable group by grouping sets((k1), (k1+1), (k2));";
    order_qt_select "select k1+1, grouping(k1) from groupingSetsTable group by grouping sets((k1));";
    order_qt_select "select sum(k2), grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1));";
    order_qt_select "select sum(k2+1), grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1));";
    order_qt_select "select sum(k2+1), grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1), (k1));";
    order_qt_select "select sum(k2+1), grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1)) having (k1+1) > 1;";
    order_qt_select "select sum(k2+1), grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1), (k1)) having (k1+1) > 1;";


    // grouping_id
    order_qt_select "select k1+1, grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1));";
    order_qt_select "select k1+1, grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1), (k1), (k2));";
    order_qt_select "select k1+1, grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1), (k1+1), (k2));";
    order_qt_select "select k1+1, grouping_id(k1) from groupingSetsTable group by grouping sets((k1));";
    order_qt_select "select k1+1, grouping_id(k1, k2) from groupingSetsTable group by grouping sets((k1), (k2));";
    order_qt_select "select sum(k2), grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1));";
    order_qt_select "select sum(k2+1), grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1));";
    order_qt_select "select sum(k2+1), grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1), (k1));";
    order_qt_select "select sum(k2+1), grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1)) having (k1+1) > 1;";
    order_qt_select "select sum(k2+1), grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1), (k1)) having (k1+1) > 1;";

    // old grouping sets
    qt_select """
                SELECT k1, k2, SUM(k3) FROM groupingSetsTable
                GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ( ) ) order by k1, k2
              """

    qt_select2 """
                 select (k1 + 1) k1_, k2, sum(k3) from groupingSetsTable group by
                 rollup(k1_, k2) order by k1_, k2
               """

    qt_select3 "select 1 as k, k3, sum(k1) from groupingSetsTable group by cube(k, k3) order by k, k3"

    qt_select4 """
                 select k2, concat(k5, k6) as k_concat, sum(k1) from groupingSetsTable group by
                 grouping sets((k2, k_concat),()) order by k2, k_concat
               """

    qt_select5 """
                 select k1_, k2_, sum(k3_) from (select (k1 + 1) k1_, k2 k2_, k3 k3_ from groupingSetsTable) as test
                 group by grouping sets((k1_, k2_), (k2_)) order by k1_, k2_
               """

    qt_select6 """
                 select if(k1 = 1, 2, k1) k_if, k1, sum(k2) k2_sum from groupingSetsTable where k3 is null or k2 = 1
                 group by grouping sets((k_if, k1),()) order by k_if, k1, k2_sum
               """

    test {
        sql """
              SELECT k1, k2, SUM(k3) FROM groupingSetsTable
              GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ( ), (k3) ) order by k1, k2
            """
        exception "java.sql.SQLException: errCode = 2, detailMessage = Unexpected exception: column: k3 cannot both in select list and aggregate functions when using GROUPING SETS/CUBE/ROLLUP, please use union instead."
    }

    test {
        sql """
              SELECT k1, k2, SUM(k3)/(SUM(k3)+1) FROM groupingSetsTable
              GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ( ), (k3) ) order by k1, k2
            """
        exception "java.sql.SQLException: errCode = 2, detailMessage = Unexpected exception: column: k3 cannot both in select list and aggregate functions when using GROUPING SETS/CUBE/ROLLUP, please use union instead."
    }

    order_qt_select """
        select k1, sum(k2) from (select k1, k2, grouping(k1), grouping(k2) from groupingSetsTableNotNullable group by grouping sets((k1), (k2)))a group by k1
    """
}

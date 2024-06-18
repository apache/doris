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

suite("group_concat") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"


    test {
        sql """select
                 group_concat(cast(number as string), ',' order by number)
               from numbers('number'='10')"""
        result([["0,1,2,3,4,5,6,7,8,9"]])
    }

    test {
        sql """select
                 group_concat(cast(number as string), ',' order by number)
               from numbers('number'='10')"""
        result([["0,1,2,3,4,5,6,7,8,9"]])
    }

    test {
        sql "select group_concat(cast(number as string)) from numbers('number'='10')"
        result([["0,1,2,3,4,5,6,7,8,9"]])
    }

    test {
        sql "select group_concat(cast(number as string), ' : ') from numbers('number'='10')"
        result([["0 : 1 : 2 : 3 : 4 : 5 : 6 : 7 : 8 : 9"]])
    }

    test {
        sql "select group_concat(cast(number as string), NULL) from numbers('number'='10')"
        result([[null]])
    }

    def testGroupByDistinct = {
        sql "drop table if exists test_group_concat_distinct_tbl1"
        sql """create table test_group_concat_distinct_tbl1(
                        tbl1_id1 int
                    ) distributed by hash(tbl1_id1)
                    properties('replication_num'='1')
                    """

        sql "insert into test_group_concat_distinct_tbl1 values(1), (2), (3), (4), (5)"


        sql "drop table if exists test_group_concat_distinct_tbl2"
        sql """create table test_group_concat_distinct_tbl2(
                        tbl2_id1 int,
                        tbl2_id2 int,
                    ) distributed by hash(tbl2_id1)
                    properties('replication_num'='1')
                    """
        sql "insert into test_group_concat_distinct_tbl2 values(1, 11), (2, 22), (3, 33), (4, 44)"


        sql "drop table if exists test_group_concat_distinct_tbl3"
        sql """create table test_group_concat_distinct_tbl3(
                        tbl3_id2 int,
                        tbl3_name varchar(255)
                    ) distributed by hash(tbl3_id2)
                    properties('replication_num'='1')
                    """
        sql "insert into test_group_concat_distinct_tbl3 values(22, 'a'), (33, 'b'), (44, 'c')"

        sql "sync"

        order_qt_group_by_distinct """
            SELECT
                 tbl1.tbl1_id1,
                 group_concat(DISTINCT tbl3.tbl3_name, ',') AS `names`
             FROM test_group_concat_distinct_tbl1 tbl1
             LEFT OUTER JOIN test_group_concat_distinct_tbl2 tbl2 ON tbl2.tbl2_id1 = tbl1.tbl1_id1
             LEFT OUTER JOIN test_group_concat_distinct_tbl3 tbl3 ON tbl3.tbl3_id2 = tbl2.tbl2_id2
             GROUP BY tbl1.tbl1_id1
           """
    }()
}

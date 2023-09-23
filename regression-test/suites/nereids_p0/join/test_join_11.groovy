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

suite("test_join_11", "nereids_p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql 'set parallel_fragment_exec_instance_num = 2;'
    sql "use nereids_test_query_db"

    def tbName1 = "test"
    def tbName2 = "baseall"
    def tbName3 = "bigtable"
    def empty_name = "empty"

    List selected =  ["a.k1, b.k1, a.k2, b.k2, a.k3, b.k3", "count(a.k1), count(b.k1), count(a.k2), count(b.k2), count(*)"]

    // test_left_join
    String i = "a.k1, b.k1, a.k2, b.k2, a.k3, b.k3"
    qt_left_join1"""select ${i} from ${tbName1} a left join ${tbName2} b 
            on a.k1 = b.k1 and a.k2 > 0 order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""
    qt_left_join2"""select ${i} from ${tbName1} a left join ${tbName2} b 
            on a.k1 = b.k1 and a.k2 > b.k2 order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""
    qt_left_join3"""select ${i} from ${tbName1} a left join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
            left join ${tbName3} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
            order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""

    for (s in selected) {
        qt_left_join4"""select ${s} from ${tbName1} a left join ${tbName2} b 
                    on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from ${tbName1} a left join ${tbName2} b
                on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from ${tbName1} a left join ${tbName2} b
                on a.k1 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        test {
            sql"""select ${s} from ${tbName1} a left join ${tbName2} b 
               order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
            sql"""select ${s} from ${tbName1} a left join ${tbName2} b
               on a.k1 = b.k1 or a.k2 = b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from ${tbName1} a left join ${tbName2} b
               on a.k1 < b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from ${tbName1} a left join ${tbName2} b
               on a.k1 = b.k1 or a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from ${tbName1} a left join ${tbName2} b
               on a.k1 = b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
            sql"""select ${s} from ${tbName1} a left join ${tbName2} b
                    on a.k1 < b.k1 or a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_left_join5"""select ${s} from ${tbName1} a left join ${tbName2} b on a.k1 = b.k1
                left join ${tbName3} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
    }

    // left_outer_join
    qt_left_outer_join1"""select ${i} from ${tbName1} a left outer join ${tbName2} b 
            on a.k1 = b.k1 and a.k2 > 0 order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""
    qt_left_outer_join2"""select ${i} from ${tbName1} a left outer join ${tbName2} b 
            on a.k1 = b.k1 and a.k2 > b.k2 order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""
    qt_left_outer_join3"""select ${i} from ${tbName1} a left outer join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
            left join ${tbName3} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
            order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""

    for (s in selected) {
        qt_left_outer_join4"""select ${s} from ${tbName1} a left outer join ${tbName2} b 
                on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b
                on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b
                on a.k1 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        test {
            sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b 
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b
                    on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b
                    on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b
                    on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_left_outer_join5"""select ${s} from ${tbName1} a left outer join ${tbName2} b on a.k1 = b.k1
                left outer join ${tbName3} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""

    }

    // right join
    qt_right_join1"""select ${i} from ${tbName1} a right join ${tbName2} b 
               on a.k1 = b.k1 and a.k2 > b.k2 order by isnull(a.k1), 1, 2, 3, 4, 5 limit 65535"""

    for (s in selected) {
        qt_right_join2"""select ${s} from ${tbName1} a right join ${tbName2} b 
               on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right join ${tbName2} b
               on a.k1 > b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right join ${tbName2} b
                    on a.k1 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_right_join3"""select ${s} from ${tbName1} a right join ${tbName2} b
                on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        test {
            sql"""select ${s} from ${tbName1} a right join ${tbName2} b 
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                logger.info(exception.message)
                assertTrue(exception != null)
            }
        }
        sql"""select ${s} from ${tbName1} a right join ${tbName2} b
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right join ${tbName2} b
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right join ${tbName2} b
                    on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right join ${tbName2} b
                    on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right join ${tbName2} b
                    on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_right_join4"""select ${s} from ${tbName1} a right join ${tbName2} b on a.k1 = b.k1
                right join ${tbName3} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
    }
    qt_right_join5"""select ${i} from ${tbName1} a right join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
            right join ${tbName3} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
            order by isnull(a.k1), 1, 2, 3, 4, 5 limit 65535"""

    // right outer join
    qt_right_outer_join1"""select ${i} from ${tbName1} a right outer join ${tbName2} b 
               on a.k1 = b.k1 and a.k2 > b.k2 order by isnull(a.k1), 1, 2, 3, 4, 5 limit 65535"""

    for (s in selected) {
        qt_right_outer_join2"""select ${s} from ${tbName1} a right  outer join ${tbName2} b 
               on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right  outer join ${tbName2} b
                    on a.k1 > b.k1 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right  outer join ${tbName2} b
                    on a.k1 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_right_outer_join3"""select ${s} from ${tbName1} a right  outer join ${tbName2} b
               on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        test {
            sql"""select ${s} from ${tbName1} a right  outer join ${tbName2} b 
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        sql"""select ${s} from ${tbName1} a right  outer join ${tbName2} b
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right  outer join ${tbName2} b
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right  outer join ${tbName2} b
                    on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right  outer join ${tbName2} b
                    on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right  outer join ${tbName2} b
                    on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_right_outer_join4"""select ${s} from ${tbName1} a right outer join ${tbName2} b on a.k1 = b.k1
                right outer join ${tbName3} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""

    }
    qt_right_outer_join5"""select ${i} from ${tbName1} a right outer join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
            right outer join ${tbName3} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
            order by isnull(a.k1), 1, 2, 3, 4, 5 limit 65535"""

    // right outer join with other join predicates
    qt_right_outer_join_wih_other_pred """
        select a.k2, b.k2, c.k2 from test a left join test b on a.k2 = b.k2 right join baseall c on b.k2 = c.k1 and 1 = 2 order by 1, 2, 3;
    """

    // full outer join
    for (s in selected) {
        sql"""select ${s} from ${tbName1} a full outer join ${tbName2} b on a.k1 = b.k1 
                 order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b on a.k1 = b.k1 
                 order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a full outer join ${tbName2} b on a.k1 > b.k1
                where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a full outer join ${tbName2} b on a.k1 > 0
                order by 1, 2, 3, 4, 5 limit 65535"""
        test {
            sql"""select ${s} from ${tbName1} a full outer join ${tbName2} b 
                where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        sql"""select ${s} from ${tbName1} a full outer join ${tbName2} b
                on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a full outer join ${tbName2} b
                on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a full outer join ${tbName2} b
                on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a full outer join ${tbName2} b
                on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a full outer join ${tbName2} b
                on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a full outer join ${tbName2} b on a.k1 = b.k1
                full outer join ${tbName3} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b on a.k1 = b.k1 
                 left outer join ${tbName3} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
    }
    sql"""select a.k1 k, a.k2, a.k3, b.k1, b.k2, b.k3 from ${tbName1} a full outer join ${tbName2} b 
             on a.k1 = b.k1 and a.k2 > b.k2 order by isnull(k), 1, 2, 3, 4, 5 limit 65535"""
    sql"""select a.k1 k, a.k2, a.k3, b.k1, b.k2, b.k3 from ${tbName1} a left outer join ${tbName2} b 
             on a.k1 = b.k1 and a.k2 > b.k2 union (select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 
             from ${tbName1} a right outer join ${tbName2} b on a.k1 = b.k1 and a.k2 > b.k2) 
             order by isnull(k), 1, 2, 3, 4, 5 limit 65535"""
    sql"""select count(*) from ${tbName1} a full outer join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
            full outer join ${tbName3} c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0"""
    sql"""select count(*) from ((select a.k1 as k1, b.k1 as k2, a.k2 as k3, b.k2 as k4, a.k3 as k5, b.k3 as k6, c.k1 as k7, c.k2 as k8, c.k3 as k9 from ${tbName1} a 
            left outer join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
            left outer join ${tbName3} c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0) union 
            (select a.k1, b.k1, a.k2, b.k2, a.k3, b.k3, c.k1, c.k2, c.k3 from ${tbName1} a 
            left outer join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
            right outer join ${tbName3} c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0) union 
            (select a.k1, b.k1, a.k2, b.k2, a.k3, b.k3, c.k1, c.k2, c.k3 from ${tbName1} a 
            right outer join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
            left outer join ${tbName3} c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0) 
            union (select a.k1, b.k1, a.k2, b.k2, a.k3, b.k3, c.k1, c.k2, c.k3 from ${tbName1} a 
            right outer join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
            right outer join ${tbName3} c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0))a"""
    sql"""select ${i} from ${tbName1} a full outer join ${tbName2} b on a.k1 = b.k1 
             and a.k2 > 0 order by 1, isnull(b.k1), 2, 3, 4, 5  limit 65535"""
    sql"""select ${i} from ${tbName1} a left outer join ${tbName2} b on a.k1 = b.k1 and a.k2 > 0 
            order by 1, isnull(b.k1), 2, 3, 4, 5 limit 65535"""
}
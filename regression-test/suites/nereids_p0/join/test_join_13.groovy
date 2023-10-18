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

suite("test_join_13", "nereids_p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql 'set parallel_fragment_exec_instance_num = 2;'
    sql "use nereids_test_query_db"

    def tbName1 = "test"
    def tbName2 = "baseall"
    def tbName3 = "bigtable"
    def empty_name = "empty"

    List selected = ["a.k1, b.k1, a.k2, b.k2, a.k3, b.k3", "count(a.k1), count(b.k1), count(a.k2), count(b.k2), count(*)"]

    // right semi join
    List left_selected = ["a.k1, a.k2, a.k3, a.k4, a.k5", "count(a.k1), count(a.k2), count(a.k4), count(a.k3), count(*)"]
    List right_selected = ["b.k1, b.k2, b.k3, b.k4, b.k5", "count(b.k1), count(b.k2), count(b.k4), count(b.k3), count(*)"]
    for (s in right_selected){
        def res23 = sql"""select ${s} from ${tbName1} a right semi join ${tbName1} b 
                on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        def res24 = sql"""select ${s} from ${tbName1} a right outer join ${tbName1} b 
                on a.k1 = b.k1 where a.k2 is not null order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res23, res24)
        sql"""select ${s} from ${tbName1} a right semi join ${tbName2} b
                on a.k1 > b.k1 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right semi join ${tbName2} b
                on a.k1 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res25 = sql"""select ${s} from ${tbName2} a right semi join ${tbName1} b
                 on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        def res26 = sql"""select ${s} from ${tbName2} a right outer join ${tbName1} b on a.k1 = b.k1 and 
                 a.k2 > 0 where a.k2 is not null order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res25, res26)
        def res27 = sql"""select ${s} from ${tbName2} a right semi join ${tbName1} b 
                    on a.k1 = b.k1 and a.k2 > b.k2  order by 1, 2, 3, 4, 5 limit 65535"""
        def res28 = sql"""select ${s} from ${tbName2} a right outer join ${tbName1} b 
                    on a.k1 = b.k1 and a.k2 > b.k2 where a.k2 is not null 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res27, res28)
        test {
            sql"""select ${s} from ${tbName1} a right semi join ${tbName2} b 
               where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        sql"""select ${s} from ${tbName1} a right semi join ${tbName2} b
                on a.k1 = b.k1 or a.k2 = b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right semi join ${tbName2} b
                on a.k1 < b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right semi join ${tbName2} b
                on a.k1 = b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right semi join ${tbName2} b
                on a.k1 = b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right semi join ${tbName2} b
                on a.k1 < b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res29 = sql"""select ${s} from ${tbName3} a right semi join ${tbName1} c on a.k1 = c.k1
                right semi join ${tbName2} b on b.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""

        def res30 = sql"""select ${s} from (select distinct b.* from ${tbName3} a right outer join ${tbName1} c on a.k1 = c.k1 
                right outer join ${tbName2} b on b.k2 = c.k2 where a.k1 is not null 
                and b.k1 is not null and c.k1 is not null) b order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res29, res30)
        def res31 = sql"""select ${s} from ${tbName2} c right semi join ${tbName1} a on c.k2 = a.k2 and c.k1 > 0 
                right semi join ${tbName3} b on a.k3 = b.k3 and b.k1 = a.k1 + 1 and a.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res32 = sql"""select ${s} from (select distinct a.* from ${tbName2} c right outer join ${tbName1} b1 on c.k2 = b1.k2 and c.k1 > 0 
                right outer join ${tbName3} a on c.k3 = a.k3 and a.k1 = c.k1 + 1 and a.k3 > 0 
                where a.k1 is not null and b1.k1 is not null and a.k1 is not null and a.k1 > 0 and c.k3 > 0) b
                order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res31, res32)
    }

    // left anti join
    for (s in left_selected){
        def res33 = sql"""select ${s} from ${tbName1} a left anti join ${tbName2} b 
                    on a.k1 = b.k1  order by 1, 2, 3, 4, 5 limit 65535"""
        def res34 = sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b 
                    on a.k1 = b.k1 where b.k3 is null order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res33, res34)
        sql"""select ${s} from ${tbName1} a left anti join ${tbName2} b
                on a.k1 > b.k1 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left anti join ${tbName2} b
                on a.k1 > 0 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res35 = sql"""select ${s} from ${tbName1} a left anti join ${tbName2} b
                    on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 50000"""
        def res36 = sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b 
                    on a.k1 = b.k1 and a.k2 > 0 where b.k3 is null 
                    order by 1, 2, 3, 4, 5 limit 50000"""
        check2_doris(res35, res36)
        def res37 = sql"""select ${s} from ${tbName1} a left anti join ${tbName2} b 
                 on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        def res38 = sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b 
                 on a.k1 = b.k1 and a.k2 > b.k2 where b.k3 is null 
                 order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res37, res38)
        test {
            sql"""select ${s} from ${tbName1} a left anti join ${tbName2} b
               where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        sql"""select ${s} from ${tbName1} a left anti join ${tbName2} b
                on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left anti join ${tbName2} b
                on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left anti join ${tbName2} b
                on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left anti join ${tbName2} b
                on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left anti join ${tbName2} b
                on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res39 = sql"""select ${s} from ${tbName1} a left anti join ${tbName2} b on a.k1 = b.k1
                left anti join ${tbName3} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        def res40 = sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b on a.k1 = b.k1 
                left outer join ${tbName3} c on a.k2 = c.k2 where 
                b.k1 is null and c.k1 is null order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res39, res40)
        def res41 = sql"""select ${s} from ${tbName1} a left anti join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
                left anti join ${tbName3} c on a.k3 = c.k3 and a.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res42 = sql"""select ${s} from (select distinct a.* from ${tbName1} a left outer join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
                left outer join ${tbName3} c on a.k3 = c.k3 and a.k1 = c.k1 + 1 and c.k3 > 0 
                where b.k1 is null and c.k1 is null) a
                order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res41, res42)
    }

    qt_left_anti_join_with_other_pred "select b.k1 from ${tbName2} b left anti join ${tbName1} t on b.k1 = t.k1 and 1 = 2 order by b.k1"

    qt_left_anti_join_null_1 "select b.k1 from ${tbName2} b left anti join ${tbName1} t on b.k1 = t.k1 order by b.k1"

    qt_left_anti_join_null_2 "select b.k1 from ${tbName2} b left anti join ${empty_name} t on b.k1 = t.k1 order by b.k1"

    qt_left_anti_join_null_3 "select b.k1 from ${tbName2} b left anti join ${tbName1} t on b.k1 > t.k2 order by b.k1"

    qt_left_anti_join_null_4 "select b.k1 from ${tbName2} b left anti join ${empty_name} t on b.k1 > t.k2 order by b.k1"

    // right anti join
    for (s in right_selected){
        def res43 = sql"""select ${s} from ${tbName2} a right anti join ${tbName1} b 
                    on a.k1 = b.k1 order by 1, 2, 3, 4, 5 limit 65535"""
        def res44 = sql"""select ${s} from ${tbName2} a right outer join ${tbName1} b 
                on a.k1 = b.k1 where a.k2 is null 
                order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res43, res44)
        sql"""select ${s} from ${tbName1} a right anti join ${tbName2} b
                on a.k1 > b.k1 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right anti join ${tbName2} b
                on a.k1 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res45 = sql"""select ${s} from ${tbName2} a right anti join ${tbName1} b
                    on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        def res46 = sql"""select ${s} from ${tbName2} a right outer join ${tbName1} b 
                    on a.k1 = b.k1 and a.k2 > 0 where a.k2 is null 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res45 , res46)
        def res47 = sql"""select ${s} from ${tbName2} a right anti join ${tbName1} b 
                    on a.k1 = b.k1 and a.k2 > b.k2  order by 1, 2, 3, 4, 5 limit 65535"""
        def res48 = sql"""select ${s} from ${tbName2} a right outer join ${tbName1} b 
                    on a.k1 = b.k1 and a.k2 > b.k2 where a.k2 is null 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res47, res48)
        test {
            sql"""select ${s} from ${tbName1} a right anti join ${tbName2} b 
               where b.k2 > 0 and b.k3 != 0 and b.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        sql"""select ${s} from ${tbName1} a right anti join ${tbName2} b
                on a.k1 = b.k1 or a.k2 = b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right anti join ${tbName2} b
                on a.k1 < b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right anti join ${tbName2} b
                on a.k1 = b.k1 or a.k2 > b.k2 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right anti join ${tbName2} b
                on a.k1 = b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right anti join ${tbName2} b
                on a.k1 < b.k1 or a.k2 > 0 where b.k2 > 0 and b.k3 != 0 and b.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right anti join ${tbName2} c on a.k1 = c.k1
                right anti join ${tbName3} b on c.k2 = b.k2 order by 1, 2, 3, 4, 5 limit 65535"""

        sql"""select ${s} from (select distinct b.k1, b.k2, b.k3, b.k4, b.k5 from 
                ${tbName1} a right outer join ${tbName2} c on a.k1 = c.k1 right outer join 
                ${tbName3} b on c.k2=b.k2) b order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a right anti join ${tbName2} c on a.k2 = c.k2 and a.k1 > 0 
                right anti join ${tbName3} b on c.k3 = b.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from (select distinct c.* from ${tbName1} a right outer join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
                right outer join ${tbName3} c on b.k3 = c.k3 and c.k1 = b.k1 + 1 and c.k3 > 0 
                where b.k1 is null and a.k1 is null and a.k1 > 0) b
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from (select distinct c.k1 k1, c.k2 k2, c.k3 k3, c.k4 k4, c.k5 k5 from 
                (select b2.* from ${tbName1} a right outer join ${tbName2} b2 on a.k2 = b2.k2 and a.k1 > 0 
                where a.k1 is null and a.k1 > 0) b1 right outer join ${tbName3} c 
                on b1.k3 = c.k3 and c.k1 = b1.k1 + 1 and c.k3 > 0 where b1.k1 is null) b 
                order by 1, 2, 3, 4, 5 limit 65535"""
    }

    qt_right_anti_join_with_other_pred "select t.k1 from ${tbName2} b right anti join ${tbName1} t on b.k1 = t.k1 and 1 = 2 order by t.k1"

    qt_right_anti_join_null_1 "select b.k1 from ${tbName1} t right anti join ${tbName2} b on b.k1 > t.k1 order by b.k1"

    qt_right_anti_join_null_2 "select b.k1 from ${empty_name} t right anti join ${tbName2} b on b.k1 > t.k1 order by b.k1"

    qt_pipelineX_max_rf """ select /* set runtime_filter_type = 4; set experimental_enable_pipeline_x_engine = true; */ a.k1, a.k2, a.k3, a.k4, a.k5 from test a left anti join baseall b on a.k1 > b.k1 where a.k2 > 0 and a.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535; """
}
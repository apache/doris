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

suite("test_join_12", "nereids_p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql 'set parallel_fragment_exec_instance_num = 2;'
    sql "use nereids_test_query_db"

    def tbName1 = "test"
    def tbName2 = "baseall"
    def tbName3 = "bigtable"
    def empty_name = "empty"

    List selected = ["a.k1, b.k1, a.k2, b.k2, a.k3, b.k3", "count(a.k1), count(b.k1), count(a.k2), count(b.k2), count(*)"]

    // cross join
    for (s in selected){
        test {
            sql"""select ${s} from ${tbName1} a cross join ${tbName2} b 
                    on a.k1 = b.k1 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from ${tbName1} a cross join ${tbName2} b 
                    on a.k1 > b.k1 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from ${tbName1} a cross join ${tbName2} b 
                    on a.k1 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from ${tbName1} a cross join ${tbName2} b 
                    on a.k1 = b.k1 and a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from ${tbName1} a cross join ${tbName2} b 
                    on a.k1 = b.k1 and a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        qt_cross_join1"""select ${s} from ${tbName1} a cross join ${tbName2} b 
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
        test {
            sql"""select ${s} from ${tbName1} a cross join ${tbName2} b 
                    on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from ${tbName1} a cross join ${tbName2} b 
                    on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from ${tbName1} a cross join ${tbName2} b 
                    on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from ${tbName1} a cross join ${tbName2} b 
                    on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from ${tbName1} a cross join ${tbName2} b 
                    on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from ${tbName1} a cross join ${tbName2} b on a.k1 = b.k1 
                cross join ${tbName3} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        test {
            sql"""select ${s} from ${tbName1} a cross join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
                cross join ${tbName3} c on a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
    }


    // left_semi_join
    List left_selected = ["a.k1, a.k2, a.k3, a.k4, a.k5", "count(a.k1), count(a.k2), count(a.k4), count(a.k3), count(*)"]
    for (s in left_selected){
        sql"""select ${s} from ${tbName1} a left semi join ${tbName2} b on a.k1 = b.k1 
                 order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b on a.k1 = b.k1 
                 where b.k3 is not null order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left semi join ${tbName2} b on a.k1 > b.k1
            where a.k2 > 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left semi join ${tbName2} b on a.k1 > 0
            where a.k2 > 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
        def res15 = sql"""select ${s} from ${tbName1} a left semi join ${tbName2} b
                on a.k1 = b.k1 and a.k2 > 0 order by 1, 2, 3, 4, 5 limit 65535"""
        def res16 = sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b 
                on a.k1 = b.k1 and a.k2 > 0 where b.k3 is not null 
                order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res15, res16)
        def res17 = sql"""select ${s} from ${tbName1} a left semi join ${tbName2} b 
                    on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        def res18 = sql"""select ${s} from ${tbName1} a left outer join ${tbName2} b 
                 on a.k1 = b.k1 and a.k2 > b.k2 where b.k3 is not null 
                 order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res17, res18)
        test {
            sql"""select ${s} from ${tbName1} a left semi join ${tbName2} b 
                where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        sql"""select ${s} from ${tbName1} a left semi join ${tbName2} b
                on a.k1 = b.k1 or a.k2 = b.k2 where a.k2 > 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left semi join ${tbName2} b
                on a.k1 < b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k6 > "000"
                order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left semi join ${tbName2} b
            on a.k1 = b.k1 or a.k2 > b.k2 where a.k2 > 0 and a.k6 > "000"
            order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left semi join ${tbName2} b
            on a.k1 = b.k1 or a.k2 > 0 where a.k2 > 0 and a.k6 > "000"
            order by 1, 2, 3, 4, 5 limit 65535"""
        sql"""select ${s} from ${tbName1} a left semi join ${tbName2} b
            on a.k1 < b.k1 or a.k2 > 0 where a.k2 > 0 and a.k6 > "000"
            order by 1, 2, 3, 4, 5 limit 65535"""
        def res19 = sql"""select ${s} from ${tbName1} a left semi join ${tbName2} b on a.k1 = b.k1
                left semi join ${tbName3} c on a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        def res20 = sql"""select ${s} from (select distinct a.* from ${tbName1} a left outer join ${tbName2} b on a.k1 = b.k1 
                left outer join ${tbName3} c on a.k2 = c.k2 where a.k1 is not null 
                and b.k1 is not null and c.k1 is not null) a order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res19, res20)
        def res21 = sql"""select ${s} from ${tbName1} a left semi join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
                left semi join ${tbName3} c on a.k3 = c.k3 and a.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
        def res22 = sql"""select ${s} from (select distinct a.* from ${tbName1} a left outer join ${tbName2} b on a.k2 = b.k2 and a.k1 > 0 
                left outer join ${tbName3} c on a.k3 = c.k3 and a.k1 = c.k1 + 1 and c.k3 > 0 
                where a.k1 is not null and b.k1 is not null and c.k1 is not null and a.k1 > 0 and c.k3 > 0) a
                order by 1, 2, 3, 4, 5 limit 65535"""
        check2_doris(res21, res22)
    }

    qt_cross_join2 """
        select t1.k1 from ${tbName1} t1 cross join ${tbName1} t2 where t1.k1 = t2.k1 + 1 group by t1.k1 order by t1.k1;
    """
}
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

suite("test_grouping_sets") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
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

    test {
        sql """
              SELECT k1, k2, SUM(k3) FROM test_query_db.test
              GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ( ), (k3) ) order by k1, k2
            """
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
    }

    test {
        sql """
              SELECT k1, k2, SUM(k3)/(SUM(k3)+1) FROM test_query_db.test
              GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ( ), (k3) ) order by k1, k2
            """
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
    }

   qt_select7 """ select k1,k2,sum(k3) from test_query_db.test where 1 = 2 group by grouping sets((k1), (k1,k2)) """ 
}

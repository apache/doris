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
suite("test_cte_filter_pushdown)") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_pipeline_engine=true"
    sql "SET enable_fallback_to_original_planner=false"

    // CTE filter pushing down with the same filter
    qt_cte_filter_pushdown_1 """
            explain shape plan
            with main AS (
               select k1, row_number() over (partition by k1) rn
               from nereids_test_query_db.test
           )
           select * from (
               select m1.* from main m1, main m2
               where m1.k1 = m2.k1
           ) temp
           where k1 = 1;
    """
    qt_cte_filter_pushdown_2 """
            explain shape plan
            with main AS (
               select k1, row_number() over (partition by k2) rn
               from nereids_test_query_db.test
           )
           select * from (
               select m1.* from main m1, main m2
               where m1.k1 = m2.k1
           ) temp
           where k1 = 1;
    """
}

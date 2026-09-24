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

suite("test_select_analytic_shuffle_join") {
    sql "DROP TABLE IF EXISTS select_analytic_join_left"
    sql "DROP TABLE IF EXISTS select_analytic_join_right"

    sql """CREATE TABLE select_analytic_join_left (
                k INT, v INT
            ) ENGINE=OLAP DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 10
            PROPERTIES ("replication_num"="1")"""
    sql """CREATE TABLE select_analytic_join_right (
                k INT, w INT
            ) ENGINE=OLAP DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 10
            PROPERTIES ("replication_num"="1")"""

    sql """INSERT INTO select_analytic_join_left VALUES
            (0,1),(1,2),(2,3),(3,4),(4,5),(5,6),(6,7),(7,8),(8,9),(9,10)"""
    sql """INSERT INTO select_analytic_join_right VALUES
            (0,100),(1,101),(2,102),(3,103),(4,104),
            (5,105),(6,106),(7,107),(8,108),(9,109)"""

    def variables = "enable_local_shuffle_planner=true,enable_local_shuffle=true," +
            "enable_bucket_shuffle_join=false,ignore_storage_data_distribution=true," +
            "parallel_pipeline_task_num=3,enable_sql_cache=false"

    order_qt_select_over_analytic_shuffle_join """SELECT /*+SET_VAR(${variables})*/
            COUNT(*), SUM(s.k), SUM(s.running_v), SUM(d.w)
        FROM (
            SELECT k, SUM(v) OVER (PARTITION BY v ORDER BY v
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_v,
                    random(0) AS r
            FROM select_analytic_join_left
        ) s JOIN [shuffle] select_analytic_join_right d ON s.k=d.k
        WHERE s.r < 2.0"""

    order_qt_select_over_aligned_analytic_shuffle_join """SELECT /*+SET_VAR(${variables})*/
            COUNT(*), SUM(s.k), SUM(s.running_v), SUM(d.w)
        FROM (
            SELECT k, SUM(v) OVER (PARTITION BY k ORDER BY k
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_v,
                    random(0) AS r
            FROM select_analytic_join_left
        ) s JOIN [shuffle] select_analytic_join_right d ON s.k=d.k
        WHERE s.r < 2.0"""
}

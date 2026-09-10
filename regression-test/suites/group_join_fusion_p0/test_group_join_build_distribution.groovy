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

suite("test_group_join_build_distribution") {
    sql "DROP TABLE IF EXISTS gj_build_distribution"
    sql """CREATE TABLE gj_build_distribution (
        id INT NOT NULL, k DATE NULL, s VARCHAR(16) NULL
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
    PROPERTIES ("replication_num"="1")"""
    sql """INSERT INTO gj_build_distribution VALUES
        (1,'2023-12-11','a'),(2,'2023-12-12','b'),(3,'2023-12-11','a')"""
    sql "SET enable_sql_cache=false"
    sql "SET query_cache_force_refresh=true"
    sql "SET eager_agg_broadcast_row_count=0"
    sql "SET broadcast_row_count_limit=0"
    sql "SET agg_phase=1"
    sql "SET eager_aggregation_mode=-1"
    sql "SET enable_bucket_shuffle_join=false"
    sql "SET parallel_pipeline_task_num=4"
    def queries = [
        """SELECT l.k FROM gj_build_distribution l JOIN [shuffle] gj_build_distribution r
            ON l.k=r.k GROUP BY l.k ORDER BY l.k""",
        """SELECT l.k, COUNT(*), SUM(l.id), SUM(r.id)
            FROM gj_build_distribution l JOIN [shuffle] gj_build_distribution r
            ON l.k=r.k GROUP BY l.k ORDER BY l.k""",
        """SELECT l.k,l.s FROM gj_build_distribution l JOIN [shuffle] gj_build_distribution r
            ON l.k=r.k AND l.s=r.s GROUP BY l.k,l.s ORDER BY l.k,l.s"""
    ]
    for (serial in [true, false]) {
        sql "SET experimental_use_serial_exchange=${serial}"
        for (planner in [false, true]) {
            sql "SET experimental_enable_local_shuffle_planner=${planner}"
            for (mode in ['OFF', 'GLOBAL']) {
                sql "SET runtime_filter_mode='${mode}'"
                for (query in queries) {
                    sql "SET experimental_enable_group_join_fusion=false"
                    def reference = sql query
                    sql "SET experimental_enable_group_join_fusion=true"
                    explain {
                        sql query
                        contains "VGROUP JOIN"
                    }
                    assertEquals(reference, sql(query))
                    qt_result query
                }
            }
        }
    }
}

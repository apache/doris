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

suite("test_join_based_group_join_state_pages") {
    sql "SET agg_phase=1"
    sql "SET eager_aggregation_mode=-1"
    sql "SET eager_agg_broadcast_row_count=0"
    sql "SET broadcast_row_count_limit=0"
    sql "SET enable_bucketed_hash_agg=false"
    sql "SET enable_bucket_shuffle_join=false"
    sql "SET enable_spill=false"
    sql "SET runtime_filter_mode='OFF'"
    sql "DROP TABLE IF EXISTS gj_join_state_pages_left"
    sql "DROP TABLE IF EXISTS gj_join_state_pages_right"
    sql """CREATE TABLE gj_join_state_pages_left (k BIGINT NOT NULL, v DOUBLE NULL)
           DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 4
           PROPERTIES('replication_num'='1')"""
    sql "CREATE TABLE gj_join_state_pages_right LIKE gj_join_state_pages_left"
    sql """INSERT INTO gj_join_state_pages_left
           SELECT number,IF(number % 3=0,NULL,CAST(number % 17 AS DOUBLE))
           FROM numbers('number'='32769')"""
    sql """INSERT INTO gj_join_state_pages_right
           SELECT number,CAST(number % 23 AS DOUBLE)
           FROM numbers('number'='32769')"""
    sql """INSERT INTO gj_join_state_pages_right
           SELECT number,IF(number % 5=0,NULL,CAST(number % 29 AS DOUBLE))
           FROM numbers('number'='32769')"""
    sql "ANALYZE TABLE gj_join_state_pages_left WITH SYNC"
    sql "ANALYZE TABLE gj_join_state_pages_right WITH SYNC"
    def groups = """SELECT /*+ LEADING(l r) */ l.k,COUNT(*) AS n,
                    SUM(l.v) AS a,SUM(r.v) AS b,AVG(l.v) AS c,
                    SUM(l.v*r.v) AS d,VAR_POP(l.v) AS e
                    FROM gj_join_state_pages_left l
                    JOIN [shuffle] gj_join_state_pages_right r ON l.k=r.k
                    GROUP BY l.k"""
    def checksum = """SELECT COUNT(*),SUM(n),SUM(a),SUM(b),SUM(c),SUM(d),SUM(e)
                      FROM (${groups}) grouped"""
    for (def parallelism : [1, 4]) {
        sql "SET parallel_pipeline_task_num=${parallelism}"
        sql "SET experimental_enable_group_join_fusion=false"
        order_qt_reference checksum
        sql "SET experimental_enable_group_join_fusion=true"
        explain {
            sql groups
            contains "implementation: JOIN_BASED"
        }
        explain {
            sql checksum
            contains "implementation: JOIN_BASED"
        }
        order_qt_pages checksum
    }
}

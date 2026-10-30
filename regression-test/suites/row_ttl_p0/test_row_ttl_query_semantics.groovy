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

suite("test_row_ttl_query_semantics") {
    sql "DROP TABLE IF EXISTS row_ttl_query_semantics"
    sql """
        CREATE TABLE row_ttl_query_semantics(k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "function_column.enable_row_ttl" = "true"
        )
    """
    sql "SET enable_sql_cache = true"
    sql "SET enable_query_cache = true"
    sql """
        INSERT INTO row_ttl_query_semantics(k, v, __DORIS_TTL_COL__) VALUES
            (0, 0, 0),
            (1, 10, now(6) + INTERVAL 30 SECOND),
            (2, 20, NULL),
            (3, 30, 9223372036854775807)
    """
    qt_count_before "SELECT count(*), sum(v) FROM row_ttl_query_semantics"
    qt_count_repeated "SELECT count(*), sum(v) FROM row_ttl_query_semantics"
    order_qt_topn_before "SELECT k, v FROM row_ttl_query_semantics ORDER BY k LIMIT 1"

    // No writes or compactions invalidate a cached result as time alone expires key 1.
    sleep(31000)
    qt_count_after "SELECT count(*), sum(v) FROM row_ttl_query_semantics"
    order_qt_topn_after "SELECT k, v FROM row_ttl_query_semantics ORDER BY k LIMIT 1"
    order_qt_join_after """
        SELECT a.k, b.v
        FROM row_ttl_query_semantics a JOIN row_ttl_query_semantics b ON a.k = b.k
        ORDER BY a.k
    """
}

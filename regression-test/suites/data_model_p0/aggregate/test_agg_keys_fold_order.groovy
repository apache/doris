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

// The rows of one AGG_KEYS key are folded newest first on every path that folds them: a scan
// without pre-aggregation, a vertical compaction and a horizontal compaction. Value columns whose
// result depends on the order of the rows show it: map_agg_v2 keeps the first value of a map key,
// group_concat lists the values in order, and a DOUBLE SUM of 1e16, -1e16 and 1 depends on the
// order of the additions. Key 1 gets three rowsets, and a query must return the same row before
// and after either compaction.
suite("test_agg_keys_fold_order", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }
    sql "SET enable_agg_state = true"
    for (boolean vertical : [true, false]) {
        def table = "test_agg_keys_fold_order_" + (vertical ? "vertical" : "horizontal")
        setBeConfigTemporary([enable_vertical_compaction: vertical]) {
            sql "DROP TABLE IF EXISTS ${table}"
            sql """
                CREATE TABLE ${table} (
                    k INT,
                    m AGG_STATE<map_agg_v2(INT, INT)> GENERIC,
                    g AGG_STATE<group_concat(STRING)> GENERIC,
                    d DOUBLE SUM
                ) AGGREGATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
            """
            sql """INSERT INTO ${table} VALUES
                    (1, map_agg_v2_state(7, 10), group_concat_state('a'), 1e16)"""
            sql """INSERT INTO ${table} VALUES
                    (1, map_agg_v2_state(7, 20), group_concat_state('b'), -1e16)"""
            sql """INSERT INTO ${table} VALUES
                    (1, map_agg_v2_state(8, 30), group_concat_state('c'), 1)"""

            // Without an aggregation above it, the scan folds the three rows of key 1 itself.
            def query = """
                SELECT k, map_agg_v2_finalize(m), group_concat_finalize(g), d FROM ${table}
            """
            explain {
                sql query
                contains "PREAGGREGATION: OFF"
            }
            def before = sql query
            "qt_${vertical ? 'vertical' : 'horizontal'}"(query)

            trigger_and_wait_compaction(table, "full")
            def tablets = sql_return_maparray("SHOW TABLETS FROM ${table}")
            def (code, out, err) = curl("GET", tablets[0].MetaUrl)
            assertEquals(0, code)
            assertEquals(1, parseJson(out.trim()).rs_metas.size(),
                    "expected the full compaction to leave one rowset")
            assertEquals(before, sql(query))
        }
    }
}

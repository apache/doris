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

suite("test_sample_minmax_batch", "nonConcurrent") {
    sql("DROP TABLE IF EXISTS minmax_batch_dup")
    sql """CREATE TABLE minmax_batch_dup (k INT, a INT, b INT)
           DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
    sql("INSERT INTO minmax_batch_dup VALUES (1, 3, 20), (1, -5, NULL), (2, 7, 10)")

    sql("DROP TABLE IF EXISTS minmax_batch_unique")
    sql """CREATE TABLE minmax_batch_unique (k INT, a INT, b INT)
           UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
           PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")"""
    // Statistics can read physical zone-map bounds. Keep the overwritten row inside the final
    // range so compaction between the two ANALYZE jobs cannot change either endpoint.
    sql("INSERT INTO minmax_batch_unique VALUES (1, 0, 15), (2, 7, 10)")
    sql("INSERT INTO minmax_batch_unique VALUES (1, -5, 20)")

    sql("DROP TABLE IF EXISTS minmax_batch_agg")
    sql """CREATE TABLE minmax_batch_agg (k INT, k2 INT, v INT SUM)
           AGGREGATE KEY(k, k2) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
    sql("INSERT INTO minmax_batch_agg VALUES (1, 3, 10), (1, 3, 11), (2, -5, 5)")

    def bounds = { rows -> rows.collectEntries { [(it[0]): [it[7], it[8]]] } }
    def analyze = {
        sql("DROP STATS minmax_batch_dup")
        sql("DROP STATS minmax_batch_unique")
        sql("DROP STATS minmax_batch_agg")
        sql("ANALYZE TABLE minmax_batch_dup WITH SAMPLE ROWS 100 WITH SYNC")
        sql("ANALYZE TABLE minmax_batch_unique WITH SAMPLE ROWS 100 WITH SYNC")
        sql("ANALYZE TABLE minmax_batch_agg WITH SAMPLE ROWS 100 WITH SYNC")
        return [bounds(sql("SHOW COLUMN STATS minmax_batch_dup")),
                bounds(sql("SHOW COLUMN STATS minmax_batch_unique")),
                bounds(sql("SHOW COLUMN STATS minmax_batch_agg"))]
    }
    // With these rows the visible bounds also give an independent check of the single-column baseline.
    def expected = { row, names ->
        names.withIndex().collectEntries { name, i -> [(name): row[2 * i..2 * i + 1].collect { it.toString() }] }
    }
    def truth = [
            expected(sql("SELECT MIN(k), MAX(k), MIN(a), MAX(a), MIN(b), MAX(b) FROM minmax_batch_dup")[0],
                    ["k", "a", "b"]),
            expected(sql("SELECT MIN(k), MAX(k), MIN(a), MAX(a), MIN(b), MAX(b) FROM minmax_batch_unique")[0],
                    ["k", "a", "b"]),
            expected(sql("SELECT MIN(k), MAX(k), MIN(k2), MAX(k2), MIN(v), MAX(v) FROM minmax_batch_agg")[0],
                    ["k", "k2", "v"])
    ]
    def saved = sql("SHOW GLOBAL VARIABLES LIKE 'statistics_minmax_batch_columns'")[0][1]
    try {
        sql("SET GLOBAL statistics_minmax_batch_columns = 1")
        def single = analyze()
        assertEquals(truth, single)
        sql("SET GLOBAL statistics_minmax_batch_columns = 64")
        assertEquals(single, analyze())
    } finally {
        sql("SET GLOBAL statistics_minmax_batch_columns = ${saved}")
    }
}

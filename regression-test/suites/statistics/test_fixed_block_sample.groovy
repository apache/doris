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

suite("test_fixed_block_sample", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS fixed_block_sample_rows"
    sql """CREATE TABLE fixed_block_sample_rows (k INT NOT NULL, v INT)
           DUPLICATE KEY(k)
           PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN (4096), PARTITION p2 VALUES LESS THAN (8192))
           DISTRIBUTED BY HASH(k) BUCKETS 1
           ROLLUP (sample_rollup(v, k))
           PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true")"""
    sql """INSERT INTO fixed_block_sample_rows SELECT number, number FROM numbers("number"="8192")"""
    def byIndex = [:]
    for (row in sql_return_maparray("SHOW TABLETS FROM fixed_block_sample_rows PARTITION(p1)")) {
        def owner = sql_return_maparray("SHOW TABLET ${row.TabletId}")
        byIndex[owner[0].IndexName] = row.TabletId
    }
    def tablet = byIndex.fixed_block_sample_rows
    def rollupTablet = byIndex.sample_rollup
    assertNotNull(tablet)
    assertNotNull(rollupTablet)
    def base = "fixed_block_sample_rows INDEX fixed_block_sample_rows"
    def sample = "TABLESAMPLE (FIXED_BLOCK unit_rows=32, GROUP (tablet_rate=1, unit_rate=0.5, " +
                 "max_units=512, TABLETS (${tablet}))) REPEATABLE 8101"
    def gate = "TableSample.allowInternalFixedBlock"
    GetDebugPoint().enableDebugPointForAllFEs(gate)
    try {
        sql "SET enable_parallel_scan = false"
        def serial = sql "SELECT k FROM ${base} ${sample} ORDER BY k"
        assertFalse(serial.isEmpty())
        assertTrue(serial.size() < 4096)
        sql "SET enable_parallel_scan = true"
        sql "SET parallel_scan_min_rows_per_scanner = 1024"
        sql "SET parallel_scan_max_scanners_count = 4"
        def parallel = sql "SELECT k FROM ${base} ${sample} ORDER BY k"
        assertEquals(serial, parallel)
        def count = sql "SELECT COUNT(*) FROM ${base} ${sample}"
        assertEquals(serial.size() as long, count[0][0] as long)
        def excluded = sql "SELECT k FROM ${base} ${sample} WHERE k >= 4096"
        assertTrue(excluded.isEmpty())
        def rollup = sample.replace("TABLETS (${tablet})", "TABLETS (${rollupTablet})").replace("unit_rate=0.5", "unit_rate=1")
        def actual = sql "SELECT k FROM fixed_block_sample_rows INDEX sample_rollup ${rollup} ORDER BY k"
        assertEquals((0..<4096).toList(), actual.collect { it[0] as int })
        test {
            sql "SELECT k FROM ${base} ${sample.replace('max_units=512', 'max_units=1')}"
            exception "exceed cap"
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(gate)
    }
}

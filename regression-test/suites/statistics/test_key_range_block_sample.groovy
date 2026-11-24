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

suite("test_key_range_block_sample", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS key_range_sample_agg"
    sql """CREATE TABLE key_range_sample_agg (k INT NOT NULL, v BIGINT SUM NOT NULL)
           AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
           PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true")"""
    sql """INSERT INTO key_range_sample_agg SELECT number, 3 FROM numbers("number"="16384")"""
    sql """INSERT INTO key_range_sample_agg SELECT number, 5 FROM numbers("number"="16384")"""
    sql """INSERT INTO key_range_sample_agg SELECT 16300 + number, 7 FROM numbers("number"="200")"""
    sql "DELETE FROM key_range_sample_agg WHERE k = 16301"

    sql "DROP TABLE IF EXISTS key_range_sample_mor"
    sql """CREATE TABLE key_range_sample_mor (k INT NOT NULL, v INT, seq BIGINT)
           UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
           PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true",
                       "enable_unique_key_merge_on_write"="false", "function_column.sequence_col"="seq")"""
    sql """INSERT INTO key_range_sample_mor SELECT number, 7, 1 FROM numbers("number"="16384")"""
    sql """INSERT INTO key_range_sample_mor (k,v,seq,__DORIS_DELETE_SIGN__)
           SELECT number, 8, 3, IF(number % 5 = 0, 1, 0)
           FROM numbers("number"="16384")"""
    // A later write with a lower sequence must neither win nor resurrect deleted rows.
    sql """INSERT INTO key_range_sample_mor SELECT number, 9, 2 FROM numbers("number"="16384")"""

    // Row TTL and sequence columns are mutually exclusive table properties.
    sql "DROP TABLE IF EXISTS key_range_sample_ttl"
    sql """CREATE TABLE key_range_sample_ttl (k INT NOT NULL, v INT) UNIQUE KEY(k)
           DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true",
                       "enable_unique_key_merge_on_write"="false", "function_column.enable_row_ttl"="true")"""
    sql """INSERT INTO key_range_sample_ttl SELECT number, 1 FROM numbers("number"="16384")"""
    sql """INSERT INTO key_range_sample_ttl(k,v,__DORIS_TTL_COL__)
           SELECT number, 2, IF(number % 7 = 0, 0, NULL) FROM numbers("number"="16384")"""

    def gate = "TableSample.allowInternalFixedBlock"
    GetDebugPoint().enableDebugPointForAllFEs(gate)
    try {
        for (def name in ["key_range_sample_agg", "key_range_sample_mor", "key_range_sample_ttl"]) {
            def tablet = sql_return_maparray("SHOW TABLETS FROM ${name}")[0].TabletId
            def clause = { rate, cap -> "TABLESAMPLE (KEY_RANGE_BLOCK unit_rows=1024, GROUP (" +
                    "tablet_rate=1, unit_rate=${rate}, max_units=${cap}, TABLETS (${tablet}))) REPEATABLE 8101" }
            def reference = sql "SELECT k, v FROM ${name} ORDER BY k"
            def full = clause(1, 64)
            assertEquals(reference, sql("SELECT k, v FROM ${name} ${full} ORDER BY k"))
            // Use the keys selected from this exact layout to build the ordinary merged-query oracle.
            def partial = clause(0.5, 64)
            def keys = sql("SELECT k FROM ${name} ${partial} ORDER BY k").collect { it[0] } as Set
            assertFalse(keys.isEmpty())
            assertTrue(keys.size() < reference.size())
            def expected = reference.findAll { keys.contains(it[0]) }
            assertEquals(expected, sql("SELECT k, v FROM ${name} ${partial} ORDER BY k"))
            assertEquals(expected.collect { [it[1]] }.sort { it[0] },
                    sql("SELECT v FROM ${name} ${partial} ORDER BY v"))
            test {
                sql "SELECT k FROM ${name} ${clause(1, 1)} ORDER BY k"
                exception "exceed cap"
            }
            test {
                sql "SELECT v FROM ${name} ${partial} /*+PREAGGOPEN*/ ORDER BY v"
                exception "PREAGGOPEN cannot read a KEY_RANGE_BLOCK sample"
            }
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(gate)
    }
}

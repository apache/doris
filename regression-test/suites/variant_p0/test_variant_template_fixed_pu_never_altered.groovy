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

// Fixed partial update on a never-ALTERed templated table copies the historical VARIANT. Template
// paths whose values already have the template type must stay typed (and keep their index) in the
// partial update segment and after compaction.
suite("test_variant_template_fixed_pu_never_altered", "p0") {
    sql "set default_variant_enable_doc_mode = false"
    for (String rowStore : ["true", "false"]) {
        def t = "test_variant_template_fixed_pu_never_altered_${rowStore}"
        sql "DROP TABLE IF EXISTS ${t}"
        sql """CREATE TABLE ${t} (k INT, c INT,
                   v variant<MATCH_NAME 'p':datetimev2(3), MATCH_NAME 'd':decimal(38,10), MATCH_NAME 'f':double,
                             PROPERTIES("variant_max_subcolumns_count" = "0")>,
                   INDEX idx_d (v) USING INVERTED PROPERTIES("field_pattern" = "d"))
               UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
               PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                           "store_row_column" = "${rowStore}", "disable_auto_compaction" = "true")"""
        sql """INSERT INTO ${t} VALUES
               (1, 1, '{"p": "2024-01-01 00:00:00.120", "d": 12345678901234567890.1234567891, "f": 0.1}'),
               (2, 2, '{"p": "2024-02-29 23:59:59.999", "d": 0.0000000001, "f": 1e300}'),
               (3, 3, '{"p": "2024-03-01 00:00:00", "d": -99999999999999999999.9999999999, "f": 3.141592653589793}')"""
        sql "set describe_extend_variant_column = true"
        def descBefore = sql "DESC ${t}"
        logger.info("R3 desc before ${descBefore}")
        def before = sql "SELECT k, CAST(v['p'] AS STRING), CAST(v['d'] AS STRING), CAST(v['f'] AS STRING) FROM ${t} ORDER BY k"
        def hit0 = sql "SELECT k FROM ${t} WHERE v['d'] = 0.0000000001"
        def hitc0 = sql "SELECT k FROM ${t} WHERE CAST(v['d'] AS DECIMAL(38,10)) = 0.0000000001"
        logger.info("R3 ${t} hit before PU=${hit0} cast=${hitc0}")
        sql "set enable_unique_key_partial_update = true"
        sql "set enable_insert_strict = false"
        sql "INSERT INTO ${t} (k, c) VALUES (1, 10), (2, 20), (3, 30)"
        sql "set enable_unique_key_partial_update = false"
        sql "set enable_insert_strict = true"
        def after = sql "SELECT k, CAST(v['p'] AS STRING), CAST(v['d'] AS STRING), CAST(v['f'] AS STRING) FROM ${t} ORDER BY k"
        assertEquals(before, after)
        trigger_and_wait_compaction(t, "full", 300)
        def compacted = sql "SELECT k, CAST(v['p'] AS STRING), CAST(v['d'] AS STRING), CAST(v['f'] AS STRING) FROM ${t} ORDER BY k"
        assertEquals(before, compacted)
        def descAfter = sql "DESC ${t}"
        logger.info("R3 desc after ${descAfter}")
        assertEquals(descBefore.toString(), descAfter.toString())
        // Typed predicates and the field index on d still answer correctly.
        def hit = sql "SELECT k FROM ${t} WHERE v['d'] = 0.0000000001"
        def hitc = sql "SELECT k FROM ${t} WHERE CAST(v['d'] AS DECIMAL(38,10)) = 0.0000000001"
        logger.info("R3 ${t} hit after=${hit} cast=${hitc}")
        assertEquals(hit0, hit)
        logger.info("R3 ${t} values=${compacted}")
    }
}

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

suite("test_variant_count_on_index_fault_injection", "p0, nonConcurrent") {
    def tbl = "test_variant_count_on_index_tbl"

    def toInt = { v -> Integer.parseInt(v.toString()) }
    def col0List = { res -> res.collect { it[0] == null ? "NULL" : it[0].toString() } }

    sql "DROP TABLE IF EXISTS ${tbl}"

    sql "set enable_common_expr_pushdown = true"
    sql "set enable_count_on_index_pushdown = true"
    sql "set enable_match_without_inverted_index = false"
    sql "set experimental_enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"
    sql "set inverted_index_skip_threshold = 0"
    sql "set default_variant_enable_doc_mode = false"

    sql """
        CREATE TABLE ${tbl} (
            k INT,
            v  VARIANT<'c':bigint, PROPERTIES("variant_max_subcolumns_count"="0")> NOT NULL,
            v2 VARIANT<PROPERTIES("variant_max_subcolumns_count"="0")> NOT NULL,
            INDEX idx_v(v) USING INVERTED PROPERTIES("parser" = "english") COMMENT '',
            INDEX idx_v2(v2) USING INVERTED PROPERTIES("parser" = "english") COMMENT '',
            INDEX idx_v_c(v) USING INVERTED PROPERTIES("field_pattern" = "c")
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        );
    """

    sql """
        INSERT INTO ${tbl} VALUES
            (1, '{"a":"hello","b":"world","c":1}', '{"b":"foo"}'),
            (2, '{"a":"hello","b":"world","c":2}', '{"b":"world"}'),
            (3, '{"a":"hello","b":"xxx","c":1}',   '{"c":1}'),
            (4, '{"a":"xxx","b":"world","c":3}',  '{"b":"bar"}'),
            (5, '{"a":"hello hello","b":"world world","c":1}', '{"b":"baz"}');
    """

    sql "sync"
    sql "analyze table ${tbl} with sync"

    // ------------------------------------------------------------
    // Case1: Correctness tests for each SQL
    // ------------------------------------------------------------

    // count on index
    def r1 = sql "select count() from ${tbl} where v['a'] match 'hello'"
    assertEquals(4, toInt(r1[0][0]))

    def r2 = sql "select count(v['b']) from ${tbl} where v['a'] match 'hello' and v['b'] match 'world'"
    assertEquals(3, toInt(r2[0][0]))

    def r3 = sql "select count(v) from ${tbl} where v['a'] match 'hello' and v['b'] match 'world'"
    assertEquals(3, toInt(r3[0][0]))

    def r4 = sql "select count(v2['b']) from ${tbl} where v['a'] match 'hello'"
    assertEquals(3, toInt(r4[0][0]))

    // non count on index
    def r5 = sql "select v['b'] from ${tbl} where v['a'] match 'hello'"
    def r5v = col0List(r5).sort()
    assertEquals(4, r5v.size())
    assertTrue(r5v.any { it.toLowerCase().contains("world") })
    assertTrue(r5v.any { it.toLowerCase().contains("xxx") })

    def r6 = sql "select v2['b'] from ${tbl} where v['a'] match 'hello'"
    def r6v = col0List(r6).sort()
    assertEquals(4, r6v.size())
    assertTrue(r6v.any { it.toLowerCase().contains("foo") })
    assertTrue(r6v.any { it.toLowerCase().contains("world") })
    assertTrue(r6v.any { it == "NULL" || it.toLowerCase().contains("null") })

    def r7 = sql "select v['a'] from ${tbl} where v['a'] match 'hello'"
    def r7v = col0List(r7).sort()
    assertEquals(4, r7v.size())
    assertTrue(r7v.any { it.toLowerCase().contains("hello hello") })

    // numeric subcolumn: ensure numeric predicates can still use index-only count optimization
    def rn1 = sql "select count() from ${tbl} where cast(v['c'] as bigint) = 1"
    assertEquals(3, toInt(rn1[0][0]))

    def rn2 = sql "select cast(v['c'] as bigint) from ${tbl} where cast(v['c'] as bigint) = 1"
    assertEquals(3, rn2.size())

    // Extra: Ensure COUNT_ON_INDEX is chosen in plan for the 4 count queries.
    explain {
        sql("select count() from ${tbl} where v['a'] match 'hello'")
        contains "pushAggOp=COUNT_ON_INDEX"
    }
    explain {
        sql("select count(v['b']) from ${tbl} where v['a'] match 'hello' and v['b'] match 'world'")
        contains "pushAggOp=COUNT_ON_INDEX"
    }
    explain {
        sql("select count(v) from ${tbl} where v['a'] match 'hello' and v['b'] match 'world'")
        contains "pushAggOp=COUNT_ON_INDEX"
    }
    explain {
        sql("select count(v2['b']) from ${tbl} where v['a'] match 'hello'")
        contains "pushAggOp=COUNT_ON_INDEX"
    }
    explain {
        sql("select count() from ${tbl} where cast(v['c'] as bigint) = 1")
        contains "pushAggOp=COUNT_ON_INDEX"
    }

    def dp3 = sql "select count(v) from ${tbl} where v['a'] match 'hello' and v['b'] match 'world'"
    assertEquals(3, toInt(dp3[0][0]))

    // ------------------------------------------------------------
    // Case2: DebugPoint validation - COUNT_ON_INDEX works and doesn't read data
    // Reference: fault_injection_p0/test_need_read_data_fault_injection.groovy
    // ------------------------------------------------------------

    try {
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator._read_columns_by_index", [column_name: "v.a"])
        def dp4 = sql "select count(v2['b']) from ${tbl} where v['a'] match 'hello'"
        assertEquals(3, toInt(dp4[0][0]))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator._read_columns_by_index")

        def dp1 = sql "select count() from ${tbl} where v['a'] match 'hello'"
        assertEquals(4, toInt(dp1[0][0]))

        def dp2 = sql "select count(v['b']) from ${tbl} where v['a'] match 'hello' and v['b'] match 'world'"
        assertEquals(3, toInt(dp2[0][0]))

        def dpn1 = sql "select count() from ${tbl} where cast(v['c'] as bigint) = 1"
        assertEquals(3, toInt(dpn1[0][0]))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
    }
}

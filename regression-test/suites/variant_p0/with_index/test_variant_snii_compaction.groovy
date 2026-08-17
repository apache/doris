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

suite("regression_test_variant_snii_compaction", "p0, nonConcurrent"){
    def variantV2Function = getFeConfig("enable_variant_v2").toBoolean() ? "parse_to_variant" : ""
    def table_name = "var_snii_compaction"
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant<'a' : int, 'b' : string>,
            INDEX idx_var(v) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true",
                   "inverted_index_storage_format" = "SNII");
    """

    // One rowset per insert, so compaction has several to merge.
    sql """insert into ${table_name} values(1, ${variantV2Function}('{"a" : 100, "b" : "hello world"}'))"""
    sql """insert into ${table_name} values(2, ${variantV2Function}('{"a" : 200, "b" : "hello doris"}'))"""
    sql """insert into ${table_name} values(3, ${variantV2Function}('{"a" : 300, "b" : "goodbye world"}'))"""
    sql """insert into ${table_name} values(4, ${variantV2Function}('{"a" : 400, "b" : "hello again"}'))"""

    sql """set enable_match_without_inverted_index = false"""

    // The filtered_rows debug point checks a CUMULATIVE count against a single OlapReaderStatistics
    // shared across every segment touched by one scan (segment_iterator.cpp:874,889). Before
    // compaction the table is still four single-row rowsets/segments, so arming it here would
    // implicitly assume the scan visits the a=100 segment before any other -- an ordering the
    // suite never asserts and that is not guaranteed. Leave the before-side range query as a plain
    // result assertion; it does not need to prove index usage for the suite to do its job.
    qt_before_match """select k from ${table_name} where cast(v["b"] as string) match 'hello' order by k"""
    qt_before_range """select k from ${table_name} where cast(v["a"] as int) >= 200 order by k"""

    trigger_and_wait_compaction(table_name, "full", 1800)

    // The same two queries must answer identically once every sub-column index has been
    // rebuilt by compaction. Index matching stays disabled, so a regression shows up as a
    // wrong answer rather than being masked by a full scan.
    qt_after_match """select k from ${table_name} where cast(v["b"] as string) match 'hello' order by k"""

    // After compaction the four rowsets have merged into one, so this scan touches a single
    // segment: the cumulative filtered_rows count is unambiguous regardless of scan order. This
    // is the assertion that carries the suite's purpose -- proving the REBUILT index still serves
    // the predicate, not a row scan. Of the four rows, only k=1 (a=100) fails ">= 200", so the
    // index must filter exactly one row.
    def checkpoint_name = "segment_iterator.inverted_index.filtered_rows"
    try {
        GetDebugPoint().enableDebugPointForAllBEs(checkpoint_name, [filtered_rows: 1])
        qt_after_range """select k from ${table_name} where cast(v["a"] as int) >= 200 order by k"""
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(checkpoint_name)
    }
    sql """set enable_match_without_inverted_index = true"""

    sql "DROP TABLE IF EXISTS ${table_name}"
}

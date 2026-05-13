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

// DORIS-25440:
// INSERT VALUES with explicit cast(<json> as variant) into a doc-mode
// variant column previously aborted BE in MutableBlock::merge_impl.
// The root cause: FE allowed implicit Variant->Variant cast even when
// configurations (max_subcolumns_count, enable_doc_mode, ...) differ,
// and BE has no real Variant->Variant conversion.
//
// Fix: FE rejects Variant->Variant cast when configurations differ.
// User-visible behavior:
//   * BAD : cast('<json>' as variant)            -> AnalysisException
//   * GOOD: '<json>'                              -> auto-coerce to target
//   * GOOD: cast('<json>' as variant<...matching...>)  -> direct
suite("test_doris_25440_variant_cast_strict", "p0") {
    // The JIRA repro uses session variables to set variant defaults (not column properties).
    // Session vars carry max_subcolumns_count + enable_doc_mode at the same time, which
    // column-level properties forbid in a single declaration.
    sql """ set default_variant_enable_doc_mode = true """
    sql """ set default_variant_max_subcolumns_count = 37 """
    sql """ set default_variant_doc_materialization_min_rows = 8 """
    sql """ set default_variant_doc_hash_shard_count = 7 """

    def t = "doris_25440_variant_cast_strict"
    sql """ DROP TABLE IF EXISTS ${t} """
    sql """
        CREATE TABLE IF NOT EXISTS ${t} (
            id bigint,
            v variant
        )
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    def jsonValue = '{"anchors":{"common_int":150025,"phase_marker":"phase_a","present":true,"row_id":15001},"dynamic":{"path_00000":15001000,"path_00001":15001001},"parent":{"child":{"name":"phase_a_15001"}},"phase_a_small":{"leaf":15001}}'

    // ---- Case 1: BAD path — implicit Variant->Variant with mismatched config should be
    // rejected by FE. Before fix: BE aborts in MutableBlock::merge_impl. After fix: FE
    // throws AnalysisException; BE never receives the plan.
    // We force a config mismatch on `variant_doc_materialization_min_rows` (target=8 from
    // session, source=999 here).
    test {
        sql """ insert into ${t} values (15001, cast('${jsonValue}' as variant<properties(
            "variant_enable_doc_mode" = "true",
            "variant_doc_materialization_min_rows" = "999",
            "variant_doc_hash_shard_count" = "7"
        )>)); """
        exception "cast"
    }

    // ---- Case 2: GOOD — drop the cast, let FE coerce String -> target Variant directly.
    sql """ insert into ${t} values (15002, '${jsonValue}'); """
    qt_case2 """ select id, cast(v['anchors']['row_id'] as bigint) from ${t} where id = 15002; """

    // ---- Case 3: GOOD — explicit cast with matching parameters (from same session).
    // Bare `as variant` reads session-default config and matches target exactly.
    sql """ insert into ${t} values (15003, cast('${jsonValue}' as variant)); """
    qt_case3 """ select id, cast(v['anchors']['row_id'] as bigint) from ${t} where id = 15003; """

    // ---- Case 4: cross-table — different variant configs need an explicit JSONB hop.
    def t_src = "doris_25440_variant_cast_strict_src"
    // Create source table with NO doc-mode by clearing session vars first, then restore.
    sql """ set default_variant_enable_doc_mode = false """
    sql """ set default_variant_max_subcolumns_count = 0 """
    sql """ DROP TABLE IF EXISTS ${t_src} """
    sql """
        CREATE TABLE IF NOT EXISTS ${t_src} (
            id bigint,
            v variant
        )
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    sql """ insert into ${t_src} values (15004, '${jsonValue}'); """
    // Restore session vars so target's column-level config keeps matching.
    sql """ set default_variant_enable_doc_mode = true """
    sql """ set default_variant_max_subcolumns_count = 37 """

    // 4a: direct copy is rejected (configs differ).
    test {
        sql """ insert into ${t} select id, v from ${t_src}; """
        exception "cast"
    }

    // 4b: routing through JSONB works.
    sql """ insert into ${t} select id, cast(cast(v as JSONB) as variant) from ${t_src}; """
    qt_case4b """ select id, cast(v['anchors']['row_id'] as bigint) from ${t} where id = 15004; """

    sql """ DROP TABLE IF EXISTS ${t} """
    sql """ DROP TABLE IF EXISTS ${t_src} """
}

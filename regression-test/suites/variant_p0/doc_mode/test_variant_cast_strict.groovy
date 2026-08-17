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

suite("test_variant_cast_strict", "p0") {
    def variantV2Function = getFeConfig("enable_variant_v2").toBoolean() ? "parse_to_variant" : ""
    // Use session variables to set variant defaults (column-level properties
    // forbid setting max_subcolumns_count and enable_doc_mode together).
    sql """ set default_variant_enable_doc_mode = true """
    sql """ set default_variant_max_subcolumns_count = 37 """
    sql """ set default_variant_doc_materialization_min_rows = 8 """
    sql """ set default_variant_doc_hash_shard_count = 7 """

    def t = "variant_cast_strict"
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

    // Parse string literals explicitly so the expression is targeted to the table's
    // Variant layout in either global storage mode.
    sql """ insert into ${t} values (15001, ${variantV2Function}('${jsonValue}')); """

    // ---- Case 2: GOOD — drop the cast, let FE coerce String -> target Variant directly.
    sql """ insert into ${t} values (15002, ${variantV2Function}('${jsonValue}')); """
    qt_case2 """ select id, cast(v['anchors']['row_id'] as bigint) from ${t} where id = 15002; """

    // ---- Case 3: another explicitly parsed value.
    sql """ insert into ${t} values (15003, ${variantV2Function}('${jsonValue}')); """
    qt_case3 """ select id, cast(v['anchors']['row_id'] as bigint) from ${t} where id = 15003; """

    // ---- Case 4: cross-table copy with the same Variant layout and global storage mode.
    def t_src = "variant_cast_strict_src"
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
    sql """ insert into ${t_src} values (15004, ${variantV2Function}('${jsonValue}')); """

    sql """ insert into ${t} select id, v from ${t_src}; """
    qt_case4b """ select id, cast(v['anchors']['row_id'] as bigint) from ${t} where id = 15004; """
}

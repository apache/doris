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

suite("reserved_internal_path_key", "p0") {
    // A single tablet and one INSERT per rowset make the promotion deterministic:
    // p occurs in three input rowsets, each hot key in only two rows.
    // Other marker positions and malformed bucket names are covered by BE unit tests.
    ["DUPLICATE", "UNIQUE"].each { model ->
        ["V2", "V3"].each { format ->
            [false, true].each { doc ->
                [1, 3].each { shards ->
                    def marker = doc ? "__DORIS_VARIANT_DOC_VALUE__" : "__DORIS_VARIANT_SPARSE__"
                    def p = "x${marker}y"
                    def name = "reserved_path_${model}_${format}_${doc}_${shards}".toLowerCase()
                    def props = """ "replication_num"="1", "disable_auto_compaction"="true", "storage_format"="${format}" """
                    if (model == "UNIQUE") {
                        props += ', "enable_unique_key_merge_on_write"="true"'
                    }
                    // FE rejects sparse-limit properties together with doc-mode properties.
                    // A three-row threshold keeps two-row inputs in doc snapshots and
                    // materializes their paths in the six-row compaction output.
                    def variantProps = doc
                        ? """ "variant_enable_doc_mode"="true", "variant_doc_materialization_min_rows"="3", "variant_doc_hash_shard_count"="${shards}" """
                        : """ "variant_max_subcolumns_count"="1", "variant_sparse_hash_shard_count"="${shards}" """
                    def variant = "variant<properties(${variantProps})>"
                    def check = { table, stage ->
                        quickTest("${name}_${stage}", """
                            select k, v['${p}'], cast(v as string)
                            from ${table} order by k
                        """)
                    }

                    // p is materialized in the very first load. The broken V3 classifier
                    // embeds its metadata as an internal column even without compaction.
                    sql "drop table if exists ${name}_load"
                    sql """create table ${name}_load (k int, v ${variant})
                        ${model} key(k) distributed by hash(k) buckets 1 properties(${props})"""
                    sql """insert into ${name}_load values
                        (1, parse_to_variant('{"${p}":701,"hot_a":11}')),
                        (2, parse_to_variant('{"${p}":702}')),
                        (3, parse_to_variant('{"${p}":703}'))"""
                    check("${name}_load", "load")

                    sql "drop table if exists ${name}"
                    sql """create table ${name} (k int, v ${variant})
                        ${model} key(k) distributed by hash(k) buckets 1 properties(${props})"""
                    sql """insert into ${name} values
                        (1, parse_to_variant('{"${p}":701,"hot_a":11}')),
                        (2, parse_to_variant('{"hot_a":12}'))"""
                    sql """insert into ${name} values
                        (3, parse_to_variant('{"${p}":702,"hot_b":21}')),
                        (4, parse_to_variant('{"hot_b":22}'))"""
                    sql """insert into ${name} values
                        (5, parse_to_variant('{"${p}":703,"hot_c":31}')),
                        (6, parse_to_variant('{"hot_c":32}'))"""
                    check(name, "before")
                    trigger_and_wait_compaction(name, "full")
                    check(name, "first_full")

                    // On MoW, replace a row that previously had p, and replace a missing-p
                    // row with a value. Check survivors as well as the original 701/702 rows.
                    if (model == "UNIQUE") {
                        sql """insert into ${name} values
                            (5, parse_to_variant('{"${p}":803,"hot_c":41}')),
                            (6, parse_to_variant('{"${p}":804,"hot_d":42}'))"""
                    } else {
                        sql """insert into ${name} values
                            (7, parse_to_variant('{"${p}":704,"hot_d":41}')),
                            (8, parse_to_variant('{"hot_d":42}'))"""
                    }
                    check(name, "new_rowset")
                    trigger_and_wait_compaction(name, "full")
                    check(name, "second_full")
                }
            }
        }
    }
}

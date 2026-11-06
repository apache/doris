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

suite("sparse_key_byte_order", "p0") {
    // The parser rejects an explicit doc-mode property beside sparse properties.
    sql "SET default_variant_enable_doc_mode = false"
    for (def keyModel : ["DUPLICATE", "UNIQUE"]) {
        for (def shardCount : [1, 3]) {
            for (def docMode : [false, true]) {
                def tableName = "sparse_key_byte_order_${keyModel.toLowerCase()}_${shardCount}_${docMode}"
                def mowProperty = keyModel == "UNIQUE" ? ', "enable_unique_key_merge_on_write"="true"' : ''
                def variantProperties = docMode ?
                    '"variant_enable_doc_mode"="true", "variant_enable_nested_group"="false", ' +
                    '"variant_doc_materialization_min_rows"="1000000", ' +
                    "\"variant_doc_hash_shard_count\"=\"${shardCount}\"" :
                    '"variant_max_subcolumns_count"="1", "variant_enable_typed_paths_to_sparse"="false", ' +
                    '"variant_enable_nested_group"="false", ' +
                    "\"variant_sparse_hash_shard_count\"=\"${shardCount}\""
                sql "DROP TABLE IF EXISTS ${tableName}"
                sql """
                    CREATE TABLE ${tableName} (
                        id INT,
                        v VARIANT<PROPERTIES(${variantProperties})>
                    ) ${keyModel} KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES("replication_num"="1", "disable_auto_compaction"="true"${mowProperty})
                """
                // z is hotter than every other leaf, leaving the punctuation keys sparse.
                sql """INSERT INTO ${tableName} VALUES
                    (1, parse_to_variant('{"z":1,"a-b":11,"a b":12,"a\$b":13,"a%2Eb":14,"a":{"c":15}}')),
                    (2, parse_to_variant('{"z":2}')),
                    (3, parse_to_variant('{"z":3}'))
                """
                sql """INSERT INTO ${tableName} VALUES
                    (4, parse_to_variant('{"z":4,"a-b":21,"a b":22,"a\$b":23,"a%2Eb":24,"a":{"c":25}}')),
                    (5, parse_to_variant('{"z":5}')),
                    (6, parse_to_variant('{"z":6}'))
                """
                if (keyModel == "UNIQUE") {
                    sql """INSERT INTO ${tableName} VALUES
                        (1, parse_to_variant('{"z":1,"a-b":31,"a b":32,"a\$b":33,"a%2Eb":34,"a":{"c":35}}')),
                        (2, parse_to_variant('{"z":2}')),
                        (3, parse_to_variant('{"z":3}'))
                    """
                }
                def projection = """SELECT id, v['a-b'], v['a b'], v['a\$b'], v['a%2Eb'],
                    v['a']['c'], CAST(v['a'] AS STRING), CAST(v AS STRING)
                    FROM ${tableName} ORDER BY id"""
                qt_before_compaction projection
                trigger_and_wait_compaction(tableName, "full", 1800)
                qt_after_compaction projection
            }
        }
    }
}

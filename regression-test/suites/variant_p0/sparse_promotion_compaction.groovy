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

suite("sparse_promotion_compaction", "p0") {
    // CIR-22588: compaction promotes sparse paths to materialized subcolumns by a raw-byte
    // lower_bound. Sparse keys written in component order are missed, filled with NULL, and
    // then removed from sparse, so the values disappear from the output file. The writer must
    // publish sparse keys in raw-byte order; after full compaction the promoted values must
    // still be present.
    setFeConfigTemporary([enable_variant_v2: true]) {
        sql "SET default_variant_enable_doc_mode = false"
        sql "SET enable_sql_cache = false"

        def tableName = "sparse_promotion_compaction"
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                id INT NOT NULL,
                v VARIANT<PROPERTIES("variant_max_subcolumns_count"="2")>
            ) ENGINE = OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true"
            )
        """

        sql """INSERT INTO ${tableName} VALUES
            (1, parse_to_variant('{"hot_a":1,"hot_b":1,"tag-labeling":{"text_info":{"open":{"tag_id":"id","tag_name":"open"}}},"tag-labeling-0-1-2":{"x":1,"y":2,"z":3}}')),
            (2, parse_to_variant('{"hot_a":2,"hot_b":2}'))
        """

        def projection = """SELECT id,
            CAST(v['tag-labeling']['text_info']['open']['tag_id'] AS STRING) AS tag_id,
            CAST(v['tag-labeling']['text_info']['open']['tag_name'] AS STRING) AS tag_name,
            CAST(v['tag-labeling'] AS STRING) AS parent
            FROM ${tableName} ORDER BY id"""
        qt_before_compaction projection

        // More rows make the promoted paths frequent enough for full compaction to rewrite
        // them; row 1 is not updated or overwritten.
        sql """INSERT INTO ${tableName} VALUES
            (3, parse_to_variant('{"tag-labeling":{"text_info":{"open":{"tag_id":"id3","tag_name":"open3"}}}}')),
            (4, parse_to_variant('{"tag-labeling":{"text_info":{"open":{"tag_id":"id4","tag_name":"open4"}}}}')),
            (5, parse_to_variant('{"tag-labeling":{"text_info":{"open":{"tag_id":"id5","tag_name":"open5"}}}}')),
            (6, parse_to_variant('{"tag-labeling":{"text_info":{"open":{"tag_id":"id6","tag_name":"open6"}}}}'))
        """
        trigger_and_wait_compaction(tableName, "full", 1800)

        qt_after_compaction projection

        def newerRows = """SELECT id,
            CAST(v['tag-labeling']['text_info']['open']['tag_id'] AS STRING) AS tag_id,
            CAST(v['tag-labeling']['text_info']['open']['tag_name'] AS STRING) AS tag_name
            FROM ${tableName} WHERE id IN (3, 4) ORDER BY id"""
        qt_newer_rows newerRows
    }
}

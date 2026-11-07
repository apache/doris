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

suite("test_variant_values_index_add_drop_compaction", "p0,nonConcurrent") {
    sql "SET enable_add_index_for_new_data=true"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_enable_nested_group=false"
    def rowsets = {
        sql_return_maparray("SHOW TABLETS FROM vvi_add_drop_compaction").collectMany { tablet ->
            def (code, out, err) = curl("GET", tablet.MetaUrl + "?byte_to_base64=true")
            assertEquals(0, code, err)
            parseJson(out).rs_metas
        }
    }
    def waitColumn = {
        waitForSchemaChangeDone {
            sql "SHOW ALTER TABLE COLUMN WHERE TableName='vvi_add_drop_compaction' ORDER BY CreateTime DESC LIMIT 1"
            time 180
        }
    }
    def compact = { String tag ->
        def before = rowsets().collect { it.rowset_id_v2 }
        assertTrue(before.size() > 1)
        trigger_and_wait_compaction("vvi_add_drop_compaction", "full", 1800)
        def after = rowsets()
        assertTrue(after.every { !before.contains(it.rowset_id_v2) })
        logger.info("values-index lifecycle ${tag}: tablets=${after.collect { it.tablet_id }}, inputs=${before}, outputs=${after.collect { it.rowset_id_v2 }}")
    }
    // Selected combinations cover each layout and compaction route without a Cartesian product.
    // max_subcolumns_count=0 keeps all paths materialized; 1 mixes materialized and sparse paths.
    def scenarios = [
        [name: "materialized_horizontal", doc: false, max: 0, shards: 1, mode: "horizontal", nativeMerge: false],
        [name: "materialized_vertical", doc: false, max: 0, shards: 1, mode: "vertical_root", nativeMerge: true],
        [name: "sparse_horizontal", doc: false, max: 1, shards: 1, mode: "horizontal", nativeMerge: true],
        [name: "sparse_vertical", doc: false, max: 1, shards: 4, mode: "vertical_paths", nativeMerge: false],
        [name: "doc_vertical_root", doc: true, max: 1, shards: 1, mode: "vertical_root", nativeMerge: false],
        [name: "doc_vertical_paths", doc: true, max: 1, shards: 4, mode: "vertical_paths", nativeMerge: true]
    ]
    for (def scenario : scenarios) {
        setBeConfigTemporary([
            enable_vertical_compaction: scenario.mode != "horizontal",
            enable_vertical_compact_variant_subcolumns: scenario.mode == "vertical_paths",
            vertical_compaction_num_columns_per_group: 1,
            enable_ordered_data_compaction: false,
            inverted_index_compaction_enable: scenario.nativeMerge
        ]) {
            sql "DROP TABLE IF EXISTS vvi_add_drop_compaction"
            // DOC inputs have 2/3 rows, below the threshold; compaction materializes the 8-row output.
            // DOC and ordinary-layout column properties are mutually exclusive in the DDL.
            String variantProperties = scenario.doc
                ? '"variant_enable_doc_mode"="true","variant_doc_hash_shard_count"="' + scenario.shards + '","variant_doc_materialization_min_rows"="4"'
                : '"variant_max_subcolumns_count"="' + scenario.max + '","variant_sparse_hash_shard_count"="' + scenario.shards + '"'
            // Start without any index container, including on historical rowsets.
            sql """CREATE TABLE vvi_add_drop_compaction(id INT,
                v VARIANT<PROPERTIES(${variantProperties})>)
                DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num"="1","disable_auto_compaction"="true",
                           "inverted_index_storage_format"="SNII")"""
            sql """INSERT INTO vvi_add_drop_compaction VALUES
                (1,parse_to_variant('{"stable":"hello","n":7,"rare1":"first"}')),
                (2,parse_to_variant('{"stable":"other"}')),(3,NULL)"""
            sql """INSERT INTO vvi_add_drop_compaction VALUES
                (4,parse_to_variant('{"stable":"hello","n":9,"rare2":["second"]}')),
                (5,parse_to_variant('{}')),(6,parse_to_variant('[]'))"""
            def historical = rowsets().collect { it.rowset_id_v2 }.sort()
            sql 'ALTER TABLE vvi_add_drop_compaction ADD INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english")'
            waitColumn()
            assertEquals(historical, rowsets().collect { it.rowset_id_v2 }.sort())
            assertTrue(rowsets().every { !it.tablet_schema.index.any { idx -> idx.index_name == "values_idx" } })
            sql """INSERT INTO vvi_add_drop_compaction VALUES
                (7,parse_to_variant('{"stable":"hello world","n":7,"rare3":{"s":"third"}}')),
                (8,parse_to_variant('null'))"""
            def queries = [
                hits: """SELECT 'any' AS kind,id FROM vvi_add_drop_compaction WHERE v MATCH_ANY 'hello'
                    UNION ALL SELECT 'all',id FROM vvi_add_drop_compaction WHERE v MATCH_ALL 'hello third'
                    UNION ALL SELECT 'rare',id FROM vvi_add_drop_compaction WHERE v MATCH_ANY 'first second third fourth'
                    UNION ALL SELECT 'number',id FROM vvi_add_drop_compaction WHERE v MATCH_ANY '7 42'
                    UNION ALL SELECT 'keys',id FROM vvi_add_drop_compaction WHERE v MATCH_ANY 'stable rare1 rare2 rare3'
                    ORDER BY kind,id""",
                values: "SELECT id,v FROM vvi_add_drop_compaction ORDER BY id"
            ]
            def check = { String phase, boolean strict ->
                queries.each { tag, query ->
                    sql "SET enable_inverted_index_query=true"
                    sql "SET enable_match_without_inverted_index=${!strict}"
                    def indexed = sql(query)
                    sql "SET enable_match_without_inverted_index=true"
                    sql "SET enable_inverted_index_query=false"
                    assertEquals(indexed, sql(query))
                    quickTest("${scenario.name}_${phase}_${tag}", query, true)
                }
                sql "SET enable_inverted_index_query=true"
            }
            check("mixed", false)
            def values = sql(queries.values)
            // A missing historical index forces rebuilding. No BUILD INDEX is involved.
            compact("${scenario.name}_missing_index")
            assertEquals(values, sql(queries.values))
            assertTrue(rowsets().every { it.tablet_schema.index.any { idx -> idx.index_name == "values_idx" } })
            check("filled", true)
            sql """INSERT INTO vvi_add_drop_compaction VALUES
                (9,parse_to_variant('{"stable":"hello","n":42,"rare4":"fourth"}')),(10,NULL)"""
            values = sql(queries.values)
            // Both inputs now have the same root index, allowing native postings merge when enabled.
            compact("${scenario.name}_same_index")
            assertEquals(values, sql(queries.values))
            check("merged", true)
            def column = rowsets()[0].tablet_schema.column.find { it.name == "v" }
            assertEquals(scenario.doc, column.variant_enable_doc_mode)
            if (scenario.doc) {
                assertEquals(scenario.shards, column.variant_doc_hash_shard_count)
                assertEquals(4, column.variant_doc_materialization_min_rows)
            } else {
                assertEquals(scenario.max, column.variant_max_subcolumns_count)
                assertEquals(scenario.shards, column.variant_sparse_hash_shard_count)
            }
            sql "DROP INDEX values_idx ON vvi_add_drop_compaction"
            waitColumn()
            wait_for_last_build_index_finish("vvi_add_drop_compaction", 180000)
            assertEquals(values, sql(queries.values))
            sql """INSERT INTO vvi_add_drop_compaction VALUES
                (11,parse_to_variant('{"stable":"hello gap","rare5":"fifth"}')),(12,parse_to_variant('[]'))"""
            values = sql(queries.values)
            compact("${scenario.name}_dropped_index")
            assertEquals(values, sql(queries.values))
            assertTrue(rowsets().every { !it.tablet_schema.index.any { idx -> idx.index_name == "values_idx" } })
            test {
                sql "SELECT id FROM vvi_add_drop_compaction WHERE v MATCH_ANY 'hello' ORDER BY id"
                exception "VARIANT root column does not support MATCH predicates."
            }
            quickTest("${scenario.name}_dropped_values", queries.values, true)
        }
    }
}

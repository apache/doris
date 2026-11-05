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

suite("test_variant_values_index_add_drop_templates", "p0,nonConcurrent") {
    sql "SET enable_add_index_for_new_data=true"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_enable_nested_group=false"
    def waitColumn = {
        waitForSchemaChangeDone {
            sql "SHOW ALTER TABLE COLUMN WHERE TableName='vvi_add_drop_templates' ORDER BY CreateTime DESC LIMIT 1"
            time 180
        }
    }
    def rowsets = {
        sql_return_maparray("SHOW TABLETS FROM vvi_add_drop_templates").collectMany { tablet ->
            def (code, out, err) = curl("GET", tablet.MetaUrl + "?byte_to_base64=true")
            assertEquals(0, code, err)
            parseJson(out).rs_metas
        }
    }
    def indexIds = {
        rowsets().find { it.num_rows > 0 }.tablet_schema.index.collectEntries { [(it.index_name): it.index_id] }
    }
    def compact = {
        def before = rowsets().collect { it.rowset_id_v2 }
        assertTrue(before.size() > 1)
        def values = sql("SELECT id,v,other FROM vvi_add_drop_templates ORDER BY id")
        trigger_and_wait_compaction("vvi_add_drop_templates", "full", 1800)
        assertTrue(rowsets().every { !before.contains(it.rowset_id_v2) })
        assertEquals(values, sql("SELECT id,v,other FROM vvi_add_drop_templates ORDER BY id"))
    }
    for (boolean typedSparse : [false, true]) {
        setBeConfigTemporary([
            enable_vertical_compaction: typedSparse,
            enable_vertical_compact_variant_subcolumns: typedSparse,
            vertical_compaction_num_columns_per_group: 1,
            enable_ordered_data_compaction: false,
            inverted_index_compaction_enable: typedSparse
        ]) {
            sql "DROP TABLE IF EXISTS vvi_add_drop_templates"
            sql """CREATE TABLE vvi_add_drop_templates(id INT,
                v VARIANT<'n':INT,'s':STRING,'tag*':STRING,PROPERTIES(
                    "variant_max_subcolumns_count"="1","variant_sparse_hash_shard_count"="4",
                    "variant_enable_typed_paths_to_sparse"="${typedSparse}")>,other VARIANT,
                INDEX english_idx(v) USING INVERTED PROPERTIES("parser"="english","lower_case"="false"),
                INDEX unicode_idx(v) USING INVERTED PROPERTIES("parser"="unicode"),
                INDEX number_idx(v) USING INVERTED PROPERTIES("field_pattern"="n"),
                INDEX tags_idx(v) USING INVERTED PROPERTIES("field_pattern"="tag*","parser"="unicode"))
                DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num"="1","disable_auto_compaction"="true",
                           "inverted_index_storage_format"="SNII")"""
            sql """INSERT INTO vvi_add_drop_templates VALUES
                (1,parse_to_variant('{"s":"HELLO","n":"00042","tagA":"CAFÉ","obj":{"s":"WORLD"}}'),parse_to_variant('{"s":"UPPER"}')),
                (2,parse_to_variant('{"s":"other","n":7,"tagB":"quiet"}'),parse_to_variant('{"s":"upper"}'))"""
            sql """INSERT INTO vvi_add_drop_templates VALUES
                (3,parse_to_variant('{"s":"hello","n":42,"tagA":"café"}'),NULL),
                (4,NULL,parse_to_variant('{"s":"UPPER"}'))"""
            def historical = rowsets().collect { it.rowset_id_v2 }.sort()
            def original = indexIds()
            test {
                sql 'ALTER TABLE vvi_add_drop_templates ADD INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"), ADD INDEX duplicate_values(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="unicode")'
                exception "VARIANT column supports only one values index"
            }
            assertFalse(sql("SHOW CREATE TABLE vvi_add_drop_templates")[0][1].contains("INDEX values_idx"))
            assertEquals(original, indexIds())
            def additions = [
                'ADD INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english")',
                'ADD INDEX other_values(other) USING INVERTED PROPERTIES("variant_index_scope"="values","analyzer"="unicode","lower_case"="false")'
            ]
            if (typedSparse) { additions = additions.reverse() }
            sql "ALTER TABLE vvi_add_drop_templates " + additions.join(",")
            waitColumn()
            assertEquals(historical, rowsets().collect { it.rowset_id_v2 }.sort())
            assertEquals(original, indexIds())
            def rootQueries = [
                root: "SELECT id FROM vvi_add_drop_templates WHERE v MATCH_ANY 'hello' ORDER BY id",
                converted: "SELECT id FROM vvi_add_drop_templates WHERE v MATCH_ANY '42' ORDER BY id",
                raw_input: "SELECT id FROM vvi_add_drop_templates WHERE v MATCH_ANY '00042' ORDER BY id",
                cross_leaf: "SELECT id FROM vvi_add_drop_templates WHERE v MATCH_ALL 'hello world' ORDER BY id"
            ]
            def retainedQueries = [
                english: "SELECT id FROM vvi_add_drop_templates WHERE v['s'] MATCH_ANY 'HELLO' USING ANALYZER english ORDER BY id",
                unicode: "SELECT id FROM vvi_add_drop_templates WHERE v['s'] MATCH_ANY 'hello' USING ANALYZER unicode ORDER BY id",
                number: "SELECT id,CAST(v['n'] AS INT) FROM vvi_add_drop_templates WHERE CAST(v['n'] AS INT)=42 ORDER BY id",
                template: "SELECT id FROM vvi_add_drop_templates WHERE v['tagA'] MATCH_ANY 'café' ORDER BY id",
                other_upper: "SELECT id FROM vvi_add_drop_templates WHERE other MATCH_ANY 'UPPER' ORDER BY id",
                other_lower: "SELECT id FROM vvi_add_drop_templates WHERE other MATCH_ANY 'upper' ORDER BY id"
            ]
            def check = { String phase, Map queries, boolean strict ->
                queries.each { tag, query ->
                    sql "SET enable_inverted_index_query=true"
                    sql "SET enable_match_without_inverted_index=${!strict}"
                    // Sparse template leaves have no path postings even with a CREATE-time index.
                    // The root values indexes must still execute with residual MATCH disabled.
                    if (strict && typedSparse && tag == "template") {
                        test {
                            sql query
                            exception "match_any not support execute_match"
                        }
                        sql "SET enable_match_without_inverted_index=true"
                    }
                    def indexed = sql(query)
                    sql "SET enable_inverted_index_query=false"
                    sql "SET enable_match_without_inverted_index=true"
                    assertEquals(indexed, sql(query))
                    quickTest("${typedSparse}_${phase}_${tag}", query, true)
                }
                sql "SET enable_inverted_index_query=true"
            }
            // Existing typed values have already converted "00042" to 42 before ADD.
            check("historical", rootQueries + retainedQueries, false)
            sql """INSERT INTO vvi_add_drop_templates VALUES
                (5,parse_to_variant('{"s":"HELLO","n":"00042","tagA":"CAFÉ"}'),parse_to_variant('{"s":"UPPER"}')),
                (6,parse_to_variant('{"s":"hello","n":7,"tagB":"other"}'),parse_to_variant('{"s":"upper"}'))"""
            compact()
            def allIds = indexIds()
            assertEquals(original, allIds.subMap(original.keySet()))
            check("added", rootQueries + retainedQueries, true)
            sql "DROP INDEX values_idx ON vvi_add_drop_templates"
            waitColumn()
            wait_for_last_build_index_finish("vvi_add_drop_templates", 180000)
            def retained = allIds.findAll { name, id -> name != "values_idx" }
            assertEquals(retained, indexIds())
            test {
                sql "SELECT id FROM vvi_add_drop_templates WHERE v MATCH_ANY 'hello' ORDER BY id"
                exception "VARIANT root column does not support MATCH predicates."
            }
            check("dropped", retainedQueries, true)
            sql """INSERT INTO vvi_add_drop_templates VALUES
                (7,parse_to_variant('{"s":"HELLO","n":"00042","tagA":"CAFÉ"}'),parse_to_variant('{"s":"UPPER"}')),
                (8,parse_to_variant('{"s":"hello","n":8,"tagB":"other"}'),parse_to_variant('{"s":"upper"}'))"""
            compact()
            assertEquals(retained, indexIds())
            check("dropped_compacted", retainedQueries, true)
            sql 'ALTER TABLE vvi_add_drop_templates ADD INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english","lower_case"="false")'
            waitColumn()
            sql """INSERT INTO vvi_add_drop_templates VALUES
                (9,parse_to_variant('{"s":"HELLO","n":42,"tagA":"CAFÉ"}'),parse_to_variant('{"s":"UPPER"}')),
                (10,parse_to_variant('{"s":"hello","n":9,"tagB":"other"}'),parse_to_variant('{"s":"upper"}'))"""
            compact()
            assertNotEquals(allIds.values_idx, indexIds().values_idx)
            assertEquals(retained, indexIds().subMap(retained.keySet()))
            check("readded", rootQueries + retainedQueries + [
                root_upper: "SELECT id FROM vvi_add_drop_templates WHERE v MATCH_ANY 'HELLO' ORDER BY id"
            ], true)
            sql "DROP INDEX other_values ON vvi_add_drop_templates"
            waitColumn()
            wait_for_last_build_index_finish("vvi_add_drop_templates", 180000)
            assertEquals(indexIds().keySet(), (original.keySet() + ["values_idx"]).toSet())
            check("other_dropped", rootQueries, true)
            test {
                sql "SELECT id FROM vvi_add_drop_templates WHERE other MATCH_ANY 'UPPER' ORDER BY id"
                exception "VARIANT root column does not support MATCH predicates."
            }
            quickTest("${typedSparse}_values", "SELECT id,v,other FROM vvi_add_drop_templates ORDER BY id", true)
        }
    }
}

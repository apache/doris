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

suite("test_variant_values_index_schema_change", "p0,nonConcurrent") {
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "SET enable_common_expr_pushdown=true"
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_enable_nested_group=false"
    sql "SET default_variant_max_subcolumns_count=10"
    sql "SET enable_add_index_for_new_data=true"
    def waitColumn = { String table ->
        waitForSchemaChangeDone {
            sql "SHOW ALTER TABLE COLUMN WHERE TableName='${table}' ORDER BY CreateTime DESC LIMIT 1"
            time 180
        }
    }
    def metadata = { String table ->
        def tablet = sql_return_maparray("SHOW TABLETS FROM ${table}")[0]
        def (code, out, err) = curl("GET", tablet.MetaUrl + "?byte_to_base64=true")
        assertEquals(0, code, err)
        def meta = parseJson(out)
        // Lightweight changes live in newly written rowset schemas; the tablet's creation schema is older.
        meta.schema = meta.rs_metas.max { it.end_version as long }.tablet_schema
        assertNotNull(meta.schema)
        meta
    }
    def indexByName = { meta, String name -> meta.schema.index.find { it.index_name == name } }
    def columnByName = { meta, String name -> meta.schema.column.find { it.name == name } }

    // Keep the historical segment's index cold: earlier queries can retain its old binding.
    sql "DROP TABLE IF EXISTS vvi_sc_cold_rename"
    sql """CREATE TABLE vvi_sc_cold_rename(id INT,v VARIANT<'tag*':STRING>,
        INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),
        INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english","lower_case"="false"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","disable_auto_compaction"="true","inverted_index_storage_format"="SNII")"""
    sql """INSERT INTO vvi_sc_cold_rename VALUES(1,parse_to_variant('{"s":"HELLO world","tagA":"alpha"}'))"""
    sql "ALTER TABLE vvi_sc_cold_rename RENAME COLUMN v payload"
    sql """INSERT INTO vvi_sc_cold_rename VALUES(2,parse_to_variant('{"s":"HELLO world","tagB":"beta"}'))"""
    sql "SET enable_match_without_inverted_index=false"
    qt_cold_renamed_path "SELECT id FROM vvi_sc_cold_rename WHERE payload['s'] MATCH_PHRASE 'HELLO world' ORDER BY id"
    qt_cold_renamed_root "SELECT id FROM vvi_sc_cold_rename WHERE payload MATCH_ALL 'hello world' ORDER BY id"
    def coldRenameBefore = metadata("vvi_sc_cold_rename").rs_metas.collect { it.rowset_id_v2 }
    trigger_and_wait_compaction("vvi_sc_cold_rename", "full", 1800)
    def coldRenameAfter = metadata("vvi_sc_cold_rename").rs_metas.collect { it.rowset_id_v2 }
    assertTrue(coldRenameBefore.any { !coldRenameAfter.contains(it) })
    assertTrue(coldRenameAfter.any { !coldRenameBefore.contains(it) })
    logger.info("Cold renamed VARIANT compaction rowsets: ${coldRenameBefore} -> ${coldRenameAfter}")
    qt_cold_renamed_compacted_path "SELECT id FROM vvi_sc_cold_rename WHERE payload['s'] MATCH_PHRASE 'HELLO world' ORDER BY id"
    qt_cold_renamed_compacted_root "SELECT id FROM vvi_sc_cold_rename WHERE payload MATCH_ALL 'hello world' ORDER BY id"

    // S1/S2: ADD on historical rowsets; separate DROP and same-name ADD on an untemplated root.
    sql "DROP TABLE IF EXISTS vvi_sc_index"
    sql """CREATE TABLE vvi_sc_index(id INT,v VARIANT,
        INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),
        INDEX keep_idx(v) USING INVERTED PROPERTIES("parser"="unicode","lower_case"="false"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","disable_auto_compaction"="true","inverted_index_storage_format"="SNII")"""
    sql """INSERT INTO vvi_sc_index VALUES(1,parse_to_variant('{"s":"HELLO old"}'))"""
    def initial = metadata("vvi_sc_index")
    sql 'ALTER TABLE vvi_sc_index ADD INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english","lower_case"="false")'
    waitColumn("vvi_sc_index")
    sql """INSERT INTO vvi_sc_index VALUES(2,parse_to_variant('{"s":"HELLO new"}'))"""
    sql "SYNC"
    def added = metadata("vvi_sc_index")
    assertEquals(indexByName(initial,"values_idx"), indexByName(added,"values_idx"))
    if (!isCloudMode()) {
        // Local light ADD must leave historical rowsets without the new logical index.
        assertTrue(added.rs_metas.any { rs -> !rs.tablet_schema.index.any { it.index_name == "path_idx" } })
    }
    assertTrue(added.rs_metas.max { it.end_version as long }.tablet_schema.index.any { it.index_name == "path_idx" })
    sql "SET enable_match_without_inverted_index=false"
    qt_added_root "SELECT id FROM vvi_sc_index WHERE v MATCH_ANY 'hello' ORDER BY id"
    qt_added_new_path "SELECT id FROM vvi_sc_index WHERE id=2 AND v['s'] MATCH_ANY 'HELLO' USING ANALYZER english ORDER BY id"
    sql "SET enable_match_without_inverted_index=true"
    qt_added_history_path "SELECT id FROM vvi_sc_index WHERE v['s'] MATCH_ANY 'HELLO' USING ANALYZER english ORDER BY id"
    trigger_and_wait_compaction("vvi_sc_index", "full", 1800)
    sql "SET enable_match_without_inverted_index=false"
    qt_added_compacted_path "SELECT id FROM vvi_sc_index WHERE v['s'] MATCH_ANY 'HELLO' USING ANALYZER english ORDER BY id"
    qt_added_lower_excluded "SELECT id FROM vvi_sc_index WHERE v['s'] MATCH_ANY 'hello' USING ANALYZER english ORDER BY id"
    sql "DROP INDEX path_idx ON vvi_sc_index"
    waitColumn("vvi_sc_index")
    sql """INSERT INTO vvi_sc_index VALUES(3,parse_to_variant('{"s":"HELLO gap"}'))"""
    def dropped = metadata("vvi_sc_index")
    assertEquals(null, indexByName(dropped,"path_idx"))
    assertEquals(indexByName(initial,"values_idx"), indexByName(dropped,"values_idx"))
    sql 'ALTER TABLE vvi_sc_index ADD INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english","lower_case"="true")'
    waitColumn("vvi_sc_index")
    sql """INSERT INTO vvi_sc_index VALUES(4,parse_to_variant('{"s":"HELLO replacement"}'))"""
    def readded = metadata("vvi_sc_index")
    assertNotEquals(indexByName(added,"path_idx").index_id, indexByName(readded,"path_idx").index_id)
    assertEquals("true", indexByName(readded,"path_idx").properties.lower_case)
    qt_readded_new_path "SELECT id FROM vvi_sc_index WHERE id=4 AND v['s'] MATCH_ANY 'hello' USING ANALYZER english ORDER BY id"
    trigger_and_wait_compaction("vvi_sc_index", "full", 1800)
    qt_readded_compacted_path "SELECT id FROM vvi_sc_index WHERE v['s'] MATCH_ANY 'hello' USING ANALYZER english ORDER BY id"
    qt_readded_keep_path "SELECT id FROM vvi_sc_index WHERE v['s'] MATCH_ANY 'HELLO' USING ANALYZER unicode ORDER BY id"
    qt_readded_root "SELECT id FROM vvi_sc_index WHERE v MATCH_ANY 'hello' ORDER BY id"
    qt_readded_values "SELECT id,v FROM vvi_sc_index ORDER BY id"

    // S3/S4/S5: keep every index on the root while altering only ordinary columns, then rename/drop it.
    sql "DROP TABLE IF EXISTS vvi_sc_shape"
    sql """CREATE TABLE vvi_sc_shape(id INT,aux INT,width VARCHAR(5),v VARIANT<'tag*':STRING>,
        INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),
        INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english","lower_case"="false"),
        INDEX tag_idx(v) USING INVERTED PROPERTIES("field_pattern"="tag*","parser"="unicode"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","disable_auto_compaction"="true","inverted_index_storage_format"="SNII")"""
    sql """INSERT INTO vvi_sc_shape VALUES(1,11,'old',parse_to_variant('{"s":"HELLO world","tagA":"alpha"}'))"""
    def original = metadata("vvi_sc_shape")
    def changes = [
        'ADD COLUMN extra INT DEFAULT "9"',
        'DROP COLUMN extra',
        'MODIFY COLUMN width VARCHAR(20)',
        'MODIFY COLUMN aux BIGINT',
        'ORDER BY(id,width,aux,v)'
    ]
    changes.eachWithIndex { change, i ->
        def beforeJobs = sql("SHOW ALTER TABLE COLUMN WHERE TableName='vvi_sc_shape'")
        def beforeMeta = metadata("vvi_sc_shape")
        sql "ALTER TABLE vvi_sc_shape ${change}"
        waitColumn("vvi_sc_shape")
        sql """INSERT INTO vvi_sc_shape(id,aux,width,v) VALUES(${i+2},${i+12},'new',
            parse_to_variant('{"s":"HELLO world","tagB":"beta"}'))"""
        sql "SYNC"
        def after = metadata("vvi_sc_shape")
        assertEquals(columnByName(original,"v").unique_id, columnByName(after,"v").unique_id)
        assertEquals(original.schema.index, after.schema.index)
        def afterJobs = sql("SHOW ALTER TABLE COLUMN WHERE TableName='vvi_sc_shape'")
        // Lightweight jobs are still recorded by FE; tablet replacement distinguishes the strategies.
        if (i >= 3) {
            assertTrue(afterJobs.size() > beforeJobs.size())
            assertNotEquals(beforeMeta.tablet_id, after.tablet_id)
        } else {
            assertEquals(beforeMeta.tablet_id, after.tablet_id)
        }
        quickTest("ordinary_${i}", "SELECT id,aux,width,v FROM vvi_sc_shape ORDER BY id", true)
        quickTest("ordinary_root_${i}", "SELECT id FROM vvi_sc_shape WHERE v MATCH_ALL 'hello world' ORDER BY id", true)
        quickTest("ordinary_template_${i}", "SELECT id FROM vvi_sc_shape WHERE v['tagB'] MATCH_ANY 'beta' ORDER BY id", true)
        trigger_and_wait_compaction("vvi_sc_shape", "full", 1800)
        quickTest("compacted_${i}", "SELECT id,aux,width,v FROM vvi_sc_shape ORDER BY id", true)
        quickTest("compacted_path_${i}", "SELECT id FROM vvi_sc_shape WHERE v['s'] MATCH_PHRASE 'HELLO world' ORDER BY id", true)
    }
    sql "ALTER TABLE vvi_sc_shape RENAME COLUMN v payload"
    sql """INSERT INTO vvi_sc_shape(id,width,aux,payload) VALUES(7,'renamed',17,
        parse_to_variant('{"s":"HELLO world","tagC":"gamma"}'))"""
    def renamed = metadata("vvi_sc_shape")
    assertEquals(columnByName(original,"v").unique_id, columnByName(renamed,"payload").unique_id)
    assertEquals(original.schema.index.collect { it.index_id }, renamed.schema.index.collect { it.index_id })
    assertTrue(sql_return_maparray("SHOW INDEX FROM vvi_sc_shape").every { it.Column_name == "payload" })
    for (String phase : ["renamed", "renamed_compacted"]) {
        if (phase.endsWith("compacted")) {
            def beforeRowsets = metadata("vvi_sc_shape").rs_metas.collect { it.rowset_id_v2 }
            trigger_and_wait_compaction("vvi_sc_shape", "full", 1800)
            def afterRowsets = metadata("vvi_sc_shape").rs_metas.collect { it.rowset_id_v2 }
            assertTrue(beforeRowsets.any { !afterRowsets.contains(it) })
            assertTrue(afterRowsets.any { !beforeRowsets.contains(it) })
            logger.info("Renamed VARIANT compaction rowsets: ${beforeRowsets} -> ${afterRowsets}")
        }
        quickTest("${phase}_root", "SELECT id FROM vvi_sc_shape WHERE payload MATCH_ALL 'hello world' ORDER BY id", true)
        quickTest("${phase}_path", "SELECT id FROM vvi_sc_shape WHERE payload['s'] MATCH_PHRASE 'HELLO world' ORDER BY id", true)
        quickTest("${phase}_template", "SELECT id FROM vvi_sc_shape WHERE payload['tagC'] MATCH_ANY 'gamma' ORDER BY id", true)
        quickTest("${phase}_search", "SELECT id FROM vvi_sc_shape WHERE SEARCH('payload.s:HELLO') ORDER BY id", true)
        quickTest("${phase}_values", "SELECT id,payload FROM vvi_sc_shape ORDER BY id", true)
    }
    sql "ALTER TABLE vvi_sc_shape DROP COLUMN payload"
    waitColumn("vvi_sc_shape")
    assertEquals([], sql("SHOW INDEX FROM vvi_sc_shape"))
    sql "ALTER TABLE vvi_sc_shape ADD COLUMN payload VARIANT"
    waitColumn("vvi_sc_shape")
    sql """INSERT INTO vvi_sc_shape(id,width,aux,payload) VALUES(8,'fresh',18,parse_to_variant('{"s":"fresh"}'))"""
    def fresh = metadata("vvi_sc_shape")
    assertNotEquals(columnByName(original,"v").unique_id, columnByName(fresh,"payload").unique_id)
    assertEquals([], sql("SHOW INDEX FROM vvi_sc_shape"))
    sql "SET enable_match_without_inverted_index=true"
    for (String phase : ["fresh", "fresh_compacted"]) {
        if (phase.endsWith("compacted")) { trigger_and_wait_compaction("vvi_sc_shape", "full", 1800) }
        quickTest("${phase}_values", "SELECT id,payload,payload IS NULL FROM vvi_sc_shape ORDER BY id", true)
        test {
            sql "SELECT id FROM vvi_sc_shape WHERE payload MATCH_ANY 'hello' ORDER BY id"
            exception "VARIANT root column does not support MATCH predicates"
        }
    }
}

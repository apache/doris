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

suite("test_variant_values_index_add_drop", "p0,nonConcurrent") {
    sql "SET enable_add_index_for_new_data=true"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET default_variant_enable_nested_group=false"
    def waitColumn = {
        waitForSchemaChangeDone {
            sql "SHOW ALTER TABLE COLUMN WHERE TableName='vvi_add_drop' ORDER BY CreateTime DESC LIMIT 1"
            time 180
        }
    }
    def rowsets = {
        sql_return_maparray("SHOW TABLETS FROM vvi_add_drop").collectMany { tablet ->
            def (code, out, err) = curl("GET", tablet.MetaUrl + "?byte_to_base64=true")
            assertEquals(0, code, err)
            parseJson(out).rs_metas
        }
    }
    for (String model : ["DUPLICATE", "UNIQUE"]) {
        for (boolean doc : [false, true]) {
            String tag = "${model.toLowerCase()}_${doc}"
            sql "SET default_variant_enable_doc_mode=${doc}"
            sql "DROP TABLE IF EXISTS vvi_add_drop"
            String mow = model == "UNIQUE" ? ',"enable_unique_key_merge_on_write"="true"' : ''
            sql """CREATE TABLE vvi_add_drop(id INT,v VARIANT,
                INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english"))
                ${model} KEY(id) PARTITION BY RANGE(id)(
                    PARTITION p_old VALUES LESS THAN(100),PARTITION p_new VALUES LESS THAN(MAXVALUE))
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num"="1","disable_auto_compaction"="true",
                           "inverted_index_storage_format"="SNII"${mow})"""
            sql """INSERT INTO vvi_add_drop VALUES
                (1,parse_to_variant('{"s":"hello old","keyonly":"quiet"}')),
                (2,parse_to_variant('{"n":42}')),(3,NULL),(4,parse_to_variant('{}'))"""
            sql """INSERT INTO vvi_add_drop VALUES(5,parse_to_variant('[]')),(6,parse_to_variant('null'))"""
            def historical = rowsets().collect { it.rowset_id_v2 }.sort()
            assertTrue(historical.every { it != null })
            def jobsBeforeAdd = sql("SHOW BUILD INDEX WHERE TableName='vvi_add_drop'").collect { it[0] }.sort()
            if (doc) {
                sql 'CREATE INDEX values_idx ON vvi_add_drop(v) USING INVERTED PROPERTIES("variant_index_scope"="values","analyzer"="english")'
            } else {
                sql 'ALTER TABLE vvi_add_drop ADD INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english")'
            }
            waitColumn()
            // ADD must leave historical rowsets untouched and submit no BUILD jobs in either mode.
            assertEquals(historical, rowsets().collect { it.rowset_id_v2 }.sort())
            assertTrue(rowsets().every { rs -> !rs.tablet_schema.index.any { it.index_name == "values_idx" } })
            assertEquals(jobsBeforeAdd,
                    sql("SHOW BUILD INDEX WHERE TableName='vvi_add_drop'").collect { it[0] }.sort())
            test {
                sql 'ALTER TABLE vvi_add_drop ADD INDEX duplicate_values(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="unicode")'
                exception "VARIANT column supports only one values index"
            }
            sql "SET enable_match_without_inverted_index=false"
            test {
                sql "SELECT id FROM vvi_add_drop PARTITION(p_old) WHERE v MATCH_ANY 'hello' ORDER BY id"
                exception "match_any not support execute_match"
            }
            sql "SET enable_match_without_inverted_index=true"
            quickTest("${tag}_historical", "SELECT id FROM vvi_add_drop WHERE v MATCH_ANY 'hello 42 keyonly' ORDER BY id", true)
            sql """INSERT INTO vvi_add_drop VALUES
                (101,parse_to_variant('{"s":"hello new","nested":["world",42]}')),
                (102,NULL),(103,parse_to_variant('{}')),(104,parse_to_variant('[]'))"""
            sql "SYNC"
            def firstIndex = rowsets().collectMany { it.tablet_schema.index }.find { it.index_name == "values_idx" }
            assertNotNull(firstIndex)
            sql "SET enable_match_without_inverted_index=false"
            quickTest("${tag}_new_index", "SELECT id FROM vvi_add_drop PARTITION(p_new) WHERE v MATCH_ALL 'hello world' ORDER BY id", true)
            quickTest("${tag}_new_numeric", "SELECT id FROM vvi_add_drop PARTITION(p_new) WHERE v MATCH_ANY '42' ORDER BY id", true)
            quickTest("${tag}_key_excluded", "SELECT id FROM vvi_add_drop PARTITION(p_new) WHERE v MATCH_ANY 'nested' ORDER BY id", true)
            sql "SET enable_match_without_inverted_index=true"
            quickTest("${tag}_mixed", "SELECT id FROM vvi_add_drop WHERE v MATCH_ANY 'hello 42' ORDER BY id", true)
            sql "SET enable_inverted_index_query=false"
            quickTest("${tag}_scan", "SELECT id FROM vvi_add_drop WHERE v MATCH_ANY 'hello 42' ORDER BY id", true)
            sql "SET enable_inverted_index_query=true"
            test {
                sql(isCloudMode() ? "BUILD INDEX ON vvi_add_drop" : "BUILD INDEX values_idx ON vvi_add_drop")
                exception "BUILD INDEX is not supported on a table with a VARIANT values index"
            }
            if (model == "UNIQUE") {
                // Replacing a historical row after ADD must hide its old leaf postings through the MoW bitmap.
                sql """INSERT INTO vvi_add_drop VALUES(1,parse_to_variant('{"s":"updated"}'))"""
                quickTest("${tag}_updated", "SELECT id FROM vvi_add_drop WHERE v MATCH_ANY 'hello updated' ORDER BY id", true)
            }
            // A new rowset in the old partition lets normal compaction fill its missing index.
            sql """INSERT INTO vvi_add_drop VALUES(7,parse_to_variant('{"s":"hello after add"}'))"""
            trigger_and_wait_compaction("vvi_add_drop", "full", 1800)
            sql "SET enable_match_without_inverted_index=false"
            quickTest("${tag}_compacted", "SELECT id FROM vvi_add_drop WHERE v MATCH_ANY 'hello 42' ORDER BY id", true)
            def valuesBeforeDrop = sql("SELECT id,v FROM vvi_add_drop ORDER BY id")
            sql "DROP INDEX values_idx ON vvi_add_drop"
            waitColumn()
            wait_for_last_build_index_finish("vvi_add_drop",180000)
            assertEquals(valuesBeforeDrop, sql("SELECT id,v FROM vvi_add_drop ORDER BY id"))
            assertFalse(sql("SHOW CREATE TABLE vvi_add_drop")[0][1].contains("INDEX values_idx"))
            assertTrue(sql("SHOW CREATE TABLE vvi_add_drop")[0][1].contains("INDEX path_idx"))
            assertTrue(rowsets().every { rs -> !rs.tablet_schema.index.any { it.index_name == "values_idx" } })
            sql "SET enable_match_without_inverted_index=true"
            // Full DROP restores the existing root predicate rejection and projection STRING coercion.
            test {
                sql "SELECT id FROM vvi_add_drop WHERE v MATCH_ANY 'hello' ORDER BY id"
                exception "VARIANT root column does not support MATCH predicates."
            }
            quickTest("${tag}_drop_projection", "SELECT id,v MATCH_ANY 'hello',CAST(v AS STRING) MATCH_ANY 'hello' FROM vvi_add_drop ORDER BY id", true)
            quickTest("${tag}_path_retained", "SELECT id FROM vvi_add_drop WHERE v['s'] MATCH_ANY 'hello' ORDER BY id", true)
            sql """INSERT INTO vvi_add_drop VALUES(105,parse_to_variant('{"s":"HELLO gap"}'))"""
            // Remove the last index too; the following ADD starts without any index container on new rows.
            sql "DROP INDEX path_idx ON vvi_add_drop"
            waitColumn()
            wait_for_last_build_index_finish("vvi_add_drop",180000)
            sql 'ALTER TABLE vvi_add_drop ADD INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english","lower_case"="false")'
            waitColumn()
            sql """INSERT INTO vvi_add_drop VALUES(106,parse_to_variant('{"s":"HELLO replacement"}'))"""
            def replacement = rowsets().collectMany { it.tablet_schema.index }.find { it.index_name == "values_idx" }
            assertNotEquals(firstIndex.index_id, replacement.index_id)
            quickTest("${tag}_replacement", "SELECT id FROM vvi_add_drop WHERE v MATCH_ANY 'HELLO' ORDER BY id", true)
            trigger_and_wait_compaction("vvi_add_drop", "full", 1800)
            sql "SET enable_match_without_inverted_index=false"
            // Only p_new received the new schema after re-ADD; p_old may still need scalar fallback.
            quickTest("${tag}_replacement_compacted", "SELECT id FROM vvi_add_drop PARTITION(p_new) WHERE v MATCH_ANY 'HELLO' ORDER BY id", true)
            quickTest("${tag}_replacement_lower", "SELECT id FROM vvi_add_drop PARTITION(p_new) WHERE v MATCH_ANY 'hello' ORDER BY id", true)
            sql "SET enable_match_without_inverted_index=true"
            quickTest("${tag}_values", "SELECT id,v FROM vvi_add_drop ORDER BY id", true)
        }
    }
}

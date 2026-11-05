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

suite("test_variant_values_index_doc_alter", "p0,nonConcurrent") {
    sql "SET default_variant_enable_nested_group=false"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "SET enable_match_without_inverted_index=false"
    sql "DROP TABLE IF EXISTS vvi_doc_alter"
    sql """CREATE TABLE vvi_doc_alter(id INT,
        v VARIANT<PROPERTIES("variant_enable_doc_mode"="true","variant_doc_materialization_min_rows"="10")>,
        INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),
        INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","disable_auto_compaction"="true","inverted_index_storage_format"="SNII")"""
    sql """INSERT INTO vvi_doc_alter VALUES(1,parse_to_variant('{"s":"hello","n":7}')),(2,NULL)"""
    qt_before_values "SELECT id,v FROM vvi_doc_alter ORDER BY id"
    qt_before_root "SELECT id FROM vvi_doc_alter WHERE v MATCH_ANY 'hello' ORDER BY id"
    def columnDDL = sql("SHOW CREATE TABLE vvi_doc_alter")[0][1].readLines().find { it.trim().startsWith('`v` ') }.trim().replaceFirst(/,$/, "")
    assertTrue(columnDDL.contains('"variant_doc_materialization_min_rows" = "10"'))
    // Reuse the complete original column definition, changing only the permitted threshold.
    sql "ALTER TABLE vvi_doc_alter MODIFY COLUMN " + columnDDL.replace('"variant_doc_materialization_min_rows" = "10"','"variant_doc_materialization_min_rows" = "2"')
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='vvi_doc_alter' ORDER BY CreateTime DESC LIMIT 1"
        time 180
    }
    sql """INSERT INTO vvi_doc_alter VALUES(3,parse_to_variant('{"s":"hello world","n":9}')),(4,parse_to_variant('{}'))"""
    for (String phase : ["altered","compacted"]) {
        if (phase == "compacted") { trigger_and_wait_compaction("vvi_doc_alter", "full", 1800) }
        quickTest("${phase}_values", "SELECT id,v,v IS NULL FROM vvi_doc_alter ORDER BY id", true)
        quickTest("${phase}_root", "SELECT id FROM vvi_doc_alter WHERE v MATCH_ANY 'hello' ORDER BY id", true)
        // Historical DOC rowsets below the old threshold need residual path evaluation.
        sql "SET enable_match_without_inverted_index=${phase == 'altered'}"
        quickTest("${phase}_path", "SELECT id FROM vvi_doc_alter WHERE v['s'] MATCH_PHRASE 'hello world' ORDER BY id", true)
        sql "SET enable_match_without_inverted_index=false"
    }
}

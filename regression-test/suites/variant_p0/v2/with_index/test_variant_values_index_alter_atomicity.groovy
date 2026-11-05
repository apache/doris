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

suite("test_variant_values_index_alter_atomicity", "p0,nonConcurrent") {
    sql "SET enable_add_index_for_new_data=true"
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_enable_nested_group=false"
    sql "SET enable_match_without_inverted_index=false"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    def database = sql("SELECT DATABASE()")[0][0]
    def dbId = sql("SHOW PROC '/dbs'").find { it[1] == database }[0]
    def state = { table ->
        def tablet = sql_return_maparray("SHOW TABLETS FROM ${table}")[0]
        def (code,out,err) = curl("GET",tablet.MetaUrl+"?byte_to_base64=true")
        assertEquals(0,code,err)
        def meta = parseJson(out)
        [create: sql("SHOW CREATE TABLE ${table}"), columns: sql("DESC ${table}"),
         indexes: sql("SHOW INDEX FROM ${table}"),
         version: sql("SHOW PROC '/dbs/${dbId}/${meta.table_id}/index_schema'"),
         columnJobs: sql("SHOW ALTER TABLE COLUMN WHERE TableName='${table}'"),
         indexJobs: sql("SHOW BUILD INDEX WHERE TableName='${table}'")]
    }
    // No Bloom Filter: rejected values-index DDL must preserve all table metadata.
    for (String model : ['DUPLICATE','UNIQUE']) {
        String table = "vvi_atomicity_${model.toLowerCase()}"
        sql "DROP TABLE IF EXISTS ${table}"
        String mow = model == 'UNIQUE' ? ',"enable_unique_key_merge_on_write"="true"' : ''
        sql """CREATE TABLE ${table}(id INT,aux INT,v VARIANT,
            INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),
            INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english"))
            ${model} KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1","disable_auto_compaction"="true","inverted_index_storage_format"="SNII"${mow})"""
        sql """INSERT INTO ${table} VALUES(1,11,parse_to_variant('{"s":"hello"}'))"""
        int rowId = 1
        String addValues = 'ADD INDEX another_values(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english")'
        for (String clauses : ["DROP COLUMN aux, ${addValues}",
                               "ADD COLUMN extra INT, ${addValues}",
                               "${addValues}, DROP COLUMN aux",
                               "${addValues}, ADD COLUMN extra INT"]) {
            def before = state(table)
            test { sql "ALTER TABLE ${table} ${clauses}"; exception "ADD VARIANT values index can not be combined with other schema changes" }
            def after = state(table)
            rowId++
            sql """INSERT INTO ${table} VALUES(${rowId},22,parse_to_variant('{"s":"world"}'))"""
            assertEquals(before,after)
            def tag = "${model.toLowerCase()}_${rowId}"
            quickTest("${tag}_values", "SELECT id,aux,v FROM ${table} ORDER BY id", true)
            quickTest("${tag}_root", "SELECT id FROM ${table} WHERE v MATCH_ANY 'hello world' ORDER BY id", true)
            quickTest("${tag}_path", "SELECT id FROM ${table} WHERE v['s'] MATCH_ANY 'hello world' ORDER BY id", true)
            quickTest("${tag}_absent", "SELECT id FROM ${table} WHERE v MATCH_ANY 'absent' ORDER BY id", true)
        }
    }
}

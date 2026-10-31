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

suite("test_variant_values_index_ddl", "p0") {
    sql "SET enable_match_without_inverted_index=true"
    sql "SET enable_inverted_index_query=true"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "SET enable_prune_nested_column=false"
    sql "SET enable_common_expr_pushdown=true"
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_max_subcolumns_count=0"
    sql "SET default_variant_enable_typed_paths_to_sparse=false"
    sql "SET default_variant_enable_nested_group=false"
    sql "SET enable_decimal256=true"
    for (String name : ["vvi_ddl_english", "vvi_ddl_unicode", "vvi_ddl_chinese", "vvi_ddl_standard",
                        "vvi_ddl_replay", "vvi_ddl_mow", "vvi_ddl_rejected"]) {
        sql "DROP TABLE IF EXISTS ${name}"
    }
    for (String parser : ["english", "unicode", "chinese", "standard"]) {
        sql """CREATE TABLE vvi_ddl_${parser} (id INT, v VARIANT,
            INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="${parser}"))
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1","inverted_index_storage_format"="SNII")"""
    }
    sql """CREATE TABLE vvi_ddl_mow (id INT, v VARIANT,
        INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"))
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","inverted_index_storage_format"="SNII",
                   "enable_unique_key_merge_on_write"="true")"""

    // Only the INDEX line is replayed/compared; SHOW CREATE table defaults are environment dependent.
    def indexLine = { String table ->
        (sql("SHOW CREATE TABLE ${table}"))[0][1].readLines()
            .find { it.trim().startsWith("INDEX values_idx ") }.trim().replaceFirst(/,$/, "")
    }
    def original = indexLine("vvi_ddl_english")
    assertTrue(original.contains('"support_phrase" = "false"'))
    assertTrue(original.contains('"lower_case" = "true"'))
    assertFalse(original.contains("variant_index_mode"))
    assertFalse(original.contains("variant_root_format_version"))
    sql """CREATE TABLE vvi_ddl_replay (id INT, v VARIANT, ${original})
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","inverted_index_storage_format"="SNII")"""
    assertEquals(original, indexLine("vvi_ddl_replay"))

    def rejected = { String type, String props, String format, String model, String extra, String message ->
        test {
            sql """CREATE TABLE vvi_ddl_rejected (id INT, v ${type},
                INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values"${props}))
                ${model} KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num"="1","inverted_index_storage_format"="${format}"${extra})"""
            exception message
        }
    }
    def english = ',"parser"="english"'
    rejected("VARIANT", ',"parser"="none"', "SNII", "DUPLICATE", "",
             "VARIANT values index requires a parser or analyzer with tokenization")
    rejected("VARIANT", "", "SNII", "DUPLICATE", "",
             "VARIANT values index requires a parser or analyzer with tokenization")
    rejected("VARIANT", english + ',"support_phrase"="true"', "SNII", "DUPLICATE", "",
             "VARIANT values index does not support support_phrase=true")
    rejected("VARIANT<'a*':STRING>", english + ',"field_pattern"="a*"', "SNII", "DUPLICATE", "",
             "VARIANT values index does not support field_pattern")
    rejected("STRING", english, "SNII", "DUPLICATE", "",
             "variant_index_scope=values can only be used on VARIANT columns")
    rejected("VARIANT", english, "V2", "DUPLICATE", "",
             "VARIANT values index requires inverted_index_storage_format=SNII")
    rejected('VARIANT<PROPERTIES("variant_enable_nested_group"="true")>', english, "SNII", "DUPLICATE", "",
             "VARIANT values index does not support NestedGroup")
    rejected("VARIANT<'wide':DECIMAL(76,2)>", english, "SNII", "DUPLICATE", "",
             "VARIANT values index does not support declared Decimal256 paths")
    rejected("VARIANT", english, "SNII", "UNIQUE", ',"enable_unique_key_merge_on_write"="false"',
             "INVERTED index with parser can NOT be used in value columns of UNIQUE_KEYS table with merge_on_write disable")
    rejected("VARIANT REPLACE", english, "SNII", "AGGREGATE", "",
             "index should only be used in columns of DUP_KEYS/UNIQUE_KEYS table or key columns of AGG_KEYS table")
    for (String key : ["variant_index_mode", "variant_root_format_version"]) {
        def value = key == "variant_index_mode" ? "all_values" : "1"
        rejected("VARIANT", english + ',"' + key + '"="' + value + '"', "SNII", "DUPLICATE", "",
                 "Invalid inverted index property key: ${key}")
    }
    test {
        sql """CREATE TABLE vvi_ddl_rejected (id INT, v VARIANT,
            INDEX first_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),
            INDEX second_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="unicode"))
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1","inverted_index_storage_format"="SNII")"""
        exception "VARIANT column supports only one values index"
    }
    for (String statement : [
        'CREATE INDEX new_values ON vvi_ddl_english(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english")',
        'ALTER TABLE vvi_ddl_english ADD INDEX new_values(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english")']) {
        test {
            sql statement
            exception "VARIANT values index can only be declared in CREATE TABLE"
        }
    }
    test {
        sql(isCloudMode() ? "BUILD INDEX ON vvi_ddl_english"
                          : "BUILD INDEX values_idx ON vvi_ddl_english")
        exception "BUILD INDEX operation failed: The values_idx index can not be built on the v column, because it is a variant type column."
    }
    test {
        sql "DROP INDEX values_idx ON vvi_ddl_english"
        exception "DROP INDEX is not supported for a VARIANT values index"
    }
    sql 'ALTER TABLE vvi_ddl_english ADD INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english")'
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='vvi_ddl_english' ORDER BY CreateTime DESC LIMIT 1"
        time 120
    }
    assertTrue(sql("SHOW CREATE TABLE vvi_ddl_english")[0][1].contains("INDEX path_idx"))
}

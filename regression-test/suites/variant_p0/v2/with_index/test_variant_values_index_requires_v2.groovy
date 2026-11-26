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

suite("test_variant_values_index_requires_v2", "p0,nonConcurrent") {
    def indexed = "vvi_requires_v2_indexed"
    def plain = "vvi_requires_v2_plain"
    def rejected = "vvi_requires_v2_rejected"
    def error = "VARIANT values index requires enable_variant_v2=true"
    def index = 'INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english")'
    // Legacy keys isolate the values-index check from the escaped-key V2 requirement.
    def properties = """DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "inverted_index_storage_format"="SNII",
                   "variant_escape_dots_in_keys"="false", "disable_auto_compaction"="true")"""
    def createIndexed = { tableName -> "CREATE TABLE ${tableName}(id INT, v VARIANT, ${index}) ${properties}" }
    def rootMatch = "SELECT id FROM ${indexed} WHERE v MATCH 'hello' ORDER BY id"
    def countRows = "SELECT COUNT(*) FROM ${indexed}"
    def streamJson = { tableName, data, expectedError ->
        streamLoad {
            table tableName
            set "format", "json"
            set "read_json_by_line", "true"
            set "group_commit", "off_mode"
            inputStream new ByteArrayInputStream(data.getBytes('UTF-8'))
            if (expectedError != null) {
                check { response, ex, start, end ->
                    assertNull(ex)
                    def result = parseJson(response)
                    assertEquals("Fail", result.Status)
                    assertTrue(result.Message.contains(expectedError), response)
                    assertEquals(0L, result.NumberLoadedRows.toLong())
                }
            }
            time 10000
        }
    }

    sql "SET enable_add_index_for_new_data=true"
    sql "SET group_commit='off_mode'"
    sql "SET debug_variant_enable_doc_mode=false"
    sql "SET debug_variant_enable_nested_group=false"
    sql "SET enable_sql_cache=true"
    sql "SET enable_inverted_index_query=true"
    sql "SET enable_match_without_inverted_index=false"
    def dropTables = {
        [indexed, plain, rejected].each { sql "DROP TABLE IF EXISTS ${it}" }
    }

    dropTables()
    try {
        setFeConfigTemporary([enable_variant_v2: true]) {
            sql createIndexed(indexed)
            sql """INSERT INTO ${indexed} VALUES (1, parse_to_variant('{"s":"hello"}'))"""
            assertEquals([[1]], sql(rootMatch))
            assertEquals([[1L]], sql(countRows))

            setFeConfigTemporary([enable_variant_v2: false]) {
                test {
                    sql createIndexed(rejected)
                    exception error
                }
                test {
                    sql "CREATE TABLE ${rejected} LIKE ${indexed}"
                    exception error
                }

                sql "CREATE TABLE ${plain}(id INT, v VARIANT) ${properties}"
                test {
                    sql "ALTER TABLE ${plain} ADD ${index}"
                    exception error
                }
                sql """INSERT INTO ${plain} VALUES (1, CAST('{"s":"control"}' AS VARIANT))"""
                assertEquals([[1]], sql("SELECT id FROM ${plain}"))

                for (String query : [rootMatch, countRows, "SELECT id, v FROM ${indexed}"]) {
                    test {
                        sql query
                        exception error
                    }
                }
                test {
                    sql """INSERT INTO ${indexed} VALUES (2, CAST('{"s":"hello"}' AS VARIANT))"""
                    exception error
                }
                streamJson(indexed, '{"id":3,"v":{"s":"hello"}}\n', error)
            }

            assertEquals([[1L]], sql(countRows))
            streamJson(indexed, '{"id":4,"v":{"s":"hello"}}\n', null)
            assertEquals([[1], [4]], sql(rootMatch))
        }
    } finally {
        dropTables()
    }
}

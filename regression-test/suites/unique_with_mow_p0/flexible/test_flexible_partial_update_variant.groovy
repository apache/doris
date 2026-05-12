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

suite("test_flexible_partial_update_variant") {
    sql "set default_variant_enable_doc_mode = false"

    def docModeUnsupportedMsg = "VARIANT flexible partial update does not support doc mode"
    def nullPatchUnsupportedMsg = "VARIANT flexible partial update does not support JSON null patch values"
    def oldValueUnsupportedMsg = "VARIANT flexible partial update only supports patching JSON object old values"
    def expect_streamload_fail = { tableName, loadBody, expectedMessage ->
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            inputStream new ByteArrayInputStream(loadBody.getBytes("UTF-8"))
            time 20000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    assertTrue(exception.getMessage().contains(expectedMessage))
                    return
                }
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains(expectedMessage))
            }
        }
    }
    def expect_streamload_filtered = { tableName, loadBody, expectedTotalRows, expectedLoadedRows,
            expectedFilteredRows ->
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'max_filter_ratio', '1'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            inputStream new ByteArrayInputStream(loadBody.getBytes("UTF-8"))
            time 20000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(expectedTotalRows, json.NumberTotalRows)
                assertEquals(expectedLoadedRows, json.NumberLoadedRows)
                assertEquals(expectedFilteredRows, json.NumberFilteredRows)
            }
        }
    }

    for (def use_row_store : [false, true]) {
        def tableName = "test_flexible_partial_update_variant_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                `k` int NOT NULL,
                `c` int NULL,
                `v` variant NULL
            ) UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true",
                "store_row_column" = "${use_row_store}");
        """

        sql """
            INSERT INTO ${tableName} VALUES
            (1, 1, '{"a": 1, "c": 3, "nested": {"x": 1}}'),
            (2, 2, '{"nested": {"x": 1}, "keep": 9}')
        """

        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "variant_patch_merge.json"
            time 20000
        }

        def merged = sql """
            SELECT k, cast(v['a'] as int), cast(v['b'] as int), cast(v['c'] as int),
                   cast(v['nested']['x'] as int), cast(v['nested']['y'] as int), c
            FROM ${tableName} ORDER BY k
        """
        assertEquals("[[1, 10, 20, 3, 1, null, 1], [2, null, null, null, 1, 2, 22]]",
                merged.toString())

        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            inputStream new ByteArrayInputStream("""{"k":4,"v":{"new_path":4}}
""".getBytes("UTF-8"))
            time 20000
        }

        def newKeyRows = sql """
            SELECT k, cast(v['new_path'] as int), c
            FROM ${tableName} WHERE k = 4 ORDER BY k
        """
        assertEquals("[[4, 4, null]]", newKeyRows.toString())

        expect_streamload_filtered(tableName, """{"k":1,"v":[1,2,3]}
{"k":1,"v":"{\\"b\\":2}"}
{"k":1,"v":null}
{"k":1,"v":{"b":30}}
""", 4, 1, 3)
        expect_streamload_fail(tableName, """{"k":1,"v":{"a":null}}\n""", nullPatchUnsupportedMsg)

        def afterRejectedRootValues = sql """
            SELECT k, cast(v['a'] as int), cast(v['b'] as int), cast(v['c'] as int),
                   cast(v['nested']['x'] as int)
            FROM ${tableName} WHERE k = 1 ORDER BY k
        """
        assertEquals("[[1, 10, 30, 3, 1]]", afterRejectedRootValues.toString())

        sql """ INSERT INTO ${tableName} VALUES (3, 3, '{"nested": {"x": 1}, "b": 1}') """
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            inputStream new ByteArrayInputStream("""{"k":3,"v":{"nested":{}}}
{"k":3,"v":{"b":2}}
""".getBytes("UTF-8"))
            time 20000
        }

        def emptyObjectRows = sql """
            SELECT k, v['nested']['x'] IS NULL, cast(v['b'] as int), c
            FROM ${tableName} WHERE k = 3 ORDER BY k
        """
        assertEquals("[[3, true, 2, 3]]", emptyObjectRows.toString())

        sql """ INSERT INTO ${tableName} VALUES (5, 5, '{"a": 1, "b": 1, "keep": 1}') """
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            inputStream new ByteArrayInputStream("""{"k":5,"v":{"a":2}}
{"k":5,"__DORIS_DELETE_SIGN__":1}
{"k":5,"v":{"b":3}}
""".getBytes("UTF-8"))
            time 20000
        }

        def patchAfterDeleteRows = sql """
            SELECT k, cast(v['a'] as int), cast(v['b'] as int), cast(v['keep'] as int), c
            FROM ${tableName} WHERE k = 5 ORDER BY k
        """
        assertEquals("[[5, null, 3, null, null]]", patchAfterDeleteRows.toString())
    }

    def oldRootTable = "test_flexible_partial_update_variant_old_root"
    sql """ DROP TABLE IF EXISTS ${oldRootTable} """
    sql """
        CREATE TABLE ${oldRootTable} (
            `k` int NOT NULL,
            `v` variant NULL
        ) UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true");
    """
    streamLoad {
        table "${oldRootTable}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'
        inputStream new ByteArrayInputStream("""{"k":1,"v":[1,2]}
{"k":2,"v":"plain"}
""".getBytes("UTF-8"))
        time 20000
    }
    expect_streamload_fail(oldRootTable, """{"k":1,"v":{"a":1}}\n""", oldValueUnsupportedMsg)
    expect_streamload_fail(oldRootTable, """{"k":2,"v":{"a":1}}\n""", oldValueUnsupportedMsg)

    def typedTable = "test_flexible_partial_update_variant_typed"
    sql """ DROP TABLE IF EXISTS ${typedTable} """
    sql """
        CREATE TABLE ${typedTable} (
            `k` int NOT NULL,
            `v` variant<'a' : int, 'b' : int, properties("variant_enable_typed_paths_to_sparse" = "false")> NULL
        ) UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true");
    """
    sql """ INSERT INTO ${typedTable} VALUES (1, '{"a": 1, "c": 3}') """

    streamLoad {
        table "${typedTable}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "variant_patch_merge.json"
        time 20000
    }

    def typedMerged = sql """
        SELECT k, cast(v['a'] as int), cast(v['b'] as int), cast(v['c'] as int)
        FROM ${typedTable} WHERE k = 1 ORDER BY k
    """
    assertEquals("[[1, 10, 20, 3]]", typedMerged.toString())

    def docTable = "test_flexible_partial_update_variant_doc"
    sql """ DROP TABLE IF EXISTS ${docTable} """
    sql """
        CREATE TABLE ${docTable} (
            `k` int NOT NULL,
            `v` variant<properties("variant_enable_doc_mode" = "true", "variant_doc_materialization_min_rows" = "0")> NULL
        ) UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true");
    """
    expect_streamload_fail(docTable, """{"k":1,"v":{"a":1}}\n""", docModeUnsupportedMsg)

    def seqTable = "test_flexible_partial_update_variant_same_batch_seq"
    sql """ DROP TABLE IF EXISTS ${seqTable} """
    sql """
        CREATE TABLE ${seqTable} (
            `k` int NOT NULL,
            `seq` int NULL,
            `v` variant NULL
        ) UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "function_column.sequence_col" = "seq");
    """
    sql """ INSERT INTO ${seqTable} VALUES (1, 10, '{"a":1,"b":1}') """

    String seqLoad = """{"k":1,"seq":20,"v":{"a":2}}
{"k":1,"seq":15,"v":{"b":3}}
{"k":1,"seq":20,"v":{"b":4}}
"""
    streamLoad {
        table "${seqTable}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        inputStream new ByteArrayInputStream(seqLoad.getBytes("UTF-8"))
        time 20000
    }

    def seqMerged = sql """
        SELECT k, seq, cast(v['a'] as int), cast(v['b'] as int)
        FROM ${seqTable} ORDER BY k
    """
    assertEquals("[[1, 20, 2, 4]]", seqMerged.toString())

    def seqMapTable = "test_flexible_partial_update_variant_seq_map"
    sql """ DROP TABLE IF EXISTS ${seqMapTable} """
    sql """
        CREATE TABLE ${seqMapTable} (
            `k` int NOT NULL,
            `seq_map` int NULL,
            `v` variant NULL
        ) UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "function_column.sequence_col" = "seq_map");
    """
    sql """
        INSERT INTO ${seqMapTable} VALUES
        (1, 10, '{"a":1,"b":1}'),
        (2, 20, '{"a":2,"b":2}')
    """

    String seqMapLoad = """{"k":1,"v":{"a":3}}
{"k":2,"seq_map":15,"v":{"a":4}}
{"k":2,"seq_map":25,"v":{"b":5}}
"""
    streamLoad {
        table "${seqMapTable}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        inputStream new ByteArrayInputStream(seqMapLoad.getBytes("UTF-8"))
        time 20000
    }

    def seqMapMerged = sql """
        SELECT k, seq_map, __DORIS_SEQUENCE_COL__, cast(v['a'] as int), cast(v['b'] as int)
        FROM ${seqMapTable} ORDER BY k
    """
    assertEquals("[[1, 10, 10, 3, 1], [2, 25, 25, 2, 5]]", seqMapMerged.toString())
}

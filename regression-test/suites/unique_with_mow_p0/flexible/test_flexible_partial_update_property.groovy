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

suite('test_flexible_partial_update_property') {
    sql "set default_variant_enable_doc_mode = false"

    def tableName = "test_flexible_partial_update_property"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL DEFAULT "9876",
        `v3` BIGINT NOT NULL,
        `v4` BIGINT NOT NULL DEFAULT "1234",
        `v5` BIGINT NULL
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "store_row_column" = "false"); """

    def show_res = sql "show create table ${tableName}"
    assertTrue(show_res.toString().contains('"enable_unique_key_skip_bitmap_column" = "true"'))

    def doSchemaChange = { cmd ->
        sql cmd
        waitForSchemaChangeDone {
            sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1"""
            time 2000
        }
    }

    def expect_flexible_streamload_fail = { targetTable, loadBody, expectedMessage ->
        streamLoad {
            table "${targetTable}"
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

    test {
        sql """alter table ${tableName} enable feature "UPDATE_FLEXIBLE_COLUMNS"; """
        exception "table ${tableName} has enabled update flexible columns feature already."
    }
    doSchemaChange """alter table ${tableName} add column v_normal variant NULL;"""
    test {
        sql """alter table ${tableName} add column v_doc variant<properties("variant_enable_doc_mode" = "true",
                "variant_doc_materialization_min_rows" = "0")> NULL;"""
        exception "VARIANT flexible partial update does not support doc mode"
    }
    test {
        sql """alter table ${tableName} add column (v_doc_multi variant<properties("variant_enable_doc_mode" = "true",
                "variant_doc_materialization_min_rows" = "0")> NULL);"""
        exception "VARIANT flexible partial update does not support doc mode"
    }

    tableName = "test_flexible_partial_update_property_doc_create"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL,
        `v` variant<properties("variant_enable_doc_mode" = "true",
                "variant_doc_materialization_min_rows" = "0")> NULL
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "store_row_column" = "false"); """
    expect_flexible_streamload_fail(tableName, """{"k":1,"v":{"a":1}}\n""",
            "VARIANT flexible partial update does not support doc mode")

    tableName = "test_flexible_partial_update_property_flatten_create"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql "set enable_variant_flatten_nested = true"
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL,
        `v` variant NULL
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "deprecated_variant_enable_flatten_nested" = "true",
        "store_row_column" = "false"); """
    sql "set enable_variant_flatten_nested = false"
    expect_flexible_streamload_fail(tableName, """{"k":1,"v":{"a":1}}\n""",
            "VARIANT flexible partial update does not support deprecated_variant_enable_flatten_nested")

    // the default value "enable_unique_key_skip_bitmap_column" is "false"
    tableName = "test_flexible_partial_update_property2"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL DEFAULT "9876",
        `v3` BIGINT NOT NULL,
        `v4` BIGINT NOT NULL DEFAULT "1234",
        `v5` BIGINT NULL
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "store_row_column" = "false"); """
    
    show_res = sql "show create table ${tableName}"
    assertTrue(!show_res.toString().contains('enable_unique_key_skip_bitmap_column'))

    sql """insert into ${tableName} select number, number, number, number, number, number from numbers("number" = "6"); """
    order_qt_sql "select k,v1,v2,v3,v4,v5 from ${tableName};"

    test {
        sql """alter table ${tableName} set ("enable_unique_key_skip_bitmap_column"="true");"""
        exception "You can not modify property 'enable_unique_key_skip_bitmap_column'."
    }

    if (!isCloudMode()) {
        tableName = "test_flexible_partial_update_property_lsc_false"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL,
            `v` BIGINT NULL
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "false",
            "store_row_column" = "false"); """
        test {
            sql """alter table ${tableName} enable feature "UPDATE_FLEXIBLE_COLUMNS";"""
            exception "light_schema_change"
        }
    }

    tableName = "test_flexible_partial_update_property_cluster_key"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL,
        `v` BIGINT NOT NULL
        ) UNIQUE KEY(`k`) ORDER BY(`v`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "store_row_column" = "false"); """
    test {
        sql """alter table ${tableName} enable feature "UPDATE_FLEXIBLE_COLUMNS";"""
        exception "cluster keys"
    }

    tableName = "test_flexible_partial_update_property_doc"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL,
        `v` variant<properties("variant_enable_doc_mode" = "true",
                "variant_doc_materialization_min_rows" = "0")> NULL
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "store_row_column" = "false"); """
    test {
        sql """alter table ${tableName} enable feature "UPDATE_FLEXIBLE_COLUMNS";"""
        exception "VARIANT flexible partial update does not support doc mode"
    }

    sql "set enable_variant_flatten_nested = true"
    tableName = "test_flexible_partial_update_property_flatten"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL,
        `v` variant NULL
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "deprecated_variant_enable_flatten_nested" = "true",
        "store_row_column" = "false"); """
    test {
        sql """alter table ${tableName} enable feature "UPDATE_FLEXIBLE_COLUMNS";"""
        exception "VARIANT flexible partial update does not support deprecated_variant_enable_flatten_nested"
    }
    sql "set enable_variant_flatten_nested = false"

    tableName = "test_flexible_partial_update_property_doc_add_enable"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "store_row_column" = "false"); """
    test {
        sql """alter table ${tableName}
                add column v variant<properties("variant_enable_doc_mode" = "true",
                        "variant_doc_materialization_min_rows" = "0")> NULL,
                enable feature "UPDATE_FLEXIBLE_COLUMNS";"""
        exception "VARIANT flexible partial update does not support doc mode"
    }

    tableName = "test_flexible_partial_update_property2"

    doSchemaChange """alter table ${tableName} enable feature "UPDATE_FLEXIBLE_COLUMNS";"""
    show_res = sql "show create table ${tableName}"
    assertTrue(show_res.toString().contains('"enable_unique_key_skip_bitmap_column" = "true"'))
    order_qt_sql "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"

    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test1.json"
        time 20000
    }

    order_qt_sql "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"

}

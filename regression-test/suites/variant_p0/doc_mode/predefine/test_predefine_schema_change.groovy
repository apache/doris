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

suite("predefine_schema_change_doc_value", "p0"){
    def tableName = "test_predefine_schema_change"
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    boolean minrowszero = new Random().nextBoolean();
    if (minrowszero) {
        sql """ set default_variant_doc_materialization_min_rows = 0 """
    } else {
        sql """ set default_variant_doc_materialization_min_rows = 10000 """
    }
    sql """ set default_variant_enable_doc_mode = true """
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant<
            MATCH_NAME 'a' : string,
            MATCH_NAME 'b' : double,
            MATCH_NAME 'c' : string,
            MATCH_NAME 'd' : string,
            properties("variant_max_subcolumns_count" = "2")
        > NULL,
        `col1` varchar(100) NOT NULL,
        INDEX idx_a_b (var) USING INVERTED PROPERTIES("field_pattern"="d", "parser"="unicode", "support_phrase" = "true") COMMENT ''
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
    BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    sql """insert into ${tableName} values(1, '{"a": "2025-04-16", "b": 123.123456789012, "c": "2025-04-17T09:09:09Z", "d": 123, "e": "2025-04-19", "f": "2025-04-20", "g": "2025-04-21", "h": "2025-04-22", "i": "2025-04-23", "j": "2025-04-24", "k": "2025-04-25", "l": "2025-04-26", "m": "2025-04-27", "n": "2025-04-28", "o": "2025-04-29", "p": "2025-04-30"}', 'col');"""
    sql """insert into ${tableName} values(1, '{"a": "2025-04-16", "b": 123.123456789012, "c": "2025-04-17T09:09:09Z", "d": 123, "e": "2025-04-19", "f": "2025-04-20", "g": "2025-04-21", "h": "2025-04-22", "i": "2025-04-23", "j": "2025-04-24", "k": "2025-04-25", "l": "2025-04-26", "m": "2025-04-27", "n": "2025-04-28", "o": "2025-04-29", "p": "2025-04-30"}', 'col');"""

    if (minrowszero) {
        sql """ set enable_match_without_inverted_index = false """
    } else {
        sql """ set enable_match_without_inverted_index = true """
    }
    
    sql """ set enable_common_expr_pushdown = true """
    qt_sql """ select count() from ${tableName} where cast (var['d'] as string) match '123' """
    qt_sql """ select * from ${tableName} """
    qt_sql """ select variant_type(var) from ${tableName} """
    
    sql """ alter table ${tableName} modify column col1 varchar(200) NULL """

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        time 60
    }

    qt_sql """ select count() from ${tableName} where cast (var['d'] as string) match '123' """
    qt_sql """ select * from ${tableName} """
    qt_sql """ select variant_type(var) from ${tableName} """

}
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

suite("test_struct_special_chars") {
    def tableName = "t_struct_special_chars"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            pk INT,
            st STRUCT<a: STRING>
        ) PROPERTIES ("replication_num" = "1")
    """

    // Insert string with double quotes
    sql """INSERT INTO ${tableName}(pk, st) VALUES (1, STRUCT('"aa"'))"""

    // Direct SELECT should output valid JSON with escaped quotes
    qt_select_struct_quotes """SELECT st FROM ${tableName} WHERE pk = 1"""

    // CAST AS STRING should match
    qt_cast_string_quotes """SELECT CAST(st AS STRING) FROM ${tableName} WHERE pk = 1"""

    // CAST AS JSON should match
    qt_cast_json_quotes """SELECT CAST(st AS JSON) FROM ${tableName} WHERE pk = 1"""

    // Insert string with backslash
    sql """INSERT INTO ${tableName}(pk, st) VALUES (2, STRUCT('a\\\\b'))"""

    qt_select_struct_backslash """SELECT st FROM ${tableName} WHERE pk = 2"""

    // Insert string with newline and tab
    sql """INSERT INTO ${tableName}(pk, st) VALUES (3, STRUCT('a\\nb\\tc'))"""

    qt_select_struct_newline_tab """SELECT st FROM ${tableName} WHERE pk = 3"""

    // Test top-level strings are NOT escaped (backward compatibility)
    def strTableName = "t_string_special_chars"
    sql "DROP TABLE IF EXISTS ${strTableName}"
    sql """
        CREATE TABLE ${strTableName} (
            pk INT,
            s STRING
        ) PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${strTableName}(pk, s) VALUES (1, '"aa"')"""
    sql """INSERT INTO ${strTableName}(pk, s) VALUES (2, 'a\\\\b')"""

    qt_select_toplevel_quotes """SELECT s FROM ${strTableName} WHERE pk = 1"""
    qt_select_toplevel_backslash """SELECT s FROM ${strTableName} WHERE pk = 2"""

    sql "DROP TABLE IF EXISTS ${strTableName}"
    sql "DROP TABLE IF EXISTS ${tableName}"
}

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

suite("test_multi_analyzer_column_types", "p0") {
    // Test multi-analyzer indexes on different column types: char, varchar, text, variant

    def analyzerStd = "multi_col_type_std_analyzer"
    def analyzerKw = "multi_col_type_kw_analyzer"

    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerStd} PROPERTIES ("tokenizer" = "standard", "token_filter" = "lowercase");"""
    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerKw} PROPERTIES ("tokenizer" = "keyword", "token_filter" = "lowercase");"""
    sleep(10000)

    // TC-COL-001: CHAR column
    def tblChar = "test_multi_analyzer_char"
    sql "DROP TABLE IF EXISTS ${tblChar}"
    sql """CREATE TABLE ${tblChar} (id INT, name CHAR(50), INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    sql """INSERT INTO ${tblChar} VALUES (1, 'alice'), (2, 'alice cooper'), (3, 'bob');"""
    sql "sync"
    qt_char_std """SELECT id FROM ${tblChar} WHERE name MATCH 'alice' USING ANALYZER ${analyzerStd} ORDER BY id"""
    qt_char_kw """SELECT id FROM ${tblChar} WHERE name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // TC-COL-002: VARCHAR column
    def tblVarchar = "test_multi_analyzer_varchar"
    sql "DROP TABLE IF EXISTS ${tblVarchar}"
    sql """CREATE TABLE ${tblVarchar} (id INT, name VARCHAR(100), INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    sql """INSERT INTO ${tblVarchar} VALUES (1, 'alice'), (2, 'alice cooper'), (3, 'bob');"""
    sql "sync"
    qt_varchar_std """SELECT id FROM ${tblVarchar} WHERE name MATCH 'alice' USING ANALYZER ${analyzerStd} ORDER BY id"""
    qt_varchar_kw """SELECT id FROM ${tblVarchar} WHERE name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // TC-COL-003: TEXT column
    def tblText = "test_multi_analyzer_text"
    sql "DROP TABLE IF EXISTS ${tblText}"
    sql """CREATE TABLE ${tblText} (id INT, content TEXT, INDEX idx_std (content) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (content) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    sql """INSERT INTO ${tblText} VALUES (1, 'alice'), (2, 'alice cooper'), (3, 'bob');"""
    sql "sync"
    qt_text_std """SELECT id FROM ${tblText} WHERE content MATCH 'alice' USING ANALYZER ${analyzerStd} ORDER BY id"""
    qt_text_kw """SELECT id FROM ${tblText} WHERE content MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // TC-COL-004: STRING column
    def tblString = "test_multi_analyzer_string"
    sql "DROP TABLE IF EXISTS ${tblString}"
    sql """CREATE TABLE ${tblString} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    sql """INSERT INTO ${tblString} VALUES (1, 'alice'), (2, 'alice cooper'), (3, 'bob');"""
    sql "sync"
    qt_string_std """SELECT id FROM ${tblString} WHERE name MATCH 'alice' USING ANALYZER ${analyzerStd} ORDER BY id"""
    qt_string_kw """SELECT id FROM ${tblString} WHERE name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // TC-COL-005: VARIANT column with field_pattern
    // VARIANT column needs typed path definition to use field_pattern
    def tblVariant = "test_multi_analyzer_variant"
    sql "DROP TABLE IF EXISTS ${tblVariant}"
    sql """CREATE TABLE ${tblVariant} (
        id INT,
        data VARIANT<'name_*' : string>,
        INDEX idx_std (data) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}", "field_pattern"="name_*"),
        INDEX idx_kw (data) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}", "field_pattern"="name_*")
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    sql """INSERT INTO ${tblVariant} VALUES (1, '{"name_1":"alice"}'), (2, '{"name_1":"alice cooper"}'), (3, '{"name_1":"bob"}');"""
    sql "sync"
    qt_variant_std """SELECT id FROM ${tblVariant} WHERE cast(data['name_1'] as varchar) MATCH 'alice' USING ANALYZER ${analyzerStd} ORDER BY id"""
    qt_variant_kw """SELECT id FROM ${tblVariant} WHERE cast(data['name_1'] as varchar) MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // TC-COL-006: Non-string column should only allow one index
    def tblInt = "test_multi_analyzer_int"
    sql "DROP TABLE IF EXISTS ${tblInt}"
    try {
        sql """CREATE TABLE ${tblInt} (id INT, v INT, INDEX i1 (v) USING INVERTED, INDEX i2 (v) USING INVERTED) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
        assertTrue(false, "Should fail for INT type")
    } catch (Exception e) {
        logger.info("Expected error for INT: ${e.message}")
    }

    // Cleanup
    //sql "DROP TABLE IF EXISTS ${tblChar}"
    //sql "DROP TABLE IF EXISTS ${tblVarchar}"
    //sql "DROP TABLE IF EXISTS ${tblText}"
    //sql "DROP TABLE IF EXISTS ${tblString}"
    //sql "DROP TABLE IF EXISTS ${tblVariant}"
    //sql "DROP TABLE IF EXISTS ${tblInt}"
}

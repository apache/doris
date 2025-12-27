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

suite("test_multi_analyzer_table_types", "p0") {
    // Test multi-analyzer indexes on different table types: DUP, MOW, MOR, AGG

    def analyzerStd = "multi_tbl_type_std_analyzer"
    def analyzerKw = "multi_tbl_type_kw_analyzer"

    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerStd} PROPERTIES ("tokenizer" = "standard", "token_filter" = "lowercase");"""
    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerKw} PROPERTIES ("tokenizer" = "keyword", "token_filter" = "lowercase");"""
    sleep(10000)

    // TC-TBL-001: DUPLICATE KEY table
    def tblDup = "test_multi_analyzer_dup"
    sql "DROP TABLE IF EXISTS ${tblDup}"
    sql """CREATE TABLE ${tblDup} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    sql """INSERT INTO ${tblDup} VALUES (1, 'alice'), (2, 'bob'), (3, 'alice bob');"""
    sql "sync"
    qt_dup_std """SELECT id FROM ${tblDup} WHERE name MATCH 'alice' USING ANALYZER ${analyzerStd} ORDER BY id"""
    qt_dup_kw """SELECT id FROM ${tblDup} WHERE name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // TC-TBL-002: UNIQUE KEY (MOW) table
    def tblMow = "test_multi_analyzer_mow"
    sql "DROP TABLE IF EXISTS ${tblMow}"
    sql """CREATE TABLE ${tblMow} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1", "enable_unique_key_merge_on_write" = "true");"""
    sql """INSERT INTO ${tblMow} VALUES (1, 'alice'), (2, 'bob'), (3, 'alice bob');"""
    sql "sync"
    qt_mow_std """SELECT id FROM ${tblMow} WHERE name MATCH 'alice' USING ANALYZER ${analyzerStd} ORDER BY id"""
    qt_mow_kw """SELECT id FROM ${tblMow} WHERE name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""
}

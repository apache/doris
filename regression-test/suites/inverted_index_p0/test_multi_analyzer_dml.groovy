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

suite("test_multi_analyzer_dml", "p0") {
    // Test DML operations with multi-analyzer indexes

    def analyzerStd = "multi_dml_std_analyzer"
    def analyzerKw = "multi_dml_kw_analyzer"

    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerStd} PROPERTIES ("tokenizer" = "standard", "token_filter" = "lowercase");"""
    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerKw} PROPERTIES ("tokenizer" = "keyword", "token_filter" = "lowercase");"""
    sleep(10000)

    // Test INSERT with multi-analyzer indexes
    def tbl1 = "test_multi_analyzer_dml_insert"
    sql "DROP TABLE IF EXISTS ${tbl1}"
    sql """CREATE TABLE ${tbl1} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    sql """INSERT INTO ${tbl1} VALUES (1, 'alice'), (2, 'alice cooper'), (3, 'bob');"""
    sql "sync"
    qt_insert_std """SELECT id FROM ${tbl1} WHERE name MATCH 'alice' USING ANALYZER ${analyzerStd} ORDER BY id"""
    qt_insert_kw """SELECT id FROM ${tbl1} WHERE name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // Test UPDATE with multi-analyzer indexes (MOW table)
    def tbl2 = "test_multi_analyzer_dml_update"
    sql "DROP TABLE IF EXISTS ${tbl2}"
    sql """CREATE TABLE ${tbl2} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1", "enable_unique_key_merge_on_write" = "true");"""
    sql """INSERT INTO ${tbl2} VALUES (1, 'alice'), (2, 'bob');"""
    sql "sync"
    qt_before_update """SELECT id FROM ${tbl2} WHERE name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""
    sql """UPDATE ${tbl2} SET name = 'alice updated' WHERE id = 1;"""
    sql "sync"
    qt_after_update_old """SELECT id FROM ${tbl2} WHERE name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""
    qt_after_update_new """SELECT id FROM ${tbl2} WHERE name MATCH 'alice updated' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // Test DELETE with multi-analyzer indexes (MOW table)
    def tbl3 = "test_multi_analyzer_dml_delete"
    sql "DROP TABLE IF EXISTS ${tbl3}"
    sql """CREATE TABLE ${tbl3} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1", "enable_unique_key_merge_on_write" = "true");"""
    sql """INSERT INTO ${tbl3} VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');"""
    sql "sync"
    qt_before_delete """SELECT id FROM ${tbl3} WHERE name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""
    sql """DELETE FROM ${tbl3} WHERE id = 1;"""
    sql "sync"
    qt_after_delete """SELECT id FROM ${tbl3} WHERE name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // Test batch INSERT
    def tbl4 = "test_multi_analyzer_dml_batch"
    sql "DROP TABLE IF EXISTS ${tbl4}"
    sql """CREATE TABLE ${tbl4} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    for (int i = 1; i <= 100; i++) {
        sql """INSERT INTO ${tbl4} VALUES (${i}, 'alice_${i}');"""
    }
    sql "sync"
    qt_batch_count """SELECT count(*) FROM ${tbl4} WHERE name MATCH 'alice' USING ANALYZER ${analyzerStd}"""

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tbl1}"
    sql "DROP TABLE IF EXISTS ${tbl2}"
    sql "DROP TABLE IF EXISTS ${tbl3}"
    sql "DROP TABLE IF EXISTS ${tbl4}"
}

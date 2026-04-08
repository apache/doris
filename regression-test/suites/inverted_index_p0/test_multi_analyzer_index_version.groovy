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

suite("test_multi_analyzer_index_version", "p0") {
    // Test multi-analyzer indexes with different index format versions

    def analyzerStd = "multi_ver_std_analyzer"
    def analyzerKw = "multi_ver_kw_analyzer"

    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerStd} PROPERTIES ("tokenizer" = "standard", "token_filter" = "lowercase");"""
    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerKw} PROPERTIES ("tokenizer" = "keyword", "token_filter" = "lowercase");"""
    sleep(10000)

    // TC-VER-001: Index format V1 should NOT support multi-analyzer indexes
    def tblV1 = "test_multi_analyzer_v1"
    sql "DROP TABLE IF EXISTS ${tblV1}"
    try {
        sql """CREATE TABLE ${tblV1} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1", "inverted_index_storage_format" = "V1");"""
        assertTrue(false, "V1 format should not support multi-analyzer indexes")
    } catch (Exception e) {
        logger.info("Expected error for V1 format: ${e.message}")
        assertTrue(e.message.contains("V1") || e.message.contains("not supported") || e.message.contains("format"))
    }

    // TC-VER-002: Index format V2 should support multi-analyzer indexes
    def tblV2 = "test_multi_analyzer_v2"
    sql "DROP TABLE IF EXISTS ${tblV2}"
    sql """CREATE TABLE ${tblV2} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1", "inverted_index_storage_format" = "V2");"""
    sql """INSERT INTO ${tblV2} VALUES (1, 'alice'), (2, 'alice cooper'), (3, 'bob');"""
    sql "sync"
    qt_v2_std """SELECT id FROM ${tblV2} WHERE name MATCH 'alice' USING ANALYZER ${analyzerStd} ORDER BY id"""
    qt_v2_kw """SELECT id FROM ${tblV2} WHERE name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // TC-VER-003: Index format V3 should support multi-analyzer indexes
    def tblV3 = "test_multi_analyzer_v3"
    sql "DROP TABLE IF EXISTS ${tblV3}"
    sql """CREATE TABLE ${tblV3} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1", "inverted_index_storage_format" = "V3");"""
    sql """INSERT INTO ${tblV3} VALUES (1, 'alice'), (2, 'alice cooper'), (3, 'bob');"""
    sql "sync"
    qt_v3_std """SELECT id FROM ${tblV3} WHERE name MATCH 'alice' USING ANALYZER ${analyzerStd} ORDER BY id"""
    qt_v3_kw """SELECT id FROM ${tblV3} WHERE name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tblV1}"
    sql "DROP TABLE IF EXISTS ${tblV2}"
    sql "DROP TABLE IF EXISTS ${tblV3}"
}

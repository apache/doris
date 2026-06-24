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

suite("test_multi_analyzer_ddl", "p0") {
    def analyzerStd = "multi_ddl_test_std_analyzer"
    def analyzerKw = "multi_ddl_test_kw_analyzer"
    def timeout = 60000
    def delta_time = 1000

    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        def useTime = 0
        for (int t = delta_time; t <= OpTimeout; t += delta_time) {
            def alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            if (alter_res.toString().contains("FINISHED")) {
                sleep(3000)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout)
    }

    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerStd} PROPERTIES ("tokenizer" = "standard", "token_filter" = "lowercase");"""
    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerKw} PROPERTIES ("tokenizer" = "keyword", "token_filter" = "lowercase");"""
    sleep(10000)

    // TC-DDL-001: Create table with multiple analyzers
    def tbl1 = "test_multi_analyzer_ddl_001"
    sql "DROP TABLE IF EXISTS ${tbl1}"
    sql """CREATE TABLE ${tbl1} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    def res1 = sql """ SHOW CREATE TABLE ${tbl1} """
    assertTrue(res1.collect { it[1].toString().toLowerCase() }.join("\n").contains(analyzerStd.toLowerCase()))
    assertTrue(res1.collect { it[1].toString().toLowerCase() }.join("\n").contains(analyzerKw.toLowerCase()))

    // TC-DDL-002: Duplicate analyzer identity should fail
    def tbl2 = "test_multi_analyzer_ddl_002"
    sql "DROP TABLE IF EXISTS ${tbl2}"
    try {
        sql """CREATE TABLE ${tbl2} (id INT, v STRING, INDEX i1 (v) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX i2 (v) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
        assertTrue(false, "Should fail with duplicate analyzer")
    } catch (Exception e) {
        // Error message: "column: xxx cannot have multiple inverted indexes of the same type."
        assertTrue(e.message.contains("multiple inverted indexes") || e.message.contains("same type") || e.message.contains("analyzer identity") || e.message.contains("already exists"))
    }

    // TC-DDL-003: Non-string type multiple indexes should fail
    def tbl3 = "test_multi_analyzer_ddl_003"
    sql "DROP TABLE IF EXISTS ${tbl3}"
    try {
        sql """CREATE TABLE ${tbl3} (id INT, v INT, INDEX i1 (v) USING INVERTED, INDEX i2 (v) USING INVERTED) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
        assertTrue(false, "Should fail for INT type")
    } catch (Exception e) {
        logger.info("Expected error: ${e.message}")
    }

    // TC-DDL-004: ALTER TABLE ADD INDEX
    def tbl4 = "test_multi_analyzer_ddl_004"
    sql "DROP TABLE IF EXISTS ${tbl4}"
    sql """CREATE TABLE ${tbl4} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    sql """ ALTER TABLE ${tbl4} ADD INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}"); """
    wait_for_latest_op_on_table_finish(tbl4, timeout)
    def res4 = sql """ SHOW CREATE TABLE ${tbl4} """
    assertTrue(res4.collect { it[1].toString().toLowerCase() }.join("\n").contains(analyzerKw.toLowerCase()))

    // TC-DDL-005: DROP INDEX
    def tbl5 = "test_multi_analyzer_ddl_005"
    sql "DROP TABLE IF EXISTS ${tbl5}"
    sql """CREATE TABLE ${tbl5} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    sql """ DROP INDEX idx_std ON ${tbl5}; """
    wait_for_latest_op_on_table_finish(tbl5, timeout)
    def res5 = sql """ SHOW CREATE TABLE ${tbl5} """
    assertFalse(res5.collect { it[1].toString().toLowerCase() }.join("\n").contains("idx_std"))
    assertTrue(res5.collect { it[1].toString().toLowerCase() }.join("\n").contains("idx_kw"))

    // TC-DDL-006: Using built-in parser
    def tbl6 = "test_multi_analyzer_ddl_006"
    sql "DROP TABLE IF EXISTS ${tbl6}"
    sql """CREATE TABLE ${tbl6} (id INT, content STRING, INDEX idx_cn (content) USING INVERTED PROPERTIES("parser"="chinese"), INDEX idx_en (content) USING INVERTED PROPERTIES("parser"="english"), INDEX idx_none (content) USING INVERTED) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    def res6 = sql """ SHOW CREATE TABLE ${tbl6} """
    assertTrue(res6.collect { it[1].toString().toLowerCase() }.join("\n").contains("chinese"))
    assertTrue(res6.collect { it[1].toString().toLowerCase() }.join("\n").contains("english"))

    // TC-DDL-007: Different analyzer names but same identity (properties order different but semantically same)
    // analyzer_order_a: {"tokenizer":"standard","token_filter":"lowercase"}
    // analyzer_order_b: {"token_filter":"lowercase","tokenizer":"standard"}
    // They have the same semantic identity, should fail when creating table with both
    def analyzerOrderA = "analyzer_order_a_ddl_test"
    def analyzerOrderB = "analyzer_order_b_ddl_test"
    
    // Create two analyzers with different property order but same semantics
    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerOrderA} PROPERTIES ("tokenizer" = "standard", "token_filter" = "lowercase");"""
    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerOrderB} PROPERTIES ("token_filter" = "lowercase", "tokenizer" = "standard");"""
    sleep(10000)
    
    def tbl7 = "test_multi_analyzer_same_identity_diff_order"
    sql "DROP TABLE IF EXISTS ${tbl7}"
    def tbl7CreateSuccess = false
    try {
        // Both analyzers have the same identity (standard + lowercase), should fail
        sql """CREATE TABLE ${tbl7} (
            k1 INT,
            v1 TEXT,
            INDEX idx_a (v1) USING INVERTED PROPERTIES("analyzer" = "${analyzerOrderA}", "support_phrase" = "true"),
            INDEX idx_b (v1) USING INVERTED PROPERTIES("analyzer" = "${analyzerOrderB}", "support_phrase" = "true")
        ) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
        // If we reach here, it means the table was created successfully - this is unexpected
        tbl7CreateSuccess = true
        def res7 = sql """ SHOW CREATE TABLE ${tbl7} """
        logger.info("SHOW CREATE TABLE result: ${res7}")
    } catch (Exception e) {
        // Expected: should fail because both analyzers have the same identity
        logger.info("Expected error for same identity analyzers: ${e.message}")
        assertTrue(e.message.contains("multiple inverted indexes") || e.message.contains("same type") || e.message.contains("analyzer identity") || e.message.contains("already exists"))
    }
    // Verify: table creation should fail because both analyzers have same identity
    assertFalse(tbl7CreateSuccess, "TC-DDL-007 FAILED: Table should NOT be created when two analyzers have same identity (same tokenizer+filter but different property order)")

    // TC-DDL-008: Different analyzer names pointing to same tokenizer + filter combination
    // When two different analyzer names resolve to same tokenizer + filter, they should be considered same identity
    def tbl8 = "test_multi_analyzer_diff_name_same_combo"
    sql "DROP TABLE IF EXISTS ${tbl8}"
    def tbl8CreateSuccess = false
    // Both analyzerStd and analyzerOrderA have same tokenizer(standard) + token_filter(lowercase)
    try {
        sql """CREATE TABLE ${tbl8} (
            k1 INT,
            v1 TEXT,
            INDEX idx_a (v1) USING INVERTED PROPERTIES("analyzer" = "${analyzerStd}", "support_phrase" = "true"),
            INDEX idx_b (v1) USING INVERTED PROPERTIES("analyzer" = "${analyzerOrderA}", "support_phrase" = "true")
        ) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
        // If we reach here, it means the table was created successfully - this is unexpected
        tbl8CreateSuccess = true
        def res8 = sql """ SHOW CREATE TABLE ${tbl8} """
        logger.info("SHOW CREATE TABLE result: ${res8}")
    } catch (Exception e) {
        logger.info("Expected error for same tokenizer+filter combination: ${e.message}")
        assertTrue(e.message.contains("multiple inverted indexes") || e.message.contains("same type") || e.message.contains("analyzer identity") || e.message.contains("already exists"))
    }
    // Verify: table creation should fail because both analyzers have same identity
    assertFalse(tbl8CreateSuccess, "TC-DDL-008 FAILED: Table should NOT be created when two different analyzer names have same tokenizer+filter combination")

    // TC-DDL-009: Drop one index should not affect other indexes on the same column
    // This test case verifies the bug fix: when dropping one inverted index,
    // other indexes on the same column should NOT be deleted
    def tbl9 = "test_multi_analyzer_drop_one_index"
    sql "DROP TABLE IF EXISTS ${tbl9}"
    sql """CREATE TABLE ${tbl9} (
        id INT,
        title VARCHAR(512) NOT NULL,
        INDEX idx_std (title) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}", "support_phrase" = "true"),
        INDEX idx_kw (title) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}", "support_phrase" = "true")
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 
    PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""

    // Insert some test data
    sql """ INSERT INTO ${tbl9} VALUES (1, 'Hello World Test'); """
    sql """ INSERT INTO ${tbl9} VALUES (2, 'Apache Doris Database'); """
    sql """ INSERT INTO ${tbl9} VALUES (3, 'Inverted Index Feature'); """

    // Verify both indexes exist
    def res9_before = sql """ SHOW CREATE TABLE ${tbl9} """
    def schema9_before = res9_before.collect { it[1].toString().toLowerCase() }.join("\n")
    assertTrue(schema9_before.contains("idx_std"))
    assertTrue(schema9_before.contains("idx_kw"))

    // Drop one index (idx_std)
    sql """ DROP INDEX idx_std ON ${tbl9}; """
    wait_for_latest_op_on_table_finish(tbl9, timeout)

    // Verify: idx_std should be dropped, but idx_kw should still exist
    def res9_after = sql """ SHOW CREATE TABLE ${tbl9} """
    def schema9_after = res9_after.collect { it[1].toString().toLowerCase() }.join("\n")
    assertFalse(schema9_after.contains("idx_std"), "TC-DDL-009 FAILED: idx_std should be dropped")
    assertTrue(schema9_after.contains("idx_kw"), "TC-DDL-009 FAILED: idx_kw should still exist after dropping idx_std")

    // Verify: idx_kw index should still work for queries
    def query_res = sql """ SELECT * FROM ${tbl9} WHERE title MATCH_PHRASE 'hello world'; """
    logger.info("Query result using remaining index: ${query_res}")

    // Cleanup
    //sql "DROP TABLE IF EXISTS ${tbl9}"

    // Cleanup
    /*sql "DROP TABLE IF EXISTS ${tbl1}"
    sql "DROP TABLE IF EXISTS ${tbl2}"
    sql "DROP TABLE IF EXISTS ${tbl3}"
    sql "DROP TABLE IF EXISTS ${tbl4}"
    sql "DROP TABLE IF EXISTS ${tbl5}"
    sql "DROP TABLE IF EXISTS ${tbl6}"
    sql "DROP TABLE IF EXISTS ${tbl7}"
    sql "DROP TABLE IF EXISTS ${tbl8}"*/
}

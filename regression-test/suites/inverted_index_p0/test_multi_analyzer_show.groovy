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

suite("test_multi_analyzer_show", "p0") {
    // Test observation commands for multi-analyzer indexes

    def analyzerStd = "multi_show_std_analyzer"
    def analyzerKw = "multi_show_kw_analyzer"
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

    def wait_for_build_index_on_partition_finish = { table_name, OpTimeout ->
        def useTime = 0
        for (int t = delta_time; t <= OpTimeout; t += delta_time) {
            def alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def expected_finished_num = alter_res.size()
            def finished_num = 0
            for (int i = 0; i < expected_finished_num; i++) {
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num
                }
            }
            if (finished_num == expected_finished_num) {
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

    def tableName = "test_multi_analyzer_show_tbl"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""

    // TC-SHOW-001: SHOW CREATE TABLE should contain all index definitions
    def showCreate = sql """ SHOW CREATE TABLE ${tableName} """
    def createStmt = showCreate.collect { it[1].toString() }.join("\n")
    assertTrue(createStmt.contains("idx_std"))
    assertTrue(createStmt.contains("idx_kw"))
    assertTrue(createStmt.toLowerCase().contains(analyzerStd.toLowerCase()))
    assertTrue(createStmt.toLowerCase().contains(analyzerKw.toLowerCase()))
    
    // Verify SHOW CREATE TABLE output can be used to recreate table
    def recreateTable = "test_multi_analyzer_show_recreate"
    sql "DROP TABLE IF EXISTS ${recreateTable}"
    def modifiedStmt = createStmt.replace(tableName, recreateTable)
    sql "${modifiedStmt}"
    def verifyCreate = sql """ SHOW CREATE TABLE ${recreateTable} """
    def verifyStmt = verifyCreate.collect { it[1].toString().toLowerCase() }.join("\n")
    assertTrue(verifyStmt.contains(analyzerStd.toLowerCase()))
    assertTrue(verifyStmt.contains(analyzerKw.toLowerCase()))

    // TC-SHOW-002: SHOW INDEX FROM should show all indexes
    def showIndex = sql """ SHOW INDEX FROM ${tableName} """
    def indexCount = showIndex.size()
    assertTrue(indexCount >= 2, "Should have at least 2 indexes")
    def indexNames = showIndex.collect { it[2].toString() }
    assertTrue(indexNames.contains("idx_std"))
    assertTrue(indexNames.contains("idx_kw"))

    // TC-SHOW-003: SHOW BUILD INDEX after build
    if (!isCloudMode()) {
        def buildTable = "test_multi_analyzer_show_build"
        sql "DROP TABLE IF EXISTS ${buildTable}"
        sql """CREATE TABLE ${buildTable} (id INT, name STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
        sql """INSERT INTO ${buildTable} VALUES (1, 'alice'), (2, 'bob');"""
        sql """ ALTER TABLE ${buildTable} ADD INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"); """
        wait_for_latest_op_on_table_finish(buildTable, timeout)
        sql """ BUILD INDEX idx_std ON ${buildTable}; """
        wait_for_build_index_on_partition_finish(buildTable, timeout)
        def showBuild = sql """ SHOW BUILD INDEX WHERE TableName = "${buildTable}" """
        assertTrue(showBuild.size() > 0, "Should have build index records")
        sql "DROP TABLE IF EXISTS ${buildTable}"
    }

    // TC-SHOW-004: SHOW ALTER TABLE COLUMN after add index
    def alterTable = "test_multi_analyzer_show_alter"
    sql "DROP TABLE IF EXISTS ${alterTable}"
    sql """CREATE TABLE ${alterTable} (id INT, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}")) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    sql """ ALTER TABLE ${alterTable} ADD INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}"); """
    wait_for_latest_op_on_table_finish(alterTable, timeout)
    def showAlter = sql """ SHOW ALTER TABLE COLUMN WHERE TableName = "${alterTable}" ORDER BY CreateTime DESC LIMIT 1; """
    assertTrue(showAlter.size() > 0, "Should have alter table records")
    assertTrue(showAlter[0].toString().contains("FINISHED"), "Alter should be finished")

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${recreateTable}"
    sql "DROP TABLE IF EXISTS ${alterTable}"
}

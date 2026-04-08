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

suite("test_multi_analyzer_partition", "p0") {
    def analyzerStd = "multi_part_std_analyzer"
    def analyzerKw = "multi_part_kw_analyzer"
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

    // Manual partition addition should inherit index properties
    def tbl1 = "test_multi_analyzer_part_manual"
    sql "DROP TABLE IF EXISTS ${tbl1}"
    sql """CREATE TABLE ${tbl1} (id INT, dt DATE, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id, dt) PARTITION BY RANGE(dt) (PARTITION p20230101 VALUES [("2023-01-01"), ("2023-02-01"))) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    sql """ALTER TABLE ${tbl1} ADD PARTITION p20230201 VALUES [("2023-02-01"), ("2023-03-01"));"""
    wait_for_latest_op_on_table_finish(tbl1, timeout)
    sql """INSERT INTO ${tbl1} VALUES (1, '2023-01-15', 'alice'), (2, '2023-02-15', 'alice cooper');"""
    sql "sync"
    qt_manual_std """SELECT id FROM ${tbl1} WHERE name MATCH 'alice' USING ANALYZER ${analyzerStd} ORDER BY id"""
    qt_manual_kw """SELECT id FROM ${tbl1} WHERE name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // Dynamic partition should inherit index properties
    def tbl2 = "test_multi_analyzer_part_dynamic"
    sql "DROP TABLE IF EXISTS ${tbl2}"
    sql """CREATE TABLE ${tbl2} (id INT, dt DATE, name STRING, INDEX idx_std (name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}"), INDEX idx_kw (name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")) DUPLICATE KEY(id, dt) PARTITION BY RANGE(dt) () DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1", "dynamic_partition.enable" = "true", "dynamic_partition.time_unit" = "DAY", "dynamic_partition.start" = "-3", "dynamic_partition.end" = "3", "dynamic_partition.prefix" = "p", "dynamic_partition.buckets" = "1");"""
    sleep(5000)
    def res = sql """ SHOW CREATE TABLE ${tbl2} """
    def createStmt = res.collect { it[1].toString().toLowerCase() }.join("\n")
    assertTrue(createStmt.contains(analyzerStd.toLowerCase()))
    assertTrue(createStmt.contains(analyzerKw.toLowerCase()))

    sql "DROP TABLE IF EXISTS ${tbl1}"
    sql "DROP TABLE IF EXISTS ${tbl2}"
}

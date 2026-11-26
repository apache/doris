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

suite("test_schema_change_string_length_limit", "p0") {
    def waitForJob = { tableName, expectedState ->
        def row = null
        int maxTrySecs = 120
        while (maxTrySecs-- > 0) {
            def res = sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
            row = res[0]
            if (row[9] == "FINISHED" || row[9] == "CANCELLED") {
                break
            }
            sleep(1000)
        }
        assertEquals(expectedState, row[9])
        return row
    }

    sql "DROP TABLE IF EXISTS test_schema_change_string_length_limit"
    sql """
        CREATE TABLE test_schema_change_string_length_limit (
            k INT,
            j JSON
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // Both JSON values are 7 bytes after being converted to text, the NULL row has no length.
    sql """INSERT INTO test_schema_change_string_length_limit VALUES (1, '{"a": 1}'), (2, '[1,2,3]'), (3, NULL)"""
    order_qt_before """SELECT k, j FROM test_schema_change_string_length_limit"""

    // The new length is too short for the old data: the job is cancelled and the table is unchanged.
    sql """ALTER TABLE test_schema_change_string_length_limit MODIFY COLUMN j VARCHAR(5)"""
    def cancelled = waitForJob("test_schema_change_string_length_limit", "CANCELLED")
    assertTrue(cancelled[10].toString().contains("exceeds the length of column j"), cancelled[10].toString())
    qt_desc_after_cancel """DESC test_schema_change_string_length_limit"""
    order_qt_after_cancel """SELECT k, j FROM test_schema_change_string_length_limit"""

    // The new length is enough: the job finishes and old and new rows obey VARCHAR(7).
    sql """ALTER TABLE test_schema_change_string_length_limit MODIFY COLUMN j VARCHAR(7)"""
    waitForJob("test_schema_change_string_length_limit", "FINISHED")
    qt_desc_after_finish """DESC test_schema_change_string_length_limit"""
    sql """INSERT INTO test_schema_change_string_length_limit VALUES (4, 'abcdefg')"""
    order_qt_after_finish """SELECT k, j, LENGTH(j) FROM test_schema_change_string_length_limit"""

    // Widening VARCHAR on a table without light schema change rewrites old data through a slot
    // ref, whose values already fit the new length.
    sql "DROP TABLE IF EXISTS test_schema_change_string_length_limit_widen"
    sql """
        CREATE TABLE test_schema_change_string_length_limit_widen (
            k INT,
            v VARCHAR(5)
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "light_schema_change" = "false")
    """
    sql """INSERT INTO test_schema_change_string_length_limit_widen VALUES (1, 'abcde'), (2, 'abc'), (3, NULL)"""
    sql """ALTER TABLE test_schema_change_string_length_limit_widen MODIFY COLUMN v VARCHAR(6)"""
    waitForJob("test_schema_change_string_length_limit_widen", "FINISHED")
    qt_desc_after_widen """DESC test_schema_change_string_length_limit_widen"""
    sql """INSERT INTO test_schema_change_string_length_limit_widen VALUES (4, 'abcdef')"""
    order_qt_after_widen """SELECT k, v, LENGTH(v) FROM test_schema_change_string_length_limit_widen"""

    // CHAR/VARCHAR length is in bytes, so a default value longer than the column in bytes is
    // rejected. '中' is 3 bytes in UTF-8.
    sql "DROP TABLE IF EXISTS test_schema_change_string_length_limit_default"
    test {
        sql """
            CREATE TABLE test_schema_change_string_length_limit_default (
                k INT,
                c VARCHAR(2) DEFAULT '中'
            ) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        exception "Default value is too long"
    }
    sql """
        CREATE TABLE test_schema_change_string_length_limit_default (
            k INT,
            v INT
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "light_schema_change" = "false")
    """
    sql """INSERT INTO test_schema_change_string_length_limit_default VALUES (1, 1), (2, 2)"""
    // A new value column is linked and filled with the default value when it is read.
    test {
        sql """ALTER TABLE test_schema_change_string_length_limit_default ADD COLUMN c VARCHAR(2) DEFAULT '中'"""
        exception "Default value is too long"
    }
    // A new CHAR key column changes the short key and rewrites old data with the default value.
    test {
        sql """ALTER TABLE test_schema_change_string_length_limit_default ADD COLUMN ck CHAR(1) KEY DEFAULT '中' AFTER k"""
        exception "Default value is too long"
    }
    sql """ALTER TABLE test_schema_change_string_length_limit_default ADD COLUMN c VARCHAR(3) DEFAULT '中'"""
    waitForJob("test_schema_change_string_length_limit_default", "FINISHED")
    sql """ALTER TABLE test_schema_change_string_length_limit_default ADD COLUMN ck CHAR(3) KEY DEFAULT '中' AFTER k"""
    waitForJob("test_schema_change_string_length_limit_default", "FINISHED")
    qt_desc_after_default """DESC test_schema_change_string_length_limit_default"""
    order_qt_after_default """SELECT k, ck, LENGTH(ck), v, c, LENGTH(c) FROM test_schema_change_string_length_limit_default"""

    // A synchronous materialized view is built by the rollup job, which also checks the length of
    // the expression results.
    sql "DROP TABLE IF EXISTS test_schema_change_string_length_limit_mv"
    sql """
        CREATE TABLE test_schema_change_string_length_limit_mv (
            k INT,
            v VARCHAR(8192),
            j JSON
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_schema_change_string_length_limit_mv VALUES
            (1, repeat('x', 8192), '{"a": 12345}'), (1, 'abc', '[1,2,3]'), (2, 'de', NULL), (3, NULL, NULL)"""
    // Values cast to a narrow CHAR/VARCHAR fit the materialized view column.
    create_sync_mv(context.dbName, "test_schema_change_string_length_limit_mv",
            "test_schema_change_string_length_limit_mv_char",
            """SELECT k AS kc, MAX(CAST(v AS CHAR(1))) AS c1 FROM test_schema_change_string_length_limit_mv GROUP BY k""")
    create_sync_mv(context.dbName, "test_schema_change_string_length_limit_mv",
            "test_schema_change_string_length_limit_mv_varchar",
            """SELECT k AS kv, MAX(CAST(j AS VARCHAR(2))) AS v2 FROM test_schema_change_string_length_limit_mv GROUP BY k""")
    order_qt_mv_char """SELECT kc, c1, LENGTH(c1) FROM test_schema_change_string_length_limit_mv INDEX test_schema_change_string_length_limit_mv_char"""
    order_qt_mv_varchar """SELECT kv, v2, LENGTH(v2) FROM test_schema_change_string_length_limit_mv INDEX test_schema_change_string_length_limit_mv_varchar"""

    // CAST(v AS CHAR(1)) keeps the first character, which is 3 bytes for '中文' and longer than the
    // CHAR(1) column in bytes, so the rollup job is cancelled like a load of the same value into the
    // materialized view fails.
    sql """DROP MATERIALIZED VIEW IF EXISTS test_schema_change_string_length_limit_mv_char ON test_schema_change_string_length_limit_mv"""
    sql """INSERT INTO test_schema_change_string_length_limit_mv VALUES (4, '中文', NULL)"""
    sql """CREATE MATERIALIZED VIEW test_schema_change_string_length_limit_mv_char AS
            SELECT k AS kc, MAX(CAST(v AS CHAR(1))) AS c1 FROM test_schema_change_string_length_limit_mv GROUP BY k"""
    def mvRow = null
    for (int i = 0; i < 120; i++) {
        // The latest job of the materialized view, an earlier job of the same name has finished.
        mvRow = sql(""" SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='test_schema_change_string_length_limit_mv' """)
                .findAll { it[5] == "test_schema_change_string_length_limit_mv_char" }
                .max { it[0] as Long }
        if (mvRow[8] == "FINISHED" || mvRow[8] == "CANCELLED") {
            break
        }
        sleep(1000)
    }
    assertEquals("CANCELLED", mvRow[8])
    assertTrue(mvRow[9].toString().contains("exceeds the length of column"), mvRow[9].toString())
    // Only the VARCHAR materialized view is left.
    qt_desc_after_mv_cancel """DESC test_schema_change_string_length_limit_mv ALL"""
}

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

// Test suite for binlog property ALTER TABLE / ALTER DATABASE rejection paths.
//
// Covered categories:
//   1. ALTER TABLE change binlog.format -> rejected
//   2. ALTER TABLE change binlog.need_historical_value -> rejected
//   3. ALTER TABLE disable ROW binlog (binlog.enable=false when format=ROW) -> rejected
//   4. ALTER TABLE online switch to ROW binlog (from non-ROW to ROW) -> rejected
//   5. Database-level binlog enabled, table-level disable -> rejected
//   6. Positive: STATEMENT_AND_SNAPSHOT format allows enable/ttl modification
//   7. Positive: ROW binlog table allows ttl_seconds / max_bytes / max_history_nums modification
//
// Error texts sourced from:
//   - BinlogConfig.mergeFromProperties (format / need_historical_value / disable ROW)
//   - SchemaChangeHandler.updateBinlogConfig (db binlog vs table binlog)
suite("test_binlog_property_alter_exception", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def dbName = "test_binlog_property_alter_exception_db"
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    try {
        // ================================================================
        // 1. ALTER TABLE change binlog.format -> rejected
        //    BinlogConfig.mergeFromProperties: when force=false and newFormat != oldFormat,
        //    returns "not support change binlog format from X to Y"
        // ================================================================

        // 1.1 ROW → STATEMENT_AND_SNAPSHOT
        sql "DROP TABLE IF EXISTS tbl_row_fmt FORCE"
        sql """
            CREATE TABLE tbl_row_fmt (
                k1 INT, v1 INT
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW"
            )
        """
        test {
            sql """ALTER TABLE tbl_row_fmt SET ("binlog.format" = "STATEMENT_AND_SNAPSHOT")"""
            exception "not support change binlog format"
        }

        // 1.2 STATEMENT_AND_SNAPSHOT → ROW
        sql "DROP TABLE IF EXISTS tbl_stmt_fmt FORCE"
        sql """
            CREATE TABLE tbl_stmt_fmt (
                k1 INT, v1 INT
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "STATEMENT_AND_SNAPSHOT"
            )
        """
        test {
            sql """ALTER TABLE tbl_stmt_fmt SET ("binlog.format" = "ROW")"""
            exception "not support change binlog format"
        }

        // ================================================================
        // 2. ALTER TABLE change binlog.need_historical_value -> rejected
        //    BinlogConfig.mergeFromProperties: when force=false and newValue != oldValue,
        //    returns "not support change binlog.need_historical_value from X to Y"
        // ================================================================

        // 2.1 true → false
        sql "DROP TABLE IF EXISTS tbl_hist_true FORCE"
        sql """
            CREATE TABLE tbl_hist_true (
                k1 INT, v1 INT
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """
        test {
            sql """ALTER TABLE tbl_hist_true SET ("binlog.need_historical_value" = "false")"""
            exception "not support change binlog.need_historical_value"
        }

        // 2.2 false → true
        sql "DROP TABLE IF EXISTS tbl_hist_false FORCE"
        sql """
            CREATE TABLE tbl_hist_false (
                k1 INT, v1 INT
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "false"
            )
        """
        test {
            sql """ALTER TABLE tbl_hist_false SET ("binlog.need_historical_value" = "true")"""
            exception "not support change binlog.need_historical_value"
        }

        // ================================================================
        // 3. ALTER TABLE disable ROW binlog -> rejected
        //    BinlogConfig.mergeFromProperties: when format=ROW, changing enable
        //    from true to false returns "can't disable binlog when format is [Row]"
        // ================================================================

        // tbl_row_fmt already has binlog.enable=true + format=ROW
        test {
            sql """ALTER TABLE tbl_row_fmt SET ("binlog.enable" = "false")"""
            exception "can't disable binlog when format is [Row]"
        }

        // ================================================================
        // 4. ALTER TABLE online switch to ROW binlog -> rejected
        //    Table created without binlog (default format=STATEMENT_AND_SNAPSHOT),
        //    ALTER attempts to set enable=true + format=ROW simultaneously.
        //    mergeFromProperties processes need_historical_value first, then format;
        //    the format change is rejected.
        // ================================================================

        sql "DROP TABLE IF EXISTS tbl_no_binlog FORCE"
        sql """
            CREATE TABLE tbl_no_binlog (
                k1 INT, v1 INT
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"
            )
        """
        test {
            sql """ALTER TABLE tbl_no_binlog SET ("binlog.enable" = "true", "binlog.format" = "ROW")"""
            exception "not support change binlog format"
        }

        // ================================================================
        // 5. Database-level binlog enabled, table-level disable -> rejected
        //    SchemaChangeHandler.updateBinlogConfig:
        //    "db binlog is enable, but table binlog is disable"
        //
        //    Note: database-level binlog uses STATEMENT_AND_SNAPSHOT (for CCR),
        //    isEnableForCCR() = enable && format != ROW.
        //    First ALTER DATABASE to enable binlog, then create table (inherits
        //    enable), then ALTER TABLE to disable -> rejected.
        // ================================================================

        def dbNameBinlog = "test_binlog_db_level_enable_db"
        sql "DROP DATABASE IF EXISTS ${dbNameBinlog}"
        sql "CREATE DATABASE ${dbNameBinlog}"
        sql "USE ${dbNameBinlog}"

        // Enable database-level binlog (default STATEMENT_AND_SNAPSHOT)
        sql """ALTER DATABASE ${dbNameBinlog} SET PROPERTIES ("binlog.enable" = "true")"""

        sql """
            CREATE TABLE tbl_db_binlog (
                k1 INT, v1 INT
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"
            )
        """

        // Table-level attempt to disable binlog -> rejected because db-level is enabled
        test {
            sql """ALTER TABLE tbl_db_binlog SET ("binlog.enable" = "false")"""
            exception "db binlog is enable, but table binlog is disable"
        }

        // Switch back to original db for subsequent cases
        sql "USE ${dbName}"

        // ================================================================
        // 6. Positive: STATEMENT_AND_SNAPSHOT format allows enable/ttl modification
        //    STATEMENT_AND_SNAPSHOT enable toggle is not restricted by "can't disable ROW".
        // ================================================================

        // tbl_stmt_fmt was created earlier (format=STATEMENT_AND_SNAPSHOT, enable=true)
        // 6.1 Disable binlog
        sql """ALTER TABLE tbl_stmt_fmt SET ("binlog.enable" = "false")"""
        sql "sync"
        // Verify via SHOW CREATE TABLE that binlog.enable is now false
        def showResult1 = sql "SHOW CREATE TABLE tbl_stmt_fmt"
        def createStmt1 = showResult1[0][1].toString()
        logger.info("after disable: ${createStmt1}")
        assertTrue(createStmt1.contains("\"binlog.enable\" = \"false\""),
            "expect binlog.enable=false after ALTER, got: ${createStmt1}")

        // 6.2 Re-enable binlog
        sql """ALTER TABLE tbl_stmt_fmt SET ("binlog.enable" = "true")"""
        sql "sync"
        def showResult2 = sql "SHOW CREATE TABLE tbl_stmt_fmt"
        def createStmt2 = showResult2[0][1].toString()
        logger.info("after re-enable: ${createStmt2}")
        assertTrue(createStmt2.contains("\"binlog.enable\" = \"true\""),
            "expect binlog.enable=true after re-enable, got: ${createStmt2}")

        // 6.3 Modify ttl_seconds
        sql """ALTER TABLE tbl_stmt_fmt SET ("binlog.ttl_seconds" = "3600")"""
        sql "sync"
        def showResult3 = sql "SHOW CREATE TABLE tbl_stmt_fmt"
        def createStmt3 = showResult3[0][1].toString()
        logger.info("after set ttl: ${createStmt3}")
        assertTrue(createStmt3.contains("\"binlog.ttl_seconds\" = \"3600\""),
            "expect binlog.ttl_seconds=3600 after ALTER, got: ${createStmt3}")

        // ================================================================
        // 7. Positive: ROW binlog table allows operational properties
        //    (ttl / max_bytes / max_history_nums) modification.
        //    These do not involve format or need_historical_value changes; should not be rejected.
        // ================================================================

        sql """ALTER TABLE tbl_row_fmt SET ("binlog.ttl_seconds" = "7200")"""
        sql "sync"
        def showResult4 = sql "SHOW CREATE TABLE tbl_row_fmt"
        def createStmt4 = showResult4[0][1].toString()
        logger.info("ROW table after set ttl: ${createStmt4}")
        assertTrue(createStmt4.contains("\"binlog.ttl_seconds\" = \"7200\""),
            "expect binlog.ttl_seconds=7200 on ROW table, got: ${createStmt4}")

        sql """ALTER TABLE tbl_row_fmt SET ("binlog.max_bytes" = "1073741824")"""
        sql "sync"
        def showResult5 = sql "SHOW CREATE TABLE tbl_row_fmt"
        def createStmt5 = showResult5[0][1].toString()
        logger.info("ROW table after set max_bytes: ${createStmt5}")
        assertTrue(createStmt5.contains("\"binlog.max_bytes\" = \"1073741824\""),
            "expect binlog.max_bytes=1073741824, got: ${createStmt5}")

        sql """ALTER TABLE tbl_row_fmt SET ("binlog.max_history_nums" = "10000")"""
        sql "sync"
        def showResult6 = sql "SHOW CREATE TABLE tbl_row_fmt"
        def createStmt6 = showResult6[0][1].toString()
        logger.info("ROW table after set max_history_nums: ${createStmt6}")
        assertTrue(createStmt6.contains("\"binlog.max_history_nums\" = \"10000\""),
            "expect binlog.max_history_nums=10000, got: ${createStmt6}")

    } finally {
        sql "DROP DATABASE IF EXISTS ${dbName}"
        sql "DROP DATABASE IF EXISTS test_binlog_db_level_enable_db"
    }
}

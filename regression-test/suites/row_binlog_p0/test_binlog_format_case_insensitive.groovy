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

// "binlog.format" is matched case-insensitively and always stored by its canonical (upper case) name,
// at both the table and the database level.
suite("test_binlog_format_case_insensitive") {
    if (isCloudMode()) {
        logger.info("skip test_binlog_format_case_insensitive in cloud mode")
        return
    }

    // enable_feature_binlog is an EXPERIMENTAL config, so SHOW FRONTEND CONFIG reports it as
    // experimental_enable_feature_binlog. checkEnableFeatureBinlog() accounts for that prefix.
    if (!getSyncer().checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_binlog_format_case_insensitive")
        return
    }

    sql "DROP TABLE IF EXISTS test_binlog_format_case_row_tbl FORCE"
    sql "DROP TABLE IF EXISTS test_binlog_format_case_statement_tbl FORCE"
    sql "DROP TABLE IF EXISTS test_binlog_format_case_invalid_tbl FORCE"
    sql "DROP DATABASE IF EXISTS test_binlog_format_case_db FORCE"
    sql "DROP DATABASE IF EXISTS test_binlog_format_case_invalid_db FORCE"

    sql """
        CREATE TABLE test_binlog_format_case_row_tbl (
            k1 INT NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "row"
        )
    """
    sql """
        CREATE TABLE test_binlog_format_case_statement_tbl (
            k1 INT NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "Statement_And_Snapshot"
        )
    """
    order_qt_table_format """
        SELECT TABLE_NAME, PROPERTY_VALUE
        FROM information_schema.table_properties
        WHERE TABLE_CATALOG = 'internal'
            AND TABLE_SCHEMA = '${context.dbName}'
            AND TABLE_NAME IN ('test_binlog_format_case_row_tbl', 'test_binlog_format_case_statement_tbl')
            AND PROPERTY_NAME = 'binlog.format'
    """

    // The lower case value created a real ROW binlog: the write below is readable from it.
    sql "INSERT INTO test_binlog_format_case_row_tbl VALUES (1, 10), (2, 20)"
    order_qt_row_binlog """
        SELECT __DORIS_BINLOG_OP__ AS op, k1, v1
        FROM binlog("table" = "test_binlog_format_case_row_tbl")
    """

    // Repeating the current format in another case is a no-op.
    sql """ALTER TABLE test_binlog_format_case_row_tbl SET ("binlog.format" = "Row")"""
    sql """ALTER TABLE test_binlog_format_case_statement_tbl SET ("binlog.format" = "statement_and_snapshot")"""

    // The format stays immutable, whatever the case of the new value.
    test {
        sql """ALTER TABLE test_binlog_format_case_row_tbl SET ("binlog.format" = "statement_and_snapshot")"""
        exception "not support change binlog format from ROW to STATEMENT_AND_SNAPSHOT"
    }
    test {
        sql """ALTER TABLE test_binlog_format_case_statement_tbl SET ("binlog.format" = "row")"""
        exception "not support change binlog format from STATEMENT_AND_SNAPSHOT to ROW"
    }

    // Values that are not a format in any case are still rejected.
    test {
        sql """
            CREATE TABLE test_binlog_format_case_invalid_tbl (
                k1 INT NOT NULL,
                v1 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.enable" = "true",
                "binlog.format" = "rows"
            )
        """
        exception "Invalid binlog format value: rows"
    }
    test {
        sql """ALTER TABLE test_binlog_format_case_row_tbl SET ("binlog.format" = "rows")"""
        exception "Invalid binlog format value: rows"
    }

    // Database properties are stored as given, so the value is rewritten to its canonical name first.
    sql """
        CREATE DATABASE test_binlog_format_case_db PROPERTIES (
            "binlog.enable" = "true",
            "binlog.format" = "row"
        )
    """
    order_qt_db_format """
        SELECT PROPERTY_NAME, PROPERTY_VALUE
        FROM information_schema.database_properties
        WHERE CATALOG_NAME = 'internal'
            AND SCHEMA_NAME = 'test_binlog_format_case_db'
            AND PROPERTY_NAME IN ('binlog.enable', 'binlog.format')
    """

    // A table created in the database inherits the ROW format.
    sql """
        CREATE TABLE test_binlog_format_case_db.test_binlog_format_case_inherited_tbl (
            k1 INT NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    order_qt_inherited_table_format """
        SELECT PROPERTY_NAME, PROPERTY_VALUE
        FROM information_schema.table_properties
        WHERE TABLE_CATALOG = 'internal'
            AND TABLE_SCHEMA = 'test_binlog_format_case_db'
            AND TABLE_NAME = 'test_binlog_format_case_inherited_tbl'
            AND PROPERTY_NAME IN ('binlog.enable', 'binlog.format')
    """

    sql """
        ALTER DATABASE test_binlog_format_case_db SET PROPERTIES (
            "binlog.enable" = "false",
            "binlog.format" = "statement_and_snapshot"
        )
    """
    order_qt_altered_db_format """
        SELECT PROPERTY_NAME, PROPERTY_VALUE
        FROM information_schema.database_properties
        WHERE CATALOG_NAME = 'internal'
            AND SCHEMA_NAME = 'test_binlog_format_case_db'
            AND PROPERTY_NAME IN ('binlog.enable', 'binlog.format')
    """

    test {
        sql """
            CREATE DATABASE test_binlog_format_case_invalid_db PROPERTIES (
                "binlog.enable" = "true",
                "binlog.format" = "rows"
            )
        """
        exception "Invalid binlog format value: rows"
    }
    test {
        sql """ALTER DATABASE test_binlog_format_case_db SET PROPERTIES ("binlog.format" = "rows")"""
        exception "Invalid binlog format value: rows"
    }
}

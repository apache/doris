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

// Immutable binlog properties must be rejected by the binlog config path
// (AlterOperations.checkBinlogConfigChange -> SchemaChangeHandler.updateBinlogConfig),
// not by the light-schema-change guard of the generic schema change path.
suite("test_binlog_property_alter_exception") {
    if (isCloudMode()) {
        logger.info("skip test_binlog_property_alter_exception in cloud mode")
        return
    }

    def featureBinlogEnabled = false
    for (row in sql("SHOW FRONTEND CONFIG LIKE '%enable_feature_binlog%'")) {
        if (row[0] == 'enable_feature_binlog' && row[1] == 'true') {
            featureBinlogEnabled = true
        }
    }
    if (!featureBinlogEnabled) {
        logger.info("enable_feature_binlog=false in frontend config, skip test_binlog_property_alter_exception")
        return
    }

    def rowTable = "test_binlog_property_alter_row_tbl"
    def plainTable = "test_binlog_property_alter_plain_tbl"

    sql "DROP TABLE IF EXISTS ${rowTable} FORCE"
    sql "DROP TABLE IF EXISTS ${plainTable} FORCE"

    sql """
        CREATE TABLE ${rowTable} (
            k1 INT NOT NULL,
            v1 INT
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

    // binlog.format can not be changed once the table is created
    test {
        sql """ALTER TABLE ${rowTable} SET ("binlog.format" = "STATEMENT_AND_SNAPSHOT")"""
        exception "not support change binlog format from ROW to STATEMENT_AND_SNAPSHOT"
    }

    // binlog.need_historical_value can not be changed either
    test {
        sql """ALTER TABLE ${rowTable} SET ("binlog.need_historical_value" = "false")"""
        exception "not support change binlog.need_historical_value from true to false"
    }

    // binlog can not be disabled while the format is ROW
    test {
        sql """ALTER TABLE ${rowTable} SET ("binlog.enable" = "false")"""
        exception "can't disable binlog when format is [Row]"
    }

    // setting the same value is a no-op and must not be rejected
    sql """ALTER TABLE ${rowTable} SET ("binlog.format" = "ROW")"""

    // mutable binlog properties are still accepted on a ROW binlog table
    sql """ALTER TABLE ${rowTable} SET ("binlog.ttl_seconds" = "7200")"""
    def rowTableDdl = sql("SHOW CREATE TABLE ${rowTable}")[0][1].toString()
    assertTrue(rowTableDdl.contains('"binlog.format" = "ROW"'), rowTableDdl)
    assertTrue(rowTableDdl.contains('"binlog.ttl_seconds" = "7200"'), rowTableDdl)

    // the same check applies to a table without binlog: turning on ROW format afterwards is rejected
    sql """
        CREATE TABLE ${plainTable} (
            k1 INT NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    test {
        sql """ALTER TABLE ${plainTable} SET ("binlog.format" = "ROW")"""
        exception "not support change binlog format from STATEMENT_AND_SNAPSHOT to ROW"
    }

    sql "DROP TABLE IF EXISTS ${rowTable} FORCE"
    sql "DROP TABLE IF EXISTS ${plainTable} FORCE"
}

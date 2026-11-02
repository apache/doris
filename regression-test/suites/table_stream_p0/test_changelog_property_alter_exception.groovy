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

// Immutable changelog properties must be rejected by the changelog config path of
// ALTER TABLE ... SET, not by the light-schema-change guard of the generic schema change path.
// CCR binlog and changelog are mutually exclusive, so neither can be turned on afterwards
// for a table that has the other one.
suite("test_changelog_property_alter_exception") {
    if (isCloudMode()) {
        logger.info("skip test_changelog_property_alter_exception in cloud mode")
        return
    }

    // Both switches are EXPERIMENTAL configs, so SHOW FRONTEND CONFIG may report them with the
    // experimental_ prefix.
    def featureEnabled = { String name ->
        def row = sql("SHOW FRONTEND CONFIG LIKE '%${name}'").find {
            it[0] == name || it[0] == "experimental_${name}"
        }
        return row != null && row[1] == "true"
    }
    if (!featureEnabled("enable_feature_changelog") || !featureEnabled("enable_feature_binlog")) {
        logger.info("fe enable_feature_changelog or enable_feature_binlog is false, " +
                "skip case test_changelog_property_alter_exception")
        return
    }

    sql "DROP TABLE IF EXISTS test_changelog_property_alter_row_tbl FORCE"
    sql "DROP TABLE IF EXISTS test_changelog_property_alter_plain_tbl FORCE"
    sql "DROP TABLE IF EXISTS test_changelog_property_alter_ccr_tbl FORCE"

    sql """
        CREATE TABLE test_changelog_property_alter_row_tbl (
            k1 INT NOT NULL,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """

    // changelog.need_historical_value can not be changed once the table is created
    test {
        sql """ALTER TABLE test_changelog_property_alter_row_tbl SET ("changelog.need_historical_value" = "false")"""
        exception "not support change changelog.need_historical_value from true to false"
    }

    // changelog can not be disabled once the table is created
    test {
        sql """ALTER TABLE test_changelog_property_alter_row_tbl SET ("changelog.enable" = "false")"""
        exception "not support change changelog.enable from true to false"
    }

    // a changelog table can not turn on CCR binlog, nor change any other binlog property
    test {
        sql """ALTER TABLE test_changelog_property_alter_row_tbl SET ("binlog.enable" = "true")"""
        exception "Can not change binlog properties of table test_changelog_property_alter_row_tbl because the table enables changelog"
    }
    test {
        sql """ALTER TABLE test_changelog_property_alter_row_tbl SET ("binlog.ttl_seconds" = "3600")"""
        exception "Can not change binlog properties of table test_changelog_property_alter_row_tbl because the table enables changelog"
    }

    // the legacy keys "binlog.format" and "binlog.need_historical_value" are rejected whatever the value is
    test {
        sql """ALTER TABLE test_changelog_property_alter_row_tbl SET ("binlog.format" = "ROW")"""
        exception 'Property binlog.format is no longer supported, use "changelog.enable" = "true" to enable changelog, or remove it for CCR binlog'
    }
    test {
        sql """ALTER TABLE test_changelog_property_alter_row_tbl SET ("binlog.need_historical_value" = "true")"""
        exception "Property binlog.need_historical_value is no longer supported, use changelog.need_historical_value instead"
    }

    // setting the same value is a no-op and must not be rejected
    sql """ALTER TABLE test_changelog_property_alter_row_tbl SET ("changelog.enable" = "true")"""

    // mutable changelog properties are still accepted on a changelog table
    sql """ALTER TABLE test_changelog_property_alter_row_tbl SET ("changelog.ttl_seconds" = "7200")"""
    def rowTableDdl = sql("SHOW CREATE TABLE test_changelog_property_alter_row_tbl")[0][1].toString()
    assertTrue(rowTableDdl.contains('"changelog.enable" = "true"'), rowTableDdl)
    assertTrue(rowTableDdl.contains('"changelog.ttl_seconds" = "7200"'), rowTableDdl)
    assertFalse(rowTableDdl.contains('"binlog.format"'), rowTableDdl)

    // the same check applies to a table without changelog: turning it on afterwards is rejected
    sql """
        CREATE TABLE test_changelog_property_alter_plain_tbl (
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
        sql """ALTER TABLE test_changelog_property_alter_plain_tbl SET ("changelog.enable" = "true")"""
        exception "not support change changelog.enable from false to true"
    }
    // and a table without changelog has no changelog retention to set
    test {
        sql """ALTER TABLE test_changelog_property_alter_plain_tbl SET ("changelog.ttl_seconds" = "3600")"""
        exception "changelog.ttl_seconds can not be set because changelog is not enabled for table: test_changelog_property_alter_plain_tbl"
    }

    // the same applies to a CCR binlog table
    sql """
        CREATE TABLE test_changelog_property_alter_ccr_tbl (
            k1 INT NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true"
        )
    """
    test {
        sql """ALTER TABLE test_changelog_property_alter_ccr_tbl SET ("changelog.enable" = "true")"""
        exception "not support change changelog.enable from false to true"
    }
}

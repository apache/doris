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

suite("test_row_binlog_ttl", "nonConcurrent") {
    sql "DROP DATABASE IF EXISTS row_binlog_ttl_inherit_db FORCE"
    sql "DROP TABLE IF EXISTS row_binlog_ttl_enabled"
    sql "DROP TABLE IF EXISTS row_binlog_ttl_disabled"
    sql "DROP TABLE IF EXISTS row_binlog_ttl_like"
    sql "DROP TABLE IF EXISTS row_binlog_ttl_alter"
    sql "DROP TABLE IF EXISTS row_binlog_ttl_delayed"

    test {
        sql """
            CREATE TABLE row_binlog_ttl_invalid (k INT)
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.ttl_seconds" = "-2"
            )
        """
        exception "Invalid binlog ttl_seconds value"
    }

    sql """
        CREATE TABLE row_binlog_ttl_enabled (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.ttl_seconds" = "0"
        )
    """
    qt_show_create_enabled "SHOW CREATE TABLE row_binlog_ttl_enabled"

    sql "CREATE TABLE row_binlog_ttl_like LIKE row_binlog_ttl_enabled"
    qt_show_create_like "SHOW CREATE TABLE row_binlog_ttl_like"

    sql """
        CREATE TABLE row_binlog_ttl_disabled (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    qt_show_create_disabled "SHOW CREATE TABLE row_binlog_ttl_disabled"

    sql "INSERT INTO row_binlog_ttl_enabled VALUES (1, 10)"
    sql "INSERT INTO row_binlog_ttl_disabled VALUES (1, 10)"
    sql "SYNC"
    qt_disabled_visible """
        SELECT k, v FROM row_binlog_ttl_disabled@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
    """
    qt_enabled_expired """
        SELECT k, v FROM row_binlog_ttl_enabled@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
    """

    sql """
        CREATE TABLE row_binlog_ttl_delayed (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.ttl_seconds" = "2"
        )
    """
    sql "INSERT INTO row_binlog_ttl_delayed VALUES (1, 10)"
    sql "SYNC"
    qt_delayed_visible """
        SELECT k, v FROM row_binlog_ttl_delayed@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
    """
    sleep(3000)
    qt_delayed_expired """
        SELECT k, v FROM row_binlog_ttl_delayed@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
    """

    test {
        sql """
            SELECT k, v FROM row_binlog_ttl_enabled@incr(
                "startTimestamp" = "1971-01-01 00:00:00",
                "incrementType" = "MIN_DELTA")
        """
        exception "Row binlog offset has expired according to binlog.ttl_seconds"
    }

    sql "CREATE DATABASE row_binlog_ttl_inherit_db"
    sql """
        ALTER DATABASE row_binlog_ttl_inherit_db SET PROPERTIES (
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.ttl_seconds" = "0"
        )
    """
    sql """
        CREATE TABLE row_binlog_ttl_inherit_db.inherited_ttl (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE row_binlog_ttl_inherit_db.override_ttl (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "binlog.ttl_seconds" = "-1")
    """
    sql "INSERT INTO row_binlog_ttl_inherit_db.inherited_ttl VALUES (1, 10)"
    sql "INSERT INTO row_binlog_ttl_inherit_db.override_ttl VALUES (1, 10)"
    sql "SYNC"
    qt_inherited_expired """
        SELECT k, v FROM row_binlog_ttl_inherit_db.inherited_ttl@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
    """
    qt_table_override_visible """
        SELECT k, v FROM row_binlog_ttl_inherit_db.override_ttl@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
    """

    sql """
        CREATE TABLE row_binlog_ttl_alter (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.ttl_seconds" = "-1"
        )
    """
    sql "INSERT INTO row_binlog_ttl_alter VALUES (1, 10)"
    sql "SYNC"
    qt_before_alter_visible """
        SELECT k, v FROM row_binlog_ttl_alter@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
    """
    sql """ALTER TABLE row_binlog_ttl_alter SET ("binlog.ttl_seconds" = "0")"""
    qt_after_alter_expired """
        SELECT k, v FROM row_binlog_ttl_alter@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
    """
    qt_final_visible """
        SELECT k, v FROM row_binlog_ttl_inherit_db.override_ttl@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
    """
}

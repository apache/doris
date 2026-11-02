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

import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_changelog_ttl", "nonConcurrent") {
    // Storage properties differ between Cloud and shared-nothing deployments.
    def showCreateTtl = { String table ->
        def ddl = sql("SHOW CREATE TABLE ${table}")[0][1]
        return (ddl =~ /"changelog\.ttl_seconds" = "(-?\d+)"/)[0][1]
    }

    sql "DROP DATABASE IF EXISTS changelog_ttl_inherit_db FORCE"
    sql "DROP TABLE IF EXISTS changelog_ttl_enabled"
    sql "DROP TABLE IF EXISTS changelog_ttl_default"
    sql "DROP TABLE IF EXISTS changelog_ttl_like"
    sql "DROP TABLE IF EXISTS changelog_ttl_alter"
    sql "DROP TABLE IF EXISTS changelog_ttl_delayed"
    sql "DROP TABLE IF EXISTS changelog_ttl_physical"

    for (ttl in ["-2", "0", "-1"]) {
        test {
            sql """CREATE TABLE changelog_ttl_invalid (k INT)
                DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ("replication_num"="1", "changelog.enable"="true",
                            "changelog.ttl_seconds"="${ttl}")"""
            exception "changelog.ttl_seconds must be greater than 0"
        }
    }

    sql """
        CREATE TABLE changelog_ttl_enabled (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true",
            "changelog.ttl_seconds" = "1"
        )
    """
    qt_show_create_enabled "SELECT ${showCreateTtl('changelog_ttl_enabled')}"
    sql "CREATE TABLE changelog_ttl_like LIKE changelog_ttl_enabled"
    qt_show_create_like "SELECT ${showCreateTtl('changelog_ttl_like')}"

    sql """
        CREATE TABLE changelog_ttl_default (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true"
        )
    """
    qt_show_create_default "SELECT ${showCreateTtl('changelog_ttl_default')}"

    sql "INSERT INTO changelog_ttl_enabled VALUES (1, 10)"
    sql "INSERT INTO changelog_ttl_default VALUES (1, 10)"
    sql "SYNC"
    qt_default_visible """
        SELECT k, v FROM changelog_ttl_default@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """
    sleep(1500)
    qt_enabled_expired """
        SELECT k, v FROM changelog_ttl_enabled@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    sql """
        CREATE TABLE changelog_ttl_delayed (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true",
            "changelog.ttl_seconds" = "2"
        )
    """
    sql "INSERT INTO changelog_ttl_delayed VALUES (1, 10)"
    sql "SYNC"
    qt_delayed_visible """
        SELECT k, v FROM changelog_ttl_delayed@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """
    sleep(3000)
    qt_delayed_expired """
        SELECT k, v FROM changelog_ttl_delayed@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    test {
        sql """
            SELECT k, v FROM changelog_ttl_enabled@incr(
                "startTimestamp" = "1971-01-01 00:00:00",
                "incrementType" = "MIN_DELTA")
        """
        exception "Changelog offset has expired according to changelog.ttl_seconds"
    }

    sql "CREATE DATABASE changelog_ttl_inherit_db"
    for (ttl in ["-1", "0"]) {
        sql """ALTER DATABASE changelog_ttl_inherit_db SET PROPERTIES (
            "changelog.enable" = "false", "changelog.ttl_seconds" = "${ttl}")"""
        test {
            sql """ALTER DATABASE changelog_ttl_inherit_db SET PROPERTIES ("changelog.enable" = "true")"""
            exception "changelog.ttl_seconds must be greater than 0"
        }
    }
    sql """
        ALTER DATABASE changelog_ttl_inherit_db SET PROPERTIES (
            "changelog.enable" = "true",
            "changelog.ttl_seconds" = "1"
        )
    """
    sql """
        CREATE TABLE changelog_ttl_inherit_db.inherited_ttl (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE changelog_ttl_inherit_db.override_ttl (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "changelog.ttl_seconds" = "86400")
    """
    sql "INSERT INTO changelog_ttl_inherit_db.inherited_ttl VALUES (1, 10)"
    sql "INSERT INTO changelog_ttl_inherit_db.override_ttl VALUES (1, 10)"
    sql "SYNC"
    sleep(1500)
    qt_inherited_expired """
        SELECT k, v FROM changelog_ttl_inherit_db.inherited_ttl@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """
    qt_table_override_visible """
        SELECT k, v FROM changelog_ttl_inherit_db.override_ttl@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    sql """
        CREATE TABLE changelog_ttl_alter (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true",
            "changelog.ttl_seconds" = "86400"
        )
    """
    sql "INSERT INTO changelog_ttl_alter VALUES (1, 10)"
    sql "SYNC"
    qt_before_alter_visible """
        SELECT k, v FROM changelog_ttl_alter@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """
    sql """ALTER TABLE changelog_ttl_alter SET ("changelog.ttl_seconds" = "1")"""
    sleep(1500)
    qt_after_alter_expired """
        SELECT k, v FROM changelog_ttl_alter@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """
    qt_final_visible """
        SELECT k, v FROM changelog_ttl_inherit_db.override_ttl@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    sql """
        CREATE TABLE changelog_ttl_physical (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true",
            "changelog.ttl_seconds" = "30",
            "disable_auto_compaction" = "true"
        )
    """
    sql "INSERT INTO changelog_ttl_physical VALUES (1, 10)"
    // The raw changelog TVF does not add @incr's TTL predicate. With no further writes,
    // this singleton can disappear only when background cleanup replaces its rowset.
    qt_before_physical_cleanup "SELECT count(*) FROM changelog('table' = 'changelog_ttl_physical')"
    sleep(45000)
    qt_paused_physical_cleanup "SELECT count(*) FROM changelog('table' = 'changelog_ttl_physical')"
    sql """ALTER TABLE changelog_ttl_physical SET ("disable_auto_compaction" = "false")"""
    Awaitility.await().atMost(120, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until {
        (sql("SELECT count(*) FROM changelog('table' = 'changelog_ttl_physical')")[0][0] as long) == 0L
    }
    if (isCloudMode()) {
        // Other BEs may still cache the pre-compaction Rowsets at the same visible version.
        for (def backend : sql_return_maparray("SHOW BACKENDS")) {
            def url = "http://${backend.Host}:${backend.HttpPort}/api/compaction_score?sync_meta=true&top_n=0"
            def (code, out, err) = curl("GET", url)
            assertEquals(0, code)
            assertTrue(parseJson(out) instanceof List)
        }
    }
    qt_after_physical_cleanup "SELECT count(*) FROM changelog('table' = 'changelog_ttl_physical')"
    qt_base_rows_preserved "SELECT k, v FROM changelog_ttl_physical ORDER BY k"
}

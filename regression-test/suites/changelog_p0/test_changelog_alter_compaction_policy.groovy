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

// Altering a compaction setting of a changelog table must not reach its hidden changelog
// tablets: they keep the changelog compaction policy they were created with.
suite("test_changelog_alter_compaction_policy") {
    if (isCloudMode()) {
        return
    }

    def backendIdToIp = [:]
    def backendIdToHttpPort = [:]
    getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)

    def getTabletMeta = { tablet ->
        def meta = null
        httpTest {
            endpoint "${backendIdToIp["${tablet.BackendId}"]}:${backendIdToHttpPort["${tablet.BackendId}"]}"
            uri "/api/meta/header/${tablet.TabletId}"
            op "get"
            check { respCode, body ->
                assertEquals(200, respCode as int)
                meta = parseJson(body.toString())
            }
        }
        return meta
    }

    // A manual compaction makes a tablet switch to the policy in its tablet meta, so after it
    // the tablet status shows the policy the tablet really compacts with.
    def checkCompactionPolicy = { String tableName, String dataTabletPolicy ->
        trigger_and_wait_compaction(tableName, "cumulative")
        def changelogTabletNum = 0
        for (def tablet : sql_return_maparray("SHOW TABLETS FROM ${tableName}")) {
            def expectedPolicy = dataTabletPolicy
            if (tablet.IsChangelog == "true") {
                expectedPolicy = "changelog"
                changelogTabletNum++
            }
            assertEquals(expectedPolicy, getTabletMeta(tablet).compaction_policy)
            def (code, out, err) = be_show_tablet_status(backendIdToIp["${tablet.BackendId}"],
                    backendIdToHttpPort["${tablet.BackendId}"], tablet.TabletId)
            assertEquals(0, code)
            assertEquals(expectedPolicy, parseJson(out.trim())["cumulative policy type"])
        }
        assertTrue(changelogTabletNum > 0)
    }

    // Case 1: change the compaction policy.
    sql "DROP TABLE IF EXISTS test_changelog_alter_compaction_policy_dup FORCE"
    sql """
        CREATE TABLE test_changelog_alter_compaction_policy_dup (
            k1 INT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "changelog.enable" = "true"
        )
    """
    sql "INSERT INTO test_changelog_alter_compaction_policy_dup VALUES (1, 10), (2, 20)"
    sql "INSERT INTO test_changelog_alter_compaction_policy_dup VALUES (1, 11), (3, 30)"

    sql """ALTER TABLE test_changelog_alter_compaction_policy_dup SET ("compaction_policy" = "time_series")"""
    checkCompactionPolicy("test_changelog_alter_compaction_policy_dup", "time_series")
    order_qt_alter_policy """
        SELECT __DORIS_CHANGELOG_OP__ AS op, k1, v1
        FROM changelog("table" = "test_changelog_alter_compaction_policy_dup")
    """

    // Case 2: change a time series setting. The changelog tablets are not on the time_series
    // policy, so the setting must not be sent to them.
    sql "DROP TABLE IF EXISTS test_changelog_alter_compaction_policy_ts FORCE"
    sql """
        CREATE TABLE test_changelog_alter_compaction_policy_ts (
            k1 INT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "compaction_policy" = "time_series",
            "changelog.enable" = "true"
        )
    """
    sql "INSERT INTO test_changelog_alter_compaction_policy_ts VALUES (1, 10), (2, 20)"
    sql "INSERT INTO test_changelog_alter_compaction_policy_ts VALUES (1, 11), (3, 30)"

    sql """
        ALTER TABLE test_changelog_alter_compaction_policy_ts
        SET ("time_series_compaction_goal_size_mbytes" = "2048")
    """
    for (def tablet : sql_return_maparray("SHOW TABLETS FROM test_changelog_alter_compaction_policy_ts")) {
        if (tablet.IsChangelog != "true") {
            assertEquals(2048L, getTabletMeta(tablet).time_series_compaction_goal_size_mbytes as Long)
        }
    }
    checkCompactionPolicy("test_changelog_alter_compaction_policy_ts", "time_series")
    order_qt_alter_time_series_setting """
        SELECT __DORIS_CHANGELOG_OP__ AS op, k1, v1
        FROM changelog("table" = "test_changelog_alter_compaction_policy_ts")
    """
}

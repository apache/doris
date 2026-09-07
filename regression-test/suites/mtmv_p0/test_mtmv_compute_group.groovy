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

// Declaring `compute_group` on an async materialized view.
//
// The property is a transitional binding whose name and value space match the final
// (owner, compute_group, workload_group) design, so metadata written here is read as an explicit
// pin by later versions. Two values are rejected on purpose: anything in non-cloud mode, and the
// reserved word DEFAULT. The non-cloud check runs first, so in non-cloud mode every value - DEFAULT
// included - is refused with the same "cloud mode" message.
suite("test_mtmv_compute_group") {
    String suiteName = "test_mtmv_compute_group"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"

    sql """drop materialized view if exists ${mvName};"""
    sql """drop table if exists `${tableName}`"""

    sql """
        CREATE TABLE `${tableName}` (
            `k1` INT NULL,
            `k2` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ('replication_num' = '1');
    """
    sql """insert into ${tableName} values(1, 1), (2, 2);"""

    // Returns the statement instead of running it: inside a `test { }` block the `sql` method must
    // be the action's own, so the SQL has to be handed to it there rather than executed here.
    def createMvSql = { String properties ->
        return """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (${properties})
            AS SELECT k1, k2 FROM ${tableName};
        """.toString()
    }

    if (!isCloudMode()) {
        // Non-cloud is out of scope for this transitional change: declaring the property must be
        // rejected whatever the value, so that non-cloud metadata never carries the key and
        // upgrading such a cluster has nothing to convert.
        for (String value : ["any_group", "DEFAULT", "default"]) {
            test {
                sql createMvSql("'replication_num' = '1', 'compute_group' = '${value}'")
                exception "only supported in cloud mode"
            }
        }

        // An MV without the property must keep working exactly as before.
        sql createMvSql("'replication_num' = '1'")
        sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO;"""
        waitingMTMVTaskFinishedByMvName(mvName)
        assertEquals(2, sql("SELECT COUNT(*) FROM ${mvName}").get(0).get(0))

        // ALTER must reject it too, not only CREATE.
        test {
            sql """ALTER MATERIALIZED VIEW ${mvName} SET ('compute_group' = 'any_group');"""
            exception "only supported in cloud mode"
        }

        sql """drop materialized view if exists ${mvName};"""
        sql """drop table if exists `${tableName}`"""
        return
    }

    // ---------------- cloud mode ----------------

    // DEFAULT is reserved by the final design ("follow the owner's default group"). Pinning a group
    // literally named DEFAULT would be silently reinterpreted after an upgrade.
    for (String reserved : ["DEFAULT", "default", "Default"]) {
        test {
            sql createMvSql("'replication_num' = '1', 'compute_group' = '${reserved}'")
            exception "reserved value"
        }
    }

    test {
        sql createMvSql("'replication_num' = '1', 'compute_group' = 'cg_that_does_not_exist'")
        exception "not found"
    }

    def currentComputeGroup = sql_return_maparray("show clusters")
            .stream().filter(cg -> cg.is_current == "TRUE").findFirst().orElse(null)
    assertNotNull(currentComputeGroup)
    def cgName = currentComputeGroup.cluster
    logger.info("current compute group: ${cgName}")

    sql createMvSql("'replication_num' = '1', 'compute_group' = '${cgName}'")

    // The declaration is persisted on the MV and visible.
    def showCreate = sql """show create materialized view ${mvName};"""
    assertTrue(showCreate.toString().contains("compute_group"))
    assertTrue(showCreate.toString().contains(cgName))

    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO;"""
    waitingMTMVTaskFinishedByMvName(mvName)
    assertEquals(2, sql("SELECT COUNT(*) FROM ${mvName}").get(0).get(0))

    // The refresh really ran in the declared compute group.
    def tasks = sql_return_maparray """select * from tasks("type"="mv") where MvName = '${mvName}'"""
    assertTrue(tasks.size() > 0)
    logger.info("mv tasks: ${tasks}")
    assertEquals(cgName, tasks.get(0).ComputeGroup)

    // ALTER keeps the same value space, including the DEFAULT rejection.
    test {
        sql """ALTER MATERIALIZED VIEW ${mvName} SET ('compute_group' = 'DEFAULT');"""
        exception "reserved value"
    }
    test {
        sql """ALTER MATERIALIZED VIEW ${mvName} SET ('compute_group' = 'cg_that_does_not_exist');"""
        exception "not found"
    }

    // Re-declaring the same group is a no-op and must keep refreshing there.
    sql """ALTER MATERIALIZED VIEW ${mvName} SET ('compute_group' = '${cgName}');"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO;"""
    waitingMTMVTaskFinishedByMvName(mvName)
    def tasksAfterAlter = sql_return_maparray """select * from tasks("type"="mv") where MvName = '${mvName}'"""
    assertEquals(cgName, tasksAfterAlter.get(0).ComputeGroup)

    sql """drop materialized view if exists ${mvName};"""
    sql """drop table if exists `${tableName}`"""
}

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

import org.apache.doris.regression.suite.ClusterOptions

// Isolation across compute groups, end to end.
//
// Two async materialized views declaring different compute groups must refresh in their own group
// and nowhere else, no matter which group the triggering session is on. The MV refresh path needs
// no external data source, which makes it the cheapest way to prove the binding end to end.
suite('test_compute_group_binding_isolation', 'multi_cluster,docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
    ]
    options.cloudMode = true

    docker(options) {
        def secondGroup = "cg_isolation_b"
        cluster.addBackend(1, secondGroup)

        def groups = sql_return_maparray """show clusters"""
        logger.info("clusters: ${groups}")
        def firstGroup = groups.stream()
                .filter(cg -> cg.is_current == "TRUE").findFirst().orElse(null)?.cluster
        assertNotNull(firstGroup)
        assertNotEquals(firstGroup, secondGroup)

        def tableName = "test_cg_isolation_tbl"
        def mvOnFirst = "test_cg_isolation_mv_a"
        def mvOnSecond = "test_cg_isolation_mv_b"

        sql """DROP TABLE IF EXISTS ${tableName} FORCE"""
        sql """
            CREATE TABLE ${tableName} (
                `k1` INT NULL,
                `k2` INT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 2
            PROPERTIES ('replication_num' = '1');
        """
        sql """INSERT INTO ${tableName} VALUES (1, 1), (2, 2), (3, 3);"""

        sql """
            CREATE MATERIALIZED VIEW ${mvOnFirst}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1', 'compute_group' = '${firstGroup}')
            AS SELECT k1, k2 FROM ${tableName};
        """
        sql """
            CREATE MATERIALIZED VIEW ${mvOnSecond}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1', 'compute_group' = '${secondGroup}')
            AS SELECT k1, k2 FROM ${tableName};
        """

        sql """REFRESH MATERIALIZED VIEW ${mvOnFirst} AUTO;"""
        sql """REFRESH MATERIALIZED VIEW ${mvOnSecond} AUTO;"""
        waitingMTMVTaskFinishedByMvName(mvOnFirst)
        waitingMTMVTaskFinishedByMvName(mvOnSecond)

        def computeGroupOfLastTask = { mvName ->
            def tasks = sql_return_maparray """
                select * from tasks("type"="mv") where MvName = '${mvName}' order by CreateTime desc
            """
            assertTrue(tasks.size() > 0)
            logger.info("tasks of ${mvName}: ${tasks}")
            return tasks.get(0).ComputeGroup
        }

        // Each MV refreshed in its own compute group, and the two are different.
        assertEquals(firstGroup, computeGroupOfLastTask(mvOnFirst))
        assertEquals(secondGroup, computeGroupOfLastTask(mvOnSecond))

        // Both produced correct data, i.e. pinning to a non-session group did not break the refresh.
        def rowsA = sql "SELECT COUNT(*) FROM ${mvOnFirst}"
        def rowsB = sql "SELECT COUNT(*) FROM ${mvOnSecond}"
        assertEquals(3, rowsA.get(0).get(0))
        assertEquals(3, rowsB.get(0).get(0))

        // The declaration wins over the triggering session: refreshing from inside the second group
        // must still send the first MV to the first group.
        sql """use @${secondGroup}"""
        sql """REFRESH MATERIALIZED VIEW ${mvOnFirst} AUTO;"""
        waitingMTMVTaskFinishedByMvName(mvOnFirst)
        assertEquals(firstGroup, computeGroupOfLastTask(mvOnFirst))

        // And an MV that declares nothing keeps the old behaviour of borrowing the session's group.
        def mvUndeclared = "test_cg_isolation_mv_none"
        sql """
            CREATE MATERIALIZED VIEW ${mvUndeclared}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS SELECT k1, k2 FROM ${tableName};
        """
        sql """REFRESH MATERIALIZED VIEW ${mvUndeclared} AUTO;"""
        waitingMTMVTaskFinishedByMvName(mvUndeclared)
        assertEquals(secondGroup, computeGroupOfLastTask(mvUndeclared))

        sql """use @${firstGroup}"""
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mvOnFirst};"""
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mvOnSecond};"""
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mvUndeclared};"""
        sql """DROP TABLE IF EXISTS ${tableName} FORCE"""
    }
}

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

// Non-cloud (storage-compute integrated) cluster with 1 FE + 4 BE. After
// stopping one BE (3 BE left alive), verify:
//   1. truncate / delete / update on the existing 3-replica table (now with
//      only 2 replicas alive per tablet) still work correctly;
//   2. creating a new 3-replica table still succeeds (alive BE count ==
//      replica count).
suite("test_dml_when_one_be_down", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.feNum = 1
    options.beNum = 4

    docker(options) {
        // All 4 BEs are alive at the start.
        def aliveBeNum = sql_return_maparray("SHOW BACKENDS")
                .count { it.Alive.toString().equalsIgnoreCase("true") }
        assertEquals(4, aliveBeNum)

        def tbl = "test_dml_be_down_tbl"
        sql """ DROP TABLE IF EXISTS ${tbl} """
        sql """
            CREATE TABLE ${tbl} (
                `k` int NOT NULL,
                `v` int NOT NULL
            )
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 4
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 3",
                "enable_unique_key_merge_on_write" = "true"
            )
        """
        sql """ INSERT INTO ${tbl} VALUES (1, 10), (2, 20), (3, 30), (4, 40) """
        def initCount = sql """ SELECT COUNT(*) FROM ${tbl} """
        assertEquals(4L, initCount[0][0])

        // Stop BE 1. stopBackends() internally waits ~7s for the FE
        // heartbeat to mark the BE as dead.
        cluster.stopBackends(1)
        cluster.checkBeIsAlive(1, false)

        // Poll SHOW BACKENDS to make sure FE sees only 3 alive BEs.
        def alive = 0
        for (int i = 0; i < 60; i++) {
            alive = sql_return_maparray("SHOW BACKENDS")
                    .count { it.Alive.toString().equalsIgnoreCase("true") }
            if (alive == 3) {
                break
            }
            sleep(1000)
        }
        assertEquals(3, alive)

        // ---- 1. update / delete / truncate on the 3-replica table
        //         (only 2 replicas alive per tablet) ----

        // 1.1 update: a MOW unique-key table write still satisfies the
        //     quorum requirement with 2 out of 3 replicas alive.
        sql """ UPDATE ${tbl} SET v = v + 100 WHERE k = 1 """
        def afterUpdate = sql """ SELECT v FROM ${tbl} WHERE k = 1 """
        assertEquals(1, afterUpdate.size())
        assertEquals(110, afterUpdate[0][0])

        // 1.2 delete
        sql """ DELETE FROM ${tbl} WHERE k = 2 """
        def afterDelete = sql """ SELECT COUNT(*) FROM ${tbl} WHERE k = 2 """
        assertEquals(0L, afterDelete[0][0])

        def remainCount = sql """ SELECT COUNT(*) FROM ${tbl} """
        assertEquals(3L, remainCount[0][0])

        // 1.3 truncate
        sql """ TRUNCATE TABLE ${tbl} """
        def afterTrunc = sql """ SELECT COUNT(*) FROM ${tbl} """
        assertEquals(0L, afterTrunc[0][0])

        // The table is still writable after truncate.
        sql """ INSERT INTO ${tbl} VALUES (100, 1000), (200, 2000) """
        def afterReinsert = sql """ SELECT k, v FROM ${tbl} ORDER BY k """
        assertEquals(2, afterReinsert.size())
        assertEquals(100, afterReinsert[0][0])
        assertEquals(1000, afterReinsert[0][1])
        assertEquals(200, afterReinsert[1][0])
        assertEquals(2000, afterReinsert[1][1])

        // ---- 2. Creating a new 3-replica table should still succeed when
        //         only 3 BEs are alive. ----
        def newTbl = "test_create_3replica_when_be_down"
        sql """ DROP TABLE IF EXISTS ${newTbl} """
        sql """
            CREATE TABLE ${newTbl} (
                `k` int NOT NULL,
                `v` int NOT NULL
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 4
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 3"
            )
        """
        sql """ INSERT INTO ${newTbl} VALUES (1, 1), (2, 2), (3, 3) """
        def newCount = sql """ SELECT COUNT(*) FROM ${newTbl} """
        assertEquals(3L, newCount[0][0])

        // Replicas should be distributed across the 3 alive BEs.
        def newReplicas = sql_return_maparray("ADMIN SHOW REPLICA DISTRIBUTION FROM ${newTbl}")
        def beWithReplica = newReplicas.count { Integer.valueOf((String) it.ReplicaNum) > 0 }
        assertEquals(3, beWithReplica)
    }
}

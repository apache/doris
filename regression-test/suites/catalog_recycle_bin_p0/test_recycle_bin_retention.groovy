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

// Test the 3-phase recycle bin retention feature:
//   Phase 1: FE visible period - user can see and RECOVER
//   Phase 2: Hidden retention period - user cannot see, and only ADMIN can RECOVER
//   Phase 3: Physical deletion with is_force=true - data is permanently gone

suite("test_recycle_bin_retention", "p0") {

    // Save original FE config values
    def origExpireResult = sql_return_maparray """ ADMIN SHOW FRONTEND CONFIG LIKE 'catalog_trash_expire_second' """
    def origExpireValue = origExpireResult[0].Value
    def origIgnoreLatencyResult = sql_return_maparray """ ADMIN SHOW FRONTEND CONFIG LIKE 'catalog_trash_ignore_min_erase_latency' """
    def origIgnoreLatencyValue = origIgnoreLatencyResult[0].Value
    String currentDbName = context.config.getDbNameByFile(context.file)
    def recoverNormalUser = "recover_normal_user"

    setBeConfigTemporary(["trash_file_expire_time_sec": "0"]) {
        // Set short expire time for testing: 30 seconds
        sql """ ADMIN SET FRONTEND CONFIG ('catalog_trash_expire_second' = '30') """
        sql """ ADMIN SET FRONTEND CONFIG ('catalog_trash_ignore_min_erase_latency' = 'false') """
        sql """ DROP USER IF EXISTS '${recoverNormalUser}'@'%' """
        sql """ CREATE USER '${recoverNormalUser}'@'%' IDENTIFIED BY '123456' """
        sql """ GRANT SELECT_PRIV, ALTER_PRIV, CREATE_PRIV ON *.*.* TO '${recoverNormalUser}'@'%' """

        try {
        // ===== Phase 1: Visible period =====
        // Drop table and verify it's visible in recycle bin and can be recovered

        sql """ DROP TABLE IF EXISTS test_retention_phase1 """
        sql """
            CREATE TABLE test_retention_phase1 (
                k1 INT,
                v1 VARCHAR(100)
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ('replication_allocation' = 'tag.location.default: 1')
        """
        sql """ INSERT INTO test_retention_phase1 VALUES (1, 'phase1_a'), (2, 'phase1_b'), (3, 'phase1_c') """

        // Verify data
        order_qt_phase1_before """ SELECT * FROM test_retention_phase1 """

        // Drop the table
        sql """ DROP TABLE test_retention_phase1 """
        def tableAfterDrop = sql """ SHOW TABLES LIKE "test_retention_phase1" """
        assertTrue(tableAfterDrop.size() == 0, "Dropped table should not be visible in SHOW TABLES")

        // Immediately check recycle bin - should be visible (Phase 1)
        def recycleBin = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = 'test_retention_phase1' """
        assertTrue(recycleBin.size() > 0, "Phase 1: Table should be visible in recycle bin immediately after drop")

        // Phase 1: non-admin user should also be able to recover
        connect("${recoverNormalUser}", "123456", context.config.jdbcUrl) {
            sql """ USE ${currentDbName}; RECOVER TABLE test_retention_phase1 """
        }
        def tableAfterRecoverPhase1 = sql """ SHOW TABLES LIKE "test_retention_phase1" """
        assertTrue(tableAfterRecoverPhase1.size() == 1, "Recovered table should be visible in SHOW TABLES")
        def recycleBinAfterRecoverPhase1 = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = 'test_retention_phase1' """
        assertTrue(recycleBinAfterRecoverPhase1.size() == 0, "Recovered table should be removed from recycle bin")
        order_qt_phase1_recover """ SELECT * FROM test_retention_phase1 """

        // ===== Phase 2: Hidden retention period =====
        // Drop again and wait for FE expire, table should be hidden but still recoverable

        sql """ DROP TABLE test_retention_phase1 """
        def tableAfterDropPhase2 = sql """ SHOW TABLES LIKE "test_retention_phase1" """
        assertTrue(tableAfterDropPhase2.size() == 0, "Dropped table should not be visible before Phase 2 checks")

        // Wait for FE visible period to expire (30s + buffer)
        sleep(35000)

        // SHOW should NOT display the table now (past FE expire)
        def recycleBinPhase2 = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = 'test_retention_phase1' """
        assertTrue(recycleBinPhase2.size() == 0, "Phase 2: Table should NOT be visible in recycle bin after FE expire")

        connect("${recoverNormalUser}", "123456", context.config.jdbcUrl) {
            test {
                sql """ USE ${currentDbName}; RECOVER TABLE test_retention_phase1 """
                exception "ADMIN"
            }
        }

        // But RECOVER should still work (data is still in FE memory, BE hasn't deleted yet)
        sql """ RECOVER TABLE test_retention_phase1 """
        def tableAfterRecoverPhase2 = sql """ SHOW TABLES LIKE "test_retention_phase1" """
        assertTrue(tableAfterRecoverPhase2.size() == 1, "Phase 2 recovered table should be visible in SHOW TABLES")
        order_qt_phase2_recover """ SELECT * FROM test_retention_phase1 """

        // ===== Phase 3: Physical deletion =====
        // Drop again and wait for total expiry (FE + BE), table should be permanently gone
        sql """ ADMIN SET FRONTEND CONFIG ('catalog_trash_ignore_min_erase_latency' = 'true') """

        sql """ DROP TABLE test_retention_phase1 """
        def tableAfterDropPhase3 = sql """ SHOW TABLES LIKE "test_retention_phase1" """
        assertTrue(tableAfterDropPhase3.size() == 0, "Dropped table should not be visible before Phase 3 checks")

        sleep(70000)

        def recycleBinPhase3 = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = 'test_retention_phase1' """
        assertTrue(recycleBinPhase3.size() == 0, "Phase 3: Table should not be visible after physical deletion")

        // After physical deletion, RECOVER should fail
        test {
            sql """ RECOVER TABLE test_retention_phase1 """
            exception "Unknown table"
        }
        sql """ ADMIN SET FRONTEND CONFIG ('catalog_trash_ignore_min_erase_latency' = 'false') """

        // ===== Test: DROP with partition =====
        // Verify the retention feature also works with partition drops

        sql """ DROP TABLE IF EXISTS test_retention_partition """
        sql """
            CREATE TABLE test_retention_partition (
                k1 INT,
                v1 VARCHAR(100)
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            PARTITION BY RANGE(k1) (
                PARTITION test_recycle_p1 VALUES LESS THAN ('100'),
                PARTITION test_recycle_p2 VALUES LESS THAN ('200'),
                PARTITION test_recycle_p3 VALUES LESS THAN ('300')
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ('replication_allocation' = 'tag.location.default: 1')
        """
        sql """ INSERT INTO test_retention_partition VALUES (10, 'part_a'), (110, 'part_b'), (210, 'part_c') """

        order_qt_partition_before """ SELECT * FROM test_retention_partition """

        // Drop partition test_recycle_p2
        sql """ ALTER TABLE test_retention_partition DROP PARTITION test_recycle_p2 """
        def partitionsAfterDropP2 = sql_return_maparray """ SHOW PARTITIONS FROM test_retention_partition """
        assertTrue(partitionsAfterDropP2.find { it.PartitionName == "test_recycle_p2" } == null, "Dropped partition test_recycle_p2 should not be visible in SHOW PARTITIONS")

        // Partition should appear in recycle bin
        def partRecycle = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = 'test_recycle_p2' """
        assertTrue(partRecycle.size() > 0, "Dropped partition test_recycle_p2 should be visible in recycle bin")

        connect("${recoverNormalUser}", "123456", context.config.jdbcUrl) {
            sql """ USE ${currentDbName}; RECOVER PARTITION test_recycle_p2 FROM test_retention_partition """
        }
        def partitionsAfterRecoverP2 = sql_return_maparray """ SHOW PARTITIONS FROM test_retention_partition """
        assertTrue(partitionsAfterRecoverP2.find { it.PartitionName == "test_recycle_p2" } != null, "Recovered partition test_recycle_p2 should be visible in SHOW PARTITIONS")
        order_qt_partition_recover """ SELECT * FROM test_retention_partition """

        // Drop partition again and wait for FE expire
        sql """ ALTER TABLE test_retention_partition DROP PARTITION test_recycle_p2 """
        def partitionsAfterDropP2Phase2 = sql_return_maparray """ SHOW PARTITIONS FROM test_retention_partition """
        assertTrue(partitionsAfterDropP2Phase2.find { it.PartitionName == "test_recycle_p2" } == null, "Dropped partition test_recycle_p2 should not be visible before Phase 2 checks")
        sleep(35000)

        // Partition should be hidden from SHOW
        def partRecycleHidden = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = 'test_recycle_p2' """
        assertTrue(partRecycleHidden.size() == 0, "Partition test_recycle_p2 should be hidden after FE expire")

        connect("${recoverNormalUser}", "123456", context.config.jdbcUrl) {
            test {
                sql """ USE ${currentDbName}; RECOVER PARTITION test_recycle_p2 FROM test_retention_partition """
                exception "ADMIN"
            }
        }
        sql """ RECOVER PARTITION test_recycle_p2 FROM test_retention_partition """
        def partitionsAfterRecoverP2Phase2 = sql_return_maparray """ SHOW PARTITIONS FROM test_retention_partition """
        assertTrue(partitionsAfterRecoverP2Phase2.find { it.PartitionName == "test_recycle_p2" } != null, "Phase 2 recovered partition test_recycle_p2 should be visible in SHOW PARTITIONS")
        order_qt_partition_recover_phase2 """ SELECT * FROM test_retention_partition """

        // ===== Partition Phase 3: Physical deletion =====
        sql """ ADMIN SET FRONTEND CONFIG ('catalog_trash_ignore_min_erase_latency' = 'true') """
        sql """ ALTER TABLE test_retention_partition DROP PARTITION test_recycle_p2 """
        def partitionsAfterDropP2Phase3 = sql_return_maparray """ SHOW PARTITIONS FROM test_retention_partition """
        assertTrue(partitionsAfterDropP2Phase3.find { it.PartitionName == "test_recycle_p2" } == null, "Dropped partition test_recycle_p2 should not be visible before Phase 3 checks")
        sleep(70000)

        def partRecyclePhase3 = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = 'test_recycle_p2' """
        assertTrue(partRecyclePhase3.size() == 0, "Partition Phase 3: test_recycle_p2 should not be visible after physical deletion")

        // After physical deletion, RECOVER should fail
        test {
            sql """ RECOVER PARTITION test_recycle_p2 FROM test_retention_partition """
            exception "No partition named"
        }

        // Verify table still has test_recycle_p1 and test_recycle_p3 data, but test_recycle_p2 is gone
        order_qt_partition_phase3 """ SELECT * FROM test_retention_partition """

        } finally {
            // Restore original FE config
            sql """ ADMIN SET FRONTEND CONFIG ('catalog_trash_expire_second' = '${origExpireValue}') """
            sql """ ADMIN SET FRONTEND CONFIG ('catalog_trash_ignore_min_erase_latency' = '${origIgnoreLatencyValue}') """

            // Cleanup test tables
            sql """ DROP TABLE IF EXISTS test_retention_phase1 """
            sql """ DROP TABLE IF EXISTS test_retention_partition """
            sql """ DROP USER IF EXISTS '${recoverNormalUser}'@'%' """
        }
    }
}

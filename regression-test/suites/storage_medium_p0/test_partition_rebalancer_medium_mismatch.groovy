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

/**
 * Reproduce OPENSOURCE-192:
 * When tablet_rebalancer_type=Partition, adding a new BE with only HDD disks
 * to a cluster where tables are created with storage_medium=SSD causes
 * the PartitionRebalancer to generate invalid moves (SSD tablets -> HDD-only BE),
 * resulting in infinite "paths has no available balance slot: []" errors.
 *
 * Root cause: In LoadStatisticForTag.init(), the beByTotalReplicaCount map
 * for each medium includes ALL available BEs without checking hasMedium().
 * Similarly, TabletInvertedIndex.buildPartitionInfoBySkew() includes all
 * availableBeIds in countMap without medium filtering. This causes the
 * greedy algorithm to generate moves targeting BEs that lack the required
 * storage medium.
 *
 * Setup:
 *   - 3 initial BEs with SSD + HDD disks
 *   - Table created with storage_medium = SSD (explicitly specified)
 *   - Add 1 new BE with HDD only (via addBackend with custom beDisks)
 *   - PartitionRebalancer generates invalid moves to the HDD-only BE
 */
suite('test_partition_rebalancer_medium_mismatch', 'docker') {
    if (isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.feConfigs += [
        'tablet_rebalancer_type=Partition',
        'schedule_slot_num_per_hdd_path=8',
        'balance_slot_num_per_path=2',
        'disable_balance=false',
        'disable_disk_balance=true',
        'tablet_checker_interval_ms=2000',
        'schedule_batch_size=1000',
    ]
    options.beConfigs += [
        'report_disk_state_interval_seconds=2',
        'report_tablet_interval_seconds=3',
    ]
    // Initial 3 BEs: each has 1 SSD + 1 HDD
    options.beDisks = ['SSD=1', 'HDD=1']
    options.beNum = 3

    docker(options) {
        // Step 1: Create table explicitly with SSD medium
        def table = 'tbl_ssd_balance'
        sql "DROP TABLE IF EXISTS ${table} FORCE"
        sql """
            CREATE TABLE ${table} (
                k1 INT,
                k2 VARCHAR(100),
                v1 INT
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 10
            PROPERTIES (
                'replication_num' = '1',
                'storage_medium' = 'SSD'
            )
        """

        // Verify partition medium is SSD
        def partitions = sql_return_maparray "SHOW PARTITIONS FROM ${table}"
        assertTrue(partitions.size() > 0)
        partitions.each {
            assertEquals('SSD', it.StorageMedium)
        }
        log.info("Table created with SSD medium, partitions: ${partitions.size()}")

        // Step 2: Insert data to distribute tablets across existing BEs
        for (int i = 0; i < 100; i++) {
            sql "INSERT INTO ${table} VALUES (${i}, 'value_${i}', ${i * 10})"
        }

        def count = sql "SELECT COUNT(*) FROM ${table}"
        assertEquals(100, count[0][0] as int)

        // Record tablet distribution before expansion
        def tabletsBefore = sql_return_maparray "SHOW TABLETS FROM ${table}"
        log.info("Tablets before expansion: ${tabletsBefore.size()}")
        def beIdsBefore = tabletsBefore.collect { it.BackendId }.unique()
        log.info("Tablets on BEs: ${beIdsBefore}")

        // Let scheduler settle
        sleep(10000)

        // Step 3: Add a new BE with HDD only (different disk config from initial BEs)
        log.info("Adding new BE with HDD-only disks...")
        def newBeIndices = cluster.addBackend(1, ['HDD=1'])
        log.info("New BE added with indices: ${newBeIndices}")

        // Wait for new BE heartbeat and disk report
        sleep(8000)

        // Verify all backends
        def backends = sql_return_maparray "SHOW BACKENDS"
        log.info("Total backends after expansion: ${backends.size()}")
        assertEquals(4, backends.size())

        // Find the new BE
        def newBeId = null
        for (def be : backends) {
            if (!(be.BackendId in beIdsBefore.collect { it as String })) {
                newBeId = be.BackendId
                break
            }
        }
        assertNotNull(newBeId, "Should find new BE")
        log.info("New BE id: ${newBeId}")

        // Verify new BE has only HDD
        def newBeDisks = sql_return_maparray "SHOW PROC '/backends/${newBeId}'"
        log.info("New BE disks: ${newBeDisks}")
        def hasSSD = newBeDisks.any { it.StorageMedium == 'SSD' }
        def hasHDD = newBeDisks.any { it.StorageMedium == 'HDD' }
        assertTrue(hasHDD, "New BE should have HDD disk")
        assertFalse(hasSSD, "New BE should NOT have SSD disk")

        // Step 4: Wait for PartitionRebalancer to attempt balance scheduling
        // The bug: algorithm generates moves targeting the HDD-only BE for SSD tablets
        log.info("Waiting for PartitionRebalancer to run (60s)...")
        sleep(60000)

        // Step 5: Check balance history for the bug signature
        def schedHistory = sql_return_maparray "SHOW PROC '/cluster_balance/history_tablets'"
        def failedWithEmptySlot = schedHistory.findAll {
            it.ErrMsg != null && it.ErrMsg.contains('paths has no available balance slot: []')
        }

        log.info("Total history entries: ${schedHistory.size()}")
        log.info("Entries with 'empty slot' error: ${failedWithEmptySlot.size()}")

        if (failedWithEmptySlot.size() > 0) {
            log.warn("BUG REPRODUCED (OPENSOURCE-192)! " +
                     "Found ${failedWithEmptySlot.size()} balance tasks " +
                     "failed with 'paths has no available balance slot: []'")
            failedWithEmptySlot.take(5).each { task ->
                log.warn("  tablet=${task.TabletId}, dest=${task.DestBe}, err=${task.ErrMsg}")
            }
            // This assertion will fail when the bug is present, and pass after fix
            fail("BUG: PartitionRebalancer generated invalid moves to HDD-only BE for SSD tablets")
        } else {
            log.info("No 'empty slot' failures. Bug not triggered or already fixed.")
        }

        // Step 6: Check that no tablets moved to the new BE
        // (since it has no SSD, SSD tablets should NOT be relocated there)
        def tabletsAfter = sql_return_maparray "SHOW TABLETS FROM ${table}"
        def tabletsOnNewBe = tabletsAfter.findAll { it.BackendId == newBeId }
        log.info("Tablets on new HDD-only BE: ${tabletsOnNewBe.size()}")
        assertEquals(0, tabletsOnNewBe.size())

        // Step 7: Verify data integrity
        def countAfter = sql "SELECT COUNT(*) FROM ${table}"
        assertEquals(100, countAfter[0][0] as int)

        // Cleanup
        sql "DROP TABLE IF EXISTS ${table} FORCE"
    }
}

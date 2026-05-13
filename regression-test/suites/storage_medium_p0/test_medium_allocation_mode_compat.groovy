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

// PR-1 compatibility check: the MediumAllocationMode enum replaces the old
// non-persisted storageMediumSpecified boolean on DataProperty. Verify that
// it does not regress user-visible behaviour across a FE restart:
//  - explicit storage_medium=SSD  -> stays on SSD after restart (STRICT persists)
//  - explicit storage_medium=HDD  -> stays on HDD after restart (STRICT persists)
//  - no storage_medium specified  -> default behaviour (ADAPTIVE) preserved
suite("test_medium_allocation_mode_compat", 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'default_storage_medium=HDD',
    ]
    // mixed cluster: both SSD and HDD are available so STRICT is actually honoured.
    options.beDisks = ['SSD=2', 'HDD=2']

    def collectPartitionMediums = { tbl ->
        def partitions = sql_return_maparray "SHOW PARTITIONS FROM ${tbl};"
        def media = []
        partitions.each { media << it.StorageMedium }
        return media
    }

    def assertAllEqual = { mediums, expected, tbl ->
        log.info("${tbl} storage mediums: ${mediums}")
        assertTrue(!mediums.isEmpty(), "${tbl} must have at least one partition")
        mediums.each {
            assertEquals(expected, it, "${tbl}: expected ${expected}, got ${it}")
        }
    }

    docker(options) {
        def tblExplicitSsd = "medium_mode_explicit_ssd"
        def tblExplicitHdd = "medium_mode_explicit_hdd"
        def tblImplicit = "medium_mode_implicit"

        [tblExplicitSsd, tblExplicitHdd, tblImplicit].each {
            sql "drop table if exists ${it}"
        }

        // (1) explicit storage_medium=SSD -> STRICT
        sql """
            CREATE TABLE ${tblExplicitSsd} (
                k1 BIGINT,
                v1 VARCHAR(64)
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 2
            PROPERTIES (
                "storage_medium" = "SSD",
                "replication_num" = "1"
            );
        """

        // (2) explicit storage_medium=HDD -> STRICT
        sql """
            CREATE TABLE ${tblExplicitHdd} (
                k1 BIGINT,
                v1 VARCHAR(64)
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 2
            PROPERTIES (
                "storage_medium" = "HDD",
                "replication_num" = "1"
            );
        """

        // (3) no storage_medium specified -> ADAPTIVE (falls back to default HDD)
        sql """
            CREATE TABLE ${tblImplicit} (
                k1 BIGINT,
                v1 VARCHAR(64)
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        sleep 1000

        assertAllEqual(collectPartitionMediums(tblExplicitSsd), "SSD", tblExplicitSsd)
        assertAllEqual(collectPartitionMediums(tblExplicitHdd), "HDD", tblExplicitHdd)
        assertAllEqual(collectPartitionMediums(tblImplicit), "HDD", tblImplicit)

        // Restart the FE to exercise the new serialized mediumAllocationMode
        // field. The legacy storageMediumSpecified boolean was never persisted,
        // so this step also covers the bug-fix aspect of PR-1: after a restart
        // the STRICT intent must still be remembered.
        cluster.restartFrontends()
        sleep 5000
        context.reconnectFe()

        assertAllEqual(collectPartitionMediums(tblExplicitSsd), "SSD", tblExplicitSsd)
        assertAllEqual(collectPartitionMediums(tblExplicitHdd), "HDD", tblExplicitHdd)
        assertAllEqual(collectPartitionMediums(tblImplicit), "HDD", tblImplicit)
    }
}

// ADAPTIVE means the configured/default medium is only a hint. On a HDD-only
// cluster with default_storage_medium=SSD, implicit tables should still be
// creatable and should land on HDD, while explicit SSD remains STRICT and fails.
suite("test_medium_allocation_mode_adaptive_fallback", 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'default_storage_medium=SSD',
    ]
    options.beDisks = ['HDD=1']

    def collectPartitionMediums = { tbl ->
        def partitions = sql_return_maparray "SHOW PARTITIONS FROM ${tbl};"
        def media = []
        partitions.each { media << it.StorageMedium }
        return media
    }

    def assertAllEqual = { mediums, expected, tbl ->
        log.info("${tbl} storage mediums: ${mediums}")
        assertTrue(!mediums.isEmpty(), "${tbl} must have at least one partition")
        mediums.each {
            assertEquals(expected, it, "${tbl}: expected ${expected}, got ${it}")
        }
    }

    docker(options) {
        def tblImplicit = "medium_mode_adaptive_fallback"
        def tblStrictSsd = "medium_mode_strict_ssd_no_disk"

        [tblImplicit, tblStrictSsd].each {
            sql "drop table if exists ${it}"
        }

        sql """
            CREATE TABLE ${tblImplicit} (
                k1 BIGINT,
                v1 VARCHAR(64)
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        sleep 1000
        assertAllEqual(collectPartitionMediums(tblImplicit), "HDD", tblImplicit)

        test {
            sql """
                CREATE TABLE ${tblStrictSsd} (
                    k1 BIGINT,
                    v1 VARCHAR(64)
                )
                DUPLICATE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 2
                PROPERTIES (
                    "storage_medium" = "SSD",
                    "replication_num" = "1"
                );
            """
            exception "Failed to find enough backend"
        }
    }
}

// Non-docker coverage for the same PR-1 user-visible semantics. The docker
// suite above owns mixed HDD/SSD and FE restart coverage; this suite is meant
// to run against an already-started regression cluster and only asserts the
// behaviours that can be verified from SQL metadata in that environment.
suite("test_medium_allocation_mode_compat_non_docker", 'p0') {
    if (isCloudMode()) {
        return
    }

    def collectAvailableMediums = { ->
        def mediums = [] as Set
        def backends = sql_return_maparray "SHOW PROC '/backends'"
        backends.each { be ->
            def disks = sql_return_maparray "SHOW PROC '/backends/${be.BackendId}'"
            disks.each { disk ->
                if (disk.State == 'ONLINE' && (disk.StorageMedium == 'SSD' || disk.StorageMedium == 'HDD')) {
                    mediums << disk.StorageMedium
                }
            }
        }
        return mediums
    }

    def collectPartitionMediums = { tbl ->
        def partitions = sql_return_maparray "SHOW PARTITIONS FROM ${tbl};"
        def media = []
        partitions.each { media << it.StorageMedium }
        return media
    }

    def assertAllEqual = { mediums, expected, tbl ->
        log.info("${tbl} storage mediums: ${mediums}")
        assertTrue(!mediums.isEmpty(), "${tbl} must have at least one partition")
        mediums.each {
            assertEquals(expected, it, "${tbl}: expected ${expected}, got ${it}")
        }
    }

    def availableMediums = collectAvailableMediums()
    log.info("available storage mediums in current non-docker cluster: ${availableMediums}")
    assertTrue(!availableMediums.isEmpty(), "current cluster must have at least one HDD or SSD disk")

    // Explicit storage_medium is the user-visible trigger for STRICT. Only test
    // mediums that are physically available in the current non-docker cluster so
    // the case is stable across developers' local environments.
    availableMediums.each { medium ->
        def tbl = "medium_mode_non_docker_${medium.toLowerCase()}"
        sql "drop table if exists ${tbl}"
        sql """
            CREATE TABLE ${tbl} (
                k1 BIGINT,
                v1 VARCHAR(64)
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 2
            PROPERTIES (
                "storage_medium" = "${medium}",
                "replication_num" = "1"
            );
        """
        assertAllEqual(collectPartitionMediums(tbl), medium, tbl)
    }

    // No storage_medium specified keeps the existing CREATE TABLE syntax and
    // maps internally to ADAPTIVE. The realized medium depends on the current
    // cluster, so only assert that it is a real medium available on this cluster.
    def tblImplicit = "medium_mode_non_docker_implicit"
    sql "drop table if exists ${tblImplicit}"
    sql """
        CREATE TABLE ${tblImplicit} (
            k1 BIGINT,
            v1 VARCHAR(64)
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    def implicitMediums = collectPartitionMediums(tblImplicit)
    log.info("${tblImplicit} storage mediums: ${implicitMediums}")
    assertTrue(!implicitMediums.isEmpty(), "${tblImplicit} must have at least one partition")
    implicitMediums.each {
        assertTrue(availableMediums.contains(it), "${tblImplicit}: unexpected medium ${it}")
    }
}

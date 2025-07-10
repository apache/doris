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

package org.apache.doris.catalog;

import org.apache.doris.catalog.DataProperty.MediumAllocationMode;
import org.apache.doris.common.DdlException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class MediumAllocationModeTest extends TestWithFeService {

    /**
     * Build CREATE TABLE SQL with medium allocation mode
     */
    private String buildCreateTableSql(String dbName, String tableName, String storageMedium, String mode) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(dbName).append(".").append(tableName)
           .append(" (pk INT, v1 INT sum) AGGREGATE KEY (pk) ")
           .append("DISTRIBUTED BY HASH(pk) BUCKETS 1 ")
           .append("PROPERTIES ('replication_num' = '1'");

        if (storageMedium != null) {
            sql.append(", 'storage_medium' = '").append(storageMedium).append("'");
        }
        if (mode != null) {
            sql.append(", 'medium_allocation_mode' = '").append(mode).append("'");
        }
        sql.append(");");
        return sql.toString();
    }

    @Override
    protected void runAfterAll() throws Exception {
        Env.getCurrentEnv().clear();
    }

    /**
     * Test Condition 1: Single Medium Environment (HDD/SSD)
     * Scenario: All backends are either SSD or HDD
     * Expected: STRICT mode succeeds on matching medium, fails on non-matching
     *           ADAPTIVE mode succeeds in all cases
     */
    @Test
    public void testSingleMediumEnvironment() throws Exception {
        setupSingleMediumEnvironment(TStorageMedium.SSD);
        createDatabase("test_single_ssd");

        // STRICT + SSD should succeed
        String sql1 = "CREATE TABLE IF NOT EXISTS test_single_ssd.t1 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'storage_medium' = 'ssd', 'medium_allocation_mode' = 'strict');";
        Assertions.assertDoesNotThrow(() -> createTables(sql1));

        // STRICT + HDD should fail
        String sql2 = "CREATE TABLE IF NOT EXISTS test_single_ssd.t2 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'storage_medium' = 'hdd', 'medium_allocation_mode' = 'strict');";
        Assertions.assertThrows(DdlException.class, () -> createTables(sql2));

        // ADAPTIVE + SSD should succeed
        String sql3 = "CREATE TABLE IF NOT EXISTS test_single_ssd.t3 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'storage_medium' = 'ssd', 'medium_allocation_mode' = 'adaptive');";
        Assertions.assertDoesNotThrow(() -> createTables(sql3));

        // ADAPTIVE + HDD should succeed (fallback to SSD)
        String sql4 = "CREATE TABLE IF NOT EXISTS test_single_ssd.t4 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'storage_medium' = 'hdd', 'medium_allocation_mode' = 'adaptive');";
        Assertions.assertDoesNotThrow(() -> createTables(sql4));

        // ADAPTIVE without medium should succeed
        String sql5 = "CREATE TABLE IF NOT EXISTS test_single_ssd.t5 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'medium_allocation_mode' = 'adaptive');";
        Assertions.assertDoesNotThrow(() -> createTables(sql5));

        // Verify table properties
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test_single_ssd");
        OlapTable table1 = (OlapTable) db.getTableOrDdlException("t1");
        OlapTable table3 = (OlapTable) db.getTableOrDdlException("t3");
        OlapTable table4 = (OlapTable) db.getTableOrDdlException("t4");

        Assertions.assertEquals(MediumAllocationMode.STRICT, table1.getTableProperty().getMediumAllocationMode());
        Assertions.assertEquals(TStorageMedium.SSD, table1.getTableProperty().getStorageMedium());
        Assertions.assertEquals(MediumAllocationMode.ADAPTIVE, table3.getTableProperty().getMediumAllocationMode());
        Assertions.assertEquals(MediumAllocationMode.ADAPTIVE, table4.getTableProperty().getMediumAllocationMode());
    }

    /**
     * Test Condition 2: Mixed Medium Environment (HDD and SSD mixed)
     * Scenario: Some backends are SSD, some are HDD
     * Expected: STRICT mode succeeds when specified medium is available
     */
    @Test
    public void testMixedMediumEnvironment() throws Exception {
        setupMixedMediumEnvironment();
        createDatabase("test_mixed_medium");

        // All should succeed in mixed environment
        String[] sqls = {
            buildCreateTableSql("test_mixed_medium", "t1", "ssd", "strict"),
            buildCreateTableSql("test_mixed_medium", "t2", "hdd", "strict"),
            buildCreateTableSql("test_mixed_medium", "t3", "ssd", "adaptive"),
            buildCreateTableSql("test_mixed_medium", "t4", "hdd", "adaptive"),
            buildCreateTableSql("test_mixed_medium", "t5", null, "adaptive")
        };

        for (String sql : sqls) {
            Assertions.assertDoesNotThrow(() -> createTables(sql));
        }

        // Verify table properties
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test_mixed_medium");
        for (int i = 1; i <= 5; i++) {
            OlapTable table = (OlapTable) db.getTableOrDdlException("t" + i);
            Assertions.assertNotNull(table);
        }
    }

    /**
     * Test Condition 3: Single Medium Insufficient Space
     * Scenario: All backends are SSD but with insufficient disk space
     * Expected: Both STRICT and ADAPTIVE policies fail due to insufficient space
     */
    @Test
    public void testSingleMediumInsufficientSpace() throws Exception {
        setupSingleMediumWithInsufficientSpace(TStorageMedium.SSD);
        createDatabase("test_single_insufficient");

        // All should fail due to insufficient space
        String[] sqls = {
            buildCreateTableSql("test_single_insufficient", "t1", "ssd", "strict"),
            buildCreateTableSql("test_single_insufficient", "t2", "ssd", "adaptive"),
            buildCreateTableSql("test_single_insufficient", "t3", null, "adaptive")
        };

        for (String sql : sqls) {
            Assertions.assertThrows(DdlException.class, () -> createTables(sql));
        }
    }

    /**
     * Test Condition 4: Mixed Medium Partial Insufficient Space
     * Scenario: Some backends are SSD, some are HDD, but SSD has insufficient space
     * Expected: STRICT + SSD fails, STRICT + HDD succeeds
     *           ADAPTIVE + SSD falls back to HDD, ADAPTIVE + HDD succeeds
     */
    @Test
    public void testMixedMediumPartialInsufficientSpace() throws Exception {
        setupMixedMediumWithPartialInsufficientSpace();
        createDatabase("test_mixed_partial_insufficient");

        // STRICT + SSD should fail (SSD insufficient space)
        Assertions.assertThrows(DdlException.class,
                () -> createTables(buildCreateTableSql("test_mixed_partial_insufficient",
                        "t1", "ssd", "strict")));

        // Others should succeed
        String[] sqls = {
            buildCreateTableSql("test_mixed_partial_insufficient", "t2", "hdd", "strict"),
            buildCreateTableSql("test_mixed_partial_insufficient", "t3", "ssd", "adaptive"),
            buildCreateTableSql("test_mixed_partial_insufficient", "t4", "hdd", "adaptive"),
            buildCreateTableSql("test_mixed_partial_insufficient", "t5", null, "adaptive")
        };

        for (String sql : sqls) {
            Assertions.assertDoesNotThrow(() -> createTables(sql));
        }

        // Verify table properties
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test_mixed_partial_insufficient");
        OlapTable table2 = (OlapTable) db.getTableOrDdlException("t2");
        Assertions.assertEquals(MediumAllocationMode.STRICT, table2.getTableProperty().getMediumAllocationMode());
        Assertions.assertEquals(TStorageMedium.HDD, table2.getTableProperty().getStorageMedium());

        OlapTable table3 = (OlapTable) db.getTableOrDdlException("t3");
        Assertions.assertEquals(MediumAllocationMode.ADAPTIVE, table3.getTableProperty().getMediumAllocationMode());
        Assertions.assertEquals(TStorageMedium.SSD, table3.getTableProperty().getStorageMedium());

        OlapTable table4 = (OlapTable) db.getTableOrDdlException("t4");
        Assertions.assertEquals(MediumAllocationMode.ADAPTIVE, table4.getTableProperty().getMediumAllocationMode());
        Assertions.assertEquals(TStorageMedium.HDD, table4.getTableProperty().getStorageMedium());
    }

    /**
     * Test Condition 5: Mixed Medium All Insufficient Space
     * Scenario: Some backends are SSD, some are HDD, but all have insufficient space
     * Expected: All policies fail
     */
    @Test
    public void testMixedMediumAllInsufficientSpace() throws Exception {
        setupMixedMediumWithAllInsufficientSpace();
        createDatabase("test_mixed_all_insufficient");

        // All should fail due to insufficient space
        String[] sqls = {
            buildCreateTableSql("test_mixed_all_insufficient", "t1", "ssd", "strict"),
            buildCreateTableSql("test_mixed_all_insufficient", "t2", "hdd", "strict"),
            buildCreateTableSql("test_mixed_all_insufficient", "t3", "ssd", "adaptive"),
            buildCreateTableSql("test_mixed_all_insufficient", "t4", "hdd", "adaptive"),
            buildCreateTableSql("test_mixed_all_insufficient", "t5", null, "adaptive")
        };

        for (String sql : sqls) {
            Assertions.assertThrows(DdlException.class, () -> createTables(sql));
        }
    }

    /**
     * Setup single medium environment
     */
    private void setupSingleMediumEnvironment(TStorageMedium medium) {
        SystemInfoService clusterInfo = Env.getCurrentEnv().getClusterInfo();
        try {
            List<Backend> allBackends = clusterInfo.getAllBackendsByAllCluster().values().asList();
            for (Backend backend : allBackends) {
                if (backend.hasPathHash()) {
                    Map<String, TDisk> diskMap = Maps.newHashMap();
                    for (DiskInfo diskInfo : backend.getDisks().values()) {
                        TDisk tDisk = new TDisk();
                        tDisk.setRootPath(diskInfo.getRootPath());
                        tDisk.setStorageMedium(medium);
                        tDisk.setDiskTotalCapacity(diskInfo.getTotalCapacityB());
                        tDisk.setDiskAvailableCapacity(diskInfo.getAvailableCapacityB());
                        tDisk.setDataUsedCapacity(diskInfo.getDataUsedCapacityB());
                        tDisk.setTrashUsedCapacity(diskInfo.getTrashUsedCapacityB());
                        tDisk.setUsed(true);
                        tDisk.setPathHash(diskInfo.getPathHash());
                        diskMap.put(diskInfo.getRootPath(), tDisk);
                    }
                    backend.updateDisks(diskMap);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Setup mixed medium environment
     */
    private void setupMixedMediumEnvironment() {
        SystemInfoService clusterInfo = Env.getCurrentEnv().getClusterInfo();
        try {
            List<Backend> allBackends = clusterInfo.getAllBackendsByAllCluster().values().asList();

            for (Backend backend : allBackends) {
                if (backend.hasPathHash()) {
                    // create two diskpaths, one SSD, one HDD
                    Map<String, DiskInfo> disks = Maps.newHashMap();

                    // create SSD disk
                    DiskInfo ssdDisk = new DiskInfo("/path_ssd_" + backend.getId());
                    ssdDisk.setPathHash(backend.getId() * 100 + 1);
                    ssdDisk.setStorageMedium(TStorageMedium.SSD);
                    ssdDisk.setTotalCapacityB(10L << 40);
                    ssdDisk.setAvailableCapacityB(5L << 40);
                    ssdDisk.setDataUsedCapacityB(480000);
                    disks.put(ssdDisk.getRootPath(), ssdDisk);

                    // create HDD disk
                    DiskInfo hddDisk = new DiskInfo("/path_hdd_" + backend.getId());
                    hddDisk.setPathHash(backend.getId() * 100 + 2);
                    hddDisk.setStorageMedium(TStorageMedium.HDD);
                    hddDisk.setTotalCapacityB(10L << 40);
                    hddDisk.setAvailableCapacityB(5L << 40);
                    hddDisk.setDataUsedCapacityB(480000);
                    disks.put(hddDisk.getRootPath(), hddDisk);

                    // update backend's disk info
                    backend.setDisks(ImmutableMap.copyOf(disks));

                    // update SystemInfoService's disk info
                    Map<String, TDisk> diskMap = Maps.newHashMap();
                    for (DiskInfo diskInfo : disks.values()) {
                        TDisk tDisk = new TDisk();
                        tDisk.setRootPath(diskInfo.getRootPath());
                        tDisk.setPathHash(diskInfo.getPathHash());
                        tDisk.setStorageMedium(diskInfo.getStorageMedium());
                        tDisk.setDiskTotalCapacity(diskInfo.getTotalCapacityB());
                        tDisk.setDiskAvailableCapacity(diskInfo.getAvailableCapacityB());
                        tDisk.setDataUsedCapacity(diskInfo.getDataUsedCapacityB());
                        tDisk.setTrashUsedCapacity(diskInfo.getTrashUsedCapacityB());
                        tDisk.setUsed(true);
                        diskMap.put(diskInfo.getRootPath(), tDisk);
                    }
                    backend.updateDisks(diskMap);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Setup single medium environment with insufficient space
     */
    private void setupSingleMediumWithInsufficientSpace(TStorageMedium medium) {
        SystemInfoService clusterInfo = Env.getCurrentEnv().getClusterInfo();
        try {
            List<Backend> allBackends = clusterInfo.getAllBackendsByAllCluster().values().asList();

            for (Backend backend : allBackends) {
                if (backend.hasPathHash()) {
                    Map<String, TDisk> diskMap = Maps.newHashMap();
                    for (DiskInfo diskInfo : backend.getDisks().values()) {
                        TDisk tDisk = new TDisk();
                        tDisk.setRootPath(diskInfo.getRootPath());
                        tDisk.setStorageMedium(medium);
                        // set disk space to a very small value, simulate insufficient space
                        tDisk.setDiskTotalCapacity(1024L * 1024L); // 1MB
                        tDisk.setDiskAvailableCapacity(1024L); // 1KB
                        tDisk.setDataUsedCapacity(diskInfo.getDataUsedCapacityB());
                        tDisk.setTrashUsedCapacity(diskInfo.getTrashUsedCapacityB());
                        tDisk.setUsed(true);
                        tDisk.setPathHash(diskInfo.getPathHash());
                        diskMap.put(diskInfo.getRootPath(), tDisk);
                    }
                    backend.updateDisks(diskMap);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Setup mixed medium environment with partial insufficient space (SSD insufficient, HDD sufficient)
     */
    private void setupMixedMediumWithPartialInsufficientSpace() {
        SystemInfoService clusterInfo = Env.getCurrentEnv().getClusterInfo();
        try {
            List<Backend> allBackends = clusterInfo.getAllBackendsByAllCluster().values().asList();

            for (Backend backend : allBackends) {
                if (backend.hasPathHash()) {
                    // create two diskPaths, one SSD (insufficient space), one HDD (sufficient space)
                    Map<String, DiskInfo> disks = Maps.newHashMap();

                    // create SSD disk (insufficient space)
                    DiskInfo ssdDisk = new DiskInfo("/path_ssd_" + backend.getId());
                    ssdDisk.setPathHash(backend.getId() * 100 + 1);
                    ssdDisk.setStorageMedium(TStorageMedium.SSD);
                    ssdDisk.setTotalCapacityB(1024L * 1024L); // 1MB
                    ssdDisk.setAvailableCapacityB(1024L); // 1KB
                    ssdDisk.setDataUsedCapacityB(480000);
                    disks.put(ssdDisk.getRootPath(), ssdDisk);

                    // create HDD disk (sufficient space)
                    DiskInfo hddDisk = new DiskInfo("/path_hdd_" + backend.getId());
                    hddDisk.setPathHash(backend.getId() * 100 + 2);
                    hddDisk.setStorageMedium(TStorageMedium.HDD);
                    hddDisk.setTotalCapacityB(1024L * 1024L * 1024L * 100L); // 100GB
                    hddDisk.setAvailableCapacityB(1024L * 1024L * 1024L * 10L); // 10GB
                    hddDisk.setDataUsedCapacityB(480000);
                    disks.put(hddDisk.getRootPath(), hddDisk);

                    // update backend's disk info
                    backend.setDisks(ImmutableMap.copyOf(disks));

                    // update SystemInfoService's disk info
                    Map<String, TDisk> diskMap = Maps.newHashMap();
                    for (DiskInfo diskInfo : disks.values()) {
                        TDisk tDisk = new TDisk();
                        tDisk.setRootPath(diskInfo.getRootPath());
                        tDisk.setPathHash(diskInfo.getPathHash());
                        tDisk.setStorageMedium(diskInfo.getStorageMedium());
                        tDisk.setDiskTotalCapacity(diskInfo.getTotalCapacityB());
                        tDisk.setDiskAvailableCapacity(diskInfo.getAvailableCapacityB());
                        tDisk.setDataUsedCapacity(diskInfo.getDataUsedCapacityB());
                        tDisk.setTrashUsedCapacity(diskInfo.getTrashUsedCapacityB());
                        tDisk.setUsed(true);
                        diskMap.put(diskInfo.getRootPath(), tDisk);
                    }
                    backend.updateDisks(diskMap);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Setup mixed medium environment with all insufficient space
     */
    private void setupMixedMediumWithAllInsufficientSpace() {
        SystemInfoService clusterInfo = Env.getCurrentEnv().getClusterInfo();
        try {
            List<Backend> allBackends = clusterInfo.getAllBackendsByAllCluster().values().asList();

            for (Backend backend : allBackends) {
                if (backend.hasPathHash()) {
                    // create two diskpaths, both SSD and HDD are insufficient space
                    Map<String, DiskInfo> disks = Maps.newHashMap();

                    // create SSD disk (insufficient space)
                    DiskInfo ssdDisk = new DiskInfo("/path_ssd_" + backend.getId());
                    ssdDisk.setPathHash(backend.getId() * 100 + 1);
                    ssdDisk.setStorageMedium(TStorageMedium.SSD);
                    ssdDisk.setTotalCapacityB(1024L * 1024L); // 1MB
                    ssdDisk.setAvailableCapacityB(1024L); // 1KB
                    ssdDisk.setDataUsedCapacityB(480000);
                    disks.put(ssdDisk.getRootPath(), ssdDisk);

                    // create HDD disk (insufficient space)
                    DiskInfo hddDisk = new DiskInfo("/path_hdd_" + backend.getId());
                    hddDisk.setPathHash(backend.getId() * 100 + 2);
                    hddDisk.setStorageMedium(TStorageMedium.HDD);
                    hddDisk.setTotalCapacityB(1024L * 1024L); // 1MB
                    hddDisk.setAvailableCapacityB(1024L); // 1KB
                    hddDisk.setDataUsedCapacityB(480000);
                    disks.put(hddDisk.getRootPath(), hddDisk);

                    // update backend's disk info
                    backend.setDisks(ImmutableMap.copyOf(disks));

                    // update SystemInfoService's disk info
                    Map<String, TDisk> diskMap = Maps.newHashMap();
                    for (DiskInfo diskInfo : disks.values()) {
                        TDisk tDisk = new TDisk();
                        tDisk.setRootPath(diskInfo.getRootPath());
                        tDisk.setPathHash(diskInfo.getPathHash());
                        tDisk.setStorageMedium(diskInfo.getStorageMedium());
                        tDisk.setDiskTotalCapacity(diskInfo.getTotalCapacityB());
                        tDisk.setDiskAvailableCapacity(diskInfo.getAvailableCapacityB());
                        tDisk.setDataUsedCapacity(diskInfo.getDataUsedCapacityB());
                        tDisk.setTrashUsedCapacity(diskInfo.getTrashUsedCapacityB());
                        tDisk.setUsed(true);
                        diskMap.put(diskInfo.getRootPath(), tDisk);
                    }
                    backend.updateDisks(diskMap);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

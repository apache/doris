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

package org.apache.doris.qe;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class PointQueryPartitionRoutingTest extends TestWithFeService {
    private boolean originalRunningUnitTest;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("partition_routing");
        useDatabase("partition_routing");
    }

    @BeforeEach
    void enablePointQuery() {
        originalRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
    }

    @AfterEach
    void restoreUnitTestFlag() {
        FeConstants.runningUnitTest = originalRunningUnitTest;
    }

    private OlapTable createRangeTable(String name) throws Exception {
        return createPartitionedTable(name, "RANGE(d) (PARTITION p1 VALUES [('0'),('10')))");
    }

    private OlapTable createPartitionedTable(String name, String partitions) throws Exception {
        createTable("CREATE TABLE " + name + " (id INT NOT NULL, d INT NOT NULL, v VARCHAR(32)) "
                + "UNIQUE KEY(id,d) PARTITION BY " + partitions + " "
                + "DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ('replication_num'='1', "
                + "'enable_unique_key_merge_on_write'='true', 'store_row_column'='true')");
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrMetaException("partition_routing").getTableOrMetaException(name);
        markNonEmpty(table);
        return table;
    }

    private void markNonEmpty(OlapTable table) {
        for (Partition partition : table.getAllPartitions()) {
            partition.updateVisibleVersion(2L);
            partition.getBaseIndex().getTablets().forEach(tablet ->
                    tablet.getReplicas().forEach(replica -> replica.updateVersion(2L)));
        }
    }

    private ShortCircuitQueryContext prepareContext(String tableName) throws Exception {
        Planner planner = getSQLPlanner("EXPLAIN SELECT * FROM " + tableName + " WHERE id=1 AND d=1");
        Assertions.assertTrue(connectContext.getStatementContext().isShortCircuitQuery());
        OlapScanNode scan = (OlapScanNode) planner.getScanNodes().get(0);
        scan.lazyEvaluateRangeLocations();
        Assertions.assertEquals(1, scan.getScanTabletIds().size());
        return new ShortCircuitQueryContext(planner, Mockito.mock(Queriable.class));
    }

    @Test
    void testAddedPartitionAfterReuseValidation() throws Exception {
        OlapTable table = createRangeTable("range_add");
        ShortCircuitQueryContext context = prepareContext("range_add");
        OlapScanNode scan = context.scanNode;
        // ExecuteCommand has validated reuse; ADD can commit before lazy routing starts.
        Assertions.assertTrue(context.isReusable(connectContext));
        StatementContext statement = connectContext.getStatementContext();
        executeSql("ALTER TABLE range_add ADD PARTITION p2 VALUES [('10'),('20'))");
        markNonEmpty(table);
        connectContext.setStatementContext(statement);
        for (Expr conjunct : scan.getConjuncts()) {
            SlotRef slot = conjunct.getChild(0).unwrapSlotRef();
            if (slot.getColumnName().equals("d")) {
                conjunct.setChild(1, new IntLiteral(11));
            }
        }
        new PointQueryExecutor(context, 1024).setScanRangeLocations();
        Assertions.assertEquals(1, scan.getScanTabletIds().size());
        Assertions.assertTrue(scan.getSelectedPartitionIds().contains(table.getPartition("p2").getId()));
    }

    @Test
    void testTruncatedPartitionAfterReuseValidation() throws Exception {
        OlapTable table = createRangeTable("range_truncate");
        ShortCircuitQueryContext context = prepareContext("range_truncate");
        OlapScanNode scan = context.scanNode;
        Assertions.assertTrue(context.isReusable(connectContext));
        StatementContext statement = connectContext.getStatementContext();
        long oldPartitionId = table.getPartition("p1").getId();
        executeSql("TRUNCATE TABLE range_truncate PARTITION(p1)");
        markNonEmpty(table);
        connectContext.setStatementContext(statement);
        Assertions.assertNotEquals(oldPartitionId, table.getPartition("p1").getId());
        new PointQueryExecutor(context, 1024).setScanRangeLocations();
        Assertions.assertEquals(1, scan.getScanTabletIds().size());
        Assertions.assertTrue(scan.getSelectedPartitionIds().contains(table.getPartition("p1").getId()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"RANGE(d) (PARTITION p1 VALUES [('0'),('10')))",
            "LIST(d) (PARTITION p1 VALUES IN ('1'))"})
    void testDropCannotInterleaveBetweenPartitionAndTabletPruning(String partitions) throws Exception {
        String name = partitions.startsWith("RANGE") ? "range_lock" : "list_lock";
        OlapTable table = createPartitionedTable(name, partitions);
        ShortCircuitQueryContext original = prepareContext(name);
        OlapScanNode scan = Mockito.spy(original.scanNode);
        ShortCircuitQueryContext context = withScan(original, scan);
        ExecutorService writer = Executors.newSingleThreadExecutor();
        try {
            Mockito.doAnswer(invocation -> {
                invocation.callRealMethod();
                // Force DROP into the interval after partition pruning, before tablet lookup.
                writer.submit(() -> {
                    if (table.tryWriteLock(0, TimeUnit.SECONDS)) {
                        try {
                            table.dropPartitionAndReserveTablet("p1");
                        } finally {
                            table.writeUnlock();
                        }
                    }
                }).get(10, TimeUnit.SECONDS);
                return null;
            }).when(scan).computePartitionInfo();
            new PointQueryExecutor(context, 1024).setScanRangeLocations();
            Assertions.assertEquals(1, scan.getScanTabletIds().size());
            Assertions.assertTrue(writer.submit(() -> {
                boolean locked = table.tryWriteLock(0, TimeUnit.SECONDS);
                if (locked) {
                    table.writeUnlock();
                }
                return locked;
            }).get(10, TimeUnit.SECONDS), "routing must release the lock before backend RPCs");
        } finally {
            writer.shutdownNow();
        }
    }

    private ShortCircuitQueryContext withScan(ShortCircuitQueryContext original, OlapScanNode scan) throws Exception {
        Planner planner = Mockito.mock(Planner.class);
        Mockito.when(planner.getDescTable()).thenReturn(original.planner.getDescTable());
        Mockito.when(planner.getScanNodes()).thenReturn(List.of(scan));
        Mockito.when(planner.getFragments()).thenReturn(original.planner.getFragments());
        return new ShortCircuitQueryContext(planner, Mockito.mock(Queriable.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCloudVersionRpcRunsOutsideTableLock(boolean cloudSnapshotVersion) throws Exception {
        String name = "range_cloud_snapshot_" + cloudSnapshotVersion;
        OlapTable table = createRangeTable(name);
        ShortCircuitQueryContext original = prepareContext(name);
        OlapScanNode scan = Mockito.spy(original.scanNode);
        ShortCircuitQueryContext context = withScan(original, scan);
        Partition previous = table.getPartition("p1");
        CloudPartition partition = Mockito.spy(new CloudPartition(previous.getId(), "p1",
                previous.getBaseIndex(), previous.getDistributionInfo(), 1L, table.getId()));
        Mockito.doThrow(new AssertionError("routing must not fetch a cloud visible version under the lock"))
                .when(partition).getVisibleVersion();
        table.addPartition(partition);
        ConnectContext routingContext = Mockito.spy(connectContext);
        Mockito.doReturn(connectContext.getComputeGroupSafely()).when(routingContext).getComputeGroupSafely();
        routingContext.setThreadLocalInfo();
        boolean originalCloudSnapshotVersion = Config.enable_cloud_snapshot_version;
        Config.enable_cloud_snapshot_version = cloudSnapshotVersion;
        boolean originalSnapshotPointQuery = connectContext.getSessionVariable().enableSnapshotPointQuery;
        connectContext.getSessionVariable().enableSnapshotPointQuery = true;
        ExecutorService writer = Executors.newSingleThreadExecutor();
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<CloudPartition> versions = Mockito.mockStatic(CloudPartition.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            versions.when(() -> CloudPartition.selectNonEmptyPartitionIds(Mockito.anyList())).thenCallRealMethod();
            versions.when(() -> CloudPartition.getSnapshotVisibleVersion(List.of(partition))).thenAnswer(invocation -> {
                Assertions.assertTrue(writer.submit(() -> {
                    boolean locked = table.tryWriteLock(0, TimeUnit.SECONDS);
                    if (locked) {
                        table.writeUnlock();
                    }
                    return locked;
                }).get(10, TimeUnit.SECONDS), "the MS version RPC must not hold the table lock");
                return List.of(2L);
            });
            versions.clearInvocations();
            new PointQueryExecutor(context, 1024).setScanRangeLocations();
            versions.verify(() -> CloudPartition.getSnapshotVisibleVersion(List.of(partition)));
        } finally {
            connectContext.setThreadLocalInfo();
            Config.enable_cloud_snapshot_version = originalCloudSnapshotVersion;
            connectContext.getSessionVariable().enableSnapshotPointQuery = originalSnapshotPointQuery;
            writer.shutdownNow();
        }
    }

    @Test
    void testRoutingReleasesLockOnEmptyResult() throws Exception {
        checkRoutingReleasesLock(false);
    }

    @Test
    void testRoutingReleasesLockOnFailure() throws Exception {
        checkRoutingReleasesLock(true);
    }

    private void checkRoutingReleasesLock(boolean fail) throws Exception {
        String name = fail ? "range_route_failure" : "range_route_empty";
        OlapTable table = createRangeTable(name);
        ShortCircuitQueryContext original = prepareContext(name);
        OlapScanNode scan = Mockito.spy(original.scanNode);
        if (fail) {
            Mockito.doThrow(new UserException("routing failed")).when(scan).lazyEvaluateRangeLocations();
        } else {
            Mockito.doReturn(List.of()).when(scan).lazyEvaluateRangeLocations();
            Mockito.doReturn(new ArrayList<Long>()).when(scan).getScanTabletIds();
        }
        PointQueryExecutor executor = new PointQueryExecutor(withScan(original, scan), 1024);
        if (fail) {
            Assertions.assertThrows(UserException.class, executor::setScanRangeLocations);
        } else {
            executor.setScanRangeLocations();
        }
        boolean locked = table.tryWriteLock(0, TimeUnit.SECONDS);
        try {
            Assertions.assertTrue(locked, "empty and failed routes must release the read lock");
        } finally {
            if (locked) {
                table.writeUnlock();
            }
        }
    }

}

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

package org.apache.doris.planner;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TOlapScanNode;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TPartitionBoundary;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OlapScanNodeTest {
    private MaterializedIndex createMaterializedIndex(List<Long> tabletIds) {
        MaterializedIndex index = new MaterializedIndex();
        List<Tablet> tablets = Lists.newArrayListWithExpectedSize(tabletIds.size());
        for (Long tabletId : tabletIds) {
            tablets.add(new LocalTablet(tabletId));
        }
        index.appendTablets(tablets);
        return index;
    }

    // columnA in (1) hashmode=3
    @Test
    public void testHashDistributionOneUser() throws AnalysisException {

        List<Long> tabletIds = Lists.newArrayList(0L, 1L, 2L);


        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("columnA", PrimitiveType.BIGINT));

        List<Expr> inList = Lists.newArrayList();
        inList.add(new IntLiteral(1));

        Expr compareExpr = new SlotRef(new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, "db", "tableName"),
                "columnA");
        InPredicate inPredicate = new InPredicate(compareExpr, inList, false);

        PartitionColumnFilter  columnFilter = new PartitionColumnFilter();
        columnFilter.setInPredicate(inPredicate);
        Map<String, PartitionColumnFilter> filterMap = new CaseInsensitiveMap();
        filterMap.put("COLUMNA", columnFilter);

        DistributionPruner partitionPruner  = new HashDistributionPruner(
                null,
                createMaterializedIndex(tabletIds),
                columns,
                filterMap,
                3,
                true);

        Collection<Long> ids = partitionPruner.prune();
        Assert.assertEquals(ids.size(), 1);

        for (Long id : ids) {
            Assert.assertEquals((1 & 0xffffffff) % 3, id.intValue());
        }
    }

    // columnA in (1, 2 ,3, 4, 5, 6) hashmode=3
    @Test
    public void testHashPartitionManyUser() throws AnalysisException {

        List<Long> tabletIds = Lists.newArrayList(0L, 1L, 2L);

        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("columnA", PrimitiveType.BIGINT));

        List<Expr> inList = Lists.newArrayList();
        inList.add(new IntLiteral(1));
        inList.add(new IntLiteral(2));
        inList.add(new IntLiteral(3));
        inList.add(new IntLiteral(4));
        inList.add(new IntLiteral(5));
        inList.add(new IntLiteral(6));

        Expr compareExpr = new SlotRef(new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, "db", "tableName"),
                "columnA");
        InPredicate inPredicate = new InPredicate(compareExpr, inList, false);

        PartitionColumnFilter  columnFilter = new PartitionColumnFilter();
        columnFilter.setInPredicate(inPredicate);
        Map<String, PartitionColumnFilter> filterMap = Maps.newHashMap();
        filterMap.put("columnA", columnFilter);

        DistributionPruner partitionPruner  = new HashDistributionPruner(
                null,
                createMaterializedIndex(tabletIds),
                columns,
                filterMap,
                3,
                true);

        Collection<Long> ids = partitionPruner.prune();
        Assert.assertEquals(ids.size(), 3);
    }

    @Test
    public void testHashForIntLiteral() {
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(1), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 1);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(2), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 0);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(3), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 0);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(4), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 1);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(5), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 2);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(6), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 2);
        } // CHECKSTYLE IGNORE THIS LINE
    }

    @Test
    public void testHasPartitionPredicateWithEquality() {
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        SlotDescriptor partitionSlot = addSlot(tupleDescriptor, 1, "p1");
        addSlot(tupleDescriptor, 2, "c1");

        List<Expr> conjuncts = Lists.newArrayList(new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(partitionSlot), new IntLiteral(1)));

        Assert.assertTrue(ScanNode.containsPartitionPredicate(
                Lists.newArrayList(partitionSlot.getColumn()), tupleDescriptor, conjuncts, null));
    }

    @Test
    public void testHasPartitionPredicateWithInPredicate() {
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        SlotDescriptor partitionSlot = addSlot(tupleDescriptor, 1, "p1");
        addSlot(tupleDescriptor, 2, "c1");

        List<Expr> inList = Lists.newArrayList(new IntLiteral(1), new IntLiteral(2));
        List<Expr> conjuncts = Lists.newArrayList(new InPredicate(new SlotRef(partitionSlot), inList, false));

        Assert.assertTrue(ScanNode.containsPartitionPredicate(
                Lists.newArrayList(partitionSlot.getColumn()), tupleDescriptor, conjuncts, null));
    }

    @Test
    public void testHasPartitionPredicateIgnoresNonPartitionColumn() {
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        SlotDescriptor partitionSlot = addSlot(tupleDescriptor, 1, "p1");
        SlotDescriptor nonPartitionSlot = addSlot(tupleDescriptor, 2, "c1");

        List<Expr> conjuncts = Lists.newArrayList(new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(nonPartitionSlot), new IntLiteral(1)));

        Assert.assertFalse(ScanNode.containsPartitionPredicate(
                Lists.newArrayList(partitionSlot.getColumn()), tupleDescriptor, conjuncts, null));
    }

    @Test
    public void testRuntimeFilterPartitionBoundariesUsePlanningSnapshot() throws AnalysisException {
        long oldTargetPartitionId = 1L;
        long afterPartitionId = 2L;
        long replacementPartitionId = 3L;
        Column partitionColumn = new Column("event_date", PrimitiveType.INT);
        RangePartitionInfo partitionInfo = new RangePartitionInfo(Lists.newArrayList(partitionColumn));
        setRangePartitionItem(partitionInfo, oldTargetPartitionId, "20260721", "20260722");
        setRangePartitionItem(partitionInfo, afterPartitionId, "20260722", "20260723");

        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("rfpp_queue_range_fact");
        Mockito.when(table.getDistributionColumnNames()).thenReturn(Collections.emptySet());
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);

        Map<Long, Partition> livePartitions = new HashMap<>();
        livePartitions.put(oldTargetPartitionId, mockPartition("p_target"));
        livePartitions.put(afterPartitionId, mockPartition("p_after"));
        Mockito.when(table.getPartition(Mockito.anyLong()))
                .thenAnswer(invocation -> livePartitions.get(invocation.getArgument(0)));
        Mockito.when(table.getPartitions()).thenAnswer(invocation -> livePartitions.values());

        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        tupleDescriptor.setTable(table);
        SlotDescriptor partitionSlot = new SlotDescriptor(new SlotId(1), tupleDescriptor.getId());
        partitionSlot.setColumn(partitionColumn);
        partitionSlot.setType(partitionColumn.getType());
        tupleDescriptor.addSlot(partitionSlot);

        OlapScanNode scanNode = new OlapScanNode(
                new PlanNodeId(1), tupleDescriptor, "rfppScanNode", ScanContext.EMPTY);
        scanNode.setSelectedPartitionIds(Lists.newArrayList(oldTargetPartitionId, afterPartitionId));
        scanNode.snapshotSelectedPartitionNames();
        scanNode.snapshotPartitionBoundariesForRuntimeFilter();

        // Simulate REPLACE PARTITION after planning but before Thrift serialization.
        partitionInfo.dropPartition(oldTargetPartitionId);
        setRangePartitionItem(partitionInfo, replacementPartitionId, "20260721", "20260722");
        livePartitions.remove(oldTargetPartitionId);
        livePartitions.put(replacementPartitionId, mockPartition("p_target"));

        scanNode.snapshotPartitionBoundariesForRuntimeFilter();
        TOlapScanNode thriftScanNode = new TOlapScanNode();
        scanNode.setPartitionBoundariesForRuntimeFilter(thriftScanNode);
        List<Long> serializedPartitionIds = thriftScanNode.getPartitionBoundaries().stream()
                .map(TPartitionBoundary::getPartitionId)
                .collect(Collectors.toList());

        Assert.assertEquals(Lists.newArrayList(oldTargetPartitionId, afterPartitionId), serializedPartitionIds);

        Assert.assertEquals("p_target,p_after", scanNode.getSelectedPartitionNamesForExplain());
    }

    @Test
    public void testIncrementalReadGetsVisibleVersionFromMetaService() throws Exception {
        long partitionId = 300L;
        long visibleVersion = 10L;
        CloudPartition partition = Mockito.mock(CloudPartition.class);
        Mockito.when(partition.getId()).thenReturn(partitionId);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getPartition(partitionId)).thenReturn(partition);

        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getSelectedPartitionIds()).thenReturn(Lists.newArrayList(partitionId));
        Mockito.when(scanNode.getScanParams()).thenReturn(new TableScanParams(
                TableScanParams.INCREMENTAL_READ, Collections.emptyMap(), Collections.emptyList()));

        try (MockedStatic<Config> mockedConfig = Mockito.mockStatic(Config.class);
                MockedStatic<CloudPartition> mockedPartition = Mockito.mockStatic(CloudPartition.class)) {
            mockedConfig.when(Config::isNotCloudMode).thenReturn(false);
            mockedPartition.when(() -> CloudPartition.getSnapshotVisibleVersionFromMs(
                    Mockito.anyList(), Mockito.eq(false))).thenReturn(Lists.newArrayList(visibleVersion));

            ScanNode.setVisibleVersionForOlapScanNodes(Lists.newArrayList(scanNode));

            mockedPartition.verify(() -> CloudPartition.getSnapshotVisibleVersionFromMs(
                    Mockito.anyList(), Mockito.eq(false)));
            mockedPartition.verify(() -> CloudPartition.getSnapshotVisibleVersion(Mockito.anyList()),
                    Mockito.never());
        }

        Mockito.verify(scanNode).updateScanRangeVersions(Collections.singletonMap(partitionId, visibleVersion));
    }

    @Test
    public void testRuntimeFilterBucketMetadataAttachedOnceAcrossWorkers() throws Exception {
        OlapScanNode scanNode = newBucketPruneScanNode(10L);
        TPaloScanRange paloScanRange = scanNode.scanRangeLocations.get(0)
                .getScanRange().getPaloScanRange();
        Map<Long, Long> bucketInfo = getBucketInfo(scanNode);
        bucketInfo.put(10L, ((long) 4 << Integer.SIZE) | 2L);

        scanNode.setRuntimeFilterBucketPruneParameters();
        bucketInfo.clear();
        scanNode.setRuntimeFilterBucketPruneParameters();

        Assert.assertEquals(2, paloScanRange.getBucketSeq());
        Assert.assertEquals(4, paloScanRange.getBucketNum());
    }

    @Test
    public void testMissingRuntimeFilterBucketMetadataDisablesScanPruning() throws Exception {
        OlapScanNode scanNode = newBucketPruneScanNode(10L, 11L);
        TPaloScanRange firstScanRange = scanNode.scanRangeLocations.get(0)
                .getScanRange().getPaloScanRange();
        TPaloScanRange secondScanRange = scanNode.scanRangeLocations.get(1)
                .getScanRange().getPaloScanRange();
        Map<Long, Long> bucketInfo = getBucketInfo(scanNode);
        bucketInfo.put(10L, ((long) 4 << Integer.SIZE) | 2L);
        bucketInfo.put(11L, ((long) 4 << Integer.SIZE) | 3L);

        boolean previousEnableDebugPoints = Config.enable_debug_points;
        try {
            Config.enable_debug_points = true;
            DebugPointUtil.addDebugPointWithValue(
                    OlapScanNode.MISSING_RF_BUCKET_METADATA_DEBUG_POINT, 11L);

            scanNode.setRuntimeFilterBucketPruneParameters();

            Assert.assertFalse(firstScanRange.isSetBucketSeq());
            Assert.assertFalse(firstScanRange.isSetBucketNum());
            Assert.assertFalse(secondScanRange.isSetBucketSeq());
            Assert.assertFalse(secondScanRange.isSetBucketNum());
        } finally {
            DebugPointUtil.removeDebugPoint(OlapScanNode.MISSING_RF_BUCKET_METADATA_DEBUG_POINT);
            Config.enable_debug_points = previousEnableDebugPoints;
        }
    }

    private OlapScanNode newBucketPruneScanNode(long... tabletIds) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("rf_bucket_fact");
        Mockito.when(table.getDistributionColumnNames()).thenReturn(Collections.emptySet());

        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        tupleDescriptor.setTable(table);
        OlapScanNode scanNode = new OlapScanNode(
                new PlanNodeId(1), tupleDescriptor, "rfBucketScanNode", ScanContext.EMPTY);
        for (long tabletId : tabletIds) {
            TPaloScanRange paloScanRange = new TPaloScanRange();
            paloScanRange.setTabletId(tabletId);
            TScanRange scanRange = new TScanRange();
            scanRange.setPaloScanRange(paloScanRange);
            TScanRangeLocations locations = new TScanRangeLocations();
            locations.setScanRange(scanRange);
            scanNode.scanRangeLocations.add(locations);
        }
        return scanNode;
    }

    @SuppressWarnings("unchecked")
    private Map<Long, Long> getBucketInfo(OlapScanNode scanNode) throws Exception {
        java.lang.reflect.Field bucketInfoField =
                OlapScanNode.class.getDeclaredField("tabletId2BucketInfo");
        bucketInfoField.setAccessible(true);
        return (Map<Long, Long>) bucketInfoField.get(scanNode);
    }

    @Test
    public void testPointQueryBackendAlivePathsOnlyUseSelectedTabletBackends() {
        Backend firstBackend = backendWithDisks(1L, 11L, 12L);
        Backend secondBackend = backendWithDisks(2L, 21L, 22L);
        Backend unrelatedBackend = backendWithDisks(3L, 31L, 32L);
        Map<Long, Backend> backends = ImmutableMap.of(
                firstBackend.getId(), firstBackend,
                secondBackend.getId(), secondBackend,
                unrelatedBackend.getId(), unrelatedBackend);

        LocalTablet selectedTablet = new LocalTablet(10L);
        selectedTablet.addReplica(new LocalReplica(101L, firstBackend.getId(), 0, ReplicaState.NORMAL), true);
        selectedTablet.addReplica(new LocalReplica(102L, secondBackend.getId(), 0, ReplicaState.NORMAL), true);
        selectedTablet.addReplica(new LocalReplica(103L, 4L, 0, ReplicaState.NORMAL), true);

        Map<Long, Set<Long>> alivePathHashes = OlapScanNode.getBackendAlivePathHashes(
                backends, Lists.<Tablet>newArrayList(selectedTablet));

        Assert.assertEquals(2, alivePathHashes.size());
        Assert.assertEquals(Collections.singleton(11L), alivePathHashes.get(firstBackend.getId()));
        Assert.assertEquals(Collections.singleton(21L), alivePathHashes.get(secondBackend.getId()));
        Assert.assertFalse(alivePathHashes.containsKey(unrelatedBackend.getId()));
        Assert.assertFalse(alivePathHashes.containsKey(4L));
    }

    private Backend backendWithDisks(long backendId, long alivePathHash, long offlinePathHash) {
        DiskInfo aliveDisk = new DiskInfo("/alive-" + backendId);
        aliveDisk.setPathHash(alivePathHash);
        DiskInfo offlineDisk = new DiskInfo("/offline-" + backendId);
        offlineDisk.setPathHash(offlinePathHash);
        offlineDisk.setState(DiskInfo.DiskState.OFFLINE);

        Backend backend = new Backend(backendId, "127.0.0." + backendId, 9050);
        backend.setDisks(ImmutableMap.of(
                aliveDisk.getRootPath(), aliveDisk,
                offlineDisk.getRootPath(), offlineDisk));
        return backend;
    }

    private Partition mockPartition(String name) {
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getName()).thenReturn(name);
        return partition;
    }

    private void setRangePartitionItem(RangePartitionInfo partitionInfo, long partitionId,
            String lowerValue, String upperValue) throws AnalysisException {
        List<Column> partitionColumns = partitionInfo.getPartitionColumns();
        PartitionKey lower = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(lowerValue)), partitionColumns);
        PartitionKey upper = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(upperValue)), partitionColumns);
        partitionInfo.setItem(partitionId, false, new RangePartitionItem(Range.closedOpen(lower, upper)));
    }

    private SlotDescriptor addSlot(TupleDescriptor tupleDescriptor, int slotId, String columnName) {
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(slotId), tupleDescriptor.getId());
        slotDescriptor.setColumn(new Column(columnName, PrimitiveType.BIGINT));
        tupleDescriptor.addSlot(slotDescriptor);
        return slotDescriptor;
    }
}

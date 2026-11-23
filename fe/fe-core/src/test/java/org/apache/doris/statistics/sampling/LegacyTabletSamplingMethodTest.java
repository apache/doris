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

package org.apache.doris.statistics.sampling;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class LegacyTabletSamplingMethodTest {
    private static TabletSamplingRequest request(long target, boolean key, boolean partition, boolean unique,
            boolean distribution) {
        return new TabletSamplingRequest(TabletSamplingScope.table(List.of()),
                new ColumnPhysicalLayout(key, partition, unique, distribution),
                new SamplingBudget(target, 1024, 512, 4, 100000), 2000000000, 42);
    }

    @Test
    void limitScaleAndAlgorithmFollowPhysicalLayout() {
        var table = Mockito.mock(OlapTable.class);
        LegacyTabletSamplingMethod method = Mockito.spy(new LegacyTabletSamplingMethod(table, -1, 0));
        Mockito.doReturn(Pair.of(List.of(1L, 2L), 100L)).when(method).getSampleTablets(Mockito.any());
        for (boolean key : List.of(false, true)) {
            for (boolean partition : List.of(false, true)) {
                for (boolean unique : List.of(false, true)) {
                    for (boolean bucket : List.of(false, true)) {
                        TabletSampleSpec spec = method.build(request(50, key, partition, unique, bucket));
                        var sem = (TabletSampleSpec.LegacySamplingSemantics) spec.semantics();
                        Assertions.assertEquals(unique || bucket, sem.linear());
                        Assertions.assertEquals(!key && !partition ? 50 : 0, sem.limit());
                        Assertions.assertEquals(2000000000.0 / (sem.limit() == 0 ? 100 : 50), sem.scaleFactor());
                        Assertions.assertEquals(new TabletSampleSpec.LegacyTabletScanClause(List.of(1L, 2L)),
                                spec.scanClause());
                    }
                }
            }
        }
        Assertions.assertEquals(TabletSampleSpec.fullScan(), method.build(request(101, false, false, false, false)));
        Assertions.assertEquals(TabletSampleSpec.fullScan(),
                method.build(request(2000000000, false, false, false, false)));
        try (MockedStatic<DebugPointUtil> debug = Mockito.mockStatic(DebugPointUtil.class)) {
            debug.when(() -> DebugPointUtil.isEnable("OlapAnalysisTask.useDUJ1Template")).thenReturn(true);
            var forced = method.build(request(101, false, false, true, true));
            Assertions.assertFalse(((TabletSampleSpec.LegacySamplingSemantics) forced.semantics()).linear());
        }
    }

    @Test
    public void testPickSamplePartition() {

        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getRowCount()).thenReturn(1000000000L);
        LegacyTabletSamplingMethod task = new LegacyTabletSamplingMethod(olapTable, -1, 0);

        Partition p1 = Mockito.mock(Partition.class);
        Partition p2 = Mockito.mock(Partition.class);
        Partition p3 = Mockito.mock(Partition.class);
        Mockito.when(p1.getId()).thenReturn(1L);
        Mockito.when(p2.getId()).thenReturn(2L);
        Mockito.when(p3.getId()).thenReturn(3L);
        Mockito.when(p1.getRowCount()).thenReturn(400000000L);
        Mockito.when(p2.getRowCount()).thenReturn(100000000L);
        Mockito.when(p3.getRowCount()).thenReturn(500000000L);

        MaterializedIndex idx1 = Mockito.mock(MaterializedIndex.class);
        MaterializedIndex idx2 = Mockito.mock(MaterializedIndex.class);
        MaterializedIndex idx3 = Mockito.mock(MaterializedIndex.class);
        Mockito.when(idx1.getTabletIdsInOrder()).thenReturn(Lists.newArrayList(0L, 1L));
        Mockito.when(idx2.getTabletIdsInOrder()).thenReturn(Lists.newArrayList(2L, 3L));
        Mockito.when(idx3.getTabletIdsInOrder()).thenReturn(Lists.newArrayList(2L, 3L));
        Mockito.when(p1.getIndex(ArgumentMatchers.anyLong())).thenReturn(idx1);
        Mockito.when(p2.getIndex(ArgumentMatchers.anyLong())).thenReturn(idx2);
        Mockito.when(p3.getIndex(ArgumentMatchers.anyLong())).thenReturn(idx3);

        List<Partition> partitions = Lists.newArrayList();
        partitions.add(p1);
        partitions.add(p2);
        partitions.add(p3);
        List<Long> ids = Lists.newArrayList();

        long rows = task.pickSamplePartition(partitions, ids, 0);
        Assertions.assertEquals(900000000, rows);
        Assertions.assertEquals(4, ids.size());
        Assertions.assertEquals(0, ids.get(0));
        Assertions.assertEquals(1, ids.get(1));
        Assertions.assertEquals(2, ids.get(2));
        Assertions.assertEquals(3, ids.get(3));
    }

    @Test
    public void testGetSkipPartitionId() throws AnalysisException {
        OlapTable tableIf = Mockito.mock(OlapTable.class);

        // test null partition list
        LegacyTabletSamplingMethod task = new LegacyTabletSamplingMethod(tableIf, -1, 0);
        long skipPartitionId = task.getSkipPartitionId(null);
        Assertions.assertEquals(LegacyTabletSamplingMethod.NO_SKIP_TABLET_ID, skipPartitionId);

        // test empty partition list
        List<Partition> partitions = Lists.newArrayList();
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(LegacyTabletSamplingMethod.NO_SKIP_TABLET_ID, skipPartitionId);

        // test partition list item less than session variable partition_sample_count
        Partition p1 = new Partition(1, "p1", new MaterializedIndex(), new RandomDistributionInfo());
        partitions.add(p1);
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(LegacyTabletSamplingMethod.NO_SKIP_TABLET_ID, skipPartitionId);

        partitions.clear();
        int partitionSampleCount = StatisticsUtil.getPartitionSampleCount();
        for (int i = 1; i <= partitionSampleCount; i++) {
            Partition p = new Partition(i, "p" + i, new MaterializedIndex(), new RandomDistributionInfo());
            partitions.add(p);
        }

        // Test List partition return NO_SKIP_TABLET_ID
        Mockito.when(tableIf.getPartitionInfo()).thenReturn(new PartitionInfo(PartitionType.LIST));
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(LegacyTabletSamplingMethod.NO_SKIP_TABLET_ID, skipPartitionId);

        // Test Unpartition return NO_SKIP_TABLET_ID
        Mockito.when(tableIf.getPartitionInfo()).thenReturn(new PartitionInfo(PartitionType.UNPARTITIONED));
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(LegacyTabletSamplingMethod.NO_SKIP_TABLET_ID, skipPartitionId);

        // Test more than 1 partition column return NO_SKIP_TABLET_ID
        ArrayList<Column> multiColumns = Lists.newArrayList();
        multiColumns.add(new Column("col1", PrimitiveType.DATEV2));
        multiColumns.add(new Column("col2", PrimitiveType.DATEV2));
        Mockito.when(tableIf.getPartitionInfo()).thenReturn(new PartitionInfo(PartitionType.RANGE, multiColumns));
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(LegacyTabletSamplingMethod.NO_SKIP_TABLET_ID, skipPartitionId);

        // Test not Date type return NO_SKIP_TABLET_ID
        ArrayList<Column> stringColumns = Lists.newArrayList();
        stringColumns.add(new Column("col1", PrimitiveType.STRING));
        Mockito.when(tableIf.getPartitionInfo()).thenReturn(new PartitionInfo(PartitionType.RANGE, stringColumns));
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(LegacyTabletSamplingMethod.NO_SKIP_TABLET_ID, skipPartitionId);

        // Test return the partition id with the oldest date range.
        ArrayList<Column> columns = Lists.newArrayList();
        Column col1 = new Column("col1", PrimitiveType.DATEV2);
        columns.add(col1);
        PartitionInfo partitionInfo = new PartitionInfo(PartitionType.RANGE, columns);

        List<PartitionValue> lowKey = Lists.newArrayList();
        lowKey.add(new PartitionValue("2025-01-01"));
        List<PartitionValue> highKey = Lists.newArrayList();
        highKey.add(new PartitionValue("2025-01-02"));
        Range<PartitionKey> range1 = Range.closedOpen(PartitionKey.createPartitionKey(lowKey, columns),
                    PartitionKey.createPartitionKey(highKey, columns));
        RangePartitionItem item1 = new RangePartitionItem(range1);

        lowKey.clear();
        lowKey.add(new PartitionValue("2024-11-01"));
        highKey.clear();
        highKey.add(new PartitionValue("2024-11-02"));
        Range<PartitionKey> range2 = Range.closedOpen(PartitionKey.createPartitionKey(lowKey, columns),
                PartitionKey.createPartitionKey(highKey, columns));
        RangePartitionItem item2 = new RangePartitionItem(range2);

        lowKey.clear();
        lowKey.add(new PartitionValue("2025-02-13"));
        highKey.clear();
        highKey.add(new PartitionValue("2025-02-14"));
        Range<PartitionKey> range3 = Range.closedOpen(PartitionKey.createPartitionKey(lowKey, columns),
                PartitionKey.createPartitionKey(highKey, columns));
        RangePartitionItem item3 = new RangePartitionItem(range3);

        partitionInfo.addPartition(1, false, item1, new DataProperty(TStorageMedium.HDD), null, false, false);
        partitionInfo.addPartition(2, false, item2, new DataProperty(TStorageMedium.HDD), null, false, false);
        partitionInfo.addPartition(3, false, item3, new DataProperty(TStorageMedium.HDD), null, false, false);

        Mockito.when(tableIf.getPartitionInfo()).thenReturn(partitionInfo);
        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(StatisticsUtil.class, Mockito.CALLS_REAL_METHODS)) {
            mockedStatisticsUtil.when(StatisticsUtil::getPartitionSampleCount).thenReturn(3);
            partitions.clear();
            for (int i = 1; i <= 3; i++) {
                Partition p = new Partition(i, "p" + i, new MaterializedIndex(), new RandomDistributionInfo());
                partitions.add(p);
            }
            skipPartitionId = task.getSkipPartitionId(partitions);
            Assertions.assertEquals(2, skipPartitionId);

            // Test less than partition
            partitions.add(new Partition(4, "p4", new MaterializedIndex(), new RandomDistributionInfo()));
            partitions.add(new Partition(5, "p5", new MaterializedIndex(), new RandomDistributionInfo()));
            mockedStatisticsUtil.when(StatisticsUtil::getPartitionSampleCount).thenReturn(5);
            highKey.clear();
            highKey.add(new PartitionValue("2024-01-01"));
            Range<PartitionKey> range4 = Range.lessThan(PartitionKey.createPartitionKey(highKey, columns));
            RangePartitionItem item4 = new RangePartitionItem(range4);
            partitionInfo.addPartition(4, false, item4, new DataProperty(TStorageMedium.HDD), null, false, false);
            lowKey.clear();
            lowKey.add(new PartitionValue("2024-03-13"));
            highKey.clear();
            highKey.add(new PartitionValue("2024-03-14"));
            Range<PartitionKey> range5 = Range.closedOpen(PartitionKey.createPartitionKey(lowKey, columns),
                    PartitionKey.createPartitionKey(highKey, columns));
            RangePartitionItem item5 = new RangePartitionItem(range5);
            partitionInfo.addPartition(5, false, item5, new DataProperty(TStorageMedium.HDD), null, false, false);
            skipPartitionId = task.getSkipPartitionId(partitions);
            Assertions.assertEquals(4, skipPartitionId);
        }
    }

    @Test
    public void testGetSampleTablets() {
        OlapTable mockTable = Mockito.mock(OlapTable.class);
        TabletSamplingRequest request = request(4000000, false, false, false, false);
        LegacyTabletSamplingMethod task = new LegacyTabletSamplingMethod(mockTable, -1, 0);
        Mockito.when(mockTable.isPartitionColumn(ArgumentMatchers.any())).thenReturn(false);

        MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
        LocalTablet t = Mockito.mock(LocalTablet.class);

        List<Long> ret = Lists.newArrayList();
        ret.add(10001L);
        ret.add(10002L);
        Mockito.when(index.getTabletIdsInOrder()).thenReturn(ret);
        Mockito.when(index.getRowCount()).thenReturn(1_200_000_000L);
        Mockito.when(index.getTablet(ArgumentMatchers.anyLong())).thenReturn(t);

        final int[] i = {0};
        long[] tabletsRowCount = {1100000000, 100000000};
        Mockito.when(t.getMinReplicaRowCount(ArgumentMatchers.anyLong())).thenAnswer(inv -> tabletsRowCount[i[0]++]);

        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getBaseIndex()).thenReturn(index);
        List<Partition> partitions = Lists.newArrayList();
        partitions.add(partition);
        Mockito.when(mockTable.getPartitions()).thenReturn((Collection) partitions);

        // Test set large tablet id back if it doesn't pick enough sample rows.
        Pair<List<Long>, Long> sampleTablets = task.getSampleTablets(request);
        Assertions.assertEquals(1, sampleTablets.first.size());
        Assertions.assertEquals(10001, sampleTablets.first.get(0));
        Assertions.assertEquals(1100000000L, sampleTablets.second);

        // Test normal pick
        task = new LegacyTabletSamplingMethod(mockTable, -1, 1);
        sampleTablets = task.getSampleTablets(request);
        Assertions.assertEquals(1, sampleTablets.first.size());
        Assertions.assertEquals(10002, sampleTablets.first.get(0));
        Assertions.assertEquals(100000000L, sampleTablets.second);
    }

}

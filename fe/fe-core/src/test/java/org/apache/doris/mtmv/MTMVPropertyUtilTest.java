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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

class MTMVPropertyUtilTest {

    private static final String WINDOW = PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT;

    @Test
    void testParsePartitionWindowLimitValid() {
        Map<TableNameInfo, Integer> parsed = MTMVPropertyUtil.parsePartitionWindowLimit("s:10,t:2");
        Assertions.assertEquals(2, parsed.size());
        Assertions.assertEquals(10, parsed.get(new TableNameInfo("s")));
        Assertions.assertEquals(2, parsed.get(new TableNameInfo("t")));
    }

    @Test
    void testParsePartitionWindowLimitEmpty() {
        Assertions.assertTrue(MTMVPropertyUtil.parsePartitionWindowLimit(null).isEmpty());
        Assertions.assertTrue(MTMVPropertyUtil.parsePartitionWindowLimit("").isEmpty());
        Assertions.assertTrue(MTMVPropertyUtil.parsePartitionWindowLimit("  ").isEmpty());
    }

    @Test
    void testParsePartitionWindowLimitInvalidFormat() {
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> MTMVPropertyUtil.parsePartitionWindowLimit("s"));
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> MTMVPropertyUtil.parsePartitionWindowLimit("s:"));
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> MTMVPropertyUtil.parsePartitionWindowLimit(":10"));
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> MTMVPropertyUtil.parsePartitionWindowLimit("s:abc"));
    }

    @Test
    void testParsePartitionWindowLimitNonPositiveCount() {
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> MTMVPropertyUtil.parsePartitionWindowLimit("s:0"));
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> MTMVPropertyUtil.parsePartitionWindowLimit("s:-1"));
    }

    @Test
    void testParsePartitionWindowLimitDuplicateTable() {
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> MTMVPropertyUtil.parsePartitionWindowLimit("s:1,s:2"));
    }

    @Test
    void testParsePartitionWindowLimitInvalidTableName() {
        // Names like ".." are rejected as AnalysisException, not a raw IllegalArgumentException.
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> MTMVPropertyUtil.parsePartitionWindowLimit("..:2"));
    }

    @Test
    void testGetIvmPartitionWindowLimitUnsetProperty() {
        Assertions.assertTrue(MTMVPropertyUtil.getIvmPartitionWindowLimit(ImmutableMap.of()).isEmpty());
        Assertions.assertTrue(MTMVPropertyUtil.getIvmPartitionWindowLimit(null).isEmpty());
    }

    @Test
    void testGetPartitionWindowLimitNameMatching() {
        Map<TableNameInfo, Integer> windowLimits =
                MTMVPropertyUtil.parsePartitionWindowLimit("s:10");
        // Unqualified "s" matches a qualified base table name.
        Assertions.assertEquals(10, MTMVPropertyUtil.getPartitionWindowLimit(
                windowLimits, new TableNameInfo("ctl", "db", "s")));
        // A different table is not configured.
        Assertions.assertEquals(-1, MTMVPropertyUtil.getPartitionWindowLimit(
                windowLimits, new TableNameInfo("ctl", "db", "t")));
    }

    @Test
    void testGetPartitionWindowLimitMultipleMatchesRejected() {
        // Both 's' and 'ctl.db.s' configure the same base table -> ambiguous, rejected.
        Map<TableNameInfo, Integer> windowLimits = Maps.newHashMap();
        windowLimits.put(new TableNameInfo("s"), 1);
        windowLimits.put(new TableNameInfo("ctl", "db", "s"), 2);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> MTMVPropertyUtil.getPartitionWindowLimit(
                        windowLimits, new TableNameInfo("ctl", "db", "s")));
    }

    @Test
    void testGetIvmPartitionWindowIdsOrdersByValueNotName() {
        // Partitions named out of value order: [1,5) -> p_high, [5,10) -> p_mid, [10,15) -> p_low.
        // Window of 2 must be p_mid and p_low (the largest values), regardless of names.
        OlapTable table = buildRangePartitionTable(1L, "t", Lists.newArrayList(
                new PartitionSpec(11L, "p_high", Range.closedOpen(intKey(1), intKey(5))),
                new PartitionSpec(12L, "p_mid", Range.closedOpen(intKey(5), intKey(10))),
                new PartitionSpec(13L, "p_low", Range.closedOpen(intKey(10), intKey(15)))));
        Map<TableNameInfo, Integer> windowLimits =
                MTMVPropertyUtil.parsePartitionWindowLimit("t:2");
        List<Long> windowIds = MTMVPropertyUtil.getIvmPartitionWindowIds(
                table, new TableNameInfo("ctl", "db", "t"), windowLimits);
        Assertions.assertEquals(Lists.newArrayList(12L, 13L), windowIds);
    }

    @Test
    void testGetIvmPartitionWindowIdsCoveringAllPartitionsReturnsNull() {
        OlapTable table = buildRangePartitionTable(1L, "t", Lists.newArrayList(
                new PartitionSpec(11L, "p1", Range.closedOpen(intKey(1), intKey(5))),
                new PartitionSpec(12L, "p2", Range.closedOpen(intKey(5), intKey(10)))));
        Map<TableNameInfo, Integer> windowLimits =
                MTMVPropertyUtil.parsePartitionWindowLimit("t:2");
        Assertions.assertNull(MTMVPropertyUtil.getIvmPartitionWindowIds(
                table, new TableNameInfo("ctl", "db", "t"), windowLimits));
        // Larger than the partition count is also unlimited.
        Map<TableNameInfo, Integer> biggerWindow = MTMVPropertyUtil.parsePartitionWindowLimit("t:100");
        Assertions.assertNull(MTMVPropertyUtil.getIvmPartitionWindowIds(
                table, new TableNameInfo("ctl", "db", "t"), biggerWindow));
    }

    @Test
    void testGetIvmPartitionWindowIdsUnconfiguredOrNonPartitionedReturnsNull() {
        OlapTable rangeTable = buildRangePartitionTable(1L, "t", Lists.newArrayList(
                new PartitionSpec(11L, "p1", Range.closedOpen(intKey(1), intKey(5))),
                new PartitionSpec(12L, "p2", Range.closedOpen(intKey(5), intKey(10)))));
        Map<TableNameInfo, Integer> windowLimits = MTMVPropertyUtil.parsePartitionWindowLimit("other:1");
        Assertions.assertNull(MTMVPropertyUtil.getIvmPartitionWindowIds(
                rangeTable, new TableNameInfo("ctl", "db", "t"), windowLimits));

        OlapTable nonPartitioned = new OlapTable(2L, "t2", Lists.newArrayList(new Column("k", ScalarType.INT)),
                KeysType.DUP_KEYS, new SinglePartitionInfo(), new RandomDistributionInfo(1));
        Map<TableNameInfo, Integer> configured = MTMVPropertyUtil.parsePartitionWindowLimit("t2:1");
        Assertions.assertNull(MTMVPropertyUtil.getIvmPartitionWindowIds(
                nonPartitioned, new TableNameInfo("ctl", "db", "t2"), configured));
    }

    private OlapTable buildRangePartitionTable(long tableId, String tableName, List<PartitionSpec> specs) {
        Column partitionColumn = new Column("dt", ScalarType.createType(PrimitiveType.INT));
        OlapTable table = new OlapTable(tableId, tableName, Lists.newArrayList(partitionColumn),
                KeysType.DUP_KEYS, new RangePartitionInfo(), new RandomDistributionInfo(1));
        for (PartitionSpec spec : specs) {
            MaterializedIndex index = new MaterializedIndex(table.getBaseIndexId(),
                    MaterializedIndex.IndexState.NORMAL);
            Partition partition = new Partition(spec.partitionId, spec.name, index,
                    new RandomDistributionInfo(1));
            table.addPartition(partition);
            table.getPartitionInfo().addPartition(spec.partitionId, new DataProperty(TStorageMedium.HDD),
                    ReplicaAllocation.DEFAULT_ALLOCATION, true, false);
            table.getPartitionInfo().setItem(spec.partitionId, false, new RangePartitionItem(spec.range));
        }
        return table;
    }

    private static PartitionKey intKey(int value) {
        try {
            return PartitionKey.createPartitionKey(
                    Collections.singletonList(new PartitionValue(String.valueOf(value))),
                    Collections.singletonList(new Column("dt", ScalarType.createType(PrimitiveType.INT))));
        } catch (AnalysisException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final class PartitionSpec {
        private final long partitionId;
        private final String name;
        private final Range<PartitionKey> range;

        private PartitionSpec(long partitionId, String name, Range<PartitionKey> range) {
            this.partitionId = partitionId;
            this.name = name;
            this.range = range;
        }
    }
}

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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.planner.LocalExchangeNode;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.SelectNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

class TopnConditionCacheTest {
    private ConnectContext previousContext;
    private TopnFilter filter;
    private TopN topn;
    private SortNode sort;
    private OlapScanNode scan;
    private PhysicalOlapScan physicalScan;
    private OlapTable table;
    private SlotReference key;
    private TPaloScanRange first;
    private TPaloScanRange second;

    @BeforeEach
    void setUp() {
        previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().enableConditionCache = true;
        context.setThreadLocalInfo();
        topn = Mockito.mock(TopN.class);
        sort = Mockito.mock(SortNode.class);
        scan = Mockito.mock(OlapScanNode.class);
        physicalScan = Mockito.mock(PhysicalOlapScan.class);
        table = Mockito.mock(OlapTable.class);
        key = new SlotReference("score", IntegerType.INSTANCE);
        Mockito.when(topn.getOrderKeys()).thenReturn(List.of(new OrderKey(key, true, false)));
        Mockito.when(sort.getChild(0)).thenReturn(scan);
        Mockito.when(sort.getLimit()).thenReturn(10L);
        Mockito.when(physicalScan.getTable()).thenReturn(table);
        Mockito.when(physicalScan.getTableSample()).thenReturn(Optional.empty());
        Mockito.when(physicalScan.getAppliedRuntimeFilters()).thenReturn(List.of());
        Mockito.when(table.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        Mockito.when(table.getId()).thenReturn(7L);
        Mockito.when(table.getBaseIndexId()).thenReturn(8L);
        Mockito.when(scan.getOlapTable()).thenReturn(table);
        Mockito.when(scan.getSelectedIndexId()).thenReturn(8L);
        Mockito.when(scan.getTopnFilterSortNodes()).thenReturn(List.of(sort));
        SlotRef slot = Mockito.mock(SlotRef.class);
        Column column = new Column("score", Type.INT);
        column.setUniqueId(3);
        Mockito.when(slot.getColumn()).thenReturn(column);
        Mockito.when(slot.getType()).thenReturn(Type.INT);
        filter = new TopnFilter(topn, physicalScan, key);
        filter.legacySortNode = sort;
        filter.legacyTargets.put(scan, slot);
        first = range(100, 2);
        second = range(200, 3);
        setRanges(first, second);
    }

    @AfterEach
    void tearDown() {
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    private TPaloScanRange range(long tablet, long version) {
        TPaloScanRange range = new TPaloScanRange();
        range.setTabletId(tablet);
        range.setVersion(Long.toString(version));
        range.setSchemaHash("123");
        return range;
    }

    private void setRanges(TPaloScanRange... ranges) {
        Mockito.when(scan.getScanRangeLocations(0)).thenReturn(java.util.Arrays.stream(ranges)
                .map(range -> new TScanRangeLocations().setScanRange(new TScanRange().setPaloScanRange(range)))
                .collect(java.util.stream.Collectors.toList()));
    }

    @Test
    void completeSnapshotInvalidatesOtherTabletEvidence() {
        long digest = TopnConditionCache.computeDigest(filter);
        Assertions.assertNotEquals(0, digest);
        // Tablet 200 is unchanged. Removing the tablet that supplied the best candidates
        // must invalidate its exclusions as well.
        setRanges(second);
        Assertions.assertNotEquals(digest, TopnConditionCache.computeDigest(filter));
        setRanges(first, second);
        first.setVersion("4");
        Assertions.assertNotEquals(digest, TopnConditionCache.computeDigest(filter));
    }

    @Test
    void scanAssignmentOrderDoesNotChangeIdentity() {
        long digest = TopnConditionCache.computeDigest(filter);
        setRanges(second, first);
        Assertions.assertEquals(digest, TopnConditionCache.computeDigest(filter));
        second.setSchemaHash("456");
        Assertions.assertNotEquals(digest, TopnConditionCache.computeDigest(filter));
    }

    @Test
    void sortSemanticsSeparateEntries() {
        long original = TopnConditionCache.computeDigest(filter);
        Mockito.when(sort.getLimit()).thenReturn(20L);
        Assertions.assertNotEquals(original, TopnConditionCache.computeDigest(filter));
        Mockito.when(sort.getLimit()).thenReturn(10L);
        Mockito.when(sort.getOffset()).thenReturn(5L);
        Assertions.assertNotEquals(original, TopnConditionCache.computeDigest(filter));
        Mockito.when(sort.getOffset()).thenReturn(0L);
        Mockito.when(topn.getOrderKeys()).thenReturn(List.of(new OrderKey(key, false, false)));
        Assertions.assertNotEquals(original, TopnConditionCache.computeDigest(filter));
        Mockito.when(topn.getOrderKeys()).thenReturn(List.of(new OrderKey(key, true, true)));
        Assertions.assertNotEquals(original, TopnConditionCache.computeDigest(filter));
    }

    @Test
    void legacyColumnsWithoutUniqueIdsRemainDistinct() {
        SlotRef slot = (SlotRef) filter.legacyTargets.get(scan);
        Column firstColumn = new Column("score", Type.INT);
        firstColumn.setUniqueId(Column.COLUMN_UNIQUE_ID_INIT_VALUE);
        Mockito.when(slot.getColumn()).thenReturn(firstColumn);
        long digest = TopnConditionCache.computeDigest(filter);
        Column secondColumn = new Column("other_score", Type.INT);
        secondColumn.setUniqueId(Column.COLUMN_UNIQUE_ID_INIT_VALUE);
        Mockito.when(slot.getColumn()).thenReturn(secondColumn);
        Assertions.assertNotEquals(digest, TopnConditionCache.computeDigest(filter));
    }

    @Test
    void localPassthroughPreservesTheInput() {
        long digest = TopnConditionCache.computeDigest(filter);
        LocalExchangeNode exchange = Mockito.mock(LocalExchangeNode.class);
        Mockito.when(sort.getChild(0)).thenReturn(exchange);
        Mockito.when(exchange.getChild(0)).thenReturn(scan);
        Mockito.when(exchange.getExchangeType()).thenReturn(LocalExchangeType.PASSTHROUGH);
        Assertions.assertEquals(digest, TopnConditionCache.computeDigest(filter));
        Mockito.when(exchange.getExchangeType()).thenReturn(LocalExchangeType.BROADCAST);
        Assertions.assertEquals(0, TopnConditionCache.computeDigest(filter));
    }

    @Test
    void rejectConditionsOutsideScanAndUnsupportedInputs() {
        Mockito.when(sort.getChild(0)).thenReturn(Mockito.mock(SelectNode.class));
        Assertions.assertEquals(0, TopnConditionCache.computeDigest(filter));
        Mockito.when(sort.getChild(0)).thenReturn(scan);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Assertions.assertEquals(0, TopnConditionCache.computeDigest(filter));
        Mockito.when(table.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        Mockito.when(table.hasRowTtl()).thenReturn(true);
        Assertions.assertEquals(0, TopnConditionCache.computeDigest(filter));
        Mockito.when(table.hasRowTtl()).thenReturn(false);
        Mockito.when(scan.hasLimit()).thenReturn(true);
        Assertions.assertEquals(0, TopnConditionCache.computeDigest(filter));
        Mockito.when(scan.hasLimit()).thenReturn(false);
        Mockito.when(scan.getSelectedIndexId()).thenReturn(9L);
        Assertions.assertEquals(0, TopnConditionCache.computeDigest(filter));
    }

    @Test
    void disabledCacheAndMultipleSortKeysDoNotGetSeed() {
        ConnectContext.get().getSessionVariable().enableConditionCache = false;
        Assertions.assertEquals(0, TopnConditionCache.computeDigest(filter));
        ConnectContext.get().getSessionVariable().enableConditionCache = true;
        ConnectContext.get().getSessionVariable().skipMissingVersion = true;
        Assertions.assertEquals(0, TopnConditionCache.computeDigest(filter));
        ConnectContext.get().getSessionVariable().skipMissingVersion = false;
        ConnectContext.get().getSessionVariable().skipDeletePredicate = true;
        Assertions.assertEquals(0, TopnConditionCache.computeDigest(filter));
        ConnectContext.get().getSessionVariable().skipDeletePredicate = false;
        Mockito.when(topn.getOrderKeys()).thenReturn(List.of(new OrderKey(key, true, false),
                new OrderKey(key, false, false)));
        Assertions.assertEquals(0, TopnConditionCache.computeDigest(filter));
        Mockito.when(topn.getOrderKeys()).thenReturn(List.of(new OrderKey(key, true, false)));
        ConnectContext.remove();
        Assertions.assertEquals(0, TopnConditionCache.computeDigest(filter));
    }
}

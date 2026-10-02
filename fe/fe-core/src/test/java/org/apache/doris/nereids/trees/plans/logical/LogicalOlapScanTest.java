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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.PartitionPrunablePredicate;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests for LogicalOlapScan
 */
public class LogicalOlapScanTest {

    private ConnectContext connectContext;
    private MockedStatic<ConnectContext> mockedConnectContext;

    @BeforeEach
    public void setUp() {
        connectContext = new ConnectContext();
        connectContext.setSessionVariable(new SessionVariable());
        mockedConnectContext = Mockito.mockStatic(ConnectContext.class);
        mockedConnectContext.when(ConnectContext::get).thenReturn(connectContext);
    }

    @AfterEach
    public void tearDown() {
        if (mockedConnectContext != null) {
            mockedConnectContext.close();
        }
    }

    private SlotReference createMockSlot(String name, boolean isVisible) {
        SlotReference slot = Mockito.mock(SlotReference.class);
        Mockito.when(slot.isVisible()).thenReturn(isVisible);
        Mockito.when(slot.getName()).thenReturn(name);
        Mockito.when(slot.getSubPath()).thenReturn(Collections.emptyList());
        Mockito.when(slot.getOriginalColumn()).thenReturn(Optional.empty());
        return slot;
    }

    private SlotReference createMockSlot(String name, String originalColumnName,
            List<String> subPath, boolean isVisible) {
        SlotReference slot = Mockito.mock(SlotReference.class);
        Column column = createColumn(originalColumnName);
        Mockito.when(slot.isVisible()).thenReturn(isVisible);
        Mockito.when(slot.getName()).thenReturn(name);
        Mockito.when(slot.getSubPath()).thenReturn(subPath);
        Mockito.when(slot.getOriginalColumn()).thenReturn(Optional.of(column));
        return slot;
    }

    private Column createColumn(String name) {
        return new Column(name, PrimitiveType.INT);
    }

    private LogicalOlapScan createMockScan(List<Slot> outputSlots) {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getId()).thenReturn(1L);
        Mockito.when(olapTable.getName()).thenReturn("test_table");
        Mockito.when(olapTable.getFullQualifiers()).thenReturn(ImmutableList.of("db", "test_table"));

        LogicalOlapScan scan = Mockito.spy(new LogicalOlapScan(
                new RelationId(1),
                olapTable,
                ImmutableList.of("db"),
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.empty(),
                Collections.emptyList()
        ));
        Mockito.doReturn(outputSlots).when(scan).getOutput();
        return scan;
    }

    /**
     * Test constructReplaceMap returns empty map when MV plan output size
     * doesn't match physical column size.
     */
    @Test
    public void testConstructReplaceMapSizeMismatch() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVCache cache = Mockito.mock(MTMVCache.class);
        Plan originalPlan = Mockito.mock(Plan.class);

        // MV plan has 3 visible output slots
        List<Slot> originOutputs = ImmutableList.of(
                createMockSlot("col1", true),
                createMockSlot("col2", true),
                createMockSlot("col3", true));

        Mockito.when(mtmv.getOrGenerateCache(Mockito.any())).thenReturn(cache);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(cache.getOriginalFinalPlan()).thenReturn(originalPlan);
        Mockito.when(originalPlan.getOutput()).thenReturn(originOutputs);

        // But MV physical table only has 2 columns (size mismatch with plan output)
        Mockito.when(mtmv.getBaseSchema()).thenReturn(ImmutableList.of(
                createColumn("col1"), createColumn("col2")));

        LogicalOlapScan scan = createMockScan(ImmutableList.of(
                createMockSlot("col1", "col1", Collections.emptyList(), true),
                createMockSlot("col2", "col2", Collections.emptyList(), true)));

        Map<Slot, Slot> replaceMap = scan.constructReplaceMap(mtmv);

        Assertions.assertTrue(replaceMap.isEmpty(),
                "replaceMap should be empty when plan output size doesn't match physical column size");
    }

    /**
     * Test constructReplaceMap ignores extra subPath slots in scan output
     * (added by VariantSubPathPruning during query optimization).
     */
    @Test
    public void testConstructReplaceMapIgnoresExtraScanSlots() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVCache cache = Mockito.mock(MTMVCache.class);
        Plan originalPlan = Mockito.mock(Plan.class);

        SlotReference mvSlot = createMockSlot("col1", true);
        Mockito.when(mtmv.getOrGenerateCache(Mockito.any())).thenReturn(cache);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(cache.getOriginalFinalPlan()).thenReturn(originalPlan);
        Mockito.when(originalPlan.getOutput()).thenReturn(ImmutableList.of(mvSlot));
        Mockito.when(mtmv.getBaseSchema()).thenReturn(ImmutableList.of(createColumn("col1")));

        // Scan has base slot + extra subPath slot from VariantSubPathPruning
        SlotReference scanSlotBase = createMockSlot("col1", "col1", Collections.emptyList(), true);
        SlotReference scanSlotHelper = createMockSlot("col1", "col1", Arrays.asList("a", "b"), true);
        LogicalOlapScan scan = createMockScan(ImmutableList.of(scanSlotBase, scanSlotHelper));

        Map<Slot, Slot> replaceMap = scan.constructReplaceMap(mtmv);

        Assertions.assertEquals(1, replaceMap.size());
        Assertions.assertSame(scanSlotBase, replaceMap.get(mvSlot));
    }

    /**
     * Test constructReplaceMap correctly handles aliased columns.
     * MV SQL: SELECT l_orderkey, sum_total AS agg3, max_total AS agg4 FROM mv1
     * Plan slots have originalColumn names from source table (sum_total, max_total),
     * but MV physical columns are named agg3, agg4 (the aliases).
     * Physical column name is used as the key, so the mapping succeeds.
     */
    @Test
    public void testConstructReplaceMapWithAliasedColumns() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVCache cache = Mockito.mock(MTMVCache.class);
        Plan originalPlan = Mockito.mock(Plan.class);

        // MV plan output: slot names are from source table.
        // Use 2-arg createMockSlot (no originalColumn) to avoid nested mock Column objects
        // causing Mockito UnfinishedStubbingException. constructReplaceMap only calls
        // getOriginalColumn() on scan slots, not on MV plan output slots.
        SlotReference mvSlot1 = createMockSlot("l_orderkey", true);
        SlotReference mvSlot2 = createMockSlot("sum_total", true);
        SlotReference mvSlot3 = createMockSlot("max_total", true);

        Mockito.when(mtmv.getOrGenerateCache(Mockito.any())).thenReturn(cache);
        Mockito.when(mtmv.getName()).thenReturn("test_alias_mv");
        Mockito.when(cache.getOriginalFinalPlan()).thenReturn(originalPlan);
        Mockito.when(originalPlan.getOutput()).thenReturn(ImmutableList.of(mvSlot1, mvSlot2, mvSlot3));

        // Physical columns have aliased names
        Mockito.when(mtmv.getBaseSchema()).thenReturn(ImmutableList.of(
                createColumn("l_orderkey"),
                createColumn("agg3"),  // aliased from sum_total
                createColumn("agg4")   // aliased from max_total
        ));

        // Scan slots reference MV's physical column names
        SlotReference scanSlot1 = createMockSlot("l_orderkey", "l_orderkey", Collections.emptyList(), true);
        SlotReference scanSlot2 = createMockSlot("agg3", "agg3", Collections.emptyList(), true);
        SlotReference scanSlot3 = createMockSlot("agg4", "agg4", Collections.emptyList(), true);
        LogicalOlapScan scan = createMockScan(ImmutableList.of(scanSlot1, scanSlot2, scanSlot3));

        Map<Slot, Slot> replaceMap = scan.constructReplaceMap(mtmv);

        // All 3 should be mapped despite plan slots having different names than scan slots
        Assertions.assertEquals(3, replaceMap.size());
        Assertions.assertSame(scanSlot1, replaceMap.get(mvSlot1));
        Assertions.assertSame(scanSlot2, replaceMap.get(mvSlot2));
        Assertions.assertSame(scanSlot3, replaceMap.get(mvSlot3));
    }

    @Test
    public void testWithPreSnapshotReturnsStreamSnapshotScan() {
        LogicalOlapScan scan = createMockScan(ImmutableList.of());
        OlapTableStream stream = Mockito.mock(OlapTableStream.class);

        LogicalPlan snapshot = scan.withPreSnapshot(Optional.of(stream));
        Assertions.assertInstanceOf(LogicalOlapTableStreamScan.class, snapshot);
        LogicalOlapTableStreamScan streamScan = (LogicalOlapTableStreamScan) snapshot;
        Assertions.assertTrue(streamScan.isSnapshot());
        Assertions.assertFalse(streamScan.isIncremental());
        Assertions.assertFalse(streamScan.isReset());
    }

    @Test
    public void testWithPostSnapshotReturnsRegularScan() {
        LogicalOlapScan scan1 = createMockScan(ImmutableList.of());
        LogicalPlan snapshot = scan1.withPostSnapshot();

        Assertions.assertInstanceOf(LogicalOlapScan.class, snapshot);
        Assertions.assertFalse(snapshot instanceof LogicalOlapTableStreamScan);
        Assertions.assertNotSame(scan1, snapshot);
    }

    @Test
    public void testWithSelectedPartitionIdsPreservesPartitionPredicateFlagForAllScanTypes() {
        LogicalOlapScan scan = createMockScan(ImmutableList.of())
                .withSelectedPartitionIds(ImmutableList.of(), true);

        LogicalOlapScan copiedScan = scan.withSelectedPartitionIds(ImmutableList.of());

        Assertions.assertTrue(copiedScan.hasPartitionPredicate());

        OlapTableStream stream = Mockito.mock(OlapTableStream.class);
        LogicalOlapTableStreamScan streamScan = (LogicalOlapTableStreamScan) scan
                .withPreSnapshot(Optional.of(stream));
        streamScan = streamScan.withSelectedPartitionIds(ImmutableList.of(), true);

        LogicalOlapTableStreamScan copiedStreamScan = streamScan.withSelectedPartitionIds(ImmutableList.of());

        Assertions.assertTrue(copiedStreamScan.hasPartitionPredicate());
    }

    @Test
    public void copyingPartitionPruningToRollupRebindsThePredicateProof() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(1L);
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getPartitionIds()).thenReturn(ImmutableList.of(1L));
        Mockito.when(table.getPartition(1L)).thenReturn(Mockito.mock(Partition.class));
        Mockito.when(table.getBaseIndexId()).thenReturn(10L);

        Column baseColumn = createColumn("p");
        Mockito.when(table.getBaseSchema(true)).thenReturn(ImmutableList.of(baseColumn));
        Column rollupColumn = createColumn("mv_p");
        SlotRef defineExpr = Mockito.mock(SlotRef.class);
        Mockito.when(defineExpr.getColumnName()).thenReturn("p");
        rollupColumn.setDefineExpr(defineExpr);
        MaterializedIndexMeta rollupMeta = Mockito.mock(MaterializedIndexMeta.class);
        Mockito.when(rollupMeta.getSchema()).thenReturn(ImmutableList.of(rollupColumn));
        Mockito.when(table.getIndexMetaByIndexId(20L)).thenReturn(rollupMeta);

        LogicalOlapScan sourceScan = new LogicalOlapScan(new RelationId(1), table, ImmutableList.of("db"));
        Slot baseSlot = sourceScan.getOutput().get(0);
        PartitionPrunablePredicate proof = new PartitionPrunablePredicate(
                ImmutableSet.of(1L), ImmutableList.of(baseSlot),
                ImmutableSet.of(new EqualTo(baseSlot, new IntegerLiteral(1))));
        sourceScan = sourceScan.withSelectedPartitionIds(ImmutableList.of(1L), true)
                .withPartitionPrunablePredicates(Optional.of(proof));
        LogicalOlapScan rollupScan = new LogicalOlapScan(
                new RelationId(2), table, ImmutableList.of("db"), ImmutableList.of(), ImmutableList.of(1L),
                20L, PreAggStatus.unset(), ImmutableList.of(), ImmutableList.of(), Optional.empty(),
                ImmutableList.of());

        LogicalOlapScan reboundScan = rollupScan.withPartitionPruningFrom(sourceScan);
        Slot rollupSlot = reboundScan.getOutput().get(0);
        PartitionPrunablePredicate reboundProof = reboundScan.getPartitionPrunablePredicates().orElseThrow();

        Assertions.assertNotEquals(baseSlot.getExprId(), rollupSlot.getExprId());
        Assertions.assertEquals(ImmutableList.of(rollupSlot), reboundProof.getPartitionSlots());
        Assertions.assertEquals(ImmutableSet.of(new EqualTo(rollupSlot, new IntegerLiteral(1))),
                reboundProof.getPrunableConjuncts());
    }

    @Test
    public void deepCopyRebindsPartitionProofToCopiedOutput() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(1L);
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getPartitionIds()).thenReturn(ImmutableList.of(1L));
        Mockito.when(table.getPartition(1L)).thenReturn(Mockito.mock(Partition.class));
        Mockito.when(table.getBaseIndexId()).thenReturn(10L);
        Mockito.when(table.getBaseSchema(true)).thenReturn(ImmutableList.of(createColumn("p")));

        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), table, ImmutableList.of("db"));
        Slot partitionSlot = scan.getOutput().get(0);
        PartitionPrunablePredicate proof = new PartitionPrunablePredicate(
                ImmutableSet.of(1L), ImmutableList.of(partitionSlot),
                ImmutableSet.of(new EqualTo(partitionSlot, new IntegerLiteral(1))));
        scan = scan.withSelectedPartitionIds(ImmutableList.of(1L), true)
                .withPartitionPrunablePredicates(Optional.of(proof));

        LogicalOlapScan copied = (LogicalOlapScan) LogicalPlanDeepCopier.INSTANCE.deepCopy(
                scan, new DeepCopierContext());
        Slot copiedSlot = copied.getOutput().get(0);
        PartitionPrunablePredicate copiedProof = copied.getPartitionPrunablePredicates().orElseThrow();

        Assertions.assertNotEquals(partitionSlot.getExprId(), copiedSlot.getExprId());
        Assertions.assertEquals(ImmutableList.of(copiedSlot), copiedProof.getPartitionSlots());
        Assertions.assertEquals(ImmutableSet.of(new EqualTo(copiedSlot, new IntegerLiteral(1))),
                copiedProof.getPrunableConjuncts());
    }

    @Test
    public void selectedPartitionUpdatesKeepOnlyCoveringProofs() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(1L);
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getPartitionIds()).thenReturn(ImmutableList.of(1L, 2L));
        Mockito.when(table.getPartition(1L)).thenReturn(Mockito.mock(Partition.class));
        Mockito.when(table.getPartition(2L)).thenReturn(Mockito.mock(Partition.class));
        Mockito.when(table.getBaseIndexId()).thenReturn(10L);
        Mockito.when(table.getBaseSchema(true)).thenReturn(ImmutableList.of(createColumn("p")));
        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), table, ImmutableList.of("db"))
                .withSelectedPartitionIds(ImmutableList.of(1L), true);
        Slot partitionSlot = scan.getOutput().get(0);
        PartitionPrunablePredicate proof = new PartitionPrunablePredicate(
                ImmutableSet.of(1L), ImmutableList.of(partitionSlot),
                ImmutableSet.of(new EqualTo(partitionSlot, new IntegerLiteral(1))));
        scan = scan.withPartitionPrunablePredicates(Optional.of(proof));

        Assertions.assertTrue(scan.withSelectedPartitionIds(ImmutableList.of())
                .getPartitionPrunablePredicates().isPresent());
        Assertions.assertFalse(scan.withSelectedPartitionIds(ImmutableList.of(1L, 2L))
                .getPartitionPrunablePredicates().isPresent());

        OlapTableStream stream = Mockito.mock(OlapTableStream.class);
        LogicalOlapTableStreamScan streamScan = (LogicalOlapTableStreamScan) scan.withPreSnapshot(Optional.of(stream));
        streamScan = streamScan.withSelectedPartitionIds(ImmutableList.of(1L), true)
                .withPartitionPrunablePredicates(Optional.of(proof));
        Assertions.assertFalse(streamScan.withSelectedPartitionIds(ImmutableList.of(1L, 2L))
                .getPartitionPrunablePredicates().isPresent());
    }

    @Test
    public void invalidatingPartitionPruningClearsTheRecordedPredicateProof() {
        SlotReference partitionSlot = new SlotReference("p", IntegerType.INSTANCE);
        PartitionPrunablePredicate proof = new PartitionPrunablePredicate(
                ImmutableSet.of(), ImmutableList.of(partitionSlot),
                ImmutableSet.of(new EqualTo(partitionSlot, new IntegerLiteral(1))));
        LogicalOlapScan scan = createMockScan(ImmutableList.of(partitionSlot))
                .withSelectedPartitionIds(ImmutableList.of(), true)
                .withPartitionPrunablePredicates(Optional.of(proof));

        Assertions.assertFalse(scan.withPartitionPruned(false).getPartitionPrunablePredicates().isPresent());

        OlapTableStream stream = Mockito.mock(OlapTableStream.class);
        LogicalOlapTableStreamScan streamScan = (LogicalOlapTableStreamScan) scan.withPreSnapshot(Optional.of(stream));
        streamScan = streamScan.withSelectedPartitionIds(ImmutableList.of(), true)
                .withPartitionPrunablePredicates(Optional.of(proof));
        Assertions.assertTrue(streamScan.getPartitionPrunablePredicates().isPresent());

        LogicalOlapTableStreamScan invalidated = streamScan.withPartitionPruned(false);
        Assertions.assertFalse(invalidated.getPartitionPrunablePredicates().isPresent());
        Assertions.assertTrue(invalidated.isSnapshot());
    }

}

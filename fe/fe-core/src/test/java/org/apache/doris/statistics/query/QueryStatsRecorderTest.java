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

package org.apache.doris.statistics.query;

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class QueryStatsRecorderTest {

    private boolean originalConfigValue;

    @BeforeEach
    public void setUp() {
        originalConfigValue = Config.enable_query_hit_stats;
    }

    @AfterEach
    public void tearDown() {
        // Restore config to avoid affecting other tests.
        Config.enable_query_hit_stats = originalConfigValue;
    }

    // ── shouldRecord guard tests ─────────────────────────────────────────────

    @Test
    public void testShouldNotRecordWhenConfigOff() {
        Config.enable_query_hit_stats = false;
        StatementContext ctx = new StatementContext();
        LogicalPlanAdapter stmt = new LogicalPlanAdapter(
                Mockito.mock(org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class), ctx);
        ctx.setParsedStatement(stmt);
        Assertions.assertFalse(QueryStatsRecorder.shouldRecord(ctx));
    }

    @Test
    public void testShouldNotRecordWhenStatementIsNull() {
        Config.enable_query_hit_stats = true;
        StatementContext ctx = new StatementContext();
        // parsedStatement not set — getParsedStatement() returns null
        Assertions.assertFalse(QueryStatsRecorder.shouldRecord(ctx));
    }

    @Test
    public void testShouldNotRecordExplain() {
        Config.enable_query_hit_stats = true;
        StatementContext ctx = new StatementContext();
        LogicalPlanAdapter stmt = new LogicalPlanAdapter(
                Mockito.mock(org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class), ctx);
        // isExplain() returns true when explainOptions is non-null.
        stmt.setIsExplain(new ExplainOptions(false, false, false));
        ctx.setParsedStatement(stmt);
        Assertions.assertFalse(QueryStatsRecorder.shouldRecord(ctx));
    }

    @Test
    public void testShouldNotRecordDml() {
        Config.enable_query_hit_stats = true;
        StatementContext ctx = new StatementContext();
        Command dmlCommand = Mockito.mock(Command.class);
        LogicalPlanAdapter stmt = new LogicalPlanAdapter(dmlCommand, ctx);
        ctx.setParsedStatement(stmt);
        Assertions.assertFalse(QueryStatsRecorder.shouldRecord(ctx));
    }

    @Test
    public void testShouldNotRecordInternalQuery() {
        Config.enable_query_hit_stats = true;
        StatementContext ctx = new StatementContext();
        org.apache.doris.nereids.trees.plans.logical.LogicalPlan selectPlan =
                Mockito.mock(org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class);
        LogicalPlanAdapter stmt = new LogicalPlanAdapter(selectPlan, ctx);
        ctx.setParsedStatement(stmt);
        QueryState state = new QueryState();
        state.setInternal(true);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getState()).thenReturn(state);
        ctx.setConnectContext(connectContext);
        Assertions.assertFalse(QueryStatsRecorder.shouldRecord(ctx));
    }

    @Test
    public void testShouldNotRecordInsert() {
        Config.enable_query_hit_stats = true;
        StatementContext ctx = new StatementContext();
        org.apache.doris.nereids.trees.plans.logical.LogicalPlan selectPlan =
                Mockito.mock(org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class);
        LogicalPlanAdapter stmt = new LogicalPlanAdapter(selectPlan, ctx);
        ctx.setParsedStatement(stmt);
        ctx.setIsInsert(true);
        Assertions.assertFalse(QueryStatsRecorder.shouldRecord(ctx));
    }

    @Test
    public void testShouldRecordNormalSelect() {
        Config.enable_query_hit_stats = true;
        StatementContext ctx = new StatementContext();
        org.apache.doris.nereids.trees.plans.logical.LogicalPlan selectPlan =
                Mockito.mock(org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class);
        LogicalPlanAdapter stmt = new LogicalPlanAdapter(selectPlan, ctx);
        ctx.setParsedStatement(stmt);
        Assertions.assertTrue(QueryStatsRecorder.shouldRecord(ctx));
    }

    // ── collectDeltas (walkPlan) tests ───────────────────────────────────────

    /**
     * Plan: Filter(k2=1) → Scan[k1(id1), k2(id2)], root output = [k1]
     * Expected: k1.queryHit=true (SELECT), k2.filterHit=true (WHERE), no cross-contamination.
     */
    @Test
    public void testFilterHitRecorded() {
        ExprId id1 = new ExprId(1);
        ExprId id2 = new ExprId(2);
        SlotReference k1Slot = mockSlot(id1, "k1");
        SlotReference k2Slot = mockSlot(id2, "k2");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot, k2Slot));

        Expression conjunct = Mockito.mock(Expression.class);
        Mockito.when(conjunct.getInputSlots()).thenReturn(ImmutableSet.of(k2Slot));

        PhysicalFilter<?> filter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(filter.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(filter.getConjuncts()).thenReturn(ImmutableSet.of(conjunct));
        // Root output: only k1 (SELECT k1 WHERE k2=1)
        Mockito.when(filter.getOutput()).thenReturn(ImmutableList.of(k1Slot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) filter);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        // k1: queryHit only (in SELECT output)
        Assertions.assertNotNull(delta.getColumnStats().get("k1"));
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit);
        Assertions.assertFalse(delta.getColumnStats().get("k1").filterHit);
        // k2: filterHit only (in WHERE conjunct, not in SELECT output)
        Assertions.assertNotNull(delta.getColumnStats().get("k2"));
        Assertions.assertTrue(delta.getColumnStats().get("k2").filterHit);
        Assertions.assertFalse(delta.getColumnStats().get("k2").queryHit);
    }

    /**
     * Plan: Scan[k1(id1), k2(id2)] as root, root output = [k1]
     * Expected: k1.queryHit=true, k2 not in delta.
     */
    @Test
    public void testQueryHitFromRootOutput() {
        ExprId id1 = new ExprId(1);
        ExprId id2 = new ExprId(2);
        SlotReference k1Slot = mockSlot(id1, "k1");
        SlotReference k2Slot = mockSlot(id2, "k2");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot, k2Slot));
        // Root output exposes only k1
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(k1Slot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas(scan);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertNotNull(delta.getColumnStats().get("k1"));
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit);
        Assertions.assertFalse(delta.getColumnStats().get("k1").filterHit);
        // k2 not in root output — no delta entry
        Assertions.assertNull(delta.getColumnStats().get("k2"));
    }

    /**
     * Calling record() twice on the same StatementContext must record only once.
     * The latch (isQueryStatsRecorded) is checked after shouldRecord passes,
     * so we need a valid statement context for shouldRecord to return true.
     */
    @Test
    public void testRecordIsIdempotent() {
        Config.enable_query_hit_stats = true;
        StatementContext ctx = new StatementContext();
        // Set up a proper SELECT so shouldRecord() returns true
        org.apache.doris.nereids.trees.plans.logical.LogicalPlan logicalPlan =
                Mockito.mock(org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class);
        LogicalPlanAdapter stmt = new LogicalPlanAdapter(logicalPlan, ctx);
        ctx.setParsedStatement(stmt);
        // Pre-set the latch — simulates a second call after first recording
        ctx.markQueryStatsRecorded();

        PhysicalOlapScan plan = Mockito.mock(PhysicalOlapScan.class);
        QueryStatsRecorder.record(plan, ctx);
        // isQueryStatsRecorded() is true → record() returns before touching the plan
        Mockito.verify(plan, Mockito.never()).getOutput();
    }

    /**
     * PhysicalLazyMaterializeOlapScan wrapping an inner scan:
     * the delta key and table metadata must come from the inner scan.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testDeferMaterializeScanUsesInnerScan() {
        ExprId id1 = new ExprId(1);
        SlotReference k1Slot = mockSlot(id1, "k1");

        // Inner scan has catalogId=2, dbId=2, tableId=2, indexId=2
        PhysicalOlapScan inner = mockScan(2L, 2L, 2L, 2L, ImmutableList.of(k1Slot));

        PhysicalLazyMaterializeOlapScan defer =
                Mockito.mock(PhysicalLazyMaterializeOlapScan.class);
        Mockito.when(defer.getScan()).thenReturn(inner);
        // Both walkPlan and collectDeltas root-output loop call getOutput()
        Mockito.when(defer.getOutput()).thenReturn(ImmutableList.of(k1Slot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) defer);

        Assertions.assertEquals(1, deltas.size());
        Assertions.assertTrue(deltas.containsKey("2_2_2_2"),
                "Delta key must use inner scan's table identifiers");
        StatsDelta delta = deltas.get("2_2_2_2");
        Assertions.assertNotNull(delta.getColumnStats().get("k1"));
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit);
    }

    /**
     * TopN lazy materialization: operative slot (sort_col/id2) is in the lazy-scan output;
     * lazy slot (cold_col/id3) is only in PhysicalLazyMaterialize's output.
     * Expected: cold_col.queryHit=true via the PhysicalLazyMaterialize walk branch.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testLazyMaterializeQueryHitRecorded() {
        ExprId idSort   = new ExprId(1); // sort_col — operative, in lazy-scan output
        ExprId idRowId  = new ExprId(2); // row-id slot
        ExprId idCold   = new ExprId(3); // cold_col — lazy, NOT in lazy-scan output

        SlotReference sortSlot  = mockSlot(idSort,  "sort_col");
        SlotReference rowIdSlot = mockSlot(idRowId, "__row_id");
        SlotReference coldSlot  = mockSlot(idCold,  "cold_col");

        // Inner scan
        PhysicalOlapScan inner = mockScan(1L, 1L, 1L, 1L,
                ImmutableList.of(sortSlot, rowIdSlot));

        // PhysicalLazyMaterializeOlapScan: output = [sort_col, row_id]
        PhysicalLazyMaterializeOlapScan lazyScan =
                Mockito.mock(PhysicalLazyMaterializeOlapScan.class);
        Mockito.when(lazyScan.getScan()).thenReturn(inner);
        Mockito.when(lazyScan.getOutput())
                .thenReturn(ImmutableList.of(sortSlot, rowIdSlot));
        Mockito.when(lazyScan.children()).thenReturn(ImmutableList.of());

        // PhysicalLazyMaterialize: knows cold_col is lazy for this relation;
        // uses rowIdSlot to link back to the relation.
        org.apache.doris.nereids.trees.plans.algebra.Relation rel =
                Mockito.mock(org.apache.doris.nereids.trees.plans.algebra.Relation.class);

        PhysicalLazyMaterialize<?> lazyMat =
                Mockito.mock(PhysicalLazyMaterialize.class);
        Mockito.when(lazyMat.children()).thenReturn(ImmutableList.of(lazyScan));
        Mockito.when(lazyMat.getRelations()).thenReturn(ImmutableList.of(rel));
        Mockito.when(lazyMat.getRowIds()).thenReturn(ImmutableList.of(rowIdSlot));
        Mockito.when(lazyMat.getLazySlots(rel)).thenReturn(ImmutableList.of(coldSlot));
        // Root output: cold_col is selected
        Mockito.when(lazyMat.getOutput()).thenReturn(ImmutableList.of(coldSlot));

        Map<String, StatsDelta> deltas =
                QueryStatsRecorder.collectDeltas((PhysicalPlan) lazyMat);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta, "delta must exist for the inner scan");
        Assertions.assertNotNull(delta.getColumnStats().get("cold_col"),
                "cold_col must be recorded via lazy-materialize branch");
        Assertions.assertTrue(delta.getColumnStats().get("cold_col").queryHit,
                "cold_col.queryHit must be true");
        // sort_col was only an operative slot, not in root output — no queryHit
        Assertions.assertNull(delta.getColumnStats().get("sort_col"),
                "sort_col must not appear (not in root output)");
    }

    /**
     * k1 appears in both the WHERE predicate and the SELECT output.
     * Expected: k1.queryHit=true AND k1.filterHit=true simultaneously.
     */
    @Test
    public void testColumnInBothSelectAndWhere() {
        ExprId id1 = new ExprId(1);
        SlotReference k1Slot = mockSlot(id1, "k1");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot));

        Expression conjunct = Mockito.mock(Expression.class);
        Mockito.when(conjunct.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot));

        PhysicalFilter<?> filter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(filter.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(filter.getConjuncts()).thenReturn(ImmutableSet.of(conjunct));
        Mockito.when(filter.getOutput()).thenReturn(ImmutableList.of(k1Slot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) filter);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertNotNull(delta.getColumnStats().get("k1"));
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit, "k1.queryHit must be true");
        Assertions.assertTrue(delta.getColumnStats().get("k1").filterHit, "k1.filterHit must be true");
    }

    /**
     * Plan has no OlapScan nodes: collectDeltas should return an empty map without throwing.
     */
    @Test
    public void testNoPlanNodesReturnsEmptyDeltas() {
        PhysicalPlan leafPlan = Mockito.mock(PhysicalPlan.class);
        Mockito.when(leafPlan.children()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas(leafPlan);

        Assertions.assertTrue(deltas.isEmpty());
    }

    /**
     * SlotReference whose getOriginalColumn() returns Optional.empty():
     * no column entry must be added to the delta (no NPE, no phantom entry).
     */
    @Test
    public void testSlotWithNoOriginalColumnIsSkipped() {
        ExprId id1 = new ExprId(1);
        SlotReference slotNoCol = Mockito.mock(SlotReference.class);
        Mockito.when(slotNoCol.getExprId()).thenReturn(id1);
        Mockito.when(slotNoCol.getOriginalColumn()).thenReturn(Optional.empty());

        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(slotNoCol));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas(scan);

        Assertions.assertEquals(1, deltas.size());
        Assertions.assertTrue(deltas.values().iterator().next().getColumnStats().isEmpty(),
                "No column stats should be recorded for a slot with no originalColumn");
    }

    /**
     * Filter with two conjuncts each referencing a different column:
     * both columns must receive filterHit; neither must receive queryHit.
     */
    @Test
    public void testMultipleConjunctsAllGetFilterHit() {
        ExprId id1 = new ExprId(1);
        ExprId id2 = new ExprId(2);
        ExprId id3 = new ExprId(3);
        SlotReference k1Slot = mockSlot(id1, "k1");
        SlotReference k2Slot = mockSlot(id2, "k2");
        SlotReference k3Slot = mockSlot(id3, "k3");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L,
                ImmutableList.of(k1Slot, k2Slot, k3Slot));

        Expression conj1 = Mockito.mock(Expression.class);
        Mockito.when(conj1.getInputSlots()).thenReturn(ImmutableSet.of(k2Slot));
        Expression conj2 = Mockito.mock(Expression.class);
        Mockito.when(conj2.getInputSlots()).thenReturn(ImmutableSet.of(k3Slot));

        PhysicalFilter<?> filter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(filter.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(filter.getConjuncts()).thenReturn(ImmutableSet.of(conj1, conj2));
        Mockito.when(filter.getOutput()).thenReturn(ImmutableList.of(k1Slot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) filter);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertTrue(delta.getColumnStats().get("k2").filterHit, "k2.filterHit must be true");
        Assertions.assertFalse(delta.getColumnStats().get("k2").queryHit, "k2.queryHit must be false");
        Assertions.assertTrue(delta.getColumnStats().get("k3").filterHit, "k3.filterHit must be true");
        Assertions.assertFalse(delta.getColumnStats().get("k3").queryHit, "k3.queryHit must be false");
    }

    /**
     * Two scans from different tables in one plan: each must produce an independent
     * StatsDelta with a distinct key, recording only its own table's columns.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testTwoDifferentTablesProduceSeparateDeltas() {
        ExprId id1 = new ExprId(1);
        ExprId id2 = new ExprId(2);
        SlotReference k1Slot = mockSlot(id1, "k1");
        SlotReference k2Slot = mockSlot(id2, "k2");

        PhysicalOlapScan scan1 = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot));
        PhysicalOlapScan scan2 = mockScan(2L, 2L, 2L, 2L, ImmutableList.of(k2Slot));

        PhysicalPlan join = Mockito.mock(PhysicalPlan.class);
        Mockito.when(join.children()).thenReturn(ImmutableList.of(scan1, scan2));
        Mockito.when(join.getOutput()).thenReturn(ImmutableList.of(k1Slot, k2Slot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas(join);

        Assertions.assertEquals(2, deltas.size(), "Each table must have its own delta");
        Assertions.assertTrue(deltas.containsKey("1_1_1_1"), "scan1 delta missing");
        Assertions.assertTrue(deltas.containsKey("2_2_2_2"), "scan2 delta missing");
        Assertions.assertTrue(deltas.get("1_1_1_1").getColumnStats().get("k1").queryHit);
        Assertions.assertNull(deltas.get("1_1_1_1").getColumnStats().get("k2"),
                "scan1 must not record scan2's column");
        Assertions.assertTrue(deltas.get("2_2_2_2").getColumnStats().get("k2").queryHit);
        Assertions.assertNull(deltas.get("2_2_2_2").getColumnStats().get("k1"),
                "scan2 must not record scan1's column");
    }

    /**
     * Scan whose getTable() returns null: getOrCreateDelta returns null and
     * no stats are recorded — no NPE.
     */
    @Test
    public void testNullTableInScanDoesNotCrash() {
        ExprId id1 = new ExprId(1);
        SlotReference k1Slot = mockSlot(id1, "k1");

        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        Mockito.when(scan.getTable()).thenReturn(null);
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(k1Slot));
        Mockito.when(scan.children()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas(scan);

        Assertions.assertTrue(deltas.isEmpty(), "Null-table scan must not create a delta");
    }

    // ── Alias / GROUP BY / ORDER BY / Window ─────────────────────────────────

    /**
 * Unit test for unwrapAlias: Alias(k1) → k1SlotReference.
     */
    @Test
    public void testUnwrapAliasReturnsUnderlyingSlot() {
        ExprId id1 = new ExprId(1);
        SlotReference k1Slot = mockSlot(id1, "k1");

        org.apache.doris.nereids.trees.expressions.Alias alias =
                Mockito.mock(org.apache.doris.nereids.trees.expressions.Alias.class);
        Mockito.when(alias.child()).thenReturn(k1Slot);

        SlotReference result = QueryStatsRecorder.unwrapAlias(alias);
        Assertions.assertEquals(k1Slot, result);
    }

    /**
     * SELECT k1 AS x FROM t: PhysicalProject.getProjects() exposes Alias so
     * unwrapAlias resolves to k1 and records k1.queryHit.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testAliasUnwrappedForQueryHit() {
        ExprId id1 = new ExprId(1);
        SlotReference k1Slot = mockSlot(id1, "k1");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot));

        org.apache.doris.nereids.trees.expressions.Alias alias =
                Mockito.mock(org.apache.doris.nereids.trees.expressions.Alias.class);
        Mockito.when(alias.getExprId()).thenReturn(new ExprId(99));
        Mockito.when(alias.child()).thenReturn(k1Slot);

        org.apache.doris.nereids.trees.plans.physical.PhysicalProject<?> project =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalProject.class);
        Mockito.when(project.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(project.getProjects()).thenReturn(ImmutableList.of(alias));
        Mockito.when(project.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas(
                (org.apache.doris.nereids.trees.plans.physical.PhysicalPlan) project);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertNotNull(delta.getColumnStats().get("k1"), "k1 must be recorded via alias unwrap");
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit);
    }

    /**
     * SELECT k1 AS x FROM t ORDER BY k2: PhysicalProject is intermediate under PhysicalSort.
     * walkPlan must propagate alias ExprId so parent's getOutput() resolves to k1.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testAliasUnwrappedForQueryHitIntermediateProject() {
        ExprId id1 = new ExprId(1);
        ExprId id2 = new ExprId(2);
        ExprId aliasId = new ExprId(99);
        SlotReference k1Slot = mockSlot(id1, "k1");
        SlotReference k2Slot = mockSlot(id2, "k2");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot, k2Slot));

        org.apache.doris.nereids.trees.expressions.Alias alias =
                Mockito.mock(org.apache.doris.nereids.trees.expressions.Alias.class);
        Mockito.when(alias.getExprId()).thenReturn(aliasId);
        Mockito.when(alias.child()).thenReturn(k1Slot);

        // x_slot — what PhysicalProject.getOutput() returns (alias's ExprId, not k1's)
        SlotReference xSlot = mockSlot(aliasId, "x");

        org.apache.doris.nereids.trees.plans.physical.PhysicalProject<?> project =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalProject.class);
        Mockito.when(project.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(project.getProjects()).thenReturn(ImmutableList.of(alias));
        Mockito.when(project.getOutput()).thenReturn(ImmutableList.of(xSlot));

        Expression sortExpr = Mockito.mock(Expression.class);
        Mockito.when(sortExpr.getInputSlots()).thenReturn(ImmutableSet.of(k2Slot));
        org.apache.doris.nereids.properties.OrderKey orderKey =
                Mockito.mock(org.apache.doris.nereids.properties.OrderKey.class);
        Mockito.when(orderKey.getExpr()).thenReturn(sortExpr);

        org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort<?> sort =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort.class);
        Mockito.when(sort.children()).thenReturn(ImmutableList.of(project));
        Mockito.when(sort.getOrderKeys()).thenReturn(ImmutableList.of(orderKey));
        Mockito.when(sort.getOutput()).thenReturn(ImmutableList.of(xSlot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas(
                (org.apache.doris.nereids.trees.plans.physical.PhysicalPlan) sort);

        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("k2").queryHit, "k2: ORDER BY");
        Assertions.assertNotNull(delta.getColumnStats().get("x"), "x slot resolves via alias propagation");
        Assertions.assertTrue(delta.getColumnStats().get("x").queryHit, "x: SELECT output via alias");
    }

    /**
     * SELECT k1, SUM(k2) FROM t GROUP BY k1: GROUP BY k1 → queryHit, SUM input k2 → queryHit.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testGroupByAndAggregateInputQueryHit() {
        ExprId id1 = new ExprId(1);
        ExprId id2 = new ExprId(2);
        SlotReference k1Slot = mockSlot(id1, "k1");
        SlotReference k2Slot = mockSlot(id2, "k2");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot, k2Slot));

        Expression groupExpr = Mockito.mock(Expression.class);
        Mockito.when(groupExpr.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot));

        NamedExpression aggExpr = Mockito.mock(NamedExpression.class);
        Mockito.when(aggExpr.getInputSlots()).thenReturn(ImmutableSet.of(k2Slot));

        org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate<?> agg =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate.class);
        Mockito.when(agg.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(agg.getGroupByExpressions()).thenReturn(ImmutableList.of(groupExpr));
        Mockito.when(agg.getOutputExpressions()).thenReturn(ImmutableList.of(aggExpr));
        Mockito.when(agg.getOutput()).thenReturn(ImmutableList.of(k1Slot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) agg);

        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit, "k1: GROUP BY key");
        Assertions.assertTrue(delta.getColumnStats().get("k2").queryHit, "k2: aggregate input");
    }

    /**
     * SELECT k1 FROM t ORDER BY k2: ORDER BY k2 → queryHit.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testOrderByQueryHit() {
        ExprId id1 = new ExprId(1);
        ExprId id2 = new ExprId(2);
        SlotReference k1Slot = mockSlot(id1, "k1");
        SlotReference k2Slot = mockSlot(id2, "k2");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot, k2Slot));

        Expression sortExpr = Mockito.mock(Expression.class);
        Mockito.when(sortExpr.getInputSlots()).thenReturn(ImmutableSet.of(k2Slot));

        org.apache.doris.nereids.properties.OrderKey orderKey =
                Mockito.mock(org.apache.doris.nereids.properties.OrderKey.class);
        Mockito.when(orderKey.getExpr()).thenReturn(sortExpr);

        org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort<?> sort =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort.class);
        Mockito.when(sort.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(sort.getOrderKeys()).thenReturn(ImmutableList.of(orderKey));
        Mockito.when(sort.getOutput()).thenReturn(ImmutableList.of(k1Slot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) sort);

        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("k2").queryHit, "k2: ORDER BY key");
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit, "k1: SELECT output");
    }

    /**
     * Window PARTITION BY k0, ORDER BY k1: both → queryHit.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testWindowPartitionAndOrderQueryHit() {
        ExprId id0 = new ExprId(1);
        ExprId id1 = new ExprId(2);
        SlotReference k0Slot = mockSlot(id0, "k0");
        SlotReference k1Slot = mockSlot(id1, "k1");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k0Slot, k1Slot));

        Expression partExpr = Mockito.mock(Expression.class);
        Mockito.when(partExpr.getInputSlots()).thenReturn(ImmutableSet.of(k0Slot));

        Expression orderExprInner = Mockito.mock(Expression.class);
        Mockito.when(orderExprInner.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot));

        org.apache.doris.nereids.trees.expressions.OrderExpression orderExpr =
                Mockito.mock(org.apache.doris.nereids.trees.expressions.OrderExpression.class);
        Mockito.when(orderExpr.child()).thenReturn(orderExprInner);

        org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup wfg =
                Mockito.mock(
                    org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup.class);
        Mockito.when(wfg.getPartitionKeys()).thenReturn(ImmutableSet.of(partExpr));
        Mockito.when(wfg.getOrderKeys()).thenReturn(ImmutableList.of(orderExpr));

        org.apache.doris.nereids.trees.plans.physical.PhysicalWindow<?> window =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalWindow.class);
        Mockito.when(window.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(window.getWindowFrameGroup()).thenReturn(wfg);
        Mockito.when(window.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) window);

        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("k0").queryHit, "k0: PARTITION BY key");
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit, "k1: window ORDER BY key");
    }

    /**
     * JOIN ON t1.k1 = t2.k2: both hash-join and other-join conjuncts → filterHit.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testJoinConditionFilterHit() {
        ExprId id1 = new ExprId(1);
        ExprId id2 = new ExprId(2);
        SlotReference k1Slot = mockSlot(id1, "k1");
        SlotReference k2Slot = mockSlot(id2, "k2");

        PhysicalOlapScan left  = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot));
        PhysicalOlapScan right = mockScan(2L, 2L, 2L, 2L, ImmutableList.of(k2Slot));

        Expression hashConjunct = Mockito.mock(Expression.class);
        Mockito.when(hashConjunct.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot, k2Slot));

        org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin<?, ?> join =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin.class);
        Mockito.when(join.children()).thenReturn(ImmutableList.of(left, right));
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(ImmutableList.of(hashConjunct));
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(ImmutableList.of());
        Mockito.when(join.getOutput()).thenReturn(ImmutableList.of(k1Slot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) join);

        Assertions.assertTrue(deltas.containsKey("1_1_1_1"));
        Assertions.assertTrue(deltas.containsKey("2_2_2_2"));
        Assertions.assertTrue(deltas.get("1_1_1_1").getColumnStats().get("k1").filterHit,
                "k1: join hash conjunct → filterHit");
        Assertions.assertTrue(deltas.get("2_2_2_2").getColumnStats().get("k2").filterHit,
                "k2: join hash conjunct → filterHit");
    }

    /**
     * JOIN ON a.k1 = b.k2 (tinyint vs smallint): Nereids inserts Alias(Cast(k1)) in a
     * PhysicalProject via PushDownExpressionsInHashCondition. The cast-alias ExprId must be
     * propagated into exprIdToScan so that recordInputSlotsAsFilterHit can record k1.filterHit.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testCastAliasInJoinPropagatesFilterHit() {
        ExprId k1Id = new ExprId(1);
        ExprId castAliasId = new ExprId(99);

        SlotReference k1Slot = mockSlot(k1Id, "k1");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot));

        // Cast(k1) — not a SlotReference, so unwrapAlias returns null for the alias below
        Expression castExpr = Mockito.mock(Expression.class);
        Mockito.when(castExpr.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot));

        // Alias(Cast(k1)) with a fresh ExprId — this is what PushDownExpressionsInHashCondition
        // creates; the hash conjunct references castAliasId, not k1Id
        org.apache.doris.nereids.trees.expressions.Alias castAlias =
                Mockito.mock(org.apache.doris.nereids.trees.expressions.Alias.class);
        Mockito.when(castAlias.getExprId()).thenReturn(castAliasId);
        Mockito.when(castAlias.child()).thenReturn(castExpr);

        SlotReference castAliasSlot = mockSlot(castAliasId, "k1");
        org.apache.doris.nereids.trees.plans.physical.PhysicalProject<?> project =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalProject.class);
        Mockito.when(project.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(project.getProjects()).thenReturn(ImmutableList.of(castAlias));
        Mockito.when(project.getOutput()).thenReturn(ImmutableList.of(castAliasSlot));

        // Hash conjunct uses the cast-alias slot, not the original k1Slot
        Expression hashConjunct = Mockito.mock(Expression.class);
        Mockito.when(hashConjunct.getInputSlots()).thenReturn(ImmutableSet.of(castAliasSlot));

        org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin<?, ?> join =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin.class);
        Mockito.when(join.children()).thenReturn(ImmutableList.of(project));
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(ImmutableList.of(hashConjunct));
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(ImmutableList.of());
        Mockito.when(join.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) join);

        Assertions.assertTrue(deltas.containsKey("1_1_1_1"), "scan delta must exist");
        Assertions.assertNotNull(deltas.get("1_1_1_1").getColumnStats().get("k1"),
                "k1 must be recorded via cast-alias propagation");
        Assertions.assertTrue(deltas.get("1_1_1_1").getColumnStats().get("k1").filterHit,
                "k1.filterHit must be set through Alias(Cast(k1)) chain");
    }

    /**
     * SUM(k2) OVER (PARTITION BY k0 ORDER BY k1): k2 (value) → queryHit in addition to k0/k1.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testWindowFunctionValueColumnQueryHit() {
        ExprId id0 = new ExprId(1);
        ExprId id1 = new ExprId(2);
        ExprId id2 = new ExprId(3);
        SlotReference k0Slot = mockSlot(id0, "k0");
        SlotReference k1Slot = mockSlot(id1, "k1");
        SlotReference k2Slot = mockSlot(id2, "k2");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L,
                ImmutableList.of(k0Slot, k1Slot, k2Slot));

        // Window function SUM(k2) — its input slots include k2
        Expression sumFunc = Mockito.mock(Expression.class);
        Mockito.when(sumFunc.getInputSlots()).thenReturn(ImmutableSet.of(k2Slot));

        WindowExpression windowExpr = Mockito.mock(WindowExpression.class);
        Mockito.when(windowExpr.getFunction()).thenReturn(sumFunc);
        Mockito.when(windowExpr.getInputSlots()).thenReturn(ImmutableSet.of(k0Slot, k1Slot, k2Slot));

        NamedExpression windowAlias = Mockito.mock(NamedExpression.class);
        Mockito.when(windowAlias.child(0)).thenReturn(windowExpr);

        Expression partExpr = Mockito.mock(Expression.class);
        Mockito.when(partExpr.getInputSlots()).thenReturn(ImmutableSet.of(k0Slot));

        OrderExpression orderExpr = Mockito.mock(OrderExpression.class);
        Expression orderInner = Mockito.mock(Expression.class);
        Mockito.when(orderInner.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot));
        Mockito.when(orderExpr.child()).thenReturn(orderInner);

        org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup wfg =
                Mockito.mock(
                    org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup.class);
        Mockito.when(wfg.getPartitionKeys()).thenReturn(ImmutableSet.of(partExpr));
        Mockito.when(wfg.getOrderKeys()).thenReturn(ImmutableList.of(orderExpr));
        Mockito.when(wfg.getGroups()).thenReturn(ImmutableList.of(windowAlias));

        org.apache.doris.nereids.trees.plans.physical.PhysicalWindow<?> window =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalWindow.class);
        Mockito.when(window.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(window.getWindowFrameGroup()).thenReturn(wfg);
        Mockito.when(window.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) window);

        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("k0").queryHit, "k0: PARTITION BY");
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit, "k1: window ORDER BY");
        Assertions.assertTrue(delta.getColumnStats().get("k2").queryHit, "k2: SUM value column");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    @Test
    public void testPhysicalStorageLayerAggregateRegistersQueryHit() {
        Config.enable_query_hit_stats = true;
        ExprId id0 = new ExprId(0);
        SlotReference k0Slot = mockSlot(id0, "k0");
        PhysicalOlapScan scan = mockScan(1, 1, 1, 1, ImmutableList.of(k0Slot));

        PhysicalStorageLayerAggregate sla = Mockito.mock(PhysicalStorageLayerAggregate.class);
        Mockito.when(sla.getRelation()).thenReturn(scan);
        Mockito.when(sla.children()).thenReturn(ImmutableList.of());
        Mockito.when(sla.getOutput()).thenReturn(ImmutableList.of(k0Slot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) sla);
        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta, "StorageLayerAggregate scan must produce a delta");
        Assertions.assertTrue(delta.getColumnStats().get("k0").queryHit, "k0: COUNT(*)/agg pushdown");
    }

    @Test
    public void testPhysicalRepeatRegistersGroupingExpressionsAsQueryHit() {
        Config.enable_query_hit_stats = true;
        ExprId id0 = new ExprId(0);
        ExprId id1 = new ExprId(1);
        SlotReference k0Slot = mockSlot(id0, "k0");
        SlotReference k1Slot = mockSlot(id1, "k1");
        PhysicalOlapScan scan = mockScan(1, 1, 1, 1, ImmutableList.of(k0Slot, k1Slot));

        Expression groupExpr = Mockito.mock(Expression.class);
        Mockito.when(groupExpr.getInputSlots()).thenReturn(ImmutableSet.of(k0Slot));

        PhysicalRepeat<?> repeat = Mockito.mock(PhysicalRepeat.class);
        Mockito.when(repeat.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(repeat.getOutput()).thenReturn(ImmutableList.of());
        Mockito.when(repeat.getGroupingSets())
                .thenReturn(ImmutableList.of(ImmutableList.of(groupExpr)));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) repeat);
        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("k0").queryHit, "k0: ROLLUP grouping key");
        Assertions.assertNull(delta.getColumnStats().get("k1"), "k1 not in grouping set — no hit");
    }

    @Test
    public void testPhysicalPartitionTopNRegistersPartitionAndOrderKeysAsQueryHit() {
        Config.enable_query_hit_stats = true;
        ExprId id0 = new ExprId(0);
        ExprId id1 = new ExprId(1);
        SlotReference k0Slot = mockSlot(id0, "k0");
        SlotReference k1Slot = mockSlot(id1, "k1");
        PhysicalOlapScan scan = mockScan(1, 1, 1, 1, ImmutableList.of(k0Slot, k1Slot));

        Expression partExpr = Mockito.mock(Expression.class);
        Mockito.when(partExpr.getInputSlots()).thenReturn(ImmutableSet.of(k0Slot));

        Expression orderInner = Mockito.mock(Expression.class);
        Mockito.when(orderInner.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot));
        org.apache.doris.nereids.properties.OrderKey orderKey =
                Mockito.mock(org.apache.doris.nereids.properties.OrderKey.class);
        Mockito.when(orderKey.getExpr()).thenReturn(orderInner);

        PhysicalPartitionTopN<?> ptn = Mockito.mock(PhysicalPartitionTopN.class);
        Mockito.when(ptn.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(ptn.getOutput()).thenReturn(ImmutableList.of());
        Mockito.when(ptn.getPartitionKeys()).thenReturn(ImmutableList.of(partExpr));
        Mockito.when(ptn.getOrderKeys()).thenReturn(ImmutableList.of(orderKey));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) ptn);
        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("k0").queryHit, "k0: PARTITION BY");
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit, "k1: ORDER BY in PartitionTopN");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private SlotReference mockSlot(ExprId exprId, String columnName) {
        SlotReference slot = Mockito.mock(SlotReference.class);
        Mockito.when(slot.getExprId()).thenReturn(exprId);
        Column col = Mockito.mock(Column.class);
        Mockito.when(col.getName()).thenReturn(columnName);
        Mockito.when(slot.getOriginalColumn()).thenReturn(Optional.of(col));
        return slot;
    }

    @SuppressWarnings("unchecked")
    private PhysicalOlapScan mockScan(long catalogId, long dbId, long tableId, long indexId,
            List<Slot> outputSlots) {
        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        DatabaseIf<OlapTable> db = (DatabaseIf<OlapTable>) Mockito.mock(DatabaseIf.class);
        Mockito.when(table.getCatalogId()).thenReturn(catalogId);
        Mockito.when(table.getId()).thenReturn(tableId);
        Mockito.when(db.getId()).thenReturn(dbId);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getDatabase()).thenReturn(db);
        Mockito.when(scan.getSelectedIndexId()).thenReturn(indexId);
        Mockito.when(scan.getOutput()).thenReturn((List<Slot>) (List<?>) outputSlots);
        Mockito.when(scan.children()).thenReturn(ImmutableList.of());
        return scan;
    }
}

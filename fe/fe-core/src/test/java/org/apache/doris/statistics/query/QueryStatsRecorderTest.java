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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnionAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWorkTableReference;
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
        Assertions.assertNotNull(delta.getColumnStats().get("k1"),
                "k1 resolves via alias propagation under intermediate Sort");
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit, "k1: SELECT output via alias");
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
     * SELECT DISTINCT UPPER(k6) ... ORDER BY UPPER(k6): the merge phase of two-phase aggregation
     * outputs a bare pass-through SlotReference reusing the local phase's ExprId. Regression test
     * for a StackOverflowError this used to cause (getInputSlots() on a bare Slot includes itself).
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testTwoPhaseAggregateBarePassthroughOutputDoesNotSelfReference() {
        ExprId id6 = new ExprId(1);
        ExprId mergeOutputId = new ExprId(2);
        SlotReference k6Slot = mockSlot(id6, "k6");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k6Slot));

        // Local (partial) phase: real Alias(Upper(k6), Y) — links Y -> k6.
        NamedExpression localOutput = Mockito.mock(NamedExpression.class);
        Mockito.when(localOutput.getExprId()).thenReturn(mergeOutputId);
        Mockito.when(localOutput.getInputSlots()).thenReturn(ImmutableSet.of(k6Slot));

        Aggregate<?> localAgg = Mockito.mock(Aggregate.class);
        Mockito.when(localAgg.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(localAgg.getGroupByExpressions()).thenReturn(ImmutableList.of());
        Mockito.when(localAgg.getOutputExpressions())
                .thenReturn((List<NamedExpression>) (List<?>) ImmutableList.of(localOutput));
        Mockito.when(localAgg.getOutput()).thenReturn(ImmutableList.of());

        // Merge (final) phase: bare pass-through SlotReference reusing the SAME ExprId Y.
        // getInputSlots() includes itself, matching real Expression.getInputSlots() for a leaf Slot.
        SlotReference mergeOutputSlot = mockSlot(mergeOutputId, "upper_k6");
        Mockito.when(mergeOutputSlot.getInputSlots()).thenReturn(ImmutableSet.of(mergeOutputSlot));

        Aggregate<?> mergeAgg = Mockito.mock(Aggregate.class);
        Mockito.when(mergeAgg.children()).thenReturn(ImmutableList.of(localAgg));
        Mockito.when(mergeAgg.getGroupByExpressions()).thenReturn(ImmutableList.of());
        Mockito.when(mergeAgg.getOutputExpressions())
                .thenReturn((List<NamedExpression>) (List<?>) ImmutableList.of(mergeOutputSlot));
        Mockito.when(mergeAgg.getOutput()).thenReturn(ImmutableList.of(mergeOutputSlot));

        // ORDER BY the merge phase's own output slot directly (same identity, no recomputation).
        Expression orderInner = Mockito.mock(Expression.class);
        Mockito.when(orderInner.getInputSlots()).thenReturn(ImmutableSet.of(mergeOutputSlot));
        org.apache.doris.nereids.properties.OrderKey orderKey =
                Mockito.mock(org.apache.doris.nereids.properties.OrderKey.class);
        Mockito.when(orderKey.getExpr()).thenReturn(orderInner);

        org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort<?> sort =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort.class);
        Mockito.when(sort.children()).thenReturn(ImmutableList.of(mergeAgg));
        Mockito.when(sort.getOrderKeys()).thenReturn(ImmutableList.of(orderKey));
        Mockito.when(sort.getOutput()).thenReturn(ImmutableList.of(mergeOutputSlot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) sort);

        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("k6").queryHit, "k6: resolves via the local phase's link");
    }

    /**
     * Regression for the `visiting` cycle guard's generality: a true 2-hop cycle (derivedSlotInputs
     * entry A points to B, and B's entry points back to A — no real scan column involved at all,
     * unlike the length-1 self-loop test above). Two stacked PhysicalProject nodes each alias the
     * other's ExprId, forming the cycle; a real k1 column exists separately so collectDeltas
     * doesn't short-circuit on an empty exprIdToScan. Must terminate with no attribution for the
     * cyclic slot, not StackOverflowError, and must not disturb resolution of the unrelated k1.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testMultiHopCycleInDerivedSlotInputsDoesNotOverflow() {
        ExprId k1Id = new ExprId(1);
        ExprId aId = new ExprId(100);
        ExprId bId = new ExprId(101);
        SlotReference k1Slot = mockSlot(k1Id, "k1");
        SlotReference slotA = mockSlot(aId, "a_ref");
        SlotReference slotB = mockSlot(bId, "b_ref");

        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot));

        // Project1: Alias(SlotRef(a_ref)) with exprId B — links B -> {slotA}.
        org.apache.doris.nereids.trees.expressions.Alias alias1 =
                Mockito.mock(org.apache.doris.nereids.trees.expressions.Alias.class);
        Mockito.when(alias1.getExprId()).thenReturn(bId);
        Mockito.when(alias1.child()).thenReturn(slotA);

        PhysicalProject<?> project1 = Mockito.mock(PhysicalProject.class);
        Mockito.when(project1.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(project1.getProjects()).thenReturn(ImmutableList.of(alias1, k1Slot));
        Mockito.when(project1.getOutput()).thenReturn(ImmutableList.of());

        // Project2: Alias(SlotRef(b_ref)) with exprId A — links A -> {slotB}, closing the cycle.
        org.apache.doris.nereids.trees.expressions.Alias alias2 =
                Mockito.mock(org.apache.doris.nereids.trees.expressions.Alias.class);
        Mockito.when(alias2.getExprId()).thenReturn(aId);
        Mockito.when(alias2.child()).thenReturn(slotB);

        PhysicalProject<?> project2 = Mockito.mock(PhysicalProject.class);
        Mockito.when(project2.children()).thenReturn(ImmutableList.of(project1));
        Mockito.when(project2.getProjects()).thenReturn(ImmutableList.of(alias2));
        Mockito.when(project2.getOutput()).thenReturn(ImmutableList.of());

        // ORDER BY the cyclic slot A, plus a real ORDER BY on k1 to prove the cycle doesn't
        // disturb unrelated resolution.
        Expression cyclicOrderExpr = Mockito.mock(Expression.class);
        Mockito.when(cyclicOrderExpr.getInputSlots()).thenReturn(ImmutableSet.of(slotA));
        org.apache.doris.nereids.properties.OrderKey cyclicOrderKey =
                Mockito.mock(org.apache.doris.nereids.properties.OrderKey.class);
        Mockito.when(cyclicOrderKey.getExpr()).thenReturn(cyclicOrderExpr);

        Expression k1OrderExpr = Mockito.mock(Expression.class);
        Mockito.when(k1OrderExpr.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot));
        org.apache.doris.nereids.properties.OrderKey k1OrderKey =
                Mockito.mock(org.apache.doris.nereids.properties.OrderKey.class);
        Mockito.when(k1OrderKey.getExpr()).thenReturn(k1OrderExpr);

        org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort<?> sort =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort.class);
        Mockito.when(sort.children()).thenReturn(ImmutableList.of(project2));
        Mockito.when(sort.getOrderKeys()).thenReturn(ImmutableList.of(cyclicOrderKey, k1OrderKey));
        Mockito.when(sort.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = Assertions.assertDoesNotThrow(
                () -> QueryStatsRecorder.collectDeltas((PhysicalPlan) sort),
                "a 2-hop cycle in derivedSlotInputs must not cause a StackOverflowError");

        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit,
                "k1: unrelated resolution must still work correctly alongside the cycle");
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
     * PhysicalBucketedHashAggregate implements Aggregate but does not extend PhysicalHashAggregate.
     * The Aggregate interface check must cover it.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testBucketedAggregateGroupByQueryHit() {
        ExprId id1 = new ExprId(1);
        SlotReference k1Slot = mockSlot(id1, "k1");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot));

        Expression groupExpr = Mockito.mock(Expression.class);
        Mockito.when(groupExpr.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot));

        org.apache.doris.nereids.trees.plans.physical.PhysicalBucketedHashAggregate<?> agg =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalBucketedHashAggregate.class);
        Mockito.when(agg.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(agg.getGroupByExpressions()).thenReturn(ImmutableList.of(groupExpr));
        Mockito.when(agg.getOutputExpressions()).thenReturn(ImmutableList.of());
        Mockito.when(agg.getOutput()).thenReturn(ImmutableList.of(k1Slot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) agg);

        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta, "bucketed aggregate must be recorded via Aggregate interface");
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit, "k1: GROUP BY in bucketed agg");
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

    /**
     * QUALIFY-style filter above a window on a value-preserving window function:
     * SELECT k1 FROM (SELECT k1, SUM(k2) OVER (PARTITION BY k0 ORDER BY k1) AS running_sum
     * FROM t) w WHERE running_sum > 100. The window alias must be linked to its function's
     * own inputs so the filter resolves back to k2, the same way HAVING resolves through an
     * aggregate output.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFilterAboveWindowValueColumnRecordsFilterHit() {
        ExprId id0 = new ExprId(1);
        ExprId id1 = new ExprId(2);
        ExprId id2 = new ExprId(3);
        ExprId windowAliasId = new ExprId(10);

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
        Mockito.when(windowAlias.getExprId()).thenReturn(windowAliasId);
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

        // QUALIFY-style filter above the window, referencing the window alias's own output.
        SlotReference windowOutputSlot = mockSlot(windowAliasId, "running_sum");
        Expression filterConjunct = Mockito.mock(Expression.class);
        Mockito.when(filterConjunct.getInputSlots()).thenReturn(ImmutableSet.of(windowOutputSlot));

        PhysicalFilter<?> filter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(filter.children()).thenReturn(ImmutableList.of(window));
        Mockito.when(filter.getConjuncts()).thenReturn(ImmutableSet.of(filterConjunct));
        Mockito.when(filter.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) filter);

        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("k2").filterHit,
                "k2: filter above window on SUM(k2) OVER(...) value column");
    }

    /**
     * QUALIFY-style filter above ROW_NUMBER() OVER (PARTITION BY k0 ORDER BY k1): the function
     * itself takes no arguments, so function.getInputSlots() is empty and cannot link the alias
     * to anything — the alias must instead link through the FULL WindowExpression (which wires
     * the partition/order keys as children too), so the filter still resolves to k0/k1.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFilterAbovePositionalWindowFunctionRecordsFilterHit() {
        ExprId id0 = new ExprId(1);
        ExprId id1 = new ExprId(2);
        ExprId windowAliasId = new ExprId(10);

        SlotReference k0Slot = mockSlot(id0, "k0");
        SlotReference k1Slot = mockSlot(id1, "k1");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k0Slot, k1Slot));

        // ROW_NUMBER() — no arguments, getInputSlots() is empty.
        Expression rowNumberFunc = Mockito.mock(Expression.class);
        Mockito.when(rowNumberFunc.getInputSlots()).thenReturn(ImmutableSet.of());

        WindowExpression windowExpr = Mockito.mock(WindowExpression.class);
        Mockito.when(windowExpr.getFunction()).thenReturn(rowNumberFunc);
        // The WindowExpression's OWN getInputSlots() still includes partition/order keys even
        // though the function itself has none — this is what the fix now links through.
        Mockito.when(windowExpr.getInputSlots()).thenReturn(ImmutableSet.of(k0Slot, k1Slot));

        NamedExpression windowAlias = Mockito.mock(NamedExpression.class);
        Mockito.when(windowAlias.getExprId()).thenReturn(windowAliasId);
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

        // QUALIFY-style filter above the window, referencing the window alias's own output.
        SlotReference windowOutputSlot = mockSlot(windowAliasId, "rn");
        Expression filterConjunct = Mockito.mock(Expression.class);
        Mockito.when(filterConjunct.getInputSlots()).thenReturn(ImmutableSet.of(windowOutputSlot));

        PhysicalFilter<?> filter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(filter.children()).thenReturn(ImmutableList.of(window));
        Mockito.when(filter.getConjuncts()).thenReturn(ImmutableSet.of(filterConjunct));
        Mockito.when(filter.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) filter);

        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("k0").filterHit,
                "k0: QUALIFY on ROW_NUMBER() resolves through the partition key");
        Assertions.assertTrue(delta.getColumnStats().get("k1").filterHit,
                "k1: QUALIFY on ROW_NUMBER() resolves through the order key");
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

    /**
     * SELECT k0, SUM(k2) FROM t GROUP BY ROLLUP(k0): k0 is a grouping key (getGroupingSets()),
     * k2 feeds the aggregate function via a non-grouping output column (getOutputExpressions()).
     * The existing repeat test never stubs getOutputExpressions(), leaving this loop untested.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testPhysicalRepeatRegistersOutputExpressionsAsQueryHit() {
        Config.enable_query_hit_stats = true;
        ExprId id0 = new ExprId(0);
        ExprId id2 = new ExprId(2);
        SlotReference k0Slot = mockSlot(id0, "k0");
        SlotReference k2Slot = mockSlot(id2, "k2");
        PhysicalOlapScan scan = mockScan(1, 1, 1, 1, ImmutableList.of(k0Slot, k2Slot));

        Expression groupExpr = Mockito.mock(Expression.class);
        Mockito.when(groupExpr.getInputSlots()).thenReturn(ImmutableSet.of(k0Slot));

        // SUM(k2): a non-grouping-key output column consumed by an aggregate function above.
        NamedExpression sumExpr = Mockito.mock(NamedExpression.class);
        Mockito.when(sumExpr.getInputSlots()).thenReturn(ImmutableSet.of(k2Slot));

        PhysicalRepeat<?> repeat = Mockito.mock(PhysicalRepeat.class);
        Mockito.when(repeat.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(repeat.getOutput()).thenReturn(ImmutableList.of());
        Mockito.when(repeat.getGroupingSets())
                .thenReturn(ImmutableList.of(ImmutableList.of(groupExpr)));
        Mockito.when(repeat.getOutputExpressions())
                .thenReturn((List<NamedExpression>) (List<?>) ImmutableList.of(sumExpr));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) repeat);
        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("k0").queryHit, "k0: ROLLUP grouping key");
        Assertions.assertTrue(delta.getColumnStats().get("k2").queryHit,
                "k2: non-grouping output column feeding SUM(k2) under ROLLUP/CUBE");
    }

    /**
     * SELECT a, ... FROM t GROUP BY ROLLUP(a) HAVING GROUPING(a) = 1: the repeat output
     * Alias(Grouping(a), G) is not scan-backed and has no Alias wrapper stubbed here (matching
     * how repeat.getOutputExpressions() elements are consumed directly) — its own ExprId (G)
     * must link back to `a` so a HAVING filter on the grouping indicator resolves to filterHit.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testPhysicalRepeatGroupingOutputRecordsHavingFilterHit() {
        ExprId aId = new ExprId(0);
        ExprId groupingId = new ExprId(2);
        SlotReference aSlot = mockSlot(aId, "a");
        PhysicalOlapScan scan = mockScan(1, 1, 1, 1, ImmutableList.of(aSlot));

        Expression groupExpr = Mockito.mock(Expression.class);
        Mockito.when(groupExpr.getInputSlots()).thenReturn(ImmutableSet.of(aSlot));

        // Alias(Grouping(a), G): a non-scan-backed output whose own getInputSlots() is {a}.
        NamedExpression groupingExpr = Mockito.mock(NamedExpression.class);
        Mockito.when(groupingExpr.getExprId()).thenReturn(groupingId);
        Mockito.when(groupingExpr.getInputSlots()).thenReturn(ImmutableSet.of(aSlot));

        PhysicalRepeat<?> repeat = Mockito.mock(PhysicalRepeat.class);
        Mockito.when(repeat.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(repeat.getOutput()).thenReturn(ImmutableList.of());
        Mockito.when(repeat.getGroupingSets())
                .thenReturn(ImmutableList.of(ImmutableList.of(groupExpr)));
        Mockito.when(repeat.getOutputExpressions())
                .thenReturn((List<NamedExpression>) (List<?>) ImmutableList.of(groupingExpr));

        // HAVING GROUPING(a) = 1: filter references the repeat output's own ExprId directly.
        SlotReference groupingSlot = mockSlot(groupingId, "grouping_a");
        Expression havingConjunct = Mockito.mock(Expression.class);
        Mockito.when(havingConjunct.getInputSlots()).thenReturn(ImmutableSet.of(groupingSlot));

        PhysicalFilter<?> having = Mockito.mock(PhysicalFilter.class);
        Mockito.when(having.children()).thenReturn(ImmutableList.of(repeat));
        Mockito.when(having.getConjuncts()).thenReturn(ImmutableSet.of(havingConjunct));
        Mockito.when(having.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) having);
        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("a").filterHit,
                "a: HAVING GROUPING(a) = 1 resolves back through the repeat output's own lineage");
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

    // ── provenance resolver: multi-hop computed-slot chains ─────────────────

    /**
     * SELECT k1+1 FROM t: single-input computed alias at the query root.
     * Before the shared resolver, single-input aliases were registered directly in
     * exprIdToScan by the PhysicalProject handler, but the root output loop's fallback
     * only checked derivedSlotInputs — so this case silently recorded nothing.
     * Expected: k1.queryHit=true.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSingleInputComputedRootSelectRecordsQueryHit() {
        ExprId k1Id = new ExprId(1);
        ExprId aliasId = new ExprId(99);
        SlotReference k1Slot = mockSlot(k1Id, "k1");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot));

        // Simulates k1+1 — one input slot; the literal contributes no slot.
        Expression addExpr = Mockito.mock(Expression.class);
        Mockito.when(addExpr.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot));

        Alias alias = Mockito.mock(Alias.class);
        Mockito.when(alias.getExprId()).thenReturn(aliasId);
        Mockito.when(alias.child()).thenReturn(addExpr);

        PhysicalProject<?> project = Mockito.mock(PhysicalProject.class);
        Mockito.when(project.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(project.getProjects()).thenReturn(ImmutableList.of(alias));
        Mockito.when(project.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) project);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertNotNull(delta.getColumnStats().get("k1"),
                "k1 must be recorded from computed SELECT k1+1");
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit);
    }

    /**
     * WITH cte AS (SELECT k1+k2 AS x FROM t) SELECT x FROM cte: CTE consumer over a
     * computed producer column. Before the shared resolver, the CTE consumer branch only
     * copied a producer slot that was already a direct exprIdToScan entry — a computed
     * producer column (in derivedSlotInputs) was silently skipped.
     * Expected: k1.queryHit=true AND k2.queryHit=true.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testCteConsumerOverComputedProducerColumnRecordsQueryHit() {
        ExprId k1Id = new ExprId(1);
        ExprId k2Id = new ExprId(2);
        ExprId producerXId = new ExprId(10);
        ExprId consumerXId = new ExprId(20);

        SlotReference k1Slot = mockSlot(k1Id, "k1");
        SlotReference k2Slot = mockSlot(k2Id, "k2");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot, k2Slot));

        // Producer: SELECT k1+k2 AS x FROM t
        Expression addExpr = Mockito.mock(Expression.class);
        Mockito.when(addExpr.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot, k2Slot));
        Alias xAlias = Mockito.mock(Alias.class);
        Mockito.when(xAlias.getExprId()).thenReturn(producerXId);
        Mockito.when(xAlias.child()).thenReturn(addExpr);

        PhysicalProject<?> producerProject = Mockito.mock(PhysicalProject.class);
        Mockito.when(producerProject.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(producerProject.getProjects()).thenReturn(ImmutableList.of(xAlias));
        Mockito.when(producerProject.getOutput()).thenReturn(ImmutableList.of());

        PhysicalCTEProducer<?> producer = Mockito.mock(PhysicalCTEProducer.class);
        Mockito.when(producer.children()).thenReturn(ImmutableList.of(producerProject));

        SlotReference producerXSlot = mockSlot(producerXId, "x");
        SlotReference consumerXSlot = mockSlot(consumerXId, "x");

        PhysicalCTEConsumer consumer = Mockito.mock(PhysicalCTEConsumer.class);
        Mockito.when(consumer.getOutput()).thenReturn(ImmutableList.of(consumerXSlot));
        Mockito.when(consumer.getProducerSlot(consumerXSlot)).thenReturn(producerXSlot);
        Mockito.when(consumer.children()).thenReturn(ImmutableList.of());

        PhysicalFilter<?> root = Mockito.mock(PhysicalFilter.class);
        Mockito.when(root.children()).thenReturn(ImmutableList.of(producer, consumer));
        Mockito.when(root.getConjuncts()).thenReturn(ImmutableSet.of());
        Mockito.when(root.getOutput()).thenReturn(ImmutableList.of(consumerXSlot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) root);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertNotNull(delta.getColumnStats().get("k1"),
                "k1 must be recorded via CTE consumer over a computed producer column");
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit);
        Assertions.assertNotNull(delta.getColumnStats().get("k2"));
        Assertions.assertTrue(delta.getColumnStats().get("k2").queryHit);
    }

    /**
     * SELECT k1+k2 FROM t UNION ALL SELECT k1+k2 FROM t: a branch's contributing column is
     * itself a computed alias. Before the shared resolver, recordSetOpChildrenOutputs only
     * checked exprIdToScan directly and skipped computed branch outputs.
     * Expected: k1.queryHit=true AND k2.queryHit=true.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSetOperationComputedBranchColumnRecordsQueryHit() {
        ExprId k1Id = new ExprId(1);
        ExprId k2Id = new ExprId(2);
        ExprId branchAliasId = new ExprId(50);

        SlotReference k1Slot = mockSlot(k1Id, "k1");
        SlotReference k2Slot = mockSlot(k2Id, "k2");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot, k2Slot));

        // Branch: SELECT k1+k2 FROM t
        Expression addExpr = Mockito.mock(Expression.class);
        Mockito.when(addExpr.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot, k2Slot));
        Alias branchAlias = Mockito.mock(Alias.class);
        Mockito.when(branchAlias.getExprId()).thenReturn(branchAliasId);
        Mockito.when(branchAlias.child()).thenReturn(addExpr);

        PhysicalProject<?> branchProject = Mockito.mock(PhysicalProject.class);
        Mockito.when(branchProject.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(branchProject.getProjects()).thenReturn(ImmutableList.of(branchAlias));
        Mockito.when(branchProject.getOutput()).thenReturn(ImmutableList.of());

        // The set operation's own child-output slot at this position shares the alias's ExprId.
        SlotReference branchOutputSlot = mockSlot(branchAliasId, "k1 + k2");

        PhysicalSetOperation union = Mockito.mock(PhysicalSetOperation.class);
        Mockito.when(union.children()).thenReturn(ImmutableList.of(branchProject));
        Mockito.when(union.getRegularChildrenOutputs())
                .thenReturn(ImmutableList.of(ImmutableList.of(branchOutputSlot)));
        Mockito.when(union.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) union);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertNotNull(delta.getColumnStats().get("k1"),
                "k1 must be recorded via a computed UNION branch column");
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit);
        Assertions.assertNotNull(delta.getColumnStats().get("k2"));
        Assertions.assertTrue(delta.getColumnStats().get("k2").queryHit);
    }

    /**
     * SELECT SUM(x) FROM (SELECT k1+k2 AS x FROM t) s HAVING SUM(x) > 0: the aggregate's
     * single input (x) is itself a computed column. Before the shared resolver, the
     * single-input HAVING branch only checked exprIdToScan for that one input and never
     * fell back to derivedSlotInputs, so a nested computed input was silently dropped.
     * Expected: k1.filterHit=true AND k2.filterHit=true.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testNestedSingleInputHavingOverComputedColumnRecordsFilterHit() {
        ExprId k1Id = new ExprId(1);
        ExprId k2Id = new ExprId(2);
        ExprId xId = new ExprId(10);
        ExprId sumXId = new ExprId(20);

        SlotReference k1Slot = mockSlot(k1Id, "k1");
        SlotReference k2Slot = mockSlot(k2Id, "k2");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot, k2Slot));

        // Subquery: SELECT k1+k2 AS x FROM t
        Expression addExpr = Mockito.mock(Expression.class);
        Mockito.when(addExpr.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot, k2Slot));
        Alias xAlias = Mockito.mock(Alias.class);
        Mockito.when(xAlias.getExprId()).thenReturn(xId);
        Mockito.when(xAlias.child()).thenReturn(addExpr);

        PhysicalProject<?> subqueryProject = Mockito.mock(PhysicalProject.class);
        Mockito.when(subqueryProject.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(subqueryProject.getProjects()).thenReturn(ImmutableList.of(xAlias));
        Mockito.when(subqueryProject.getOutput()).thenReturn(ImmutableList.of());

        SlotReference xSlot = mockSlot(xId, "x");

        // Outer aggregate: SUM(x) — one input slot, but x is itself computed.
        NamedExpression sumExpr = Mockito.mock(NamedExpression.class);
        Mockito.when(sumExpr.getExprId()).thenReturn(sumXId);
        Mockito.when(sumExpr.getInputSlots()).thenReturn(ImmutableSet.of(xSlot));

        Aggregate<?> agg = Mockito.mock(Aggregate.class);
        Mockito.when(agg.children()).thenReturn(ImmutableList.of(subqueryProject));
        Mockito.when(agg.getGroupByExpressions()).thenReturn(ImmutableList.of());
        Mockito.when(agg.getOutputExpressions())
                .thenReturn((List<NamedExpression>) (List<?>) ImmutableList.of(sumExpr));
        Mockito.when(agg.getOutput()).thenReturn(ImmutableList.of());

        SlotReference sumSlot = mockSlot(sumXId, "sum_x");
        Expression havingConjunct = Mockito.mock(Expression.class);
        Mockito.when(havingConjunct.getInputSlots()).thenReturn(ImmutableSet.of(sumSlot));

        PhysicalFilter<?> having = Mockito.mock(PhysicalFilter.class);
        Mockito.when(having.children()).thenReturn(ImmutableList.of(agg));
        Mockito.when(having.getConjuncts()).thenReturn(ImmutableSet.of(havingConjunct));
        Mockito.when(having.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) having);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertNotNull(delta.getColumnStats().get("k1"),
                "k1 must get filterHit via HAVING SUM(x) where x is itself computed");
        Assertions.assertTrue(delta.getColumnStats().get("k1").filterHit);
        Assertions.assertNotNull(delta.getColumnStats().get("k2"));
        Assertions.assertTrue(delta.getColumnStats().get("k2").filterHit);
    }

    /**
     * SELECT * FROM (SELECT k1 FROM t1 UNION ALL SELECT k1 FROM t2) v WHERE k1 > 0, where
     * the filter sits above the set operation and references its output slot directly (the
     * shape PushDownFilterThroughSetOperation leaves in place for predicates it cannot push
     * into a branch, e.g. involving a volatile function). Before the shared resolver, the
     * set operation's own output ExprId was never linked to its branches, so a parent filter
     * referencing it resolved to nothing.
     * Expected: k1.filterHit=true on both underlying tables.
     */
    @Test
    public void testFilterAboveSetOperationResolvesToScanColumns() {
        ExprId leftId = new ExprId(1);
        ExprId rightId = new ExprId(3);
        ExprId setOutputId = new ExprId(50);

        SlotReference k1Left = mockSlot(leftId, "k1");
        SlotReference k1Right = mockSlot(rightId, "k1");
        PhysicalOlapScan scan1 = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Left));
        PhysicalOlapScan scan2 = mockScan(1L, 2L, 2L, 1L, ImmutableList.of(k1Right));

        SlotReference setOutputSlot = mockSlot(setOutputId, "k1");

        PhysicalSetOperation union = Mockito.mock(PhysicalSetOperation.class);
        Mockito.when(union.children()).thenReturn(ImmutableList.of(scan1, scan2));
        Mockito.when(union.getRegularChildrenOutputs())
                .thenReturn(ImmutableList.of(ImmutableList.of(k1Left), ImmutableList.of(k1Right)));
        Mockito.when(union.getOutput()).thenReturn(ImmutableList.of(setOutputSlot));

        // A filter kept above the set operation.
        Expression filterConjunct = Mockito.mock(Expression.class);
        Mockito.when(filterConjunct.getInputSlots()).thenReturn(ImmutableSet.of(setOutputSlot));

        PhysicalFilter<?> filter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(filter.children()).thenReturn(ImmutableList.of(union));
        Mockito.when(filter.getConjuncts()).thenReturn(ImmutableSet.of(filterConjunct));
        Mockito.when(filter.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) filter);

        Assertions.assertEquals(2, deltas.size());
        for (StatsDelta delta : deltas.values()) {
            Assertions.assertNotNull(delta.getColumnStats().get("k1"));
            Assertions.assertTrue(delta.getColumnStats().get("k1").filterHit,
                    "k1.filterHit must be true via a filter kept above the set operation");
        }
    }

    /**
     * SELECT * FROM (SELECT k1 FROM t1 EXCEPT SELECT k1 FROM t2) v WHERE k1 > 0: only t1's
     * values ever reach the EXCEPT output (t2 is scanned only to test exclusion), so a filter
     * kept above the EXCEPT must resolve to t1's column only, not t2's.
     */
    @Test
    public void testFilterAboveExceptOnlyResolvesToFirstBranch() {
        ExprId leftId = new ExprId(1);
        ExprId rightId = new ExprId(3);
        ExprId setOutputId = new ExprId(50);

        SlotReference k1Left = mockSlot(leftId, "k1");
        SlotReference k1Right = mockSlot(rightId, "k1");
        PhysicalOlapScan scan1 = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Left));
        PhysicalOlapScan scan2 = mockScan(1L, 2L, 2L, 1L, ImmutableList.of(k1Right));

        SlotReference setOutputSlot = mockSlot(setOutputId, "k1");

        PhysicalExcept except = Mockito.mock(PhysicalExcept.class);
        Mockito.when(except.children()).thenReturn(ImmutableList.of(scan1, scan2));
        Mockito.when(except.getRegularChildrenOutputs())
                .thenReturn(ImmutableList.of(ImmutableList.of(k1Left), ImmutableList.of(k1Right)));
        Mockito.when(except.getOutput()).thenReturn(ImmutableList.of(setOutputSlot));

        Expression filterConjunct = Mockito.mock(Expression.class);
        Mockito.when(filterConjunct.getInputSlots()).thenReturn(ImmutableSet.of(setOutputSlot));

        PhysicalFilter<?> filter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(filter.children()).thenReturn(ImmutableList.of(except));
        Mockito.when(filter.getConjuncts()).thenReturn(ImmutableSet.of(filterConjunct));
        Mockito.when(filter.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) filter);

        // Both branches still get queryHit (they're both genuinely scanned/compared), but only
        // the first branch's column gets filterHit from the filter kept above the EXCEPT.
        Assertions.assertEquals(2, deltas.size());
        StatsDelta leftDelta = deltas.get("1_1_1_1");
        StatsDelta rightDelta = deltas.get("1_2_2_1");
        Assertions.assertTrue(leftDelta.getColumnStats().get("k1").queryHit);
        Assertions.assertTrue(leftDelta.getColumnStats().get("k1").filterHit,
                "k1.filterHit must be true on the first (kept) EXCEPT branch");
        Assertions.assertTrue(rightDelta.getColumnStats().get("k1").queryHit);
        Assertions.assertFalse(rightDelta.getColumnStats().get("k1").filterHit,
                "k1.filterHit must NOT be attributed to the second (subtracted-away) EXCEPT branch");
    }

    /**
     * SELECT * FROM (SELECT k1 FROM t1 INTERSECT SELECT k1 FROM t2) v WHERE k1 > 0: like EXCEPT,
     * INTERSECT's output block is materialized only from the build-side (first) branch at
     * execution time (SetSourceOperatorX::_add_result_columns reads only build_block; probe
     * branches only flip a visited bit) — so a filter kept above the INTERSECT must resolve to
     * the first branch's column only, not the second.
     */
    @Test
    public void testFilterAboveIntersectOnlyResolvesToFirstBranch() {
        ExprId leftId = new ExprId(1);
        ExprId rightId = new ExprId(3);
        ExprId setOutputId = new ExprId(50);

        SlotReference k1Left = mockSlot(leftId, "k1");
        SlotReference k1Right = mockSlot(rightId, "k1");
        PhysicalOlapScan scan1 = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Left));
        PhysicalOlapScan scan2 = mockScan(1L, 2L, 2L, 1L, ImmutableList.of(k1Right));

        SlotReference setOutputSlot = mockSlot(setOutputId, "k1");

        PhysicalIntersect intersect = Mockito.mock(PhysicalIntersect.class);
        Mockito.when(intersect.children()).thenReturn(ImmutableList.of(scan1, scan2));
        Mockito.when(intersect.getRegularChildrenOutputs())
                .thenReturn(ImmutableList.of(ImmutableList.of(k1Left), ImmutableList.of(k1Right)));
        Mockito.when(intersect.getOutput()).thenReturn(ImmutableList.of(setOutputSlot));

        Expression filterConjunct = Mockito.mock(Expression.class);
        Mockito.when(filterConjunct.getInputSlots()).thenReturn(ImmutableSet.of(setOutputSlot));

        PhysicalFilter<?> filter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(filter.children()).thenReturn(ImmutableList.of(intersect));
        Mockito.when(filter.getConjuncts()).thenReturn(ImmutableSet.of(filterConjunct));
        Mockito.when(filter.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) filter);

        // Both branches still get queryHit (they're both genuinely scanned/compared), but only
        // the first (build-side) branch's column gets filterHit from the filter kept above.
        Assertions.assertEquals(2, deltas.size());
        StatsDelta leftDelta = deltas.get("1_1_1_1");
        StatsDelta rightDelta = deltas.get("1_2_2_1");
        Assertions.assertTrue(leftDelta.getColumnStats().get("k1").queryHit);
        Assertions.assertTrue(leftDelta.getColumnStats().get("k1").filterHit,
                "k1.filterHit must be true on the first (build-side) INTERSECT branch");
        Assertions.assertTrue(rightDelta.getColumnStats().get("k1").queryHit);
        Assertions.assertFalse(rightDelta.getColumnStats().get("k1").filterHit,
                "k1.filterHit must NOT be attributed to the second (probe-side) INTERSECT branch");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /**
     * Plan: CTEConsumer (consumerSlot#3 → producerSlot#1=k1) → Scan[k1(#1)]
     * Expected: k1.queryHit=true via consumer slot.
     */
    @Test
    public void testCteConsumerMapsToProducerScan() {
        ExprId prodId = new ExprId(1);
        ExprId consId = new ExprId(3);
        SlotReference prodSlot = mockSlot(prodId, "k1");
        SlotReference consSlot = mockSlot(consId, "k1");

        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(prodSlot));

        PhysicalCTEConsumer consumer = Mockito.mock(PhysicalCTEConsumer.class);
        Mockito.when(consumer.getOutput()).thenReturn(ImmutableList.of(consSlot));
        Mockito.when(consumer.getProducerSlot(consSlot)).thenReturn(prodSlot);
        Mockito.when(consumer.children()).thenReturn(ImmutableList.of());

        // Root plan: scan (producer) visited first, consumer slots then get registered.
        PhysicalFilter<?> root = Mockito.mock(PhysicalFilter.class);
        Mockito.when(root.children()).thenReturn(ImmutableList.of(scan, consumer));
        Mockito.when(root.getConjuncts()).thenReturn(ImmutableSet.of());
        Mockito.when(root.getOutput()).thenReturn(ImmutableList.of(consSlot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) root);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertNotNull(delta.getColumnStats().get("k1"));
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit);
    }

    /**
     * Plan: UNION of Scan1[k1(#1)] and Scan2[k1(#3)]
     * Expected: k1.queryHit=true on both scan tables.
     */
    @Test
    public void testUnionRecordsQueryHitOnAllBranches() {
        ExprId id1 = new ExprId(1);
        ExprId id3 = new ExprId(3);
        SlotReference k1Left = mockSlot(id1, "k1");
        SlotReference k1Right = mockSlot(id3, "k1");

        PhysicalOlapScan scan1 = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Left));
        PhysicalOlapScan scan2 = mockScan(1L, 2L, 2L, 1L, ImmutableList.of(k1Right));

        PhysicalSetOperation union = Mockito.mock(PhysicalSetOperation.class);
        Mockito.when(union.children()).thenReturn(ImmutableList.of(scan1, scan2));
        Mockito.when(union.getRegularChildrenOutputs())
                .thenReturn(ImmutableList.of(ImmutableList.of(k1Left), ImmutableList.of(k1Right)));
        Mockito.when(union.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) union);

        Assertions.assertEquals(2, deltas.size());
        for (StatsDelta delta : deltas.values()) {
            Assertions.assertNotNull(delta.getColumnStats().get("k1"));
            Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit);
        }
    }

    /**
     * Plan: Filter(SUM(k2)#5 > 0) → Agg[SUM(k2)] → Scan[k2(#2)]
     * Expected: k2.filterHit=true from HAVING SUM(k2) > 0.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testHavingAggregateFilterHitRecorded() {
        ExprId k2Id = new ExprId(2);
        ExprId sumId = new ExprId(5);
        SlotReference k2Slot = mockSlot(k2Id, "k2");
        // Aggregate output slots have no originalColumn in real Doris; use Optional.empty()
        // so recordInputSlotsAsFilterHit falls back to exprIdToColName.get(sumId) = "k2".
        SlotReference sumSlot = Mockito.mock(SlotReference.class);
        Mockito.when(sumSlot.getExprId()).thenReturn(sumId);
        Mockito.when(sumSlot.getOriginalColumn()).thenReturn(Optional.empty());

        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k2Slot));

        NamedExpression sumExpr = Mockito.mock(NamedExpression.class);
        Mockito.when(sumExpr.getExprId()).thenReturn(sumId);
        Mockito.when(sumExpr.getInputSlots()).thenReturn(ImmutableSet.of(k2Slot));

        Aggregate<?> agg = Mockito.mock(Aggregate.class);
        Mockito.when(agg.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(agg.getGroupByExpressions()).thenReturn(ImmutableList.of());
        Mockito.when(agg.getOutputExpressions())
                .thenReturn((List<NamedExpression>) (List<?>) ImmutableList.of(sumExpr));
        Mockito.when(agg.getOutput()).thenReturn(ImmutableList.of(sumSlot));

        Expression havingConjunct = Mockito.mock(Expression.class);
        Mockito.when(havingConjunct.getInputSlots()).thenReturn(ImmutableSet.of(sumSlot));

        PhysicalFilter<?> havingFilter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(havingFilter.children()).thenReturn(ImmutableList.of(agg));
        Mockito.when(havingFilter.getConjuncts()).thenReturn(ImmutableSet.of(havingConjunct));
        Mockito.when(havingFilter.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas =
                QueryStatsRecorder.collectDeltas((PhysicalPlan) havingFilter);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertNotNull(delta.getColumnStats().get("k2"));
        Assertions.assertTrue(delta.getColumnStats().get("k2").filterHit);
    }

    /**
     * Plan: Generate(EXPLODE(tags#2)) → Scan[tags(#2)]
     * Expected: tags.queryHit=true from the generator input column.
     */
    @Test
    public void testLateralViewExplodeRecordsGeneratorInputAsQueryHit() {
        ExprId tagsId = new ExprId(2);
        SlotReference tagsSlot = mockSlot(tagsId, "tags");

        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(tagsSlot));

        Function explodeFn = Mockito.mock(Function.class);
        Mockito.when(explodeFn.getInputSlots()).thenReturn(ImmutableSet.of(tagsSlot));

        PhysicalGenerate<?> generate = Mockito.mock(PhysicalGenerate.class);
        Mockito.when(generate.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(generate.getGenerators()).thenReturn(ImmutableList.of(explodeFn));
        Mockito.when(generate.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas =
                QueryStatsRecorder.collectDeltas((PhysicalPlan) generate);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertNotNull(delta.getColumnStats().get("tags"));
        Assertions.assertTrue(delta.getColumnStats().get("tags").queryHit);
        Assertions.assertFalse(delta.getColumnStats().get("tags").filterHit);
    }

    /**
     * SELECT e1 FROM t LATERAL VIEW EXPLODE(arr) tmp AS e1 WHERE e1 > 100: the exploded output
     * slot e1 must resolve back to the real scan column (arr) for a filter above the generate.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testGeneratorOutputSlotResolvesFilterToScanColumn() {
        ExprId arrId = new ExprId(2);
        ExprId e1Id = new ExprId(3);
        SlotReference arrSlot = mockSlot(arrId, "arr");
        SlotReference e1Slot = mockSlot(e1Id, "e1");

        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(arrSlot));

        Function explodeFn = Mockito.mock(Function.class);
        Mockito.when(explodeFn.getInputSlots()).thenReturn(ImmutableSet.of(arrSlot));

        PhysicalGenerate<?> generate = Mockito.mock(PhysicalGenerate.class);
        Mockito.when(generate.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(generate.getGenerators()).thenReturn(ImmutableList.of(explodeFn));
        Mockito.when(generate.getGeneratorOutput()).thenReturn(ImmutableList.of(e1Slot));
        Mockito.when(generate.getConjuncts()).thenReturn(ImmutableList.of());
        Mockito.when(generate.getOutput()).thenReturn(ImmutableList.of(e1Slot));

        Expression filterConjunct = Mockito.mock(Expression.class);
        Mockito.when(filterConjunct.getInputSlots()).thenReturn(ImmutableSet.of(e1Slot));

        PhysicalFilter<?> filter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(filter.children()).thenReturn(ImmutableList.of(generate));
        Mockito.when(filter.getConjuncts()).thenReturn(ImmutableSet.of(filterConjunct));
        Mockito.when(filter.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) filter);

        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta);
        Assertions.assertTrue(delta.getColumnStats().get("arr").filterHit,
                "arr: filter on the exploded output slot resolves back to the real scan column");
    }

    /**
     * Plan: PhysicalCTEProducer(Scan[k1(#1)]) + CTEConsumer(consSlot#3 → prodSlot#1)
     * PhysicalCTEProducer must walk its child so the scan is registered before the
     * consumer is processed by the sibling walk.
     * Expected: k1.queryHit=true via consumer slot resolved through producer scan.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testCTEProducerRegistersScansForConsumer() {
        ExprId prodId = new ExprId(1);
        ExprId consId = new ExprId(3);
        SlotReference prodSlot = mockSlot(prodId, "k1");
        SlotReference consSlot = mockSlot(consId, "k1");

        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(prodSlot));

        PhysicalCTEProducer<?> producer = Mockito.mock(PhysicalCTEProducer.class);
        Mockito.when(producer.children()).thenReturn(ImmutableList.of(scan));

        PhysicalCTEConsumer consumer = Mockito.mock(PhysicalCTEConsumer.class);
        Mockito.when(consumer.getOutput()).thenReturn(ImmutableList.of(consSlot));
        Mockito.when(consumer.getProducerSlot(consSlot)).thenReturn(prodSlot);
        Mockito.when(consumer.children()).thenReturn(ImmutableList.of());

        PhysicalPlan root = Mockito.mock(PhysicalPlan.class);
        Mockito.when(root.children()).thenReturn(ImmutableList.of(producer, consumer));
        Mockito.when(root.getOutput()).thenReturn(ImmutableList.of(consSlot));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas(root);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertNotNull(delta.getColumnStats().get("k1"),
                "k1 must be registered via PhysicalCTEProducer→scan path");
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit,
                "k1.queryHit must be true via CTE producer→scan→consumer chain");
    }

    /**
     * Plan: Filter(SUM(k2+k3)#5 > 0) → Agg[SUM(k2+k3)] → Scan[k2(#2), k3(#3)]
     * Multi-input aggregate output is stored in derivedSlotInputs so that a HAVING
     * filter on SUM(k2+k3) records filterHit on both k2 and k3.
     * Expected: k2.filterHit=true AND k3.filterHit=true.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testHavingMultiInputAggregateFilterHitRecorded() {
        ExprId k2Id = new ExprId(2);
        ExprId k3Id = new ExprId(3);
        ExprId sumId = new ExprId(5);
        SlotReference k2Slot = mockSlot(k2Id, "k2");
        SlotReference k3Slot = mockSlot(k3Id, "k3");
        SlotReference sumSlot = mockSlot(sumId, "sum_k2_k3");

        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k2Slot, k3Slot));

        // SUM(k2+k3): two input slots → must go into derivedSlotInputs
        NamedExpression sumExpr = Mockito.mock(NamedExpression.class);
        Mockito.when(sumExpr.getExprId()).thenReturn(sumId);
        Mockito.when(sumExpr.getInputSlots()).thenReturn(ImmutableSet.of(k2Slot, k3Slot));

        Aggregate<?> agg = Mockito.mock(Aggregate.class);
        Mockito.when(agg.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(agg.getGroupByExpressions()).thenReturn(ImmutableList.of());
        Mockito.when(agg.getOutputExpressions())
                .thenReturn((List<NamedExpression>) (List<?>) ImmutableList.of(sumExpr));
        Mockito.when(agg.getOutput()).thenReturn(ImmutableList.of(sumSlot));

        // HAVING SUM(k2+k3) > 0: conjunct references the aggregate output slot
        Expression havingConjunct = Mockito.mock(Expression.class);
        Mockito.when(havingConjunct.getInputSlots()).thenReturn(ImmutableSet.of(sumSlot));

        PhysicalFilter<?> havingFilter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(havingFilter.children()).thenReturn(ImmutableList.of(agg));
        Mockito.when(havingFilter.getConjuncts()).thenReturn(ImmutableSet.of(havingConjunct));
        Mockito.when(havingFilter.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas =
                QueryStatsRecorder.collectDeltas((PhysicalPlan) havingFilter);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertNotNull(delta.getColumnStats().get("k2"),
                "k2 must be recorded via multi-input HAVING expansion");
        Assertions.assertTrue(delta.getColumnStats().get("k2").filterHit,
                "k2.filterHit must be true from HAVING SUM(k2+k3)");
        Assertions.assertNotNull(delta.getColumnStats().get("k3"),
                "k3 must be recorded via multi-input HAVING expansion");
        Assertions.assertTrue(delta.getColumnStats().get("k3").filterHit,
                "k3.filterHit must be true from HAVING SUM(k2+k3)");
    }

    /**
     * Mark join conjuncts (from IN/EXISTS subquery rewriting) are stored separately
     * in AbstractPhysicalJoin and must be processed independently of hash/other conjuncts.
     * Plan: HashJoin with k1=k2 in hashJoinConjuncts and k3>0 in markJoinConjuncts.
     * Expected: k1.filterHit, k2.filterHit (hash), k3.filterHit (mark).
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testMarkJoinConjunctsRecordFilterHit() {
        ExprId id1 = new ExprId(1);
        ExprId id2 = new ExprId(2);
        ExprId id3 = new ExprId(3);
        SlotReference k1Slot = mockSlot(id1, "k1");
        SlotReference k2Slot = mockSlot(id2, "k2");
        SlotReference k3Slot = mockSlot(id3, "k3");

        PhysicalOlapScan left  = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot, k3Slot));
        PhysicalOlapScan right = mockScan(2L, 2L, 2L, 2L, ImmutableList.of(k2Slot));

        Expression hashConjunct = Mockito.mock(Expression.class);
        Mockito.when(hashConjunct.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot, k2Slot));

        // Mark conjunct: k3 > 0 — only reachable via getMarkJoinConjuncts()
        Expression markConjunct = Mockito.mock(Expression.class);
        Mockito.when(markConjunct.getInputSlots()).thenReturn(ImmutableSet.of(k3Slot));

        org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin<?, ?> join =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin.class);
        Mockito.when(join.children()).thenReturn(ImmutableList.of(left, right));
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(ImmutableList.of(hashConjunct));
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(ImmutableList.of());
        Mockito.when(join.getMarkJoinConjuncts()).thenReturn(ImmutableList.of(markConjunct));
        Mockito.when(join.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) join);

        Assertions.assertTrue(deltas.containsKey("1_1_1_1"), "left scan delta missing");
        Assertions.assertTrue(deltas.containsKey("2_2_2_2"), "right scan delta missing");
        Assertions.assertTrue(deltas.get("1_1_1_1").getColumnStats().get("k1").filterHit,
                "k1.filterHit: hash join conjunct");
        Assertions.assertTrue(deltas.get("2_2_2_2").getColumnStats().get("k2").filterHit,
                "k2.filterHit: hash join conjunct");
        Assertions.assertTrue(deltas.get("1_1_1_1").getColumnStats().get("k3").filterHit,
                "k3.filterHit: mark join conjunct — previously missed");
    }

    /**
     * Other join conjuncts (non-equi predicates, e.g. a range condition in a nested-loop join)
     * are stored separately from hash/mark conjuncts and must be processed too.
     * Plan: NestedLoopJoin with k1=k2 in hashJoinConjuncts and k3<k4 in otherJoinConjuncts.
     * Expected: k1/k2.filterHit (hash), k3/k4.filterHit (other) — every existing join test in
     * this suite stubs getOtherJoinConjuncts() empty, so this path was previously untested.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testOtherJoinConjunctsRecordFilterHit() {
        ExprId id1 = new ExprId(1);
        ExprId id2 = new ExprId(2);
        ExprId id3 = new ExprId(3);
        ExprId id4 = new ExprId(4);
        SlotReference k1Slot = mockSlot(id1, "k1");
        SlotReference k2Slot = mockSlot(id2, "k2");
        SlotReference k3Slot = mockSlot(id3, "k3");
        SlotReference k4Slot = mockSlot(id4, "k4");

        PhysicalOlapScan left  = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot, k3Slot));
        PhysicalOlapScan right = mockScan(2L, 2L, 2L, 2L, ImmutableList.of(k2Slot, k4Slot));

        Expression hashConjunct = Mockito.mock(Expression.class);
        Mockito.when(hashConjunct.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot, k2Slot));

        // Other (non-equi) conjunct: k3 < k4 — only reachable via getOtherJoinConjuncts()
        Expression otherConjunct = Mockito.mock(Expression.class);
        Mockito.when(otherConjunct.getInputSlots()).thenReturn(ImmutableSet.of(k3Slot, k4Slot));

        org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin<?, ?> join =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin.class);
        Mockito.when(join.children()).thenReturn(ImmutableList.of(left, right));
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(ImmutableList.of(hashConjunct));
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(ImmutableList.of(otherConjunct));
        Mockito.when(join.getMarkJoinConjuncts()).thenReturn(ImmutableList.of());
        Mockito.when(join.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) join);

        Assertions.assertTrue(deltas.containsKey("1_1_1_1"), "left scan delta missing");
        Assertions.assertTrue(deltas.containsKey("2_2_2_2"), "right scan delta missing");
        Assertions.assertTrue(deltas.get("1_1_1_1").getColumnStats().get("k1").filterHit,
                "k1.filterHit: hash join conjunct");
        Assertions.assertTrue(deltas.get("2_2_2_2").getColumnStats().get("k2").filterHit,
                "k2.filterHit: hash join conjunct");
        Assertions.assertTrue(deltas.get("1_1_1_1").getColumnStats().get("k3").filterHit,
                "k3.filterHit: other join conjunct — previously untested");
        Assertions.assertTrue(deltas.get("2_2_2_2").getColumnStats().get("k4").filterHit,
                "k4.filterHit: other join conjunct — previously untested");
    }

    /**
     * PhysicalRecursiveUnion extends PhysicalBinary: left() is the base case, right() is the
     * recursive case. The base-case slots (from an OlapScan) must get queryHit.
     * Expected: k1.queryHit=true (base case only, exactly one scan delta).
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testRecursiveUnionBaseColumnGetsQueryHit() {
        ExprId baseScanId = new ExprId(1);
        ExprId recursiveId = new ExprId(5); // WorkTableReference slot, no CTEId link established
        SlotReference baseScanSlot   = mockSlot(baseScanId, "k1");
        SlotReference recursiveSlot  = mockSlot(recursiveId, "k1");

        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(baseScanSlot));

        // Mock the recursive child (not a PhysicalWorkTableReference, no CTEId link is set up).
        Plan recursiveChild = Mockito.mock(Plan.class);
        Mockito.when(recursiveChild.children()).thenReturn(ImmutableList.of());
        Mockito.when(recursiveChild.getOutput()).thenReturn(ImmutableList.of(recursiveSlot));

        PhysicalRecursiveUnion<Plan, Plan> recUnion = Mockito.mock(PhysicalRecursiveUnion.class);
        Mockito.when(recUnion.left()).thenReturn((Plan) scan);
        Mockito.when(recUnion.right()).thenReturn(recursiveChild);
        Mockito.when(recUnion.getRegularChildrenOutputs()).thenReturn(
                ImmutableList.of(
                        ImmutableList.of(baseScanSlot),   // base case: resolves to scan
                        ImmutableList.of(recursiveSlot)   // recursive case: no work-table CTEId link
                ));
        Mockito.when(recUnion.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) recUnion);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta, "base-case scan delta must exist");
        Assertions.assertNotNull(delta.getColumnStats().get("k1"),
                "k1 must be recorded from base case via getRegularChildrenOutputs");
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit,
                "k1.queryHit must be true for recursive union base case");
        // Exactly one delta entry — recursive-case slot (#5) must not create a second scan entry.
        Assertions.assertEquals(1, deltas.size(), "only base-case scan must appear in deltas");
    }

    /**
     * SELECT * FROM t1 WHERE k1 = 1 UNION ALL SELECT * FROM cte WHERE k1 = 2 (recursive branch,
     * filtering on the work-table's own re-scanned column). The base case is wrapped in a
     * PhysicalRecursiveUnionAnchor exposing a CTEId; the recursive case contains a filter over a
     * PhysicalWorkTableReference with that same CTEId. Before this fix, the work-table slot's
     * ExprId had no entry in derivedSlotInputs by the time the recursive branch's own filter was
     * walked, so k1.filterHit could never be recorded for the recursive-side predicate.
     * Expected: k1.filterHit=true (recursive-side WHERE resolves back to the real scan column).
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFilterOnWorkTableSlotInRecursiveBranchRecordsFilterHit() {
        CTEId cteId = new CTEId(1);
        ExprId baseScanId = new ExprId(1);
        ExprId workTableId = new ExprId(5);
        SlotReference baseScanSlot = mockSlot(baseScanId, "k1");
        SlotReference workTableSlot = mockSlot(workTableId, "k1");

        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(baseScanSlot));

        PhysicalRecursiveUnionAnchor<?> anchor = Mockito.mock(PhysicalRecursiveUnionAnchor.class);
        Mockito.when(anchor.getCteId()).thenReturn(cteId);
        Mockito.when(anchor.children()).thenReturn(ImmutableList.of(scan));

        PhysicalWorkTableReference workTable = Mockito.mock(PhysicalWorkTableReference.class);
        Mockito.when(workTable.getCteId()).thenReturn(cteId);
        Mockito.when(workTable.getOutput()).thenReturn(ImmutableList.of(workTableSlot));
        Mockito.when(workTable.children()).thenReturn(ImmutableList.of());

        // WHERE k1 = 2 inside the recursive branch, referencing the work-table's own slot.
        Expression recursiveFilterExpr = Mockito.mock(Expression.class);
        Mockito.when(recursiveFilterExpr.getInputSlots()).thenReturn(ImmutableSet.of(workTableSlot));

        PhysicalFilter<?> recursiveFilter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(recursiveFilter.children()).thenReturn(ImmutableList.of(workTable));
        Mockito.when(recursiveFilter.getConjuncts()).thenReturn(ImmutableSet.of(recursiveFilterExpr));

        PhysicalRecursiveUnion<Plan, Plan> recUnion = Mockito.mock(PhysicalRecursiveUnion.class);
        Mockito.when(recUnion.left()).thenReturn((Plan) anchor);
        Mockito.when(recUnion.right()).thenReturn(recursiveFilter);
        Mockito.when(recUnion.getRegularChildrenOutputs()).thenReturn(
                ImmutableList.of(
                        ImmutableList.of(baseScanSlot),
                        ImmutableList.of(workTableSlot)));
        Mockito.when(recUnion.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) recUnion);

        StatsDelta delta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(delta, "base scan delta must exist");
        Assertions.assertTrue(delta.getColumnStats().get("k1").filterHit,
                "recursive-branch filter on work-table slot must resolve back to the real scan column");
    }

    /**
     * SELECT k1+k2 AS result FROM t: computed alias with two input slots.
     * unwrapAlias returns null (not a plain SlotReference), so the PhysicalProject handler
     * stores {k1, k2} in the expansion map (derivedSlotInputs). The root output loop in
     * collectDeltas then expands via that map and records k1.queryHit and k2.queryHit.
     * Expected: k1.queryHit=true AND k2.queryHit=true.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testComputedProjectExpressionRecordsBothInputsAsQueryHit() {
        ExprId id1 = new ExprId(1);
        ExprId id2 = new ExprId(2);
        ExprId aliasId = new ExprId(99);
        SlotReference k1Slot = mockSlot(id1, "k1");
        SlotReference k2Slot = mockSlot(id2, "k2");
        PhysicalOlapScan scan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot, k2Slot));

        // Simulates k1+k2 — a binary expression with two input slots
        Expression addExpr = Mockito.mock(Expression.class);
        Mockito.when(addExpr.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot, k2Slot));

        org.apache.doris.nereids.trees.expressions.Alias alias =
                Mockito.mock(org.apache.doris.nereids.trees.expressions.Alias.class);
        Mockito.when(alias.getExprId()).thenReturn(aliasId);
        Mockito.when(alias.child()).thenReturn(addExpr);

        org.apache.doris.nereids.trees.plans.physical.PhysicalProject<?> project =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalProject.class);
        Mockito.when(project.children()).thenReturn(ImmutableList.of(scan));
        Mockito.when(project.getProjects()).thenReturn(ImmutableList.of(alias));
        Mockito.when(project.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas(
                (org.apache.doris.nereids.trees.plans.physical.PhysicalPlan) project);

        Assertions.assertEquals(1, deltas.size());
        StatsDelta delta = deltas.values().iterator().next();
        Assertions.assertNotNull(delta.getColumnStats().get("k1"),
                "k1 must be recorded as queryHit from computed SELECT k1+k2");
        Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit,
                "k1.queryHit must be true");
        Assertions.assertNotNull(delta.getColumnStats().get("k2"),
                "k2 must be recorded as queryHit from computed SELECT k1+k2");
        Assertions.assertTrue(delta.getColumnStats().get("k2").queryHit,
                "k2.queryHit must be true");
    }

    /**
     * PushDownExpressionsInHashCondition inserts a PhysicalProject below the join with
     * Alias(k1+k2) as the hash-join key. Walking this intermediate project must NOT record
     * k1/k2 as queryHit; instead, the join hash conjunct (which references the alias slot)
     * must expand via the map and record k1.filterHit and k2.filterHit.
     * Plan: HashJoin(aliasSlot = k3) → Project([Alias(k1+k2)]) → Scan[k1,k2]; Scan[k3]
     * Expected: k1.filterHit=true, k2.filterHit=true, k1.queryHit=false, k2.queryHit=false.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testIntermediateProjectComputedJoinKeyRecordsFilterHitNotQueryHit() {
        ExprId id1 = new ExprId(1);
        ExprId id2 = new ExprId(2);
        ExprId id3 = new ExprId(3);
        ExprId aliasId = new ExprId(99);
        SlotReference k1Slot = mockSlot(id1, "k1");
        SlotReference k2Slot = mockSlot(id2, "k2");
        SlotReference k3Slot = mockSlot(id3, "k3");
        PhysicalOlapScan leftScan = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Slot, k2Slot));
        PhysicalOlapScan rightScan = mockScan(2L, 2L, 2L, 2L, ImmutableList.of(k3Slot));

        // Simulates k1+k2 — a binary expression with two input slots
        Expression addExpr = Mockito.mock(Expression.class);
        Mockito.when(addExpr.getInputSlots()).thenReturn(ImmutableSet.of(k1Slot, k2Slot));

        org.apache.doris.nereids.trees.expressions.Alias alias =
                Mockito.mock(org.apache.doris.nereids.trees.expressions.Alias.class);
        Mockito.when(alias.getExprId()).thenReturn(aliasId);
        Mockito.when(alias.child()).thenReturn(addExpr);

        // The slot the join conjunct references (the alias output, not k1/k2 directly)
        SlotReference aliasSlot = mockSlot(aliasId, "k1_plus_k2");

        org.apache.doris.nereids.trees.plans.physical.PhysicalProject<?> project =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalProject.class);
        Mockito.when(project.children()).thenReturn(ImmutableList.of(leftScan));
        Mockito.when(project.getProjects()).thenReturn(ImmutableList.of(alias));
        Mockito.when(project.getOutput()).thenReturn(ImmutableList.of(aliasSlot));

        // Hash conjunct: (k1+k2) = k3 — references aliasSlot, not k1/k2 directly
        Expression hashConjunct = Mockito.mock(Expression.class);
        Mockito.when(hashConjunct.getInputSlots()).thenReturn(ImmutableSet.of(aliasSlot, k3Slot));

        org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin<?, ?> join =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin.class);
        Mockito.when(join.children()).thenReturn(ImmutableList.of(project, rightScan));
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(ImmutableList.of(hashConjunct));
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(ImmutableList.of());
        Mockito.when(join.getOutput()).thenReturn(ImmutableList.of());

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) join);

        Assertions.assertTrue(deltas.containsKey("1_1_1_1"), "left scan delta must exist");
        StatsDelta leftDelta = deltas.get("1_1_1_1");
        Assertions.assertNotNull(leftDelta.getColumnStats().get("k1"),
                "k1 must be recorded via join-key alias expansion");
        Assertions.assertTrue(leftDelta.getColumnStats().get("k1").filterHit,
                "k1.filterHit must be true: used only in join predicate");
        Assertions.assertFalse(leftDelta.getColumnStats().get("k1").queryHit,
                "k1.queryHit must be false: intermediate project must not record queryHit");
        Assertions.assertNotNull(leftDelta.getColumnStats().get("k2"),
                "k2 must be recorded via join-key alias expansion");
        Assertions.assertTrue(leftDelta.getColumnStats().get("k2").filterHit,
                "k2.filterHit must be true: used only in join predicate");
        Assertions.assertFalse(leftDelta.getColumnStats().get("k2").queryHit,
                "k2.queryHit must be false: intermediate project must not record queryHit");
    }

    /**
     * SELECT u.k1 FROM (SELECT k1, k2 FROM t1 EXCEPT SELECT k1, k2 FROM t2) u
     * The outer PhysicalProject only selects k1; k2 is never read by the visible query.
     * Adversarial check: does k2 still get queryHit=true via recordSetOpChildrenOutputs?
     */
    @Test
    public void adversarialExceptUnusedColumnShouldNotGetQueryHit() {
        ExprId k1LeftId = new ExprId(1);
        ExprId k2LeftId = new ExprId(2);
        ExprId k1RightId = new ExprId(3);
        ExprId k2RightId = new ExprId(4);

        SlotReference k1Left = mockSlot(k1LeftId, "k1");
        SlotReference k2Left = mockSlot(k2LeftId, "k2");
        SlotReference k1Right = mockSlot(k1RightId, "k1");
        SlotReference k2Right = mockSlot(k2RightId, "k2");

        PhysicalOlapScan scan1 = mockScan(1L, 1L, 1L, 1L, ImmutableList.of(k1Left, k2Left));
        PhysicalOlapScan scan2 = mockScan(1L, 2L, 2L, 1L, ImmutableList.of(k1Right, k2Right));

        ExprId setK1Id = new ExprId(10);
        ExprId setK2Id = new ExprId(11);
        SlotReference setK1 = mockSlot(setK1Id, "k1");
        SlotReference setK2 = mockSlot(setK2Id, "k2");

        PhysicalSetOperation except = Mockito.mock(PhysicalSetOperation.class);
        Mockito.when(except.children()).thenReturn(ImmutableList.of(scan1, scan2));
        Mockito.when(except.getRegularChildrenOutputs())
                .thenReturn(ImmutableList.of(
                        ImmutableList.of(k1Left, k2Left),
                        ImmutableList.of(k1Right, k2Right)));
        Mockito.when(except.getOutput()).thenReturn(ImmutableList.of(setK1, setK2));

        // Outer PhysicalProject: SELECT u.k1 -- only k1 is a pass-through, k2 is dropped.
        PhysicalProject<?> outerProject = Mockito.mock(PhysicalProject.class);
        Mockito.when(outerProject.children()).thenReturn(ImmutableList.of(except));
        Mockito.when(outerProject.getProjects()).thenReturn(ImmutableList.of(setK1));
        Mockito.when(outerProject.getOutput()).thenReturn(ImmutableList.of(setK1));

        Map<String, StatsDelta> deltas = QueryStatsRecorder.collectDeltas((PhysicalPlan) outerProject);

        Assertions.assertEquals(2, deltas.size());
        for (StatsDelta delta : deltas.values()) {
            Assertions.assertNotNull(delta.getColumnStats().get("k1"));
            Assertions.assertTrue(delta.getColumnStats().get("k1").queryHit,
                    "k1 is selected by the outer query and must get queryHit");
            if (delta.getColumnStats().containsKey("k2")) {
                Assertions.assertFalse(delta.getColumnStats().get("k2").queryHit,
                        "k2 is never selected by the outer query and must NOT get queryHit");
            }
        }
    }

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

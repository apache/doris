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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.builder.SPMExprSqlBuilder;
import org.apache.doris.nereids.spm.builder.SPMPlan2SQLBuilder;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.spm.matcher.PlaceholderBindings;
import org.apache.doris.nereids.spm.matcher.SPMAstCheckVisitor;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.spm.placeholder.TestSPMPlaceholderBuilder;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUsingJoin;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/** Verifies traversal bounds and state isolation of SPM performance paths. */
public class SPMPerformanceTest {
    private static class CountingRow extends UnboundOneRowRelation {
        private final AtomicInteger reads;
        private List<Plan> extra = List.of();

        CountingRow(int id, Expression expression, AtomicInteger reads) {
            super(new RelationId(id), List.of(new Alias(new ExprId(id), expression, "v")));
            this.reads = reads;
        }

        @Override
        public List<? extends Expression> getExpressions() {
            reads.incrementAndGet();
            return super.getExpressions();
        }

        @Override
        public List<? extends Plan> extraPlans() {
            return extra;
        }
    }

    private static class CountingLimit extends LogicalLimit<Plan> {
        private final AtomicInteger reads;

        CountingLimit(int limit, Plan child, AtomicInteger reads) {
            super(limit, 0L, LimitPhase.ORIGIN, child);
            this.reads = reads;
        }

        @Override
        public List<? extends Expression> getExpressions() {
            reads.incrementAndGet();
            return super.getExpressions();
        }
    }

    private static CountingRow nested(Expression leaf, int depth, AtomicInteger reads) {
        CountingRow row = new CountingRow(0, leaf, reads);
        for (int i = 1; i <= depth; i++) {
            row = new CountingRow(i, new ScalarSubquery(row), reads);
        }
        return row;
    }

    @Test
    public void testEmptyBaselineStoreDoesNotInspectTheQuery() {
        BaselineManager manager = Mockito.mock(BaselineManager.class);
        LogicalPlan plan = Mockito.mock(LogicalPlan.class);
        try (MockedStatic<BaselineManager> managers = Mockito.mockStatic(BaselineManager.class);
                MockedStatic<ConnectContext> contexts = Mockito.mockStatic(ConnectContext.class)) {
            managers.when(BaselineManager::getInstance).thenReturn(manager);
            contexts.when(ConnectContext::get).thenReturn(null);
            Assertions.assertNull(new SPMPlanner().tryRewritePlan(plan, Long.MAX_VALUE));
            Mockito.verify(manager).hasBaselines();
            Mockito.verifyNoInteractions(plan);
        }
    }

    @Test
    public void testNestedSubqueryScansAreLinear() {
        AtomicInteger reads = new AtomicInteger();
        CountingRow tree = nested(new IntegerLiteral(1), 16, reads);
        reads.set(0);
        Assertions.assertTrue(SPMPlanTreeSupport.collectPlaceholderIds(tree).isEmpty());
        Assertions.assertTrue(reads.get() <= 4 * 17, "placeholder scan reads: " + reads);
        reads.set(0);
        Assertions.assertFalse(SPMPlanTreeSupport.containsReplayContextExpression(tree));
        Assertions.assertTrue(reads.get() <= 4 * 17, "context scan reads: " + reads);
        reads.set(0);
        Assertions.assertFalse(SPMPlanTreeSupport.containsReplayContextInAliasBodies(null, tree));
        Assertions.assertTrue(reads.get() <= 4 * 17, "alias scan reads: " + reads);
    }

    @Test
    public void testPositiveContextScanStopsBeforeUnrelatedSubtrees() {
        AtomicInteger rootReads = new AtomicInteger();
        AtomicInteger tailReads = new AtomicInteger();
        CountingRow root = new CountingRow(50, new NereidsParser().parseExpression("@v"), rootReads);
        root.extra = List.of(nested(new IntegerLiteral(1), 16, tailReads));
        tailReads.set(0);
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(root));
        Assertions.assertEquals(0, tailReads.get(), "a positive guard must stop the whole traversal");
    }

    @Test
    public void testNestedPayloadsAndIndependentExpressionEntryRemainVisible() {
        AtomicInteger reads = new AtomicInteger();
        CountingRow markers = nested(SpmConstVar.of(7L, new IntegerLiteral(1)), 8, reads);
        Assertions.assertEquals(Set.of(7L), SPMPlanTreeSupport.collectPlaceholderIds(markers));
        Expression variable = new NereidsParser().parseExpression("@v");
        CountingRow context = nested(variable, 8, reads);
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(context));
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(new ScalarSubquery(context)));
    }

    @Test
    public void testCapSummariesReuseSubtreeInputsAndRemainImmutable() {
        AtomicInteger reads = new AtomicInteger();
        Plan tree = new CountingRow(0, new IntegerLiteral(1), reads);
        for (int i = 1; i <= 64; i++) {
            tree = new CountingLimit(i, tree, reads);
        }
        reads.set(0);
        SPMPlanTreeSupport.ReplayContracts summary = new SPMPlanTreeSupport.ReplayContracts(tree);
        Map<String, Integer> caps = new HashMap<>(summary.caps());
        Assertions.assertEquals(64, caps.values().stream().mapToInt(Integer::intValue).sum());
        Assertions.assertTrue(reads.get() <= 6 * 65, "cap collection reads: " + reads);
        reads.set(0);
        Assertions.assertTrue(SPMPlanTreeSupport.rowLimitsWithin(summary, summary));
        Assertions.assertTrue(SPMPlanTreeSupport.rowLimitsSurviveReplay(summary, summary));
        Assertions.assertTrue(SPMPlanTreeSupport.topCapContractPreserved(summary, summary));
        Assertions.assertEquals(caps, summary.caps());
        Assertions.assertEquals(0, reads.get(), "contract comparisons must reuse the captured caps");
    }

    @Test
    public void testCyclicExtraPlanKeepsReachabilitySemantics() {
        AtomicInteger reads = new AtomicInteger();
        CountingRow row = new CountingRow(0, new IntegerLiteral(1), reads);
        row.extra = List.of(row);
        Plan tree = new LogicalLimit<>(1, 0, LimitPhase.ORIGIN, row);
        Assertions.assertTrue(SpmTreeTestAccess.rowLimitsWithin(tree, tree));
        Assertions.assertTrue(SpmTreeTestAccess.rowLimitsSurviveReplay(tree, tree));
    }

    @Test
    public void testSharedDagCapsKeepRelationOccurrenceIdentity() {
        NereidsParser parser = new NereidsParser();
        LogicalPlan shared = (LogicalPlan) parser.parseSingle("SELECT k FROM t");
        CountingRow row = new CountingRow(60, new IntegerLiteral(1), new AtomicInteger());
        row.extra = List.of(new LogicalLimit<>(3, 0, LimitPhase.ORIGIN, shared),
                new LogicalLimit<>(5, 0, LimitPhase.ORIGIN, shared));
        Plan outer = new LogicalLimit<>(7, 0, LimitPhase.ORIGIN, row);
        Assertions.assertEquals(Map.of("3:0:-:t#1", 1, "5:0:-:t#1", 1, "7:0:-:t#1", 1),
                new SPMPlanTreeSupport.ReplayContracts(outer).caps());
        CountingRow separate = new CountingRow(61, new IntegerLiteral(1), new AtomicInteger());
        separate.extra = List.of(new LogicalLimit<>(3, 0, LimitPhase.ORIGIN,
                        parser.parseSingle("SELECT k FROM t")),
                new LogicalLimit<>(5, 0, LimitPhase.ORIGIN, parser.parseSingle("SELECT k FROM t")));
        Assertions.assertFalse(SpmTreeTestAccess.rowLimitsWithin(outer,
                new LogicalLimit<>(7, 0, LimitPhase.ORIGIN, separate)));
    }

    @Test
    public void testWideOccurrenceIndexReusesIdsAcrossTrees() {
        NereidsParser parser = new NereidsParser();
        List<Expression> expressions = new ArrayList<>();
        for (int i = 0; i < 2048; i++) {
            expressions.add(parser.parseExpression("a = " + i));
        }
        TestSPMPlaceholderBuilder builder = new TestSPMPlaceholderBuilder();
        List<Expression> first = builder.parameterizeExpressions(expressions);
        Assertions.assertEquals(2048, builder.getPlaceholderExprs().size());
        Assertions.assertEquals(first, builder.parameterizeExpressions(expressions));
        Assertions.assertEquals(2048, builder.getPlaceholderExprs().size());
        for (int i = 0; i < 2048; i++) {
            Assertions.assertEquals(i + 1L,
                    ((SpmConstVar) builder.getPlaceholderExprs().get(i).placeholderExpr()).getId());
        }
    }

    @Test
    public void testNestedBindingCheckpointsRestoreOnlyTheirOwnWrites() {
        Map<Long, Expression> values = new HashMap<>();
        values.put(1L, new IntegerLiteral(1));
        PlaceholderBindings bindings = PlaceholderBindings.track(values);
        int outer = bindings.checkpoint();
        bindings.put(2L, new IntegerLiteral(2));
        int inner = bindings.checkpoint();
        bindings.put(1L, new IntegerLiteral(3));
        bindings.put(4L, new IntegerLiteral(4));
        bindings.rollback(inner);
        Assertions.assertEquals(Map.of(1L, new IntegerLiteral(1), 2L, new IntegerLiteral(2)), values);
        bindings.rollback(outer);
        Assertions.assertEquals(Map.of(1L, new IntegerLiteral(1)), values);
        Assertions.assertSame(bindings, PlaceholderBindings.track(bindings));
    }

    @Test
    public void testSlotPrefilterPreservesDigestEquality() {
        List<UnboundSlot> slots = List.of(new UnboundSlot("a"), new UnboundSlot("a.b"),
                new UnboundSlot("a", "b"), new UnboundSlot("a`b"), new UnboundSlot("`a.b`"),
                new UnboundSlot(""), new UnboundSlot("", "a"));
        for (UnboundSlot left : slots) {
            for (UnboundSlot right : slots) {
                Assertions.assertEquals(left.toDigest().equals(right.toDigest()),
                        SPMAstCheckVisitor.mayMatch(left, right));
            }
        }
    }

    @Test
    public void testUnorderedMatchingPreservesCallerBindingsAndValues() {
        NereidsParser parser = new NereidsParser();
        LogicalPlan bind = SPMPlanTreeSupport.transform((LogicalPlan) parser.parseSingle(
                "SELECT a FROM t WHERE a = 1 AND b = 2 AND c = 3"), new TestSPMPlaceholderBuilder());
        LogicalPlan user = (LogicalPlan) parser.parseSingle(
                "SELECT a FROM t WHERE c = 9 AND b = 8 AND a = 7");
        Map<Long, Expression> values = new HashMap<>();
        values.put(99L, new UnboundSlot("sentinel"));
        Assertions.assertTrue(SPMPlanTreeSupport.check(bind, user, values));
        Assertions.assertEquals(new UnboundSlot("sentinel"), values.get(99L));
        Assertions.assertEquals(4, values.size());
        Map<Long, Expression> failed = new HashMap<>();
        failed.put(99L, new IntegerLiteral(99));
        Assertions.assertFalse(SPMPlanTreeSupport.check(bind, (LogicalPlan) parser.parseSingle(
                "SELECT a FROM t WHERE c = 9 AND b = 8 AND z = 7"), failed));
        Assertions.assertEquals(Map.of(99L, new IntegerLiteral(99)), failed);
    }

    @Test
    public void testOutOfBandSubqueryStillScansItsPayload() {
        AtomicInteger reads = new AtomicInteger();
        CountingRow marker = nested(SpmConstVar.of(17L, new IntegerLiteral(1)), 3, reads);
        LogicalPlan left = new CountingRow(30, new IntegerLiteral(1), reads);
        LogicalPlan right = new CountingRow(31, new IntegerLiteral(1), reads);
        LogicalPlan join = new LogicalUsingJoin<>(JoinType.ASOF_LEFT_INNER_JOIN, left, right,
                List.of(new UnboundSlot("k")), Optional.of(new ScalarSubquery(marker)),
                new DistributeHint(DistributeType.NONE));
        Assertions.assertEquals(Set.of(17L), SPMPlanTreeSupport.collectPlaceholderIds(join));
        CountingRow variable = nested(new NereidsParser().parseExpression("@v"), 3, reads);
        LogicalPlan contextJoin = new LogicalUsingJoin<>(JoinType.ASOF_LEFT_INNER_JOIN, left, right,
                List.of(new UnboundSlot("k")), Optional.of(new ScalarSubquery(variable)),
                new DistributeHint(DistributeType.NONE));
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(contextJoin));
    }

    @Test
    public void testFusedFreezeChecksKeepErrorPrecedence() {
        NereidsParser parser = new NereidsParser();
        Expression reserved = parser.parseExpression(
                SPMExprSqlBuilder.INTERNAL_PSEUDO_FUNCTION_NAMES.iterator().next() + "(1)");
        Expression subquery = parser.parseExpression("(SELECT 1)");
        Expression guard = new SessionVarGuardExpr(new IntegerLiteral(1), Map.of("enable_decimal256", "1"));
        LogicalPlan plan = Mockito.mock(LogicalPlan.class);
        Mockito.doReturn(List.of(reserved, subquery, guard)).when(plan).getExpressions();
        Assertions.assertTrue(Assertions.assertThrows(UnsupportedOperationException.class,
                () -> new SPMPlan2SQLBuilder().toSQL(plan)).getMessage().contains("session-variable guard"));
        Mockito.doReturn(List.of(reserved, subquery)).when(plan).getExpressions();
        Assertions.assertTrue(Assertions.assertThrows(UnsupportedOperationException.class,
                () -> new SPMPlan2SQLBuilder().toSQL(plan)).getMessage().contains("carrying a subquery"));
        Mockito.doReturn(List.of(reserved)).when(plan).getExpressions();
        Assertions.assertTrue(Assertions.assertThrows(UnsupportedOperationException.class,
                () -> new SPMPlan2SQLBuilder().toSQL(plan)).getMessage().contains("reserved internal name"));
    }

    @Test
    public void testSharedFactsPreserveManualPlanRejectionOrder() {
        NereidsParser parser = new NereidsParser();
        Plan bind = parser.parseSingle("SELECT k FROM t PARTITION(p1) WHERE k > 1");
        Plan plan = parser.parseSingle("SELECT v FROM t PARTITION(p2) WHERE k > 2");
        RuntimeException expected = Assertions.assertThrows(RuntimeException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanMismatch(bind, plan, "bind"));
        RuntimeException combined = Assertions.assertThrows(RuntimeException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanMismatch(bind, plan, "bind"));
        Assertions.assertEquals(expected.getMessage(), combined.getMessage());
    }
}

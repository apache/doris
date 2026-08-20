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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.analysis.IvmNormalizeMTMV;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash3128;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

class IvmNormalizeMTMVJoinTest extends IvmDeltaTestBase {

    private static final String JOIN_LEFT_MATCH_COL =
            Column.IVM_HIDDEN_COLUMN_PREFIX + "JOIN_LEFT_MATCH_COL__";
    private static final String JOIN_RIGHT_MATCH_COL =
            Column.IVM_HIDDEN_COLUMN_PREFIX + "JOIN_RIGHT_MATCH_COL__";

    private LogicalOlapScan buildMowScan(long tableId, String name) {
        OlapTable table = PlanConstructor.newOlapTable(tableId, name, 0, KeysType.UNIQUE_KEYS);
        table.setEnableUniqueKeyMergeOnWrite(true);
        enableRowBinlog(table);
        table.setQualifiedDbName("test_db");
        return new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                ImmutableList.of("test_db"));
    }

    private LogicalOlapScan buildDupScan(long tableId, String name) {
        OlapTable table = PlanConstructor.newOlapTable(tableId, name, 0, KeysType.DUP_KEYS);
        enableRowBinlog(table);
        table.setQualifiedDbName("test_db");
        return new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                ImmutableList.of("test_db"));
    }

    private Plan normalizeJoinPlan(Plan joinPlan) {
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(joinPlan.getOutput());
        LogicalProject<?> project = new LogicalProject<>(exprs, joinPlan);
        LogicalResultSink<?> sink = new LogicalResultSink<>(exprs, project);
        ConnectContext ctx = newConnectContext();
        JobContext jobContext = newJobContextForRoot(sink, ctx);
        return new IvmNormalizeMTMV().rewriteRoot(sink, jobContext);
    }

    private IvmRewriteResult getRewriteResult(Plan joinPlan) {
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(joinPlan.getOutput());
        LogicalProject<?> project = new LogicalProject<>(exprs, joinPlan);
        LogicalResultSink<?> sink = new LogicalResultSink<>(exprs, project);
        ConnectContext ctx = newConnectContext();
        JobContext jobContext = newJobContextForRoot(sink, ctx);
        new IvmNormalizeMTMV().rewriteRoot(sink, jobContext);
        return jobContext.getCascadesContext().getIvmRewriteResult().get();
    }

    private IvmRewriteResult getRewriteResult(Plan joinPlan, ImmutableSet<TableNameInfo> excludedTriggerTables) {
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(joinPlan.getOutput());
        LogicalProject<?> project = new LogicalProject<>(exprs, joinPlan);
        LogicalResultSink<?> sink = new LogicalResultSink<>(exprs, project);
        ConnectContext ctx = newConnectContext();
        ctx.getStatementContext().setExcludedTriggerTables(excludedTriggerTables);
        JobContext jobContext = newJobContextForRoot(sink, ctx);
        new IvmNormalizeMTMV().rewriteRoot(sink, jobContext);
        return jobContext.getCascadesContext().getIvmRewriteResult().get();
    }

    private LogicalUnion buildUnionAll(Plan... children) {
        return buildUnion(Qualifier.ALL, children);
    }

    private LogicalUnion buildUnion(Qualifier qualifier, Plan... children) {
        List<Slot> firstOutput = children[0].getOutput();
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        for (Slot slot : firstOutput) {
            outputs.add(new SlotReference(StatementScopeIdGenerator.newExprId(),
                    slot.getName(), slot.getDataType(), slot.nullable(), ImmutableList.of()));
        }
        ImmutableList.Builder<List<SlotReference>> childrenOutputs = ImmutableList.builder();
        for (Plan child : children) {
            ImmutableList.Builder<SlotReference> childMapping = ImmutableList.builder();
            for (Slot slot : child.getOutput()) {
                childMapping.add((SlotReference) slot);
            }
            childrenOutputs.add(childMapping.build());
        }
        return new LogicalUnion(qualifier, outputs.build(), childrenOutputs.build(),
                ImmutableList.of(), false, ImmutableList.copyOf(children));
    }

    /**
     * Helper: check if the composed join row_id (found in the normalized plan output) is deterministic.
     */
    private boolean isComposedRowIdDeterministic(Plan joinPlan) {
        IvmRewriteResult result = getRewriteResult(joinPlan);
        Plan normalized = result.getNormalizedPlan();
        Slot rowIdSlot = IvmUtil.findRowIdSlot(normalized.getOutput(), "test plan");
        return result.isDeterministic(rowIdSlot);
    }

    @Test
    void testNormalizeInnerJoin() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        Plan normalized = normalizeJoinPlan(join);

        // The normalized plan should contain exactly one __DORIS_IVM_ROW_ID_COL__ in the output
        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount, "Should have exactly one composed row_id");
    }

    @Test
    void testNormalizeCrossJoin() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.CROSS_JOIN,
                scanA, scanB, JoinReorderContext.EMPTY);

        Plan normalized = normalizeJoinPlan(join);

        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount, "Cross join should also have one composed row_id");
    }

    @Test
    void testNormalizeInnerJoinWithSubQueryAlias() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalSubQueryAlias<LogicalOlapScan> aliasA = new LogicalSubQueryAlias<>("alias_a", scanA);
        LogicalSubQueryAlias<LogicalOlapScan> aliasB = new LogicalSubQueryAlias<>("alias_b", scanB);
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(aliasA.getOutput().get(0), aliasB.getOutput().get(0))),
                aliasA, aliasB, JoinReorderContext.EMPTY);

        Plan normalized = normalizeJoinPlan(join);

        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount);
        Assertions.assertFalse(normalized.collectToList(plan -> plan instanceof LogicalSubQueryAlias).isEmpty());
    }

    @Test
    void testNormalizeJoinUnderDerivedAlias() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(scanA.getOutput().get(0), scanB.getOutput().get(0))),
                scanA, scanB, JoinReorderContext.EMPTY);
        LogicalSubQueryAlias<LogicalJoin<?, ?>> alias = new LogicalSubQueryAlias<>("joined_ab", join);

        Plan normalized = normalizeJoinPlan(alias);

        Assertions.assertInstanceOf(LogicalResultSink.class, normalized);
        Assertions.assertFalse(normalized.collectToList(plan -> plan instanceof LogicalSubQueryAlias).isEmpty());
        Assertions.assertEquals(1, normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName())).count());
    }

    @Test
    void testNormalizeMowMowDeterministic() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        Assertions.assertTrue(isComposedRowIdDeterministic(join),
                "MOW × MOW join should be deterministic");
    }

    @Test
    void testNormalizeMowDupNonDeterministic() {
        LogicalOlapScan scanMow = buildMowScan(1, "mow_t");
        LogicalOlapScan scanDup = buildDupScan(2, "dup_t");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanMow, scanDup, JoinReorderContext.EMPTY);

        Assertions.assertFalse(isComposedRowIdDeterministic(join),
                "MOW × DUP join should be non-deterministic");
    }

    @Test
    void testNormalizeDupDupNonDeterministic() {
        LogicalOlapScan scanA = buildDupScan(1, "dup_a");
        LogicalOlapScan scanB = buildDupScan(2, "dup_b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        Assertions.assertFalse(isComposedRowIdDeterministic(join),
                "DUP × DUP join should be non-deterministic");
    }

    @Test
    void testNormalizeNestedJoin() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildDupScan(3, "c");
        LogicalJoin<?, ?> abJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);
        LogicalJoin<?, ?> abcJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), abJoin, scanC, JoinReorderContext.EMPTY);

        Plan normalized = normalizeJoinPlan(abcJoin);

        // Should still have exactly one row_id in the final output
        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount,
                "Nested join should have one composed row_id");

        // A(MOW) × B(MOW) × C(DUP) → non-deterministic
        Assertions.assertFalse(isComposedRowIdDeterministic(abcJoin),
                "Nested join with DUP should be non-deterministic");
    }

    @Test
    void testNormalizeNestedJoinAllMow() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> abJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);
        LogicalJoin<?, ?> abcJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), abJoin, scanC, JoinReorderContext.EMPTY);

        Assertions.assertTrue(isComposedRowIdDeterministic(abcJoin),
                "Nested join all MOW should be deterministic");
    }

    @Test
    void testNormalizeSelfJoin() {
        LogicalOlapScan scanA1 = buildMowScan(1, "a");
        LogicalOlapScan scanA2 = buildMowScan(1, "a");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA1, scanA2, JoinReorderContext.EMPTY);

        Plan normalized = normalizeJoinPlan(join);

        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount, "Self-join should have one composed row_id");
    }

    @Test
    void testNormalizeLeftOuterJoin() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(join);
        Plan normalized = result.getNormalizedPlan();

        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount, "Left outer join should have one composed row_id");
    }

    @Test
    void testNormalizeLeftOuterJoinWithInnerJoinChild() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> innerJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);
        LogicalJoin<?, ?> outerJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), innerJoin, scanC, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(outerJoin);

        Assertions.assertNotNull(result.getNormalizedPlan(),
                "Root LEFT_OUTER_JOIN should allow inner joins in its children");
    }

    @Test
    void testNormalizeLeftOuterJoinWithNonDetRetainedSideThrows() {
        LogicalOlapScan scanA = buildDupScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        IvmException ex = Assertions.assertThrows(IvmException.class, () -> normalizeJoinPlan(join));
        Assertions.assertEquals(IvmFailureReason.NON_DETERMINISTIC_ROW_ID, ex.getFailureReason());
        Assertions.assertTrue(ex.getMessage().contains("retained side"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    void testNormalizeLeftOuterJoinWithNullSideUnionAll() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalUnion union = buildUnionAll(scanB, scanC);
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, union, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(join);

        Assertions.assertNotNull(result.getNormalizedPlan(),
                "LEFT_OUTER_JOIN should allow UNION ALL on null side");
    }

    @Test
    void testNormalizeLeftOuterJoinWithNullSideUnionDistinctThrowsPlanPattern() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalUnion union = buildUnion(Qualifier.DISTINCT, scanB, scanC);
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, union, JoinReorderContext.EMPTY);

        assertIvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                () -> normalizeJoinPlan(join));
    }

    @Test
    void testNormalizeLeftOuterJoinWithNullSideUnionAllProjectScan() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalProject<?> projectB = new LogicalProject<>(ImmutableList.copyOf(scanB.getOutput()), scanB);
        LogicalUnion union = buildUnionAll(projectB, scanC);
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, union, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(join);

        Assertions.assertNotNull(result.getNormalizedPlan(),
                "LEFT_OUTER_JOIN should allow UNION ALL with project on null side");
    }

    @Test
    void testNormalizeLeftOuterJoinWithNullSideExcludedUnionAll() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalUnion union = buildUnionAll(scanB, scanC);
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, union, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(join,
                ImmutableSet.of(new TableNameInfo("test_db", "b"), new TableNameInfo("test_db", "c")));

        Assertions.assertNotNull(result.getNormalizedPlan(),
                "Null-side UNION ALL should be allowed when all OlapScans are excluded trigger tables");
    }

    @Test
    void testNormalizeRightOuterJoin() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(join);
        Plan normalized = result.getNormalizedPlan();

        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount, "Right outer join should have one composed row_id");
    }

    @Test
    void testNormalizeRightOuterJoinWithNonDetRetainedSideThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildDupScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        IvmException ex = Assertions.assertThrows(IvmException.class, () -> normalizeJoinPlan(join));
        Assertions.assertEquals(IvmFailureReason.NON_DETERMINISTIC_ROW_ID, ex.getFailureReason());
        Assertions.assertTrue(ex.getMessage().contains("retained side"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    void testNormalizeRightOuterJoinWithNullSideUnionAll() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalUnion union = buildUnionAll(scanA, scanB);
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(), union, scanC, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(join);

        Assertions.assertNotNull(result.getNormalizedPlan(),
                "RIGHT_OUTER_JOIN should allow UNION ALL on null side");
    }

    @Test
    void testNormalizeRightOuterJoinWithRetainedSideUnionAll() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalUnion union = buildUnionAll(scanB, scanC);
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(), scanA, union, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(join);

        Assertions.assertNotNull(result.getNormalizedPlan(),
                "RIGHT_OUTER_JOIN should allow UNION ALL on retained side");
    }

    @Test
    void testNormalizeFullOuterJoin() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(join);
        Plan normalized = result.getNormalizedPlan();

        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount, "Full outer join should have one composed row_id");
    }

    @Test
    void testNormalizeFullOuterJoinRequiresBothSidesDeterministic() {
        LogicalOlapScan scanA = buildDupScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> leftNonDetJoin = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        IvmException leftEx = Assertions.assertThrows(IvmException.class, () -> normalizeJoinPlan(leftNonDetJoin));
        Assertions.assertEquals(IvmFailureReason.NON_DETERMINISTIC_ROW_ID, leftEx.getFailureReason());
        Assertions.assertTrue(leftEx.getMessage().contains("left side"),
                "unexpected message: " + leftEx.getMessage());

        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalOlapScan scanD = buildDupScan(4, "d");
        LogicalJoin<?, ?> rightNonDetJoin = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(), scanC, scanD, JoinReorderContext.EMPTY);

        IvmException rightEx = Assertions.assertThrows(IvmException.class, () -> normalizeJoinPlan(rightNonDetJoin));
        Assertions.assertEquals(IvmFailureReason.NON_DETERMINISTIC_ROW_ID, rightEx.getFailureReason());
        Assertions.assertTrue(rightEx.getMessage().contains("right side"),
                "unexpected message: " + rightEx.getMessage());
    }

    @Test
    void testNormalizeFullOuterJoinWithUnionAllOnEitherSide() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalUnion leftUnion = buildUnionAll(scanA, scanB);
        LogicalJoin<?, ?> leftUnionJoin = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(), leftUnion, scanC, JoinReorderContext.EMPTY);

        IvmRewriteResult leftResult = getRewriteResult(leftUnionJoin);

        Assertions.assertNotNull(leftResult.getNormalizedPlan(),
                "FULL_OUTER_JOIN should allow UNION ALL on left null side");

        LogicalOlapScan scanD = buildMowScan(4, "d");
        LogicalOlapScan scanE = buildMowScan(5, "e");
        LogicalOlapScan scanF = buildMowScan(6, "f");
        LogicalUnion rightUnion = buildUnionAll(scanE, scanF);
        LogicalJoin<?, ?> rightUnionJoin = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(), scanD, rightUnion, JoinReorderContext.EMPTY);

        IvmRewriteResult rightResult = getRewriteResult(rightUnionJoin);

        Assertions.assertNotNull(rightResult.getNormalizedPlan(),
                "FULL_OUTER_JOIN should allow UNION ALL on right null side");
    }

    @Test
    void testNormalizeFullOuterJoinWithOuterJoinOnEitherSide() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> leftChildOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);
        LogicalJoin<?, ?> leftNestedJoin = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(), leftChildOuterJoin, scanC, JoinReorderContext.EMPTY);

        Plan leftNormalized = normalizeJoinPlan(leftNestedJoin);
        Assertions.assertNotNull(leftNormalized,
                "FULL_OUTER_JOIN should allow an outer join on the left side");

        LogicalOlapScan scanD = buildMowScan(4, "d");
        LogicalOlapScan scanE = buildMowScan(5, "e");
        LogicalOlapScan scanF = buildMowScan(6, "f");
        LogicalJoin<?, ?> rightChildOuterJoin = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(), scanE, scanF, JoinReorderContext.EMPTY);
        LogicalJoin<?, ?> rightNestedJoin = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(), scanD, rightChildOuterJoin, JoinReorderContext.EMPTY);

        Plan rightNormalized = normalizeJoinPlan(rightNestedJoin);
        Assertions.assertNotNull(rightNormalized,
                "FULL_OUTER_JOIN should allow an outer join on the right side");
    }

    @Test
    void testNormalizeFilterAboveLeftOuterJoin() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);
        Plan filter = new LogicalFilter<>(ImmutableSet.of(new EqualTo(
                join.getOutput().get(0), join.getOutput().get(0))), join);

        IvmRewriteResult result = getRewriteResult(filter);

        Assertions.assertNotNull(result.getNormalizedPlan(),
                "Filter above LEFT_OUTER_JOIN should keep outer join IVM routing");
    }

    @Test
    void testNormalizeLeftOuterJoinBelowInnerJoin() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> outerJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);
        LogicalJoin<?, ?> topJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), outerJoin, scanC, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(topJoin);

        Assertions.assertNotNull(result.getNormalizedPlan(),
                "LEFT_OUTER_JOIN below a linear parent join should be normalized");
    }

    @Test
    void testNormalizeLeftDeepOuterJoinChain() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> firstOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);
        LogicalJoin<?, ?> secondOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), firstOuterJoin, scanC, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(secondOuterJoin);

        Assertions.assertNotNull(result.getNormalizedPlan(),
                "LEFT_OUTER_JOIN chain on retained side should be normalized");
    }

    @Test
    void testNormalizeProjectedLeftDeepOuterJoinChain() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> firstOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);
        LogicalProject<?> projectedFirstJoin = new LogicalProject<>(
                ImmutableList.copyOf(firstOuterJoin.getOutput()), firstOuterJoin);
        LogicalJoin<?, ?> secondOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), projectedFirstJoin, scanC, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(secondOuterJoin);

        Assertions.assertNotNull(result.getNormalizedPlan(),
                "LEFT_OUTER_JOIN chain should allow projects on the retained-side path");
    }

    @Test
    void testNormalizeLeftOuterJoinOnNullSide() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> nullSideOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanB, scanC, JoinReorderContext.EMPTY);
        LogicalJoin<?, ?> rootOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, nullSideOuterJoin, JoinReorderContext.EMPTY);

        Plan normalized = normalizeJoinPlan(rootOuterJoin);
        Assertions.assertNotNull(normalized, "Nested outer join on the null side should be normalized");
    }

    @Test
    void testNormalizeProjectedLeftOuterJoinOnNullSide() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> nullSideOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanB, scanC, JoinReorderContext.EMPTY);
        LogicalProject<?> projectedNullSide = new LogicalProject<>(
                ImmutableList.copyOf(nullSideOuterJoin.getOutput()), nullSideOuterJoin);
        LogicalJoin<?, ?> rootOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, projectedNullSide, JoinReorderContext.EMPTY);

        Plan normalized = normalizeJoinPlan(rootOuterJoin);
        Assertions.assertNotNull(normalized, "Nested projected outer join on the null side should be normalized");
    }

    @Test
    void testNormalizeFilteredLeftOuterJoinOnNullSide() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> nullSideOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanB, scanC, JoinReorderContext.EMPTY);
        LogicalFilter<?> filteredNullSide = new LogicalFilter<>(ImmutableSet.of(new EqualTo(
                nullSideOuterJoin.getOutput().get(0), nullSideOuterJoin.getOutput().get(0))), nullSideOuterJoin);
        LogicalJoin<?, ?> rootOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, filteredNullSide, JoinReorderContext.EMPTY);

        Plan normalized = normalizeJoinPlan(rootOuterJoin);
        Assertions.assertNotNull(normalized, "Nested filtered outer join on the null side should be normalized");
    }

    @Test
    void testNormalizeRootAggregateAboveLeftOuterJoin() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> outerJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);
        Slot groupSlot = outerJoin.getOutput().get(0);
        Alias countAlias = new Alias(new Count(), "cnt");
        LogicalAggregate<Plan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(groupSlot), ImmutableList.of(groupSlot, countAlias),
                true, Optional.empty(), outerJoin);

        IvmRewriteResult result = getRewriteResult(aggregate);

        Assertions.assertNotNull(result.getAggMeta(),
                "Root aggregate should keep aggregate IVM rewrite plan");
    }

    @Test
    void testNormalizeRowIdAccumulatesEntries() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(join);
        Map<Slot, Boolean> rowIdDet = result.getRowIdDeterminism();
        // 2 scan entries + 1 composed join entry = 3
        Assertions.assertEquals(3, rowIdDet.size(),
                "After join normalization, map should have scan entries + composed entry");
    }

    @Test
    void testNormalizeUnsupportedJoinTypeThrowsPlanPattern() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN,
                ImmutableList.of(new EqualTo(scanA.getOutput().get(0), scanB.getOutput().get(0))),
                scanA, scanB, JoinReorderContext.EMPTY);

        assertIvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                () -> normalizeJoinPlan(join));
    }

    @Test
    void testNormalizeMarkJoinThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        // Construct a proper mark join: set markJoinSlotReference so isMarkJoin() returns true
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE),
                Optional.of(new MarkJoinSlotReference("$mark")),
                scanA, scanB, JoinReorderContext.EMPTY);

        assertIvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                () -> normalizeJoinPlan(join));
    }

    private void assertIvmException(IvmFailureReason failureReason, Executable executable) {
        IvmException exception = Assertions.assertThrows(IvmException.class, executable);
        Assertions.assertEquals(failureReason, exception.getFailureReason());
    }

    @Test
    void testNormalizeJoinWithHashConjuncts() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        EqualTo condition = new EqualTo(scanA.getOutput().get(0), scanB.getOutput().get(0));
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(condition), scanA, scanB, JoinReorderContext.EMPTY);

        Plan normalized = normalizeJoinPlan(join);

        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount, "Hash-conjunct join should have exactly one composed row_id");
    }

    @Test
    void testNormalizeCrossJoinDupDup() {
        LogicalOlapScan scanA = buildDupScan(1, "dup_a");
        LogicalOlapScan scanB = buildDupScan(2, "dup_b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.CROSS_JOIN,
                scanA, scanB, JoinReorderContext.EMPTY);

        Assertions.assertFalse(isComposedRowIdDeterministic(join),
                "DUP × DUP cross join should be non-deterministic");
    }

    /**
     * Extract the slot from the second encoding of a buildRowIdHash key, i.e. the slot inside
     * {@code Cast(IsNull(slot) AS VARCHAR)} at odd child positions (1, 3, 5, ...).
     * Returns null when the position does not encode a bare slot (e.g. a nested composed hash).
     */
    private Slot slotOfIsNullEncoding(Expression expr) {
        if (expr instanceof Cast && ((Cast) expr).child() instanceof IsNull
                && ((IsNull) ((Cast) expr).child()).child() instanceof Slot) {
            return (Slot) ((IsNull) ((Cast) expr).child()).child();
        }
        return null;
    }

    /**
     * Assert the compose hash encodes its keys in the given order. buildRowIdHash emits two
     * arguments per key, and the second one (odd index) always carries the key slot under
     * {@code Cast(IsNull(slot) AS VARCHAR)}.
     */
    private void assertComposeHashKeyOrder(MurmurHash3128 hash, String... expectedKeyNames) {
        List<Expression> children = hash.children();
        Assertions.assertEquals(expectedKeyNames.length * 2, children.size(),
                "each compose hash key is encoded as two arguments");
        for (int i = 0; i < expectedKeyNames.length; i++) {
            Slot slot = slotOfIsNullEncoding(children.get(i * 2 + 1));
            Assertions.assertNotNull(slot, "hash key " + i + " should be encoded as a slot");
            Assertions.assertEquals(expectedKeyNames[i], slot.getName(),
                    "compose hash keys should be encoded in order");
        }
    }

    /**
     * Find the compose project's MurmurHash3128 row-id expression in the normalized plan.
     * Returns null when the row-id is not a MurmurHash3128 (e.g. an aggregate rebuilt it).
     */
    private MurmurHash3128 findComposedRowIdHash(Plan normalized) {
        List<LogicalProject<?>> projects = normalized.collectToList(p -> p instanceof LogicalProject);
        for (LogicalProject<?> project : projects) {
            for (NamedExpression ne : project.getProjects()) {
                if (ne instanceof Alias && Column.IVM_ROW_ID_COL.equals(ne.getName())
                        && ((Alias) ne).child() instanceof MurmurHash3128) {
                    return (MurmurHash3128) ((Alias) ne).child();
                }
            }
        }
        return null;
    }

    private long countMatchFlagProjects(Plan plan, String flagColumnName) {
        return plan.collectToList(p -> p instanceof LogicalProject
                && ((LogicalProject<?>) p).getOutput().stream()
                        .anyMatch(s -> flagColumnName.equals(s.getName()))).size();
    }

    /**
     * Find the topmost compose project (outputs __DORIS_IVM_ROW_ID_COL__), whose child is the
     * outermost join of the normalized plan.
     */
    private LogicalProject<?> findComposeProject(Plan normalized) {
        List<LogicalProject<?>> projects = normalized.collectToList(p -> p instanceof LogicalProject);
        for (LogicalProject<?> project : projects) {
            boolean hasRowId = project.getProjects().stream()
                    .anyMatch(ne -> ne instanceof Alias && Column.IVM_ROW_ID_COL.equals(ne.getName()));
            if (hasRowId) {
                return project;
            }
        }
        return null;
    }

    /**
     * The join below the compose project must expose the injected match flags in its output
     * (they survive the join's null filling), while every compose project must consume them.
     */
    private void assertJoinOutputHasMatchFlags(Plan normalized, boolean expectLeftFlag, boolean expectRightFlag) {
        LogicalProject<?> composeProject = findComposeProject(normalized);
        Assertions.assertNotNull(composeProject, "normalized plan should have a compose project");
        Assertions.assertTrue(composeProject.child() instanceof LogicalJoin,
                "compose project child should be the composed join");
        LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) composeProject.child();
        Assertions.assertEquals(expectLeftFlag,
                join.getOutput().stream().anyMatch(s -> JOIN_LEFT_MATCH_COL.equals(s.getName())),
                "join output should " + (expectLeftFlag ? "" : "not ") + "contain the left match flag");
        Assertions.assertEquals(expectRightFlag,
                join.getOutput().stream().anyMatch(s -> JOIN_RIGHT_MATCH_COL.equals(s.getName())),
                "join output should " + (expectRightFlag ? "" : "not ") + "contain the right match flag");
    }

    /**
     * Every compose project (the one above the join that computes the composed row-id) must
     * consume the match flags: its output must not contain left/right match flag columns.
     */
    private void assertComposeProjectsConsumeMatchFlags(Plan normalized) {
        List<LogicalProject<?>> projects = normalized.collectToList(p -> p instanceof LogicalProject);
        for (LogicalProject<?> project : projects) {
            boolean hasRowId = project.getProjects().stream()
                    .anyMatch(ne -> ne instanceof Alias && Column.IVM_ROW_ID_COL.equals(ne.getName()));
            if (!hasRowId) {
                continue;
            }
            Assertions.assertTrue(project.getOutput().stream()
                            .noneMatch(s -> JOIN_LEFT_MATCH_COL.equals(s.getName())
                                    || JOIN_RIGHT_MATCH_COL.equals(s.getName())),
                    "compose project must not output match flags");
        }
    }

    @Test
    void testNormalizeLeftOuterJoinInjectsRightMatchFlag() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(join);
        Plan normalized = result.getNormalizedPlan();

        // Null side (right) gets a match flag project; preserved side (left) does not.
        Assertions.assertEquals(1, countMatchFlagProjects(normalized, JOIN_RIGHT_MATCH_COL),
                "LEFT_OUTER_JOIN should inject one right match flag project");
        Assertions.assertEquals(0, countMatchFlagProjects(normalized, JOIN_LEFT_MATCH_COL),
                "LEFT_OUTER_JOIN should not inject a left match flag");
        // The flag survives the join output and is consumed by the compose project above it.
        assertJoinOutputHasMatchFlags(normalized, false, true);
        assertComposeProjectsConsumeMatchFlags(normalized);
        // The flag never leaks into the final output.
        Assertions.assertTrue(normalized.getOutput().stream()
                        .noneMatch(s -> JOIN_LEFT_MATCH_COL.equals(s.getName())
                                || JOIN_RIGHT_MATCH_COL.equals(s.getName())),
                "match flags must not leak into the normalized output");
        // Compose hash = hash(l_rid, r_rid, r_flag): 3 keys x 2 encodings each = 6 args.
        MurmurHash3128 hash = findComposedRowIdHash(normalized);
        Assertions.assertNotNull(hash, "composed row-id should be a MurmurHash3128");
        Assertions.assertEquals(6, hash.children().size(),
                "LOJ compose should hash left_rid, right_rid and right match flag");
        assertComposeHashKeyOrder(hash, Column.IVM_ROW_ID_COL, Column.IVM_ROW_ID_COL, JOIN_RIGHT_MATCH_COL);
    }

    @Test
    void testNormalizeRightOuterJoinInjectsLeftMatchFlag() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(join);
        Plan normalized = result.getNormalizedPlan();

        Assertions.assertEquals(1, countMatchFlagProjects(normalized, JOIN_LEFT_MATCH_COL),
                "RIGHT_OUTER_JOIN should inject one left match flag project");
        Assertions.assertEquals(0, countMatchFlagProjects(normalized, JOIN_RIGHT_MATCH_COL),
                "RIGHT_OUTER_JOIN should not inject a right match flag");
        assertJoinOutputHasMatchFlags(normalized, true, false);
        assertComposeProjectsConsumeMatchFlags(normalized);
        Assertions.assertTrue(normalized.getOutput().stream()
                        .noneMatch(s -> JOIN_LEFT_MATCH_COL.equals(s.getName())
                                || JOIN_RIGHT_MATCH_COL.equals(s.getName())),
                "match flags must not leak into the normalized output");
        MurmurHash3128 hash = findComposedRowIdHash(normalized);
        Assertions.assertNotNull(hash, "composed row-id should be a MurmurHash3128");
        Assertions.assertEquals(6, hash.children().size(),
                "ROJ compose should hash left_rid, left match flag and right_rid");
        assertComposeHashKeyOrder(hash, Column.IVM_ROW_ID_COL, JOIN_LEFT_MATCH_COL, Column.IVM_ROW_ID_COL);
    }

    @Test
    void testNormalizeFullOuterJoinInjectsBothMatchFlags() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(join);
        Plan normalized = result.getNormalizedPlan();

        Assertions.assertEquals(1, countMatchFlagProjects(normalized, JOIN_LEFT_MATCH_COL),
                "FULL_OUTER_JOIN should inject a left match flag project");
        Assertions.assertEquals(1, countMatchFlagProjects(normalized, JOIN_RIGHT_MATCH_COL),
                "FULL_OUTER_JOIN should inject a right match flag project");
        assertJoinOutputHasMatchFlags(normalized, true, true);
        assertComposeProjectsConsumeMatchFlags(normalized);
        Assertions.assertTrue(normalized.getOutput().stream()
                        .noneMatch(s -> JOIN_LEFT_MATCH_COL.equals(s.getName())
                                || JOIN_RIGHT_MATCH_COL.equals(s.getName())),
                "match flags must not leak into the normalized output");
        // Compose hash = hash(l_rid, l_flag, r_rid, r_flag): 4 keys x 2 encodings = 8 args.
        MurmurHash3128 hash = findComposedRowIdHash(normalized);
        Assertions.assertNotNull(hash, "composed row-id should be a MurmurHash3128");
        Assertions.assertEquals(8, hash.children().size(),
                "FULL OUTER JOIN compose should hash both row-ids and both match flags");
        assertComposeHashKeyOrder(hash, Column.IVM_ROW_ID_COL, JOIN_LEFT_MATCH_COL,
                Column.IVM_ROW_ID_COL, JOIN_RIGHT_MATCH_COL);
    }

    @Test
    void testNormalizeInnerJoinHasNoMatchFlag() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(join);
        Plan normalized = result.getNormalizedPlan();

        Assertions.assertEquals(0, countMatchFlagProjects(normalized, JOIN_LEFT_MATCH_COL)
                        + countMatchFlagProjects(normalized, JOIN_RIGHT_MATCH_COL),
                "INNER_JOIN should not inject any match flag");
        assertJoinOutputHasMatchFlags(normalized, false, false);
        assertComposeProjectsConsumeMatchFlags(normalized);
        // Compose hash stays hash(l_rid, r_rid): 2 keys x 2 encodings = 4 args.
        MurmurHash3128 hash = findComposedRowIdHash(normalized);
        Assertions.assertNotNull(hash, "composed row-id should be a MurmurHash3128");
        Assertions.assertEquals(4, hash.children().size(),
                "INNER_JOIN compose should hash only left_rid and right_rid");
        assertComposeHashKeyOrder(hash, Column.IVM_ROW_ID_COL, Column.IVM_ROW_ID_COL);
    }

    @Test
    void testNormalizeNestedLeftOuterJoinFlagsDoNotCollide() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> inner = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanB, scanC, JoinReorderContext.EMPTY);
        LogicalJoin<?, ?> outer = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, inner, JoinReorderContext.EMPTY);

        IvmRewriteResult result = getRewriteResult(outer);
        Plan normalized = result.getNormalizedPlan();

        // The inner LOJ injects and consumes its own right flag; the outer LOJ injects a new one.
        // Both projects use the same column name, so exactly two right-flag projects survive.
        Assertions.assertEquals(2, countMatchFlagProjects(normalized, JOIN_RIGHT_MATCH_COL),
                "nested LOJ should inject a right flag per join level");
        Assertions.assertEquals(0, countMatchFlagProjects(normalized, JOIN_LEFT_MATCH_COL),
                "nested LOJ should not inject left flags");
        // The outermost join output carries only the outer flag; both compose projects consume flags.
        assertJoinOutputHasMatchFlags(normalized, false, true);
        assertComposeProjectsConsumeMatchFlags(normalized);
        Assertions.assertTrue(normalized.getOutput().stream()
                        .noneMatch(s -> JOIN_LEFT_MATCH_COL.equals(s.getName())
                                || JOIN_RIGHT_MATCH_COL.equals(s.getName())),
                "match flags must not leak into the normalized output");
        // The outer compose hashes (a_rid, inner_composed_rid, outer_right_flag): the middle
        // key references the inner composed row-id slot (projected by the inner compose project).
        MurmurHash3128 hash = findComposedRowIdHash(normalized);
        Assertions.assertNotNull(hash, "composed row-id should be a MurmurHash3128");
        List<Expression> hashChildren = hash.children();
        Assertions.assertEquals(6, hashChildren.size(),
                "outer LOJ compose should hash left_rid, inner row-id and right match flag");
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, slotOfIsNullEncoding(hashChildren.get(1)).getName(),
                "first outer compose key is the left child row-id");
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, slotOfIsNullEncoding(hashChildren.get(3)).getName(),
                "middle outer compose key is the inner composed row-id slot");
        Assertions.assertEquals(JOIN_RIGHT_MATCH_COL, slotOfIsNullEncoding(hashChildren.get(5)).getName(),
                "last outer compose key is the outer right match flag");
    }
}

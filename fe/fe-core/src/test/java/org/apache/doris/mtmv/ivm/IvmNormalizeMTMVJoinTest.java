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
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.rules.rewrite.IvmNormalizeMTMV;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
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
    void testNormalizeLeftOuterJoinWithNullSideUnionAllThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalUnion union = buildUnionAll(scanB, scanC);
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, union, JoinReorderContext.EMPTY);

        assertIvmException(IvmFailureReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED,
                () -> normalizeJoinPlan(join));
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
    void testNormalizeLeftOuterJoinWithNullSideUnionAllProjectScanThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalProject<?> projectB = new LogicalProject<>(ImmutableList.copyOf(scanB.getOutput()), scanB);
        LogicalUnion union = buildUnionAll(projectB, scanC);
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, union, JoinReorderContext.EMPTY);

        assertIvmException(IvmFailureReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED,
                () -> normalizeJoinPlan(join));
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
    void testNormalizeRightOuterJoinWithNullSideUnionAllThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalUnion union = buildUnionAll(scanA, scanB);
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(), union, scanC, JoinReorderContext.EMPTY);

        assertIvmException(IvmFailureReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED,
                () -> normalizeJoinPlan(join));
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
    void testNormalizeFullOuterJoinWithUnionAllOnEitherSideThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalUnion leftUnion = buildUnionAll(scanA, scanB);
        LogicalJoin<?, ?> leftUnionJoin = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(), leftUnion, scanC, JoinReorderContext.EMPTY);

        assertIvmException(IvmFailureReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED,
                () -> normalizeJoinPlan(leftUnionJoin));

        LogicalOlapScan scanD = buildMowScan(4, "d");
        LogicalOlapScan scanE = buildMowScan(5, "e");
        LogicalOlapScan scanF = buildMowScan(6, "f");
        LogicalUnion rightUnion = buildUnionAll(scanE, scanF);
        LogicalJoin<?, ?> rightUnionJoin = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(), scanD, rightUnion, JoinReorderContext.EMPTY);

        assertIvmException(IvmFailureReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED,
                () -> normalizeJoinPlan(rightUnionJoin));
    }

    @Test
    void testNormalizeFullOuterJoinWithOuterJoinOnEitherSideThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> leftChildOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);
        LogicalJoin<?, ?> leftNestedJoin = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(), leftChildOuterJoin, scanC, JoinReorderContext.EMPTY);

        assertIvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                () -> normalizeJoinPlan(leftNestedJoin));

        LogicalOlapScan scanD = buildMowScan(4, "d");
        LogicalOlapScan scanE = buildMowScan(5, "e");
        LogicalOlapScan scanF = buildMowScan(6, "f");
        LogicalJoin<?, ?> rightChildOuterJoin = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(), scanE, scanF, JoinReorderContext.EMPTY);
        LogicalJoin<?, ?> rightNestedJoin = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(), scanD, rightChildOuterJoin, JoinReorderContext.EMPTY);

        assertIvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                () -> normalizeJoinPlan(rightNestedJoin));
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
    void testNormalizeLeftOuterJoinOnNullSideThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> nullSideOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanB, scanC, JoinReorderContext.EMPTY);
        LogicalJoin<?, ?> rootOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, nullSideOuterJoin, JoinReorderContext.EMPTY);

        IvmException ex = Assertions.assertThrows(IvmException.class, () -> normalizeJoinPlan(rootOuterJoin));
        Assertions.assertEquals(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED, ex.getFailureReason());
        Assertions.assertTrue(ex.getMessage().contains("null side"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    void testNormalizeProjectedLeftOuterJoinOnNullSideThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> nullSideOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanB, scanC, JoinReorderContext.EMPTY);
        LogicalProject<?> projectedNullSide = new LogicalProject<>(
                ImmutableList.copyOf(nullSideOuterJoin.getOutput()), nullSideOuterJoin);
        LogicalJoin<?, ?> rootOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, projectedNullSide, JoinReorderContext.EMPTY);

        IvmException ex = Assertions.assertThrows(IvmException.class, () -> normalizeJoinPlan(rootOuterJoin));
        Assertions.assertEquals(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED, ex.getFailureReason());
        Assertions.assertTrue(ex.getMessage().contains("null side"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    void testNormalizeFilteredLeftOuterJoinOnNullSideThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalJoin<?, ?> nullSideOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanB, scanC, JoinReorderContext.EMPTY);
        LogicalFilter<?> filteredNullSide = new LogicalFilter<>(ImmutableSet.of(new EqualTo(
                nullSideOuterJoin.getOutput().get(0), nullSideOuterJoin.getOutput().get(0))), nullSideOuterJoin);
        LogicalJoin<?, ?> rootOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, filteredNullSide, JoinReorderContext.EMPTY);

        IvmException ex = Assertions.assertThrows(IvmException.class, () -> normalizeJoinPlan(rootOuterJoin));
        Assertions.assertEquals(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED, ex.getFailureReason());
        Assertions.assertTrue(ex.getMessage().contains("null side"),
                "unexpected message: " + ex.getMessage());
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
}

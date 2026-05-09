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
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.rules.rewrite.IvmNormalizeMtmv;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Map;
import java.util.Optional;

class IvmNormalizeMtmvJoinTest extends IvmDeltaTestBase {

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
        return new IvmNormalizeMtmv().rewriteRoot(sink, jobContext);
    }

    private IvmNormalizeResult getNormalizeResult(Plan joinPlan) {
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(joinPlan.getOutput());
        LogicalProject<?> project = new LogicalProject<>(exprs, joinPlan);
        LogicalResultSink<?> sink = new LogicalResultSink<>(exprs, project);
        ConnectContext ctx = newConnectContext();
        JobContext jobContext = newJobContextForRoot(sink, ctx);
        new IvmNormalizeMtmv().rewriteRoot(sink, jobContext);
        return jobContext.getCascadesContext().getIvmNormalizeResult().get();
    }

    /**
     * Helper: check if the composed join row_id (found in the normalized plan output) is deterministic.
     */
    private boolean isComposedRowIdDeterministic(Plan joinPlan) {
        IvmNormalizeResult result = getNormalizeResult(joinPlan);
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
    void testNormalizeUnsupportedLeftOuterJoinThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        assertIvmException(IvmFailureReason.OUTER_JOIN_RETRACTION_UNSUPPORTED,
                () -> normalizeJoinPlan(join));
    }

    @Test
    void testNormalizeUnsupportedRightOuterJoinThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        assertIvmException(IvmFailureReason.OUTER_JOIN_RETRACTION_UNSUPPORTED,
                () -> normalizeJoinPlan(join));
    }

    @Test
    void testNormalizeRowIdAccumulatesEntries() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        IvmNormalizeResult result = getNormalizeResult(join);
        Map<Slot, Boolean> rowIdDet = result.getRowIdDeterminism();
        // 2 scan entries + 1 composed join entry = 3
        Assertions.assertEquals(3, rowIdDet.size(),
                "After join normalization, map should have scan entries + composed entry");
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

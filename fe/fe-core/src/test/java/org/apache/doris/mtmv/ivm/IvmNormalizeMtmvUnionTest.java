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
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.rules.rewrite.IvmNormalizeMtmv;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.List;

class IvmNormalizeMtmvUnionTest extends IvmDeltaTestBase {

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

    private LogicalUnion buildUnionAll(Plan... children) {
        List<Slot> firstOutput = children[0].getOutput();

        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        for (int i = 0; i < firstOutput.size(); i++) {
            Slot slot = firstOutput.get(i);
            outputs.add(new SlotReference(
                    StatementScopeIdGenerator.newExprId(),
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

        return new LogicalUnion(Qualifier.ALL, outputs.build(), childrenOutputs.build(),
                ImmutableList.of(), false, ImmutableList.copyOf(children));
    }

    private Plan normalizeUnionPlan(Plan unionPlan) {
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(unionPlan.getOutput());
        LogicalProject<?> project = new LogicalProject<>(exprs, unionPlan);
        LogicalResultSink<?> sink = new LogicalResultSink<>(exprs, project);
        ConnectContext ctx = newConnectContext();
        JobContext jobContext = newJobContextForRoot(sink, ctx);
        return new IvmNormalizeMtmv().rewriteRoot(sink, jobContext);
    }

    private IvmRewriteResult getRewriteResult(Plan unionPlan) {
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(unionPlan.getOutput());
        LogicalProject<?> project = new LogicalProject<>(exprs, unionPlan);
        LogicalResultSink<?> sink = new LogicalResultSink<>(exprs, project);
        ConnectContext ctx = newConnectContext();
        JobContext jobContext = newJobContextForRoot(sink, ctx);
        new IvmNormalizeMtmv().rewriteRoot(sink, jobContext);
        return jobContext.getCascadesContext().getIvmRewriteResult().get();
    }

    private boolean isUnionRowIdDeterministic(Plan unionPlan) {
        IvmRewriteResult result = getRewriteResult(unionPlan);
        Plan normalized = result.getNormalizedPlan();
        Slot rowIdSlot = IvmUtil.findRowIdSlot(normalized.getOutput(), "test plan");
        return result.isDeterministic(rowIdSlot);
    }

    @Test
    void testNormalizeBasicUnionAll() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalUnion union = buildUnionAll(scanA, scanB);

        Plan normalized = normalizeUnionPlan(union);

        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount, "Should have exactly one row_id");
    }

    @Test
    void testNormalizeThreeWayUnionAll() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");
        LogicalUnion union = buildUnionAll(scanA, scanB, scanC);

        Plan normalized = normalizeUnionPlan(union);

        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount, "Three-way union should have exactly one row_id");
    }

    @Test
    void testNormalizeUnionAllMowMowDeterministic() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalUnion union = buildUnionAll(scanA, scanB);

        Assertions.assertTrue(isUnionRowIdDeterministic(union),
                "MOW + MOW union should be deterministic");
    }

    @Test
    void testNormalizeUnionAllMowDupNonDeterministic() {
        LogicalOlapScan scanMow = buildMowScan(1, "mow_t");
        LogicalOlapScan scanDup = buildDupScan(2, "dup_t");
        LogicalUnion union = buildUnionAll(scanMow, scanDup);

        Assertions.assertFalse(isUnionRowIdDeterministic(union),
                "MOW + DUP union should be non-deterministic");
    }

    @Test
    void testNormalizeUnionAllDupDupNonDeterministic() {
        LogicalOlapScan scanA = buildDupScan(1, "dup_a");
        LogicalOlapScan scanB = buildDupScan(2, "dup_b");
        LogicalUnion union = buildUnionAll(scanA, scanB);

        Assertions.assertFalse(isUnionRowIdDeterministic(union),
                "DUP + DUP union should be non-deterministic");
    }

    @Test
    void testNormalizeSelfUnion() {
        LogicalOlapScan scanA1 = buildMowScan(1, "a");
        LogicalOlapScan scanA2 = buildMowScan(1, "a");
        LogicalUnion union = buildUnionAll(scanA1, scanA2);

        Plan normalized = normalizeUnionPlan(union);

        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount, "Self-union should have one row_id");

        // Verify that the two arms use different arm_index in their hash expressions
        // to prevent cross-arm row_id collision for the same source row.
        // Navigate: ResultSink → Project → Union → children (each is a Project with hash)
        Plan sinkChild = ((LogicalResultSink<?>) normalized).child(0);
        Plan unionNode = ((LogicalProject<?>) sinkChild).child(0);
        Assertions.assertEquals(2, unionNode.children().size(),
                "Self-union should still have 2 children");

        // Each child's first project expression is an Alias wrapping hash(arm_index, row_id).
        // Verify the two hash expressions are structurally different (different arm_index).
        LogicalProject<?> arm0 = (LogicalProject<?>) unionNode.child(0);
        LogicalProject<?> arm1 = (LogicalProject<?>) unionNode.child(1);
        String arm0RowIdExpr = arm0.getProjects().get(0).toSql();
        String arm1RowIdExpr = arm1.getProjects().get(0).toSql();
        Assertions.assertNotEquals(arm0RowIdExpr, arm1RowIdExpr,
                "Self-union arms must have different row_id hash expressions (different arm_index)");
    }

    @Test
    void testNormalizeNestedUnionAll() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");

        LogicalUnion innerUnion = buildUnionAll(scanA, scanB);
        LogicalUnion outerUnion = buildUnionAll(innerUnion, scanC);

        Plan normalized = normalizeUnionPlan(outerUnion);

        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount, "Nested union should have one row_id");
    }

    @Test
    void testNormalizeUnionDistinctThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        List<Slot> firstOutput = scanA.getOutput();

        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        for (int i = 0; i < firstOutput.size(); i++) {
            Slot slot = firstOutput.get(i);
            outputs.add(new SlotReference(
                    StatementScopeIdGenerator.newExprId(),
                    slot.getName(), slot.getDataType(), slot.nullable(), ImmutableList.of()));
        }

        ImmutableList.Builder<List<SlotReference>> childrenOutputs = ImmutableList.builder();
        for (Plan child : new Plan[]{scanA, scanB}) {
            ImmutableList.Builder<SlotReference> childMapping = ImmutableList.builder();
            for (Slot slot : child.getOutput()) {
                childMapping.add((SlotReference) slot);
            }
            childrenOutputs.add(childMapping.build());
        }

        LogicalUnion union = new LogicalUnion(Qualifier.DISTINCT, outputs.build(),
                childrenOutputs.build(), ImmutableList.of(), false,
                ImmutableList.of(scanA, scanB));

        assertIvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                () -> normalizeUnionPlan(union));
    }

    @Test
    void testNormalizeUnionWithConstantExprsThrows() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        List<Slot> firstOutput = scanA.getOutput();

        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        for (int i = 0; i < firstOutput.size(); i++) {
            Slot slot = firstOutput.get(i);
            outputs.add(new SlotReference(
                    StatementScopeIdGenerator.newExprId(),
                    slot.getName(), slot.getDataType(), slot.nullable(), ImmutableList.of()));
        }

        ImmutableList.Builder<List<SlotReference>> childrenOutputs = ImmutableList.builder();
        for (Plan child : new Plan[]{scanA, scanB}) {
            ImmutableList.Builder<SlotReference> childMapping = ImmutableList.builder();
            for (Slot slot : child.getOutput()) {
                childMapping.add((SlotReference) slot);
            }
            childrenOutputs.add(childMapping.build());
        }

        NamedExpression constExpr = new Alias(new IntegerLiteral(1), "const_col");
        List<List<NamedExpression>> constantExprs = ImmutableList.of(
                ImmutableList.of(constExpr));
        LogicalUnion union = new LogicalUnion(Qualifier.ALL, outputs.build(),
                childrenOutputs.build(), constantExprs, false,
                ImmutableList.of(scanA, scanB));

        assertIvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                () -> normalizeUnionPlan(union));
    }

    private void assertIvmException(IvmFailureReason failureReason, Executable executable) {
        IvmException exception = Assertions.assertThrows(IvmException.class, executable);
        Assertions.assertEquals(failureReason, exception.getFailureReason());
    }

    @Test
    void testNormalizeUnionJoinCombo() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalOlapScan scanC = buildMowScan(3, "c");

        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);
        LogicalUnion union = buildUnionAll(join, scanC);

        Plan normalized = normalizeUnionPlan(union);

        long rowIdCount = normalized.getOutput().stream()
                .filter(s -> Column.IVM_ROW_ID_COL.equals(s.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount,
                "Union of join and scan should have one row_id");
    }
}

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
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class IvmOuterJoinDeltaStrategyTest extends IvmDeltaTestBase {

    private static final class TestableIvmOuterJoinDeltaStrategy extends IvmOuterJoinDeltaStrategy {
        private RewriteResult exposeRewritePlan(Plan plan, IvmRefreshContext ctx) {
            return rewritePlan(plan, ctx);
        }
    }

    private static final class NormalizedOuterJoinPlan {
        private final LogicalProject<Plan> topProject;
        private final int leftOutputSize;
        private final IvmNormalizeResult normalizeResult;

        private NormalizedOuterJoinPlan(LogicalProject<Plan> topProject, int leftOutputSize,
                IvmNormalizeResult normalizeResult) {
            this.topProject = topProject;
            this.leftOutputSize = leftOutputSize;
            this.normalizeResult = normalizeResult;
        }
    }

    @Test
    void testPreservedSideDeltaUsesLeftOuterJoinUnderTopProject() {
        LogicalOlapScan leftDelta = (LogicalOlapScan) buildScanForTable(1, "t1").withIsDelta(true);
        LogicalOlapScan rightSnapshot = buildScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedOuterJoin(rowIdProject(leftDelta), rowIdProject(rightSnapshot));

        TestableIvmOuterJoinDeltaStrategy strategy = new TestableIvmOuterJoinDeltaStrategy();
        IvmRefreshContext ctx = context(bundle, ImmutableMap.<TableNameInfo, IvmStreamRef>of());

        IvmLinearDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(bundle.topProject, ctx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
        LogicalProject<?> topProject = (LogicalProject<?>) result.plan;
        Assertions.assertInstanceOf(LogicalJoin.class, topProject.child());
        Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN, ((LogicalJoin<?, ?>) topProject.child()).getJoinType());
        assertSingleFinalRowId(topProject);
    }

    @Test
    void testNullableSideDeltaBuildsRightEventsAndProbesLeftOnce() {
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan rightDelta = (LogicalOlapScan) buildScanForTable(2, "t2").withIsDelta(true);
        NormalizedOuterJoinPlan bundle = normalizedOuterJoin(rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        IvmStreamRef rightStream = stream(10, 20);
        TestableIvmOuterJoinDeltaStrategy strategy = new TestableIvmOuterJoinDeltaStrategy();
        IvmRefreshContext ctx = context(bundle, ImmutableMap.of(IvmRefreshContext.toTableNameInfo(rightDelta),
                rightStream));

        IvmLinearDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(bundle.topProject, ctx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
        LogicalProject<?> topProject = (LogicalProject<?>) result.plan;
        assertSingleFinalRowId(topProject);
        Assertions.assertInstanceOf(LogicalProject.class, topProject.child());
        LogicalProject<?> joinOutputProject = (LogicalProject<?>) topProject.child();
        Assertions.assertInstanceOf(LogicalJoin.class, joinOutputProject.child());
        LogicalJoin<?, ?> eventJoin = (LogicalJoin<?, ?>) joinOutputProject.child();
        Assertions.assertEquals(JoinType.INNER_JOIN, eventJoin.getJoinType());
        Assertions.assertInstanceOf(LogicalUnion.class, eventJoin.right());
        LogicalUnion union = (LogicalUnion) eventJoin.right();
        Assertions.assertEquals(3, union.children().size());
        assertUnionChildrenAlign(union);
        assertUnionOutputsDoNotReuseChildExprIds(union);

        LogicalProject<?> preNullProject = (LogicalProject<?>) union.child(1);
        LogicalProject<?> postNullProject = (LogicalProject<?>) union.child(2);
        assertRightNullEvent(preNullProject, (byte) -1);
        assertRightNullEvent(postNullProject, (byte) 1);
        assertContainsSubQueryAlias(preNullProject, "__DORIS_IVM_NULLABLE_KEY_DELTA__");
        assertContainsSubQueryAlias(preNullProject, "__DORIS_IVM_NULLABLE_PRE_SNAPSHOT__");
        assertContainsSubQueryAlias(postNullProject, "__DORIS_IVM_NULLABLE_KEY_DELTA__");
        assertContainsSubQueryAlias(postNullProject, "__DORIS_IVM_NULLABLE_POST_SNAPSHOT__");
        assertNoDuplicateScanRelationIds(result.plan);
    }

    @Test
    void testRightOuterJoinNullableSideDeltaBuildsNullableEvents() {
        LogicalOlapScan leftDelta = (LogicalOlapScan) buildScanForTable(1, "t1").withIsDelta(true);
        LogicalOlapScan rightSnapshot = buildScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedRightOuterJoin(rowIdProject(leftDelta),
                rowIdProject(rightSnapshot));

        IvmStreamRef leftStream = stream(10, 20);
        TestableIvmOuterJoinDeltaStrategy strategy = new TestableIvmOuterJoinDeltaStrategy();
        IvmRefreshContext ctx = context(bundle, ImmutableMap.of(IvmRefreshContext.toTableNameInfo(leftDelta),
                leftStream));

        IvmLinearDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> topProject = (LogicalProject<?>) result.plan;
        LogicalProject<?> joinOutputProject = (LogicalProject<?>) topProject.child();
        Assertions.assertInstanceOf(LogicalJoin.class, joinOutputProject.child());
        LogicalJoin<?, ?> eventJoin = (LogicalJoin<?, ?>) joinOutputProject.child();
        Assertions.assertEquals(JoinType.INNER_JOIN, eventJoin.getJoinType());
        Assertions.assertInstanceOf(LogicalUnion.class, eventJoin.right());
        LogicalUnion union = (LogicalUnion) eventJoin.right();
        Assertions.assertEquals(3, union.children().size());
        assertUnionChildrenAlign(union);
        assertRightNullEvent((LogicalProject<?>) union.child(1), (byte) -1);
        assertRightNullEvent((LogicalProject<?>) union.child(2), (byte) 1);
        assertNoDuplicateScanRelationIds(result.plan);
    }

    @Test
    void testNullableSideDeltaExtractsHashConjunctFromOtherConjuncts() {
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan rightDelta = (LogicalOlapScan) buildScanForTable(2, "t2").withIsDelta(true);
        NormalizedOuterJoinPlan bundle = normalizedOuterJoinWithOnlyOtherHashConjunct(
                rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        IvmStreamRef rightStream = stream(10, 20);
        TestableIvmOuterJoinDeltaStrategy strategy = new TestableIvmOuterJoinDeltaStrategy();
        IvmRefreshContext ctx = context(bundle, ImmutableMap.of(IvmRefreshContext.toTableNameInfo(rightDelta),
                rightStream));

        IvmLinearDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) result.plan).child();
        Assertions.assertInstanceOf(LogicalJoin.class, joinOutputProject.child());
        LogicalJoin<?, ?> eventJoin = (LogicalJoin<?, ?>) joinOutputProject.child();
        Assertions.assertEquals(JoinType.INNER_JOIN, eventJoin.getJoinType());
        Assertions.assertInstanceOf(LogicalUnion.class, eventJoin.right());
        Assertions.assertEquals(3, ((LogicalUnion) eventJoin.right()).children().size());
    }

    @Test
    void testNullableSideDeltaWithNonHashOtherConjunctFallsBackToRepairBranches() {
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan rightDelta = (LogicalOlapScan) buildScanForTable(2, "t2").withIsDelta(true);
        NormalizedOuterJoinPlan bundle = normalizedOuterJoinWithNonHashOtherConjunct(
                rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        IvmStreamRef rightStream = stream(10, 20);
        TestableIvmOuterJoinDeltaStrategy strategy = new TestableIvmOuterJoinDeltaStrategy();
        IvmRefreshContext ctx = context(bundle, ImmutableMap.of(IvmRefreshContext.toTableNameInfo(rightDelta),
                rightStream));

        IvmLinearDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) result.plan).child();
        Assertions.assertInstanceOf(LogicalUnion.class, joinOutputProject.child());
        LogicalUnion union = (LogicalUnion) joinOutputProject.child();
        Assertions.assertEquals(3, union.children().size());
        LogicalProject<?> preNullProject = (LogicalProject<?>) union.child(1);
        LogicalProject<?> postNullProject = (LogicalProject<?>) union.child(2);
        assertPaddedRightRowId(preNullProject, bundle.leftOutputSize, (byte) -1);
        assertPaddedRightRowId(postNullProject, bundle.leftOutputSize, (byte) 1);
    }

    @Test
    void testRightOuterJoinNullableSideDeltaWithNonHashOtherConjunctPadsLeftSide() {
        LogicalOlapScan leftDelta = (LogicalOlapScan) buildScanForTable(1, "t1").withIsDelta(true);
        LogicalOlapScan rightSnapshot = buildScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedRightOuterJoinWithNonHashOtherConjunct(
                rowIdProject(leftDelta), rowIdProject(rightSnapshot));

        IvmStreamRef leftStream = stream(10, 20);
        TestableIvmOuterJoinDeltaStrategy strategy = new TestableIvmOuterJoinDeltaStrategy();
        IvmRefreshContext ctx = context(bundle, ImmutableMap.of(IvmRefreshContext.toTableNameInfo(leftDelta),
                leftStream));

        IvmLinearDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) result.plan).child();
        Assertions.assertInstanceOf(LogicalUnion.class, joinOutputProject.child());
        LogicalUnion union = (LogicalUnion) joinOutputProject.child();
        Assertions.assertEquals(3, union.children().size());
        assertPaddedLeftRowId((LogicalProject<?>) union.child(1), bundle.leftOutputSize, (byte) -1);
        assertPaddedLeftRowId((LogicalProject<?>) union.child(2), bundle.leftOutputSize, (byte) 1);
    }

    @Test
    void testNullableSideDeltaWithUniqueFunctionHashConjunctFallsBackToRepairBranches() {
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan rightDelta = (LogicalOlapScan) buildScanForTable(2, "t2").withIsDelta(true);
        NormalizedOuterJoinPlan bundle = normalizedOuterJoinWithUniqueFunctionHashConjunct(
                rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        IvmStreamRef rightStream = stream(10, 20);
        TestableIvmOuterJoinDeltaStrategy strategy = new TestableIvmOuterJoinDeltaStrategy();
        IvmRefreshContext ctx = context(bundle, ImmutableMap.of(IvmRefreshContext.toTableNameInfo(rightDelta),
                rightStream));

        IvmLinearDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) result.plan).child();
        Assertions.assertInstanceOf(LogicalUnion.class, joinOutputProject.child());
        Assertions.assertEquals(3, ((LogicalUnion) joinOutputProject.child()).children().size());
    }

    @Test
    void testRewriteBuildsSinkWithFinalRowIdAndDeleteSign() {
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan rightDelta = (LogicalOlapScan) buildScanForTable(2, "t2").withIsDelta(true);
        NormalizedOuterJoinPlan bundle = normalizedOuterJoin(rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        IvmStreamRef rightStream = stream(10, 20);
        IvmRefreshContext ctx = context(bundle, ImmutableMap.of(
                IvmRefreshContext.toTableNameInfo(rightDelta), rightStream));

        List<Command> commands = IvmAggDeltaStrategy.INSTANCE.rewrite(bundle.topProject, ctx);

        Assertions.assertEquals(1, commands.size());
        UnboundTableSink<?> sink = getSink((InsertIntoTableCommand) commands.get(0));
        Assertions.assertTrue(sink.getColNames().contains(Column.IVM_ROW_ID_COL));
        Assertions.assertEquals(Column.DELETE_SIGN, sink.getColNames().get(sink.getColNames().size() - 1));
        Assertions.assertInstanceOf(LogicalProject.class, sink.child());
    }

    @Test
    void testNullableSideSnapshotOnlyReplacesDeltaScanInsideNullablePlan() {
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan rightDelta = (LogicalOlapScan) buildScanForTable(2, "t2").withIsDelta(true);
        LogicalOlapScan rightSnapshot = buildScanForTable(3, "t3").withTso(77);
        LogicalProject<Plan> nullableSide = normalizedInnerJoin(rowIdProject(rightDelta), rowIdProject(rightSnapshot));
        NormalizedOuterJoinPlan bundle = normalizedOuterJoin(rowIdProject(leftSnapshot), nullableSide);

        IvmStreamRef rightStream = stream(10, 20);
        TestableIvmOuterJoinDeltaStrategy strategy = new TestableIvmOuterJoinDeltaStrategy();
        IvmRefreshContext ctx = context(bundle, ImmutableMap.of(IvmRefreshContext.toTableNameInfo(rightDelta),
                rightStream));

        IvmLinearDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(bundle.topProject, ctx);
        LogicalUnion union = nullableSideRightEventUnion(result.plan);

        assertSnapshotBranch(union.child(1), 10);
        assertSnapshotBranch(union.child(2), 20);
    }

    @Test
    void testLeftDeepOuterJoinChainPropagatesPreservedSideDelta() {
        LogicalOlapScan leftDelta = (LogicalOlapScan) buildScanForTable(1, "t1").withIsDelta(true);
        LogicalOlapScan middleSnapshot = buildScanForTable(2, "t2");
        LogicalOlapScan rightSnapshot = buildScanForTable(3, "t3");
        NormalizedOuterJoinPlan firstJoin = normalizedOuterJoin(rowIdProject(leftDelta), rowIdProject(middleSnapshot));
        NormalizedOuterJoinPlan topJoin = normalizedOuterJoin(firstJoin.topProject, rowIdProject(rightSnapshot));

        TestableIvmOuterJoinDeltaStrategy strategy = new TestableIvmOuterJoinDeltaStrategy();
        IvmRefreshContext ctx = context(topJoin, ImmutableMap.<TableNameInfo, IvmStreamRef>of());

        IvmLinearDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(topJoin.topProject, ctx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(2, result.plan.collectToList(node ->
                node instanceof LogicalJoin
                        && ((LogicalJoin<?, ?>) node).getJoinType() == JoinType.LEFT_OUTER_JOIN).size());
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
        assertSingleFinalRowId((LogicalProject<?>) result.plan);
    }

    @Test
    void testLeftDeepOuterJoinChainRewritesTopNullableSideDelta() {
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan middleSnapshot = buildScanForTable(2, "t2");
        LogicalOlapScan rightDelta = (LogicalOlapScan) buildScanForTable(3, "t3").withIsDelta(true);
        NormalizedOuterJoinPlan firstJoin = normalizedOuterJoin(rowIdProject(leftSnapshot), rowIdProject(middleSnapshot));
        NormalizedOuterJoinPlan topJoin = normalizedOuterJoin(firstJoin.topProject, rowIdProject(rightDelta));

        IvmStreamRef rightStream = stream(10, 20);
        TestableIvmOuterJoinDeltaStrategy strategy = new TestableIvmOuterJoinDeltaStrategy();
        IvmRefreshContext ctx = context(topJoin, ImmutableMap.of(IvmRefreshContext.toTableNameInfo(rightDelta),
                rightStream));

        IvmLinearDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(topJoin.topProject, ctx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(1, result.plan.collectToList(node ->
                node instanceof LogicalJoin
                        && ((LogicalJoin<?, ?>) node).getJoinType() == JoinType.LEFT_OUTER_JOIN).size());
        LogicalUnion union = nullableSideRightEventUnion(result.plan);
        Assertions.assertEquals(3, union.children().size());
        assertUnionChildrenAlign(union);
        assertNoDuplicateScanRelationIds(result.plan);
    }

    private void assertSnapshotBranch(Plan branch, long expectedDeltaSnapshotTso) {
        List<LogicalOlapScan> scans = branch.collectToList(node -> node instanceof LogicalOlapScan);
        LogicalOlapScan deltaSnapshot = scans.stream()
                .filter(scan -> scan.getTable().getId() == 2 && !scan.isDelta())
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing copied nullable-side delta scan"));
        Assertions.assertEquals(expectedDeltaSnapshotTso, deltaSnapshot.getTso());

        LogicalOlapScan otherSnapshot = scans.stream()
                .filter(scan -> scan.getTable().getId() == 3)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing existing nullable-side snapshot scan"));
        Assertions.assertFalse(otherSnapshot.isDelta());
        Assertions.assertEquals(77, otherSnapshot.getTso());
    }

    private void assertSingleFinalRowId(LogicalProject<?> topProject) {
        long rowIdCount = topProject.getOutput().stream()
                .filter(slot -> Column.IVM_ROW_ID_COL.equals(slot.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount);
    }

    private void assertUnionChildrenAlign(LogicalUnion union) {
        int outputSize = union.getOutput().size();
        for (Plan child : union.children()) {
            Assertions.assertEquals(outputSize, child.getOutput().size());
            for (int i = 0; i < outputSize; i++) {
                Assertions.assertEquals(union.getOutput().get(i).getName(), child.getOutput().get(i).getName());
            }
        }
    }

    private void assertPaddedRightRowId(LogicalProject<?> project, int rightRowIdIndex, byte expectedDmlFactor) {
        Assertions.assertInstanceOf(Alias.class, project.getProjects().get(0));
        Alias leftRowIdAlias = (Alias) project.getProjects().get(0);
        Assertions.assertFalse(leftRowIdAlias.child() instanceof NullLiteral);

        Assertions.assertInstanceOf(Alias.class, project.getProjects().get(rightRowIdIndex));
        Alias rightRowIdAlias = (Alias) project.getProjects().get(rightRowIdIndex);
        Assertions.assertInstanceOf(NullLiteral.class, rightRowIdAlias.child());

        NamedExpression lastProject = project.getProjects().get(project.getProjects().size() - 1);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, lastProject.getName());
        Assertions.assertInstanceOf(Alias.class, lastProject);
        Expression dmlFactor = ((Alias) lastProject).child();
        Assertions.assertInstanceOf(TinyIntLiteral.class, dmlFactor);
        Assertions.assertEquals(expectedDmlFactor, ((TinyIntLiteral) dmlFactor).getValue());
    }

    private void assertPaddedLeftRowId(LogicalProject<?> project, int rightRowIdIndex, byte expectedDmlFactor) {
        Assertions.assertInstanceOf(Alias.class, project.getProjects().get(0));
        Alias leftRowIdAlias = (Alias) project.getProjects().get(0);
        Assertions.assertInstanceOf(NullLiteral.class, leftRowIdAlias.child());

        Assertions.assertInstanceOf(Alias.class, project.getProjects().get(rightRowIdIndex));
        Alias rightRowIdAlias = (Alias) project.getProjects().get(rightRowIdIndex);
        Assertions.assertFalse(rightRowIdAlias.child() instanceof NullLiteral);

        NamedExpression lastProject = project.getProjects().get(project.getProjects().size() - 1);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, lastProject.getName());
        Assertions.assertInstanceOf(Alias.class, lastProject);
        Expression dmlFactor = ((Alias) lastProject).child();
        Assertions.assertInstanceOf(TinyIntLiteral.class, dmlFactor);
        Assertions.assertEquals(expectedDmlFactor, ((TinyIntLiteral) dmlFactor).getValue());
    }

    private void assertRightNullEvent(LogicalProject<?> project, byte expectedDmlFactor) {
        int eventKeyCount = 1;
        for (int i = eventKeyCount; i < project.getProjects().size() - 1; i++) {
            Assertions.assertInstanceOf(Alias.class, project.getProjects().get(i));
            Alias rightValueAlias = (Alias) project.getProjects().get(i);
            Assertions.assertInstanceOf(NullLiteral.class, rightValueAlias.child());
        }

        NamedExpression lastProject = project.getProjects().get(project.getProjects().size() - 1);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, lastProject.getName());
        Assertions.assertInstanceOf(Alias.class, lastProject);
        Expression dmlFactor = ((Alias) lastProject).child();
        Assertions.assertInstanceOf(TinyIntLiteral.class, dmlFactor);
        Assertions.assertEquals(expectedDmlFactor, ((TinyIntLiteral) dmlFactor).getValue());
    }

    private LogicalUnion nullableSideRightEventUnion(Plan plan) {
        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) plan).child();
        LogicalJoin<?, ?> eventJoin = (LogicalJoin<?, ?>) joinOutputProject.child();
        return (LogicalUnion) eventJoin.right();
    }

    private void assertContainsSubQueryAlias(Plan plan, String alias) {
        boolean found = plan.collectToList(node -> node instanceof LogicalSubQueryAlias).stream()
                .map(node -> (LogicalSubQueryAlias<?>) node)
                .anyMatch(node -> alias.equals(node.getAlias()));
        Assertions.assertTrue(found, "Missing internal alias: " + alias);
    }

    private void assertUnionOutputsDoNotReuseChildExprIds(LogicalUnion union) {
        Set<Integer> childExprIds = new HashSet<>();
        for (Plan child : union.children()) {
            for (Slot slot : child.getOutput()) {
                childExprIds.add(slot.getExprId().asInt());
            }
        }
        for (Slot slot : union.getOutput()) {
            Assertions.assertFalse(childExprIds.contains(slot.getExprId().asInt()),
                    "Union output reuses a child ExprId: " + slot);
        }
    }

    private void assertNoDuplicateScanRelationIds(Plan plan) {
        Set<Integer> relationIds = new HashSet<>();
        for (LogicalOlapScan scan : plan.<LogicalOlapScan>collectToList(node -> node instanceof LogicalOlapScan)) {
            Assertions.assertTrue(relationIds.add(scan.getRelationId().asInt()),
                    "Duplicate scan relation id: " + scan.getRelationId());
        }
    }

    private IvmRefreshContext context(NormalizedOuterJoinPlan bundle, Map<TableNameInfo, IvmStreamRef> streams) {
        return new IvmRefreshContext(buildMtmvFromPlan(bundle.topProject.getOutput()),
                new ConnectContext(), bundle.normalizeResult, streams);
    }

    private IvmStreamRef stream(long consumedTso, long latestTso) {
        IvmStreamRef stream = new IvmStreamRef(consumedTso);
        stream.setLatestTso(latestTso);
        return stream;
    }

    private LogicalProject<Plan> rowIdProject(LogicalOlapScan scan) {
        Alias rowIdAlias = new Alias(scan.getOutput().get(0), Column.IVM_ROW_ID_COL);
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        projects.add(rowIdAlias);
        scan.getOutput().forEach(slot -> projects.add((NamedExpression) slot));
        return new LogicalProject<>(projects.build(), (Plan) scan);
    }

    private LogicalProject<Plan> normalizedInnerJoin(Plan left, Plan right) {
        LogicalJoin<?, ?> join = innerJoin(left, right);
        return normalizedJoinProject(join);
    }

    private NormalizedOuterJoinPlan normalizedOuterJoin(Plan left, Plan right) {
        LogicalJoin<?, ?> join = leftOuterJoin(left, right);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmNormalizeResult normalizeResult = deterministicNormalizeResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), normalizeResult);
    }

    private NormalizedOuterJoinPlan normalizedRightOuterJoin(Plan left, Plan right) {
        LogicalJoin<?, ?> join = rightOuterJoin(left, right);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmNormalizeResult normalizeResult = deterministicNormalizeResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), normalizeResult);
    }

    private NormalizedOuterJoinPlan normalizedOuterJoinWithOnlyOtherHashConjunct(Plan left, Plan right) {
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(),
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmNormalizeResult normalizeResult = deterministicNormalizeResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), normalizeResult);
    }

    private NormalizedOuterJoinPlan normalizedOuterJoinWithNonHashOtherConjunct(Plan left, Plan right) {
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                ImmutableList.of(new GreaterThan(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmNormalizeResult normalizeResult = deterministicNormalizeResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), normalizeResult);
    }

    private NormalizedOuterJoinPlan normalizedRightOuterJoinWithNonHashOtherConjunct(Plan left, Plan right) {
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                ImmutableList.of(new GreaterThan(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmNormalizeResult normalizeResult = deterministicNormalizeResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), normalizeResult);
    }

    private NormalizedOuterJoinPlan normalizedOuterJoinWithUniqueFunctionHashConjunct(Plan left, Plan right) {
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(new Add(firstUserSlot(left), new Random()), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmNormalizeResult normalizeResult = deterministicNormalizeResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), normalizeResult);
    }

    private LogicalProject<Plan> normalizedJoinProject(LogicalJoin<?, ?> join) {
        Slot leftRowId = IvmUtil.findRowIdSlot(join.left().getOutput(), "left child of join");
        Slot rightRowId = IvmUtil.findRowIdSlot(join.right().getOutput(), "right child of join");
        Alias rowIdAlias = new Alias(IvmUtil.buildRowIdHash(ImmutableList.of(leftRowId, rightRowId)),
                Column.IVM_ROW_ID_COL);
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        projects.add(rowIdAlias);
        for (Slot slot : join.getOutput()) {
            if (!Column.IVM_ROW_ID_COL.equals(slot.getName())) {
                projects.add(slot);
            }
        }
        return new LogicalProject<>(projects.build(), (Plan) join);
    }

    private IvmNormalizeResult deterministicNormalizeResult(Plan plan) {
        IvmNormalizeResult normalizeResult = new IvmNormalizeResult();
        List<Plan> nodes = plan.collectToList(node -> node instanceof Plan);
        for (Plan node : nodes) {
            for (Slot slot : node.getOutput()) {
                if (Column.IVM_ROW_ID_COL.equals(slot.getName())) {
                    normalizeResult.addRowId(slot, true);
                }
            }
        }
        return normalizeResult;
    }

    private LogicalJoin<?, ?> leftOuterJoin(Plan left, Plan right) {
        return new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
    }

    private LogicalJoin<?, ?> rightOuterJoin(Plan left, Plan right) {
        return new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
    }

    private LogicalJoin<?, ?> innerJoin(Plan left, Plan right) {
        return new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
    }

    private Slot firstUserSlot(Plan plan) {
        return plan.getOutput().stream()
                .filter(slot -> !Column.IVM_ROW_ID_COL.equals(slot.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing user slot in plan: " + plan));
    }
}

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

package org.apache.doris.nereids.rules.exploration;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.cascades.ApplyRuleJob;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.cascades.OptimizeGroupJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.MemoValidator;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class MergeProjectsCBOTest {
    private final ConnectContext connectContext = MemoTestUtils.createConnectContext();
    private final LogicalOlapScan scan = new LogicalOlapScan(
            StatementScopeIdGenerator.newRelationId(), PlanConstructor.student);

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIndirectDuplicateInEitherAlternativeOrder(boolean reverseAlternatives) {
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        LogicalProject<?> bottom = new LogicalProject<>(ImmutableList.of(slot), scan);
        LogicalProject<?> middle = new LogicalProject<>(
                ImmutableList.of(new Alias(new TinyIntLiteral((byte) 1)), slot), bottom);
        CascadesContext cascades = MemoTestUtils.createCascadesContext(connectContext,
                new LogicalProject<>(ImmutableList.of(slot), middle));
        Memo memo = cascades.getMemo();
        Group root = memo.getRoot();
        Group middleGroup = root.getLogicalExpression().child(0);
        Group bottomGroup = middleGroup.getLogicalExpression().child(0);
        Rule rule = new MergeProjectsCBO().build();

        new ApplyRuleJob(middleGroup.getLogicalExpression(), rule, cascades.getCurrentJobContext()).execute();
        Assertions.assertEquals(2, middleGroup.getLogicalExpressions().size());
        if (reverseAlternatives) {
            Collections.reverse(middleGroup.getLogicalExpressions());
        }
        new ApplyRuleJob(root.getLogicalExpression(), rule, cascades.getCurrentJobContext()).execute();

        MemoValidator.validate(memo);
        Assertions.assertSame(root, memo.getRoot());
        Assertions.assertEquals(4, memo.getGroups().size());
        Assertions.assertEquals(2, root.getLogicalExpressions().size());
        Assertions.assertTrue(root.getLogicalExpressions().stream()
                .anyMatch(expression -> expression.child(0) == bottomGroup));
        Assertions.assertEquals(6, memo.getGroupExpressionsSize());
    }

    @Test
    void testNewMergedProjectIsStillAdded() {
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        LogicalProject<?> middle = new LogicalProject<>(
                ImmutableList.of(new Alias(new TinyIntLiteral((byte) 1)), slot), scan);
        CascadesContext cascades = MemoTestUtils.createCascadesContext(connectContext,
                new LogicalProject<>(ImmutableList.of(slot), middle));
        Memo memo = cascades.getMemo();
        Group root = memo.getRoot();
        Group scanGroup = root.getLogicalExpression().child(0).getLogicalExpression().child(0);

        new ApplyRuleJob(root.getLogicalExpression(), new MergeProjectsCBO().build(),
                cascades.getCurrentJobContext()).execute();

        Assertions.assertEquals(2, root.getLogicalExpressions().size());
        Assertions.assertTrue(root.getLogicalExpressions().stream()
                .anyMatch(expression -> expression.child(0) == scanGroup));
        MemoValidator.validate(memo);
    }

    @Test
    void testRegisteredProjectUnifiesIndependentGroups() {
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        LogicalProject<?> middle = new LogicalProject<>(
                ImmutableList.of(new Alias(new TinyIntLiteral((byte) 1)), slot), scan);
        CascadesContext cascades = MemoTestUtils.createCascadesContext(connectContext,
                new LogicalProject<>(ImmutableList.of(slot), middle));
        Memo memo = cascades.getMemo();
        Group root = memo.getRoot();
        Group scanGroup = root.getLogicalExpression().child(0).getLogicalExpression().child(0);
        GroupExpression registered = memo.copyIn(new LogicalProject<>(ImmutableList.of(slot),
                new GroupPlan(scanGroup)), null, false).correspondingExpression;

        new ApplyRuleJob(root.getLogicalExpression(), new MergeProjectsCBO().build(),
                cascades.getCurrentJobContext()).execute();

        Assertions.assertSame(registered.getOwnerGroup(), memo.getRoot());
        Assertions.assertNull(memo.getGroup(root.getGroupId()));
        Assertions.assertEquals(2, memo.getRoot().getLogicalExpressions().size());
        Assertions.assertEquals(3, memo.getGroups().size());
        Assertions.assertSame(registered, memo.getGroupExpressions().get(registered));
        MemoValidator.validate(memo);
    }

    @Test
    void testDifferentProjectionWithSameHashIsNotSkipped() {
        LogicalProject<?> middle = new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), scan);
        LogicalProject<?> top = new LogicalProject<>(
                ImmutableList.of(scan.getOutput().get(0), scan.getOutput().get(3)), middle);
        CascadesContext cascades = MemoTestUtils.createCascadesContext(connectContext, top);
        Memo memo = cascades.getMemo();
        Group root = memo.getRoot();
        Group scanGroup = root.getLogicalExpression().child(0).getLogicalExpression().child(0);
        LogicalProject<?> collision = new LogicalProject<>(
                ImmutableList.of(scan.getOutput().get(1), scan.getOutput().get(2)), new GroupPlan(scanGroup));
        LogicalProject<?> merged = new LogicalProject<>(top.getProjects(), new GroupPlan(scanGroup));
        Assertions.assertEquals(collision.hashCode(), merged.hashCode());
        Assertions.assertNotEquals(collision, merged);
        memo.copyIn(collision, null, false);

        new ApplyRuleJob(root.getLogicalExpression(), new MergeProjectsCBO().build(),
                cascades.getCurrentJobContext()).execute();

        Assertions.assertEquals(2, root.getLogicalExpressions().size());
        Assertions.assertTrue(root.getLogicalExpressions().stream()
                .anyMatch(expression -> expression.child(0) == scanGroup));
        MemoValidator.validate(memo);
    }

    @Test
    void testOrdinaryCopyInStillMergesGroups() {
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        LogicalProject<?> middle = new LogicalProject<>(
                ImmutableList.of(new Alias(new TinyIntLiteral((byte) 1)), slot), scan);
        CascadesContext cascades = MemoTestUtils.createCascadesContext(connectContext,
                new LogicalProject<>(ImmutableList.of(slot), middle));
        Memo memo = cascades.getMemo();
        Group oldRoot = memo.getRoot();
        Group scanGroup = oldRoot.getLogicalExpression().child(0).getLogicalExpression().child(0);
        LogicalProject<?> merged = new LogicalProject<>(ImmutableList.of(slot), new GroupPlan(scanGroup));
        GroupExpression registered = memo.copyIn(merged, null, false).correspondingExpression;

        memo.copyIn(merged, oldRoot, false);

        Assertions.assertSame(registered.getOwnerGroup(), memo.getRoot());
        Assertions.assertNull(memo.getGroup(oldRoot.getGroupId()));
        MemoValidator.validate(memo);
    }

    @Test
    void testIndependentMergePreservesBestPlanAndCost() {
        Double referenceCost = null;
        String referenceShape = null;
        for (boolean useRule : new boolean[] {false, true}) {
            LogicalOlapScan localScan = new LogicalOlapScan(
                    StatementScopeIdGenerator.newRelationId(), PlanConstructor.student);
            SlotReference slot = (SlotReference) localScan.getOutput().get(0);
            LogicalProject<?> middle = new LogicalProject<>(
                    ImmutableList.of(new Alias(new TinyIntLiteral((byte) 1)), slot), localScan);
            CascadesContext cascades = MemoTestUtils.createCascadesContext(connectContext,
                    new LogicalProject<>(ImmutableList.of(slot), middle));
            Memo memo = cascades.getMemo();
            Group root = memo.getRoot();
            Group leaf = root.getLogicalExpression().child(0).getLogicalExpression().child(0);
            LogicalProject<?> merged = new LogicalProject<>(ImmutableList.of(slot), new GroupPlan(leaf));
            memo.copyIn(merged, null, false);
            if (useRule) {
                new ApplyRuleJob(root.getLogicalExpression(), new MergeProjectsCBO().build(),
                        cascades.getCurrentJobContext()).execute();
            } else {
                memo.copyIn(merged, root, false);
            }
            // Preserve this Memo: Optimizer.execute() would initialize a new one and discard the test setup.
            cascades.setJobContext(PhysicalProperties.GATHER);
            memo.getRoot().getLogicalExpressions().forEach(expression -> cascades.pushJob(
                    new DeriveStatsJob(expression, cascades.getCurrentJobContext())));
            cascades.getJobScheduler().executeJobPool(cascades);
            cascades.pushJob(new OptimizeGroupJob(memo.getRoot(), cascades.getCurrentJobContext()));
            cascades.getJobScheduler().executeJobPool(cascades);
            double cost = memo.getRoot().getLowestCostPlan(PhysicalProperties.GATHER).orElseThrow().first.getValue();
            PlanChecker checker = PlanChecker.from(cascades);
            String shape = checker.getBestPlanTree().shape("");
            Assertions.assertEquals(1, checker.getBestPlanTree()
                    .collect(plan -> plan instanceof PhysicalProject).size());
            if (useRule) {
                Assertions.assertEquals(referenceCost, cost);
                Assertions.assertEquals(referenceShape, shape);
            } else {
                referenceCost = cost;
                referenceShape = shape;
            }
            MemoValidator.validate(memo);
        }
    }

    @Test
    void testRetiredRunningInputKeepsRemainingMatchesInLiveGroup() {
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        List<NamedExpression> key = ImmutableList.of(slot);
        // Reuse the constant's ExprId so E0 has the same output in every group.
        List<NamedExpression> extra = ImmutableList.of(slot, new Alias(new TinyIntLiteral((byte) 0)));
        CascadesContext cascades = MemoTestUtils.createCascadesContext(connectContext,
                new LogicalProject<>(key, new LogicalProject<>(extra,
                        new LogicalFilter<>(ImmutableSet.of(BooleanLiteral.TRUE), scan))));
        Memo memo = cascades.getMemo();
        Group root = memo.getRoot();
        GroupExpression rootInput = root.getLogicalExpression();
        Group a = rootInput.child(0);
        Group filtered = a.getLogicalExpression().child(0);
        Group leaf = filtered.getLogicalExpression().child(0);

        Group e2 = addProject(memo, ImmutableList.of(slot, new Alias(new TinyIntLiteral((byte) 2))),
                filtered, null).getOwnerGroup();
        GroupExpression bFirst = addProject(memo, extra, e2, null);
        Group b = bFirst.getOwnerGroup();
        Group e6 = addProject(memo, ImmutableList.of(slot, new Alias(new TinyIntLiteral((byte) 6))),
                leaf, null).getOwnerGroup();
        GroupExpression bSecond = addProject(memo, extra, e6, b);
        Group e3 = addProject(memo, ImmutableList.of(slot, new Alias(new TinyIntLiteral((byte) 3))),
                filtered, null).getOwnerGroup();
        GroupExpression c = addProject(memo, extra, e3, null);
        GroupExpression q = addProject(memo, key, b, null);
        Group r = addProject(memo, key, filtered, null).getOwnerGroup();
        GroupExpression rInput = addProject(memo, key, c.getOwnerGroup(), r);
        memo.copyIn(new LogicalFilter<>(ImmutableSet.of(BooleanLiteral.TRUE),
                new GroupPlan(q.getOwnerGroup())), root, false);
        memo.copyIn(new LogicalFilter<>(ImmutableSet.of(BooleanLiteral.TRUE), new GroupPlan(r)), root, false);

        Rule rule = new MergeProjectsCBO().build();
        new ApplyRuleJob(rootInput, rule, cascades.getCurrentJobContext()).execute();
        new ApplyRuleJob(rInput, rule, cascades.getCurrentJobContext()).execute();
        GroupExpression rIntermediate = r.getLogicalExpressions().stream()
                .filter(expression -> expression.child(0) == e3).findFirst().orElseThrow();
        new ApplyRuleJob(rIntermediate, rule, cascades.getCurrentJobContext()).execute();
        new ApplyRuleJob(bFirst, rule, cascades.getCurrentJobContext()).execute();
        new ApplyRuleJob(c, rule, cascades.getCurrentJobContext()).execute();
        Assertions.assertSame(a, bSecond.getOwnerGroup());
        Assertions.assertSame(a, rInput.child(0));
        Assertions.assertTrue(rootInput.hasApplied(rule));
        Assertions.assertTrue(rInput.hasApplied(rule));
        // Introduce E0(L) after the rooted P(A) expressions have already run this rule.
        new ApplyRuleJob(bSecond, rule, cascades.getCurrentJobContext()).execute();

        // The first cached match merges Q into R and retires q. The last one produces P(L).
        new ApplyRuleJob(q, rule, cascades.getCurrentJobContext()).execute();

        Assertions.assertTrue(q.isUnused());
        Assertions.assertNull(q.getOwnerGroup());
        GroupExpression flat = memo.getGroupExpressions().values().stream()
                .filter(expression -> expression.getPlan() instanceof LogicalProject
                        && expression.arity() == 1 && expression.child(0) == leaf
                        && ((LogicalProject<?>) expression.getPlan()).getProjects().equals(key))
                .findFirst().orElseThrow();
        Set<Group> reachable = new HashSet<>();
        ArrayDeque<Group> pending = new ArrayDeque<>();
        pending.add(memo.getRoot());
        while (!pending.isEmpty()) {
            Group group = pending.removeFirst();
            if (reachable.add(group)) {
                group.getLogicalExpressions().forEach(expression -> pending.addAll(expression.children()));
            }
        }
        Assertions.assertTrue(reachable.contains(flat.getOwnerGroup()),
                "The remaining P(L) match must participate in rooted costing");
        Assertions.assertSame(r, flat.getOwnerGroup());
        MemoValidator.validate(memo);

        cascades.setJobContext(PhysicalProperties.GATHER);
        memo.getRoot().getLogicalExpressions().forEach(expression -> cascades.pushJob(
                new DeriveStatsJob(expression, cascades.getCurrentJobContext())));
        cascades.getJobScheduler().executeJobPool(cascades);
        cascades.pushJob(new OptimizeGroupJob(memo.getRoot(), cascades.getCurrentJobContext()));
        cascades.getJobScheduler().executeJobPool(cascades);
        Assertions.assertEquals(1, PlanChecker.from(cascades).getBestPlanTree()
                .collect(plan -> plan instanceof PhysicalProject).size());
        MemoValidator.validate(memo);
    }

    private static GroupExpression addProject(Memo memo, List<NamedExpression> projects, Group child, Group target) {
        return memo.copyIn(new LogicalProject<>(projects, new GroupPlan(child)), target, false)
                .correspondingExpression;
    }
}

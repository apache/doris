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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.rewrite.AddProjectForJoin;
import org.apache.doris.nereids.rules.rewrite.MergeProjectable;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

class ProjectAwareJoinReorderComplexProjectTest {

    @Test
    void completeClassicOptimizerReordersPlanAfterAddProjectForJoin() {
        LogicalPlan barePlan = buildBarePlan(4);
        List<String> expectedOutput = outputSignature(barePlan);
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.enableDPHypOptimizer = false;
        sessionVariable.setMaxTableCountUseCascadesJoinReorder(64);
        sessionVariable.joinReorderTimeLimit = 600_000;

        PlanChecker checker = PlanChecker.from(connectContext, barePlan)
                .customRewrite(new AddProjectForJoin())
                .applyTopDown(new MergeProjectable());
        Plan rewrittenPlan = checker.getCascadesContext().getRewritePlan();

        long joinCount = rewrittenPlan.collect(plan -> plan instanceof LogicalJoin).size();
        long projectOnJoinCount = rewrittenPlan.collect(plan -> plan instanceof LogicalProject
                && plan.child(0) instanceof LogicalJoin).size();
        Assertions.assertEquals(3, joinCount);
        Assertions.assertEquals(joinCount, projectOnJoinCount);
        Assertions.assertEquals(expectedOutput, outputSignature(rewrittenPlan));

        checker.optimize();
        Assertions.assertEquals(expectedOutput, outputSignature(checker.getBestPlanTree()));
        Set<String> joinTopologies = checker.getAllPlan().stream()
                .map(ProjectAwareJoinReorderComplexProjectTest::joinTopology)
                .filter(topology -> topology.startsWith("("))
                .collect(Collectors.toSet());
        Assertions.assertTrue(joinTopologies.size() > 1, joinTopologies::toString);
    }

    @Test
    void innerLAsscomMovesComplexExpressionWithItsInput() {
        LogicalOlapScan a = scan(0, "a");
        LogicalOlapScan b = scan(1, "b");
        LogicalOlapScan c = scan(2, "c");
        Alias alias = alias(a, "a_alias");
        LogicalPlan ab = project(join(a, b, JoinType.INNER_JOIN, 0, 0),
                alias, a.getOutput().get(1), b.getOutput().get(0), b.getOutput().get(1));
        LogicalPlan original = projectAll(join(ab, c, JoinType.INNER_JOIN, 0, 0));

        Plan reordered = applyAndFind(InnerJoinLAsscomProject.INSTANCE.build(), original,
                plan -> hasJoin(plan, JoinType.INNER_JOIN, names("a"), names("c"))
                        && hasJoin(plan, JoinType.INNER_JOIN, names("a", "c"), names("b")));

        assertSemanticsPreserved(original, reordered);
        assertAliasPlacedOnScan(reordered, alias, "a");
    }

    @Test
    void innerAssociatesInBothDirectionsWithComplexExpression() {
        LogicalOlapScan a = scan(0, "a");
        LogicalOlapScan b = scan(1, "b");
        LogicalOlapScan c = scan(2, "c");
        Alias rightAssociateAlias = alias(a, "a_alias");
        LogicalPlan ab = project(join(a, b, JoinType.INNER_JOIN, 0, 0),
                rightAssociateAlias, a.getOutput().get(1), b.getOutput().get(0), b.getOutput().get(1));
        LogicalPlan rightAssociateOriginal = projectAll(join(ab, c, JoinType.INNER_JOIN, 2, 0));

        Plan rightAssociated = applyAndFind(InnerJoinRightAssociateProject.INSTANCE.build(),
                rightAssociateOriginal,
                plan -> hasJoin(plan, JoinType.INNER_JOIN, names("b"), names("c"))
                        && hasJoin(plan, JoinType.INNER_JOIN, names("a"), names("b", "c")));
        assertSemanticsPreserved(rightAssociateOriginal, rightAssociated);
        assertAliasPlacedOnScan(rightAssociated, rightAssociateAlias, "a");

        Alias leftAssociateAlias = alias(b, "b_alias");
        LogicalPlan bc = project(join(b, c, JoinType.INNER_JOIN, 0, 0),
                leftAssociateAlias, b.getOutput().get(0), b.getOutput().get(1),
                c.getOutput().get(0), c.getOutput().get(1));
        LogicalPlan leftAssociateOriginal = projectAll(join(a, bc, JoinType.INNER_JOIN, 0, 1));

        Plan leftAssociated = applyAndFind(InnerJoinLeftAssociateProject.INSTANCE.build(),
                leftAssociateOriginal,
                plan -> hasJoin(plan, JoinType.INNER_JOIN, names("a"), names("b"))
                        && hasJoin(plan, JoinType.INNER_JOIN, names("a", "b"), names("c")));
        assertSemanticsPreserved(leftAssociateOriginal, leftAssociated);
        assertAliasPlacedOnScan(leftAssociated, leftAssociateAlias, "b");
    }

    @Test
    void exchangePushesIndependentComplexExpressionsOnBothBranches() {
        LogicalOlapScan a = scan(0, "a");
        LogicalOlapScan b = scan(1, "b");
        LogicalOlapScan c = scan(2, "c");
        LogicalOlapScan d = scan(3, "d");
        Alias aAlias = alias(a, "a_alias");
        Alias cAlias = alias(c, "c_alias");
        LogicalPlan ab = project(join(a, b, JoinType.INNER_JOIN, 0, 0),
                aAlias, a.getOutput().get(0), a.getOutput().get(1),
                b.getOutput().get(0), b.getOutput().get(1));
        LogicalPlan cd = project(join(c, d, JoinType.INNER_JOIN, 0, 0),
                cAlias, c.getOutput().get(0), c.getOutput().get(1),
                d.getOutput().get(0), d.getOutput().get(1));
        LogicalPlan original = projectAll(new LogicalPlanBuilder(ab)
                .join(cd, JoinType.INNER_JOIN, ImmutableList.of(Pair.of(1, 1), Pair.of(3, 3)))
                .build());

        Plan reordered = applyAndFind(JoinExchangeBothProject.INSTANCE.build(), original,
                plan -> hasJoin(plan, JoinType.INNER_JOIN, names("a"), names("c"))
                        && hasJoin(plan, JoinType.INNER_JOIN, names("b"), names("d")));

        assertSemanticsPreserved(original, reordered);
        assertAliasPlacedOnScan(reordered, aAlias, "a");
        assertAliasPlacedOnScan(reordered, cAlias, "c");
    }

    @Test
    void transposeLogicalJoinAndSemiJoinWithComplexExpression() {
        LogicalOlapScan a = scan(0, "a");
        LogicalOlapScan b = scan(1, "b");
        LogicalOlapScan c = scan(2, "c");
        Alias alias = alias(a, "a_alias");
        LogicalPlan semi = project(join(a, b, JoinType.LEFT_SEMI_JOIN, 0, 0),
                alias, a.getOutput().get(0), a.getOutput().get(1));
        LogicalPlan original = projectAll(join(semi, c, JoinType.INNER_JOIN, 0, 0));

        Plan reordered = applyAndFind(
                LogicalJoinSemiJoinTransposeProject.INSTANCE.buildRules().get(0), original,
                plan -> hasJoin(plan, JoinType.INNER_JOIN, names("a"), names("c"))
                        && hasJoin(plan, JoinType.LEFT_SEMI_JOIN, names("a", "c"), names("b")));

        assertSemanticsPreserved(original, reordered);
        assertAliasPlacedOnScan(reordered, alias, "a");
    }

    @Test
    void transposeTwoSemiJoinsWithComplexExpression() {
        LogicalOlapScan a = scan(0, "a");
        LogicalOlapScan b = scan(1, "b");
        LogicalOlapScan c = scan(2, "c");
        Alias alias = alias(a, "a_alias");
        LogicalPlan bottomSemi = project(join(a, b, JoinType.LEFT_SEMI_JOIN, 0, 0),
                alias, a.getOutput().get(0), a.getOutput().get(1));
        LogicalPlan original = projectAll(join(bottomSemi, c, JoinType.LEFT_SEMI_JOIN, 0, 0));

        Plan reordered = applyAndFind(SemiJoinSemiJoinTransposeProject.INSTANCE.build(), original,
                plan -> hasJoin(plan, JoinType.LEFT_SEMI_JOIN, names("a"), names("c"))
                        && hasJoin(plan, JoinType.LEFT_SEMI_JOIN, names("a", "c"), names("b")));

        assertSemanticsPreserved(original, reordered);
        assertAliasPlacedOnScan(reordered, alias, "a");
    }

    @Test
    void outerJoinReordersKeepComplexExpressionOnNonNullableInput() {
        LogicalOlapScan a = scan(0, "a");
        LogicalOlapScan b = scan(1, "b");
        LogicalOlapScan c = scan(2, "c");
        Alias lAsscomAlias = alias(a, "a_lasscom_alias");
        LogicalPlan abForLAsscom = project(join(a, b, JoinType.INNER_JOIN, 0, 0),
                lAsscomAlias, a.getOutput().get(0), a.getOutput().get(1),
                b.getOutput().get(0), b.getOutput().get(1));
        LogicalPlan lAsscomOriginal = projectAll(join(abForLAsscom, c, JoinType.LEFT_OUTER_JOIN, 0, 0));

        Plan lAsscom = applyAndFind(OuterJoinLAsscomProject.INSTANCE.build(), lAsscomOriginal,
                plan -> hasJoin(plan, JoinType.LEFT_OUTER_JOIN, names("a"), names("c"))
                        && hasJoin(plan, JoinType.INNER_JOIN, names("a", "c"), names("b")));
        assertSemanticsPreserved(lAsscomOriginal, lAsscom);
        assertAliasPlacedOnScan(lAsscom, lAsscomAlias, "a");

        Alias assocAlias = alias(a, "a_assoc_alias");
        LogicalPlan abForAssoc = project(join(a, b, JoinType.INNER_JOIN, 0, 0),
                assocAlias, a.getOutput().get(0), a.getOutput().get(1),
                b.getOutput().get(0), b.getOutput().get(1));
        LogicalPlan assocOriginal = projectAll(join(abForAssoc, c, JoinType.LEFT_OUTER_JOIN, 3, 0));

        Plan assoc = applyAndFind(OuterJoinAssocProject.INSTANCE.build(), assocOriginal,
                plan -> hasJoin(plan, JoinType.LEFT_OUTER_JOIN, names("b"), names("c"))
                        && hasJoin(plan, JoinType.INNER_JOIN, names("a"), names("b", "c")));
        assertSemanticsPreserved(assocOriginal, assoc);
        assertAliasPlacedOnScan(assoc, assocAlias, "a");
    }

    private static Plan applyAndFind(Rule rule, LogicalPlan original, Predicate<Plan> predicate) {
        List<Plan> plans = PlanChecker.from(MemoTestUtils.createConnectContext(), original)
                .applyExploration(rule)
                .getAllPlan();
        return plans.stream()
                .filter(predicate)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected reordered alternative, found " + plans.size()));
    }

    private static void assertSemanticsPreserved(Plan original, Plan reordered) {
        Assertions.assertEquals(outputSignature(original), outputSignature(reordered));
        Assertions.assertEquals(conditionSignatures(original), conditionSignatures(reordered));
    }

    private static void assertAliasPlacedOnScan(Plan plan, Alias alias, String scanName) {
        Assertions.assertTrue(anyPlan(plan, candidate -> candidate instanceof LogicalProject
                && ((LogicalProject<?>) candidate).getProjects().contains(alias)
                && scanNames(candidate.child(0)).equals(names(scanName))));
    }

    private static boolean hasJoin(Plan plan, JoinType type, Set<String> left, Set<String> right) {
        return anyPlan(plan, candidate -> candidate instanceof LogicalJoin
                && ((LogicalJoin<?, ?>) candidate).getJoinType() == type
                && scanNames(candidate.child(0)).equals(left)
                && scanNames(candidate.child(1)).equals(right));
    }

    private static boolean anyPlan(Plan plan, Predicate<Plan> predicate) {
        if (predicate.test(plan)) {
            return true;
        }
        for (Plan child : plan.children()) {
            if (anyPlan(child, predicate)) {
                return true;
            }
        }
        return false;
    }

    private static Set<String> scanNames(Plan plan) {
        ImmutableSet.Builder<String> names = ImmutableSet.builder();
        collectScanNames(plan, names);
        return names.build();
    }

    private static void collectScanNames(Plan plan, ImmutableSet.Builder<String> names) {
        if (plan instanceof LogicalOlapScan) {
            names.add(((LogicalOlapScan) plan).getTable().getName());
        }
        plan.children().forEach(child -> collectScanNames(child, names));
    }

    private static List<String> conditionSignatures(Plan plan) {
        List<String> signatures = new ArrayList<>();
        collectConditionSignatures(plan, signatures);
        Collections.sort(signatures);
        return signatures;
    }

    private static void collectConditionSignatures(Plan plan, List<String> signatures) {
        if (plan instanceof LogicalJoin) {
            LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
            join.getHashJoinConjuncts().stream().map(Expression::toSql).forEach(signatures::add);
            join.getOtherJoinConjuncts().stream().map(Expression::toSql).forEach(signatures::add);
            join.getMarkJoinConjuncts().stream().map(Expression::toSql).forEach(signatures::add);
        }
        plan.children().forEach(child -> collectConditionSignatures(child, signatures));
    }

    private static List<String> outputSignature(Plan plan) {
        return plan.getOutput().stream()
                .map(slot -> slot.getExprId() + ":" + slot.getName() + ":" + slot.nullable())
                .collect(Collectors.toList());
    }

    private static LogicalPlan buildBarePlan(int tableCount) {
        LogicalPlan plan = scan(0, "t0");
        for (int i = 1; i < tableCount; i++) {
            LogicalOlapScan right = scan(i, "t" + i);
            plan = join(plan, right, JoinType.INNER_JOIN, 2 * (i - 1), 0);
        }
        return plan;
    }

    private static String joinTopology(Plan plan) {
        if (plan instanceof LogicalProject) {
            return joinTopology(plan.child(0));
        }
        if (plan instanceof LogicalOlapScan) {
            return ((LogicalOlapScan) plan).getTable().getName();
        }
        if (plan instanceof LogicalJoin) {
            return "(" + joinTopology(plan.child(0)) + "," + joinTopology(plan.child(1)) + ")";
        }
        return plan.getClass().getSimpleName();
    }

    private static LogicalOlapScan scan(int id, String name) {
        return PlanConstructor.newLogicalOlapScan(id, name, 0);
    }

    private static Alias alias(LogicalOlapScan scan, String name) {
        return new Alias(new Add(scan.getOutput().get(0), new IntegerLiteral(1)), name);
    }

    private static LogicalPlan join(LogicalPlan left, LogicalPlan right, JoinType type,
            int leftIndex, int rightIndex) {
        return new LogicalPlanBuilder(left)
                .join(right, type, Pair.of(leftIndex, rightIndex))
                .build();
    }

    private static LogicalPlan project(LogicalPlan child, NamedExpression... projects) {
        return new LogicalProject<>(ImmutableList.copyOf(projects), child);
    }

    private static LogicalPlan projectAll(LogicalPlan child) {
        return new LogicalProject<>(ImmutableList.copyOf(child.getOutput()), child);
    }

    private static Set<String> names(String... names) {
        return ImmutableSet.copyOf(names);
    }
}

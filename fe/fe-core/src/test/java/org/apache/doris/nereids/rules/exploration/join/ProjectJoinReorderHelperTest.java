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
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class ProjectJoinReorderHelperTest {
    private final LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "left", 0);
    private final LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "right", 0);

    @Test
    void keepSlotOnlyProjectUnchanged() {
        LogicalJoin<LogicalPlan, LogicalPlan> join = join(JoinType.INNER_JOIN);
        LogicalProject<LogicalJoin<LogicalPlan, LogicalPlan>> project
                = new LogicalProject<>(ImmutableList.copyOf(join.getOutput()), join);

        Optional<LogicalProject<LogicalJoin<Plan, Plan>>> normalized
                = ProjectJoinReorderHelper.normalize(project);

        Assertions.assertTrue(normalized.isPresent());
        Assertions.assertSame(project, normalized.get());
    }

    @Test
    void registerStandalonePushDownRulesOnlyAfterDpHyp() {
        List<RuleType> classicRuleTypes = RuleSet.OTHER_REORDER_RULES.stream()
                .map(Rule::getRuleType)
                .collect(Collectors.toList());
        List<RuleType> afterDpHypRuleTypes = RuleSet.AFTER_DPHYP_REORDER_RULES.stream()
                .map(Rule::getRuleType)
                .collect(Collectors.toList());

        Assertions.assertFalse(classicRuleTypes.contains(
                RuleType.PUSH_DOWN_PROJECT_THROUGH_INNER_OUTER_JOIN_LEFT));
        Assertions.assertFalse(classicRuleTypes.contains(
                RuleType.PUSH_DOWN_PROJECT_THROUGH_INNER_OUTER_JOIN_RIGHT));
        Assertions.assertFalse(classicRuleTypes.contains(
                RuleType.PUSH_DOWN_PROJECT_THROUGH_SEMI_JOIN_LEFT));
        Assertions.assertFalse(classicRuleTypes.contains(
                RuleType.PUSH_DOWN_PROJECT_THROUGH_SEMI_JOIN_RIGHT));
        Assertions.assertTrue(afterDpHypRuleTypes.contains(
                RuleType.PUSH_DOWN_PROJECT_THROUGH_INNER_OUTER_JOIN_LEFT));
        Assertions.assertTrue(afterDpHypRuleTypes.contains(
                RuleType.PUSH_DOWN_PROJECT_THROUGH_INNER_OUTER_JOIN_RIGHT));
        Assertions.assertTrue(afterDpHypRuleTypes.contains(
                RuleType.PUSH_DOWN_PROJECT_THROUGH_SEMI_JOIN_LEFT));
        Assertions.assertTrue(afterDpHypRuleTypes.contains(
                RuleType.PUSH_DOWN_PROJECT_THROUGH_SEMI_JOIN_RIGHT));
    }

    @Test
    void pushComplexExpressionsToBothInputsAndRestoreConditionSlots() {
        LogicalJoin<LogicalPlan, LogicalPlan> join = join(JoinType.INNER_JOIN);
        Alias leftAlias = new Alias(new Add(left.getOutput().get(1), new IntegerLiteral(1)), "left_alias");
        Alias rightAlias = new Alias(new Add(right.getOutput().get(1), new IntegerLiteral(2)), "right_alias");
        LogicalProject<LogicalJoin<LogicalPlan, LogicalPlan>> project
                = new LogicalProject<>(ImmutableList.of(leftAlias, rightAlias), join);

        LogicalProject<LogicalJoin<Plan, Plan>> normalized
                = ProjectJoinReorderHelper.normalize(project).orElseThrow();
        LogicalJoin<? extends Plan, ? extends Plan> normalizedJoin = normalized.child();

        Assertions.assertTrue(normalized.getProjects().stream().allMatch(Slot.class::isInstance));
        Assertions.assertEquals(outputSignature(project), outputSignature(normalized));
        Assertions.assertEquals(join.getHashJoinConjuncts(), normalizedJoin.getHashJoinConjuncts());
        assertInputProject(normalizedJoin.left(), leftAlias, left.getOutput().get(0));
        assertInputProject(normalizedJoin.right(), rightAlias, right.getOutput().get(0));
    }

    @Test
    void rejectHyperEdgeProject() {
        LogicalJoin<LogicalPlan, LogicalPlan> join = join(JoinType.INNER_JOIN);
        Alias hyperEdge = new Alias(new Add(left.getOutput().get(0), right.getOutput().get(0)), "both");
        LogicalProject<LogicalJoin<LogicalPlan, LogicalPlan>> project
                = new LogicalProject<>(ImmutableList.of(hyperEdge), join);

        Assertions.assertTrue(ProjectJoinReorderHelper.normalize(project).isEmpty());
    }

    @Test
    void respectOuterJoinNullableSides() {
        Alias leftAlias = new Alias(new Add(left.getOutput().get(1), new IntegerLiteral(1)), "left_alias");
        Alias rightAlias = new Alias(new Add(right.getOutput().get(1), new IntegerLiteral(1)), "right_alias");

        LogicalJoin<LogicalPlan, LogicalPlan> leftOuter = join(JoinType.LEFT_OUTER_JOIN);
        LogicalProject<LogicalJoin<LogicalPlan, LogicalPlan>> leftOnly
                = new LogicalProject<>(ImmutableList.of(leftAlias, leftOuter.getOutput().get(2)), leftOuter);
        LogicalProject<LogicalJoin<Plan, Plan>> normalized
                = ProjectJoinReorderHelper.normalize(leftOnly).orElseThrow();
        Assertions.assertEquals(outputSignature(leftOnly), outputSignature(normalized));
        Assertions.assertFalse(normalized.child().right() instanceof LogicalProject);

        LogicalProject<LogicalJoin<LogicalPlan, LogicalPlan>> leftAndNullableRight
                = new LogicalProject<>(ImmutableList.of(leftAlias, rightAlias), leftOuter);
        Assertions.assertTrue(ProjectJoinReorderHelper.normalize(leftAndNullableRight).isEmpty());

        LogicalJoin<LogicalPlan, LogicalPlan> rightOuter = join(JoinType.RIGHT_OUTER_JOIN);
        LogicalProject<LogicalJoin<LogicalPlan, LogicalPlan>> nullableLeft
                = new LogicalProject<>(ImmutableList.of(leftAlias), rightOuter);
        Assertions.assertTrue(ProjectJoinReorderHelper.normalize(nullableLeft).isEmpty());

        LogicalJoin<LogicalPlan, LogicalPlan> fullOuter = join(JoinType.FULL_OUTER_JOIN);
        LogicalProject<LogicalJoin<LogicalPlan, LogicalPlan>> fullOuterProject
                = new LogicalProject<>(ImmutableList.of(leftAlias, rightAlias), fullOuter);
        Assertions.assertTrue(ProjectJoinReorderHelper.normalize(fullOuterProject).isEmpty());
    }

    @Test
    void pushLeftSemiProjectAndRejectMarkJoin() {
        LogicalJoin<LogicalPlan, LogicalPlan> semiJoin = join(JoinType.LEFT_SEMI_JOIN);
        Alias alias = new Alias(new Add(left.getOutput().get(1), new IntegerLiteral(1)), "semi_alias");
        LogicalProject<LogicalJoin<LogicalPlan, LogicalPlan>> project
                = new LogicalProject<>(ImmutableList.of(alias), semiJoin);

        LogicalProject<LogicalJoin<Plan, Plan>> normalized
                = ProjectJoinReorderHelper.normalize(project).orElseThrow();
        assertInputProject(normalized.child().left(), alias, left.getOutput().get(0));
        Assertions.assertEquals(outputSignature(project), outputSignature(normalized));

        LogicalJoin<LogicalPlan, LogicalPlan> markJoin = (LogicalJoin<LogicalPlan, LogicalPlan>)
                new LogicalPlanBuilder(left)
                        .markJoin(right, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                        .build();
        LogicalProject<LogicalJoin<LogicalPlan, LogicalPlan>> markProject
                = new LogicalProject<>(ImmutableList.of(alias), markJoin);
        Assertions.assertTrue(ProjectJoinReorderHelper.normalize(markProject).isEmpty());
    }

    @Test
    void preserveExistingLiteralAndVolatilePlacementBehavior() {
        LogicalJoin<LogicalPlan, LogicalPlan> join = join(JoinType.INNER_JOIN);
        Alias literal = new Alias(new IntegerLiteral(1), "literal_alias");
        Alias random = new Alias(new Random(), "random_alias");
        LogicalProject<LogicalJoin<LogicalPlan, LogicalPlan>> project
                = new LogicalProject<>(ImmutableList.of(literal, random), join);

        LogicalProject<LogicalJoin<Plan, Plan>> normalized
                = ProjectJoinReorderHelper.normalize(project).orElseThrow();

        assertInputProject(normalized.child().left(), literal, random, left.getOutput().get(0));
        Assertions.assertEquals(outputSignature(project), outputSignature(normalized));
    }

    private LogicalJoin<LogicalPlan, LogicalPlan> join(JoinType joinType) {
        return (LogicalJoin<LogicalPlan, LogicalPlan>) new LogicalPlanBuilder(left)
                .join(right, joinType, Pair.of(0, 0))
                .build();
    }

    private static void assertInputProject(Plan input, NamedExpression... expectedProjects) {
        Assertions.assertInstanceOf(LogicalProject.class, input);
        LogicalProject<?> project = (LogicalProject<?>) input;
        Assertions.assertEquals(ImmutableList.copyOf(expectedProjects), project.getProjects());
    }

    private static List<String> outputSignature(Plan plan) {
        return plan.getOutput().stream()
                .map(slot -> slot.getExprId() + ":" + slot.getName() + ":" + slot.nullable())
                .collect(Collectors.toList());
    }
}

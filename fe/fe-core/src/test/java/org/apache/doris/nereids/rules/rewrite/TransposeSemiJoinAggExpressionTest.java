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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.exploration.TransposeAggSemiJoin;
import org.apache.doris.nereids.rules.exploration.TransposeAggSemiJoinProject;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

class TransposeSemiJoinAggExpressionTest {
    enum Location {
        HASH_CONDITION, OTHER_CONDITION, PROJECT, GROUP_KEY, AGG_ARGUMENT
    }

    enum ExpressionKind {
        SAFE, VOLATILE, NON_MOVABLE
    }

    static Stream<Arguments> cases() {
        List<Arguments> cases = new ArrayList<>();
        for (JoinType joinType : new JoinType[] {JoinType.LEFT_SEMI_JOIN, JoinType.LEFT_ANTI_JOIN}) {
            for (boolean eager : new boolean[] {true, false}) {
                for (boolean withProject : new boolean[] {true, false}) {
                    for (Location location : Location.values()) {
                        if (location == Location.PROJECT && !withProject) {
                            continue;
                        }
                        for (ExpressionKind kind : ExpressionKind.values()) {
                            cases.add(Arguments.of(joinType, eager, withProject, location, kind));
                        }
                    }
                }
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0}, eager={1}, project={2}, {3}, {4}")
    @MethodSource("cases")
    void expressionMovement(JoinType joinType, boolean eager, boolean withProject,
            Location location, ExpressionKind kind) {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        Slot key = left.getOutput().get(0);
        Slot value = left.getOutput().get(1);
        Expression expression;
        switch (kind) {
            case VOLATILE:
                expression = new Cast(new Random(), IntegerType.INSTANCE);
                break;
            case NON_MOVABLE:
                expression = new Cast(new AssertTrue(new GreaterThan(key, new IntegerLiteral(0)),
                        new VarcharLiteral("positive key")), IntegerType.INSTANCE);
                break;
            default:
                expression = new Add(key, new IntegerLiteral(1));
        }
        Expression hashLeft = location == Location.HASH_CONDITION ? new Add(key, expression) : key;
        List<Expression> hash = ImmutableList.of(new EqualTo(hashLeft, right.getOutput().get(0)));
        List<Expression> other = location == Location.OTHER_CONDITION
                ? ImmutableList.of(new GreaterThan(expression, new IntegerLiteral(0))) : ImmutableList.of();
        List<Expression> groupKeys = location == Location.GROUP_KEY
                ? ImmutableList.of(key, expression) : ImmutableList.of(key);
        Alias projected = new Alias(expression, "checked");
        Expression argument = location == Location.AGG_ARGUMENT ? expression : value;
        LogicalPlanBuilder builder = new LogicalPlanBuilder(left);
        if (eager) {
            builder = builder.join(right, joinType, hash, other);
            if (withProject) {
                List<NamedExpression> projects = new ArrayList<>(left.getOutput());
                if (location == Location.PROJECT && kind != ExpressionKind.SAFE) {
                    projects.add(projected);
                    argument = projected.toSlot();
                }
                builder = builder.projectExprs(projects);
            }
        }
        builder = builder.agg(groupKeys, ImmutableList.of(key, new Alias(new Sum(argument), "sum")));
        if (!eager) {
            if (withProject) {
                List<NamedExpression> projects = new ArrayList<>(builder.build().getOutput());
                if (location == Location.PROJECT && kind != ExpressionKind.SAFE) {
                    projects.add(projected);
                }
                builder = builder.projectExprs(projects);
            }
            builder = builder.join(right, joinType, hash, other);
        }
        LogicalPlan plan = builder.build();
        PlanChecker checker = PlanChecker.from(MemoTestUtils.createConnectContext(), plan);
        if (eager) {
            checker.applyExploration(withProject ? TransposeAggSemiJoinProject.INSTANCE.build()
                    : TransposeAggSemiJoin.INSTANCE.build());
            Assertions.assertEquals(kind == ExpressionKind.SAFE ? 2 : 1, checker.getAllPlan().size());
        } else {
            checker.applyTopDown(withProject ? new TransposeSemiJoinAggProject() : new TransposeSemiJoinAgg());
            Assertions.assertEquals(kind == ExpressionKind.SAFE,
                    !plan.treeString().equals(checker.getPlan().treeString()));
        }
    }
}

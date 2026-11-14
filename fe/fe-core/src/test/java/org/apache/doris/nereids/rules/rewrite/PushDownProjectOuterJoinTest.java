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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.PreferPushDownProject;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayFilter;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsEntry;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsKey;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsValue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapEntries;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapFromArrays;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapFromEntries;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapKeys;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapValues;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PushDownProjectOuterJoinTest {
    private static final List<JoinType> JOIN_TYPES = ImmutableList.of(
            JoinType.INNER_JOIN, JoinType.CROSS_JOIN,
            JoinType.LEFT_OUTER_JOIN, JoinType.RIGHT_OUTER_JOIN, JoinType.FULL_OUTER_JOIN,
            JoinType.ASOF_LEFT_INNER_JOIN, JoinType.ASOF_RIGHT_INNER_JOIN,
            JoinType.ASOF_LEFT_OUTER_JOIN, JoinType.ASOF_RIGHT_OUTER_JOIN);

    @Test
    public void testProjectPushdownRespectsNullExtendedSide() {
        checkPushdown(false);
    }

    @Test
    public void testFilterPushdownRespectsNullExtendedSide() {
        checkPushdown(true);
    }

    private void checkPushdown(boolean filter) {
        ConnectContext context = MemoTestUtils.createConnectContext();
        context.getSessionVariable().enablePruneNestedColumns = true;
        for (JoinType type : JOIN_TYPES) {
            LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "left_table", 0);
            LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "right_table", 0);
            // Materialize both scans before PlanChecker installs a new statement ID generator.
            left.getOutput();
            right.getOutput();
            LogicalJoin<Plan, Plan> join = new LogicalJoin<>(type, left, right, null);
            for (int side = 0; side < 2; side++) {
                SlotReference slot = (SlotReference) join.child(side).getOutput().get(0);
                boolean nullableSide = side == 0 ? type.isLeftSideNullable() : type.isRightSideNullable();
                for (Expression expression : expressions(slot)) {
                    Plan plan = filter
                            ? new LogicalFilter<>(ImmutableSet.of(new IsNull(expression)), join)
                            : new LogicalProject<>(ImmutableList.of(new Alias(expression)), join);
                    Plan result = PlanChecker.from(context, plan)
                            .applyTopDown(new PushDownProject()).getPlan();
                    Plan rewrittenJoin = result.child(0);
                    String message = type + ", side=" + side + ", expression=" + expression;
                    Assertions.assertEquals(!nullableSide,
                            rewrittenJoin.child(side) instanceof LogicalProject, message);
                    Assertions.assertFalse(rewrittenJoin.child(1 - side) instanceof LogicalProject, message);
                    Expression remaining = filter
                            ? ((LogicalFilter<?>) result).getConjuncts().iterator().next()
                            : ((LogicalProject<?>) result).getProjects().get(0);
                    Assertions.assertEquals(nullableSide,
                            remaining.containsType(PreferPushDownProject.class), message);
                }
            }
        }
    }

    @Test
    public void testNullPropagatingExpressionsStillPushDown() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        context.getSessionVariable().enablePruneNestedColumns = true;
        for (JoinType type : JOIN_TYPES) {
            for (int side = 0; side < 2; side++) {
                SlotReference array = new SlotReference("arr", ArrayType.of(StringType.INSTANCE));
                SlotReference map = new SlotReference("m", MapType.of(StringType.INSTANCE, StringType.INSTANCE));
                Plan input = new LogicalOneRowRelation(new RelationId(100), ImmutableList.of(array, map));
                Plan other = PlanConstructor.newLogicalOlapScan(0, "other_table", 0);
                other.getOutput();
                LogicalJoin<Plan, Plan> join = new LogicalJoin<>(type,
                        side == 0 ? input : other, side == 0 ? other : input, null);
                ArrayItemReference item = new ArrayItemReference("x", array);
                Lambda lambda = new Lambda(ImmutableList.of("x"), item.toSlot(), ImmutableList.of(item));
                for (Expression expression : ImmutableList.of(
                        new ArrayMap(lambda), new MapKeys(map), new MapValues(map),
                        new ElementAt(array, new IntegerLiteral(1)))) {
                    for (boolean filter : ImmutableList.of(false, true)) {
                        Plan plan = filter
                                ? new LogicalFilter<>(ImmutableSet.of(new IsNull(expression)), join)
                                : new LogicalProject<>(ImmutableList.of(new Alias(expression)), join);
                        Plan result = PlanChecker.from(context, plan)
                                .applyTopDown(new PushDownProject()).getPlan();
                        Assertions.assertInstanceOf(LogicalProject.class, result.child(0).child(side),
                                type + ", side=" + side + ", expression=" + expression);
                    }
                }
            }
        }
    }

    @Test
    public void testOnConditionCanStillPushToNullExtendedSide() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "left_table", 0);
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "right_table", 0);
        Expression expression = expressions((SlotReference) right.getOutput().get(0)).get(0);
        LogicalJoin<Plan, Plan> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), ImmutableList.of(new IsNull(expression)), left, right, null);
        LogicalJoin<?, ?> result = (LogicalJoin<?, ?>) PlanChecker.from(context, join)
                .applyTopDown(new PushDownProject()).getPlan();
        // ON is evaluated before null extension, unlike the Project and Filter above the join.
        Assertions.assertInstanceOf(LogicalProject.class, result.right());
        Assertions.assertFalse(result.getOtherJoinConjuncts().get(0)
                .containsType(PreferPushDownProject.class));
    }

    private List<Expression> expressions(SlotReference slot) {
        Expression array = new If(new IsNull(slot),
                new Array(new StringLiteral("n")), new Array(new StringLiteral("p")));
        Expression map = new MapFromArrays(array, array);
        ArrayItemReference item = new ArrayItemReference("x", array);
        Lambda identity = new Lambda(ImmutableList.of("x"), item.toSlot(), ImmutableList.of(item));
        Lambda predicate = new Lambda(ImmutableList.of("x"),
                new EqualTo(item.toSlot(), new StringLiteral("n")), ImmutableList.of(item));
        return ImmutableList.of(
                new ElementAt(array, new IntegerLiteral(1)),
                new ArrayMap(identity),
                // This nested extraction must stay inside the lambda when ArrayMap cannot move.
                new ArrayMap(new Lambda(ImmutableList.of("x"),
                        new ElementAt(new Array(item.toSlot()), new IntegerLiteral(1)), ImmutableList.of(item))),
                new ArrayFilter(predicate),
                new ArrayCount(predicate),
                new MapKeys(map),
                new MapValues(map),
                new MapEntries(map),
                new MapContainsKey(map, new StringLiteral("n")),
                new MapContainsValue(map, new StringLiteral("n")),
                new MapContainsEntry(map, new StringLiteral("n"), new StringLiteral("n")),
                map,
                new MapFromEntries(new MapEntries(map)));
    }
}

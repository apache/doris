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

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class OuterJoinAsscomProjectTest {

    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    public void testJoinConjunctNullableWhenAssociate() {
        List<Expression> bottomHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0)));
        List<Expression> bottomOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan2.getOutput().get(1)));
        LogicalPlan bottomJoin = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, bottomHashJoinConjunct, bottomOtherJoinConjunct)
                .build();
        LogicalPlan bottomProject = new LogicalProject<>(
                bottomJoin.getOutput().stream().map(NamedExpression.class::cast).collect(Collectors.toList()),
                bottomJoin);

        List<Expression> topHashJoinConjunct = ImmutableList.of(
                new EqualTo(bottomProject.getOutput().get(2).withNullable(true), scan3.getOutput().get(0)));
        List<Expression> topOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(bottomProject.getOutput().get(3).withNullable(true), scan3.getOutput().get(1)));
        LogicalPlan topJoin = new LogicalPlanBuilder(bottomProject)
                .join(scan3, JoinType.LEFT_OUTER_JOIN, topHashJoinConjunct, topOtherJoinConjunct)
                .build();
        LogicalPlan plan = new LogicalProject<>(
                topJoin.getOutput().stream().map(NamedExpression.class::cast).collect(
                Collectors.toList()), topJoin);

        List<Plan> allPlan = PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .printlnOrigin()
                .applyExploration(OuterJoinAssocProject.INSTANCE.build())
                .getAllPlan();
        Assertions.assertEquals(2, allPlan.size());

        // check optimized join plan conjuncts null property, should be false
        Set<LogicalJoin<Plan, Plan>> joinSet = allPlan.get(1).collect(LogicalJoin.class::isInstance);
        for (LogicalJoin<Plan, Plan> newJoin : joinSet) {
            Plan child0 = newJoin.child(0);
            Plan child1 = newJoin.child(1);
            if ((child0 instanceof LogicalOlapScan && ((LogicalOlapScan) child0).getTable().getName().equals("t3"))
                    || (child1 instanceof LogicalOlapScan
                    && ((LogicalOlapScan) child1).getTable().getName().equals("t3"))) {
                for (Expression expr : newJoin.getHashJoinConjuncts()) {
                    expr.collectToSet(SlotReference.class::isInstance)
                            .forEach(slot -> Assertions.assertFalse(((SlotReference) slot).nullable()));
                }
                for (Expression expr : newJoin.getOtherJoinConjuncts()) {
                    expr.collectToSet(SlotReference.class::isInstance)
                            .forEach(slot -> Assertions.assertFalse(((SlotReference) slot).nullable()));
                }
            }
        }
    }
}

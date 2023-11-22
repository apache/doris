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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Test;

import java.util.Objects;

class EliminateOuterJoinTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1;
    private final LogicalOlapScan scan2;

    public EliminateOuterJoinTest() throws Exception {
        // clear id so that slot id keep consistent every running
        StatementScopeIdGenerator.clear();
        scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    }

    @Test
    void testEliminateLeft() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))  // t1.id = t2.id
                .filter(new GreaterThan(scan2.getOutput().get(0), Literal.of(1)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyTopDown(new EliminateOuterJoin())
                .applyTopDown(new EliminateNotNull())
                .printlnTree()
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin().when(join -> join.getJoinType().isInnerJoin())
                        ).when(filter -> filter.getConjuncts().size() == 1)
                );
    }

    @Test
    void testEliminateRight() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.RIGHT_OUTER_JOIN, Pair.of(0, 0))  // t1.id = t2.id
                .filter(new GreaterThan(scan1.getOutput().get(0), Literal.of(1)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyTopDown(new EliminateOuterJoin())
                .applyTopDown(new EliminateNotNull())
                .printlnTree()
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin().when(join -> join.getJoinType().isInnerJoin())
                        ).when(filter -> filter.getConjuncts().size() == 1)
                                .when(filter -> Objects.equals(filter.getConjuncts().toString(), "[(id#0 > 1)]"))
                );
    }

    @Test
    void testEliminateBoth() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.FULL_OUTER_JOIN, Pair.of(0, 0))  // t1.id = t2.id
                .filter(new And(
                        new GreaterThan(scan2.getOutput().get(0), Literal.of(1)),
                        new GreaterThan(scan1.getOutput().get(0), Literal.of(1))))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyTopDown(new EliminateOuterJoin())
                .applyTopDown(new EliminateNotNull())
                .printlnTree()
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin().when(join -> join.getJoinType().isInnerJoin())
                        ).when(filter -> filter.getConjuncts().size() == 2)
                );
    }
}

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

import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement.Assertion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class EliminateAssertNumRowsTest implements MemoPatternMatchSupported {
    @Test
    void testFailedMatch() {
        LogicalPlan plan = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .limit(10, 10)
                .assertNumRows(Assertion.LT, 10)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateAssertNumRows())
                .matchesFromRoot(
                        logicalAssertNumRows(logicalLimit(logicalOlapScan()))
                );
    }

    @Test
    void testLimit() {
        LogicalPlan plan = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .limit(10, 10)
                .assertNumRows(Assertion.LE, 10)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateAssertNumRows())
                .matchesFromRoot(
                        logicalLimit(logicalOlapScan())
                );
    }

    @Test
    void testScalarAgg() {
        LogicalPlan plan = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .agg(ImmutableList.of(), ImmutableList.of((new Count()).alias("cnt")))
                .assertNumRows(Assertion.LE, 10)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateAssertNumRows())
                .matchesFromRoot(
                        logicalAggregate(logicalOlapScan())
                );
    }

    @Test
    void skipProject() {
        LogicalPlan plan = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .limit(10, 10)
                .project(ImmutableList.of(0, 1))
                .project(ImmutableList.of(0, 1))
                .assertNumRows(Assertion.LE, 10)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateAssertNumRows())
                .matchesFromRoot(
                        logicalProject(logicalProject(logicalLimit(logicalOlapScan())))
                );
    }

    @Test
    void skipSemiJoin() {
        LogicalPlan plan = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .limit(10, 10)
                .joinEmptyOn(PlanConstructor.newLogicalOlapScan(1, "t2", 0), JoinType.LEFT_SEMI_JOIN)
                .assertNumRows(Assertion.LE, 10)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateAssertNumRows())
                .matchesFromRoot(
                        leftSemiLogicalJoin(logicalLimit(logicalOlapScan()), logicalOlapScan())
                );
    }
}

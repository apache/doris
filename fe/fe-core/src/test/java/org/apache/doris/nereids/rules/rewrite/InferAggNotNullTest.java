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
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class InferAggNotNullTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testInfer() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .aggGroupUsingIndex(ImmutableList.of(),
                        ImmutableList.of(new Alias(new Count(true, scan1.getOutput().get(1)), "dnt")))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferAggNotNull())
                .matches(
                        logicalAggregate(
                                logicalFilter().when(filter -> filter.getConjuncts().stream()
                                        .allMatch(e -> ((Not) e).isGeneratedIsNotNull()))
                        )
                );
    }

    @Test
    void testInferMultipleAggregateSameInput() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .aggGroupUsingIndex(ImmutableList.of(),
                        ImmutableList.of(
                                new Alias(new Avg(scan1.getOutput().get(1)), "avg_k"),
                                new Alias(new Sum(scan1.getOutput().get(1)), "sum_k")))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferAggNotNull())
                .matches(
                        logicalAggregate(
                                logicalFilter().when(filter -> filter.getConjuncts().size() == 1
                                        && filter.getConjuncts().stream()
                                        .allMatch(e -> ((Not) e).isGeneratedIsNotNull()))
                        )
                );
    }

    @Test
    void testNotInferMultipleAggregateDifferentInputs() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .aggGroupUsingIndex(ImmutableList.of(),
                        ImmutableList.of(
                                new Alias(new Avg(scan1.getOutput().get(1)), "avg_k1"),
                                new Alias(new Sum(scan1.getOutput().get(2)), "sum_k2")))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferAggNotNull())
                .matches(
                        logicalAggregate(
                                logicalOlapScan()
                        )
                );
    }

    @Test
    void testNotInferMultipleAggregateWithCountStar() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .aggGroupUsingIndex(ImmutableList.of(),
                        ImmutableList.of(
                                new Alias(new Avg(scan1.getOutput().get(1)), "avg_k"),
                                new Alias(new Count(), "count_star")))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferAggNotNull())
                .matches(
                        logicalAggregate(
                                logicalOlapScan()
                        )
                );
    }

    @Test
    void testCountStar() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .aggGroupUsingIndex(ImmutableList.of(), ImmutableList.of(new Alias(new Count(), "dnt")))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferAggNotNull())
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalOlapScan()
                        )
                );
    }
}

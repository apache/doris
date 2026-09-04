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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
                                new Alias(new Sum(scan1.getOutput().get(0)), "sum_k2")))
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

    @Test
    void testInferPartialWhenArgsExceedSlotLimit() {
        // count(distinct c0..c32) has 33 nullable arguments. inferNotNull merges input slots up to
        // a 32-slot limit, so the first 32 arguments get a generated IS NOT NULL and the 33rd is
        // skipped. This partial-inference path is only reachable after the all-children cheapness
        // gate was removed from InferAggNotNull.
        LogicalOlapScan wideScan = newWideNullableScan(33);
        List<Expression> args = new ArrayList<>(wideScan.getOutput());

        LogicalPlan plan = new LogicalPlanBuilder(wideScan)
                .aggGroupUsingIndex(ImmutableList.of(),
                        ImmutableList.of(new Alias(
                                new Count(true, args.get(0), args.subList(1, 33).toArray(new Expression[0])),
                                "cnt")))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferAggNotNull())
                .matches(
                        logicalAggregate(
                                logicalFilter().when(filter -> {
                                    Set<Expression> conjuncts = filter.getConjuncts();
                                    return conjuncts.size() == 32
                                            && conjuncts.stream().allMatch(e -> e instanceof Not
                                                    && ((Not) e).isGeneratedIsNotNull()
                                                    && ((Not) e).child() instanceof IsNull);
                                })
                        )
                );
    }

    @Test
    void testGetAggregateFunctionsStopsAtAggregateFunction() {
        // Use different agg function types for inner (Avg) and outer (Count),
        // so we can verify by instanceof regardless of how the plan builder
        // clones/transforms expressions internally.
        Avg inner = new Avg(scan1.getOutput().get(1));
        Count outer = new Count(false, inner);
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .aggGroupUsingIndex(ImmutableList.of(), ImmutableList.of(new Alias(outer, "cnt")))
                .build();

        Set<AggregateFunction> aggregateFunctions = ((LogicalAggregate<?>) plan).getAggregateFunctions();
        System.out.println("aggregateFunctions: " + aggregateFunctions);
        Assertions.assertEquals(1, aggregateFunctions.size());
        Assertions.assertTrue(aggregateFunctions.stream().allMatch(f -> f instanceof Count),
                "should collect only the outer Count, got: " + aggregateFunctions);
    }

    private LogicalOlapScan newWideNullableScan(int columnCount) {
        List<Column> columns = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            columns.add(new Column("c" + i, Type.INT, false, AggregateType.NONE, true, "", ""));
        }
        OlapTable table = new OlapTable(100L, "wide", columns,
                KeysType.DUP_KEYS, new PartitionInfo(), null);
        table.setIndexMeta(-1, "wide", table.getFullSchema(), 0, 0, (short) 0,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        return new LogicalOlapScan(RelationId.createGenerator().getNextId(), table, ImmutableList.of("db"));
    }
}

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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

class SimplifyAggGroupByTest implements MemoPatternMatchSupported {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void test() {
        Slot id = scan1.getOutput().get(0);
        List<NamedExpression> output = ImmutableList.of(
                id,
                new Add(id, Literal.of(1)).alias("id1"),
                new Add(id, Literal.of(2)).alias("id2"),
                new Add(id, Literal.of(3)).alias("id3"),
                new Count().alias("count")
        );
        List<Expression> groupBy = ImmutableList.of(
                id,
                new Add(id, Literal.of(1)),
                new Add(id, Literal.of(2)),
                new Add(id, Literal.of(3))
        );
        LogicalPlan agg = new LogicalPlanBuilder(scan1)
                .agg(groupBy, output)
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        PlanChecker.from(connectContext, agg)
                .applyTopDown(new SimplifyAggGroupBy())
                .matchesFromRoot(
                        logicalAggregate().when(a -> a.getGroupByExpressions().size() == 1)
                );
        PlanChecker.from(connectContext, agg)
                .analyze()
                .matchesFromRoot(
                        logicalProject(logicalAggregate().when(a -> a.getGroupByExpressions().size() == 1))
                );
    }

    @Test
    void testSqrt() {
        Slot id = scan1.getOutput().get(0);
        List<NamedExpression> output = ImmutableList.of(
                id,
                new Multiply(id, id).alias("sqrt"),
                new Count().alias("count")
        );
        List<Expression> groupBy = ImmutableList.of(
                id,
                new Multiply(id, id)
        );
        LogicalPlan agg = new LogicalPlanBuilder(scan1)
                .agg(groupBy, output)
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        PlanChecker.from(connectContext, agg)
                .applyTopDown(new SimplifyAggGroupBy())
                .matchesFromRoot(
                        logicalAggregate().when(a -> a.equals(agg))
                );
        PlanChecker.from(connectContext, agg)
                .analyze()
                .matchesFromRoot(
                        logicalProject(logicalAggregate().when(a -> a.getGroupByExpressions().size() == 2))
                );
    }

    @Test
    void testAbs() {
        Slot id = scan1.getOutput().get(0);
        List<NamedExpression> output = ImmutableList.of(
                id,
                new Abs(id).alias("abs"),
                new Count().alias("count")
        );
        List<Expression> groupBy = ImmutableList.of(
                id,
                new Abs(id)
        );
        LogicalPlan agg = new LogicalPlanBuilder(scan1)
                .agg(groupBy, output)
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        PlanChecker.from(connectContext, agg)
                .applyTopDown(new SimplifyAggGroupBy())
                .matchesFromRoot(
                        logicalAggregate().when(a -> a.equals(agg))
                );
        PlanChecker.from(connectContext, agg)
                .analyze()
                .matchesFromRoot(
                        logicalProject(logicalAggregate().when(a -> a.getGroupByExpressions().size() == 2))
                );
    }
}

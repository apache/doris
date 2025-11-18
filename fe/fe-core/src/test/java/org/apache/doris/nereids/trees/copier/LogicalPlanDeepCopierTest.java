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

package org.apache.doris.nereids.trees.copier;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class LogicalPlanDeepCopierTest {

    @Test
    public void testDeepCopyOlapScan() {
        LogicalOlapScan relationPlan = PlanConstructor.newLogicalOlapScan(0, "a", 0);
        relationPlan = (LogicalOlapScan) relationPlan.withOperativeSlots(relationPlan.getOutput());
        LogicalOlapScan aCopy =
                (LogicalOlapScan) relationPlan.accept(LogicalPlanDeepCopier.INSTANCE, new DeepCopierContext());
        for (Slot opSlot : aCopy.getOperativeSlots()) {
            Assertions.assertTrue(aCopy.getOutputSet().contains(opSlot));
        }
    }

    @Test
    public void testDeepCopyAggregateWithSourceRepeat() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t", 0);
        List<? extends NamedExpression> groupingKeys = scan.getOutput().subList(0, 1);
        List<List<Expression>> groupingSets = ImmutableList.of(
                ImmutableList.of(groupingKeys.get(0)),
                ImmutableList.of()
        );
        LogicalRepeat<Plan> repeat = new LogicalRepeat<>(
                groupingSets,
                scan.getOutput().stream().map(NamedExpression.class::cast).collect(Collectors.toList()),
                scan
        );
        List<? extends NamedExpression> groupByExprs = repeat.getOutput().subList(0, 1).stream()
                .map(e -> (NamedExpression) e)
                .collect(ImmutableList.toImmutableList());
        List<? extends NamedExpression> outputExprs = repeat.getOutput();
        LogicalAggregate aggregate = new LogicalAggregate(
                groupByExprs,
                outputExprs,
                repeat
        );
        aggregate = aggregate.withSourceRepeat(repeat);
        DeepCopierContext context = new DeepCopierContext();
        LogicalAggregate<? extends Plan> copiedAggregate = (LogicalAggregate<? extends Plan>) aggregate.accept(
                LogicalPlanDeepCopier.INSTANCE,
                context
        );
        Assertions.assertTrue(copiedAggregate.getSourceRepeat().isPresent());

        Optional<LogicalRepeat<? extends Plan>> copiedRepeat =
                copiedAggregate.collectFirst(LogicalRepeat.class::isInstance);
        Assertions.assertTrue(copiedRepeat.isPresent());
        Assertions.assertSame(copiedAggregate.getSourceRepeat().get(), copiedRepeat.get());

        Assertions.assertNotSame(aggregate, copiedAggregate);
        Assertions.assertNotSame(repeat, copiedRepeat.get());
    }

    @Test
    public void testDeepCopyAggregateWithoutSourceRepeat() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t", 0);
        List<Expression> groupByExprs = scan.getOutput().subList(0, 1).stream()
                .map(e -> (Expression) e)
                .collect(ImmutableList.toImmutableList());
        List<? extends NamedExpression> outputExprs = scan.getOutput();

        LogicalAggregate aggregate = new LogicalAggregate(
                groupByExprs,
                outputExprs,
                scan
        );
        DeepCopierContext context = new DeepCopierContext();
        LogicalAggregate<? extends Plan> copiedAggregate = (LogicalAggregate<? extends Plan>) aggregate.accept(
                LogicalPlanDeepCopier.INSTANCE,
                context
        );
        Assertions.assertFalse(copiedAggregate.getSourceRepeat().isPresent());
        Assertions.assertNotSame(aggregate, copiedAggregate);
        Assertions.assertEquals(aggregate.getGroupByExpressions().size(),
                copiedAggregate.getGroupByExpressions().size());
    }
}

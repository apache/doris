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

package org.apache.doris.nereids.cost;

import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.model.Statistics;
import org.apache.doris.statistics.model.StatisticsBuilder;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

class CostModelV1Test extends SqlTestBase {

    @Test
    void testAddCost() {
        CostWeight costWeight = connectContext.getStatementContext().getCostWeight();
        Cost planCost = Cost.of(costWeight, 1, 2, 3);
        Cost childCost = Cost.of(costWeight, 4, 5, 6);

        Cost totalCost = planCost.add(childCost);

        Assertions.assertEquals(planCost.getValue() + childCost.getValue(), totalCost.getValue());
        Assertions.assertEquals(5, totalCost.getCpuCost());
        Assertions.assertEquals(7, totalCost.getMemoryCost());
        Assertions.assertEquals(9, totalCost.getNetworkCost());
    }

    @Test
    void testShareCostWeightInStatementContext() {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        double originalCpuWeight = sessionVariable.getCboCpuWeight();
        StatementContext statementContext = new StatementContext(connectContext, null);
        try {
            sessionVariable.setCboCpuWeight(2);
            CostWeight firstWeight = statementContext.getCostWeight();

            Assertions.assertSame(firstWeight, statementContext.getCostWeight());
            Assertions.assertEquals(2, Cost.ofCpu(firstWeight, 1).getValue());

            sessionVariable.setCboCpuWeight(3);
            Assertions.assertSame(firstWeight, statementContext.getCostWeight());
            Assertions.assertEquals(2, Cost.ofCpu(firstWeight, 1).getValue());

            statementContext.setConnectContext(connectContext);
            CostWeight nextExecutionWeight = statementContext.getCostWeight();

            Assertions.assertNotSame(firstWeight, nextExecutionWeight);
            Assertions.assertEquals(3, Cost.ofCpu(nextExecutionWeight, 1).getValue());
        } finally {
            sessionVariable.setCboCpuWeight(originalCpuWeight);
        }
    }

    @Test
    void testMaterializingCost() {
        String sql = "select T1.id, T2.id, T2.score from T1 left join T2 "
                + "on T1.id = T2.id";
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        Plan p = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .deriveStats()
                .optimize()
                .getBestPlanTree();
        p.anyMatch(j -> j instanceof PhysicalHashJoin && ((PhysicalHashJoin<?, ?>) j).getJoinType().isRightJoin());
    }

    @Test
    void testPartitionedScalarAggregateCostUsesClusterScale() {
        int originBeNumberForTest = connectContext.getSessionVariable().getBeNumberForTest();
        connectContext.getSessionVariable().setBeNumberForTest(4);
        try {
            Plan child = PlanConstructor.newLogicalOlapScan(101, "partitioned_scalar_agg_t", 0);
            Slot partitionKey = child.getOutput().get(0);
            PhysicalHashAggregate<Plan> aggregate = new PhysicalHashAggregate<Plan>(
                    ImmutableList.of(), ImmutableList.of(partitionKey), Optional.of(ImmutableList.of(partitionKey)),
                    new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT), false, null, false, child);
            PhysicalHashAggregate<Plan> singlePointAggregate = new PhysicalHashAggregate<Plan>(
                    ImmutableList.of(), ImmutableList.of(partitionKey), Optional.empty(),
                    new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT), false, null, false, child);
            PhysicalHashAggregate<Plan> groupByAggregate = new PhysicalHashAggregate<Plan>(
                    ImmutableList.of(partitionKey), ImmutableList.of(partitionKey), Optional.empty(),
                    new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT), false, null, false, child);
            Statistics childStats = new StatisticsBuilder().setRowCount(1000).build();
            PlanContext context = Mockito.mock(PlanContext.class);
            Mockito.when(context.getChildStatistics(0)).thenReturn(childStats);
            Mockito.when(context.getSessionVariable()).thenReturn(connectContext.getSessionVariable());
            Mockito.when(context.getCostWeight()).thenReturn(connectContext.getStatementContext().getCostWeight());

            Cost cost = new CostModel(connectContext).visitPhysicalHashAggregate(aggregate, context);
            Cost singlePointCost = new CostModel(connectContext).visitPhysicalHashAggregate(singlePointAggregate,
                    context);
            Cost groupByCost = new CostModel(connectContext).visitPhysicalHashAggregate(groupByAggregate, context);

            Assertions.assertEquals(250, cost.getCpuCost(), 1e-9);
            Assertions.assertEquals(250, cost.getMemoryCost(), 1e-9);
            Assertions.assertEquals(1000, singlePointCost.getCpuCost(), 1e-9);
            Assertions.assertEquals(1000, singlePointCost.getMemoryCost(), 1e-9);
            Assertions.assertEquals(250, groupByCost.getCpuCost(), 1e-9);
            Assertions.assertEquals(250, groupByCost.getMemoryCost(), 1e-9);
        } finally {
            connectContext.getSessionVariable().setBeNumberForTest(originBeNumberForTest);
        }
    }
}

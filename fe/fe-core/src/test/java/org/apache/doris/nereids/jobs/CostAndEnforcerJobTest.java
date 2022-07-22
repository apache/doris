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

package org.apache.doris.nereids.jobs;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.jobs.cascades.OptimizeGroupJob;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class CostAndEnforcerJobTest {
    /*
     *   topJoin
     *   /     \
     *  C   bottomJoin
     *        /    \
     *       A      B
     */
    @Test
    public void testExecute(@Mocked LogicalProperties logicalProperties) {
        new MockUp<CostCalculator>() {
            @Mock
            public double calculateCost(GroupExpression groupExpression) {
                return 0;
            }
        };

        OlapTable aOlapTable = new OlapTable(0L, "a",
                ImmutableList.of(new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, "", "")),
                KeysType.PRIMARY_KEYS,
                null, null);
        OlapTable bOlapTable = new OlapTable(0L, "b",
                ImmutableList.of(new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, "", "")),
                KeysType.PRIMARY_KEYS,
                null, null);
        PhysicalOlapScan aScan = new PhysicalOlapScan(aOlapTable, Lists.newArrayList("a"), Optional.empty(),
                logicalProperties);
        PhysicalOlapScan bScan = new PhysicalOlapScan(bOlapTable, Lists.newArrayList("b"), Optional.empty(),
                logicalProperties);


        OlapTable cOlapTable = new OlapTable(0L, "c",
                ImmutableList.of(new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, "", "")),
                KeysType.PRIMARY_KEYS,
                null, null);
        PhysicalPlan cScan = new PhysicalOlapScan(cOlapTable, Lists.newArrayList("c"), Optional.empty(),
                logicalProperties);

        Expression bottomJoinOnCondition = new EqualTo(
                new SlotReference("id", new IntegerType(), true, ImmutableList.of("a")),
                new SlotReference("id", new IntegerType(), true, ImmutableList.of("b")));
        Expression topJoinOnCondition = new EqualTo(
                new SlotReference("id", new IntegerType(), true, ImmutableList.of("a")),
                new SlotReference("id", new IntegerType(), true, ImmutableList.of("c")));

        PhysicalHashJoin bottomJoin = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                Optional.of(bottomJoinOnCondition),
                logicalProperties, aScan, bScan);
        PhysicalHashJoin topJoin = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                Optional.of(topJoinOnCondition),
                logicalProperties, cScan, bottomJoin);


        PlannerContext plannerContext = new Memo(topJoin).newPlannerContext(new ConnectContext())
                .setDefaultJobContext();

        OptimizeGroupJob optimizeGroupJob = new OptimizeGroupJob(plannerContext.getMemo().getRoot(),
                plannerContext.getCurrentJobContext());
        plannerContext.pushJob(optimizeGroupJob);
        plannerContext.getJobScheduler().executeJobPool(plannerContext);
    }
}

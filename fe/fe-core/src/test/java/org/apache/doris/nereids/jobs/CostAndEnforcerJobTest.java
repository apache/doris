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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.cascades.OptimizeGroupJob;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.Test;

import java.util.Optional;

public class CostAndEnforcerJobTest {
    @Test
    public void testExecute(@Mocked OlapTable table, @Mocked LogicalProperties logicalProperties) {
        //        Table table = new Table(0L, "a", Table.TableType.OLAP,
        //                ImmutableList.of(new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
        //                        new Column("name", Type.STRING, true, AggregateType.NONE, "", "")));

        PhysicalPlan left = new PhysicalOlapScan(table, Lists.newArrayList("left"), Optional.empty(),
                logicalProperties);
        PhysicalPlan right = new PhysicalOlapScan(table, Lists.newArrayList("left"), Optional.empty(),
                logicalProperties);
        PhysicalPlan topLeft = new PhysicalOlapScan(table, Lists.newArrayList("left"), Optional.empty(),
                logicalProperties);

        PhysicalHashJoin bottomJoin = new PhysicalHashJoin<>(JoinType.INNER_JOIN, Optional.empty(), logicalProperties,
                left, right);
        PhysicalHashJoin topJoin = new PhysicalHashJoin<>(JoinType.INNER_JOIN, Optional.empty(), logicalProperties,
                topLeft, bottomJoin);


        Memo memo = new Memo();
        memo.initialize(topJoin);
        PlannerContext plannerContext = new PlannerContext(memo, new ConnectContext());
        JobContext jobContext = new JobContext(plannerContext, new PhysicalProperties(), Double.MAX_VALUE);
        plannerContext.setCurrentJobContext(jobContext);
        OptimizeGroupJob optimizeGroupJob = new OptimizeGroupJob(memo.getRoot(), plannerContext.getCurrentJobContext());
        plannerContext.pushJob(optimizeGroupJob);
        plannerContext.getJobScheduler().executeJobPool(plannerContext);
    }
}

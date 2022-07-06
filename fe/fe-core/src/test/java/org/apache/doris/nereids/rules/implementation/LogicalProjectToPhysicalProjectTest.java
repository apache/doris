// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the notICE file
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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.logical.LogicalProject;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.Plans;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class LogicalProjectToPhysicalProjectTest implements Plans {
    @Test
    public void projectionImplTest(@Mocked Group group) {
        LogicalProject logicalProject = new LogicalProject(Lists.newArrayList());
        Plan plan = plan(logicalProject, new GroupPlan(group));

        Rule<Plan> rule = new LogicalProjectToPhysicalProject().build();

        PlannerContext plannerContext = new PlannerContext(new Memo(), new ConnectContext());
        JobContext jobContext = new JobContext(plannerContext, new PhysicalProperties(), Double.MAX_VALUE);
        plannerContext.setCurrentJobContext(jobContext);
        List<Plan> transform = rule.transform(plan, plannerContext);
        Assert.assertEquals(1, transform.size());

        Plan implPlan = transform.get(0);
        Assert.assertEquals(OperatorType.PHYSICAL_PROJECT, implPlan.getOperator().getType());
    }
}

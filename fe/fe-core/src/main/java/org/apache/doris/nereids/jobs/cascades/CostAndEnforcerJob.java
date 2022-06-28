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

package org.apache.doris.nereids.jobs.cascades;

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.plans.Plan;

/**
 * Job to compute cost and add enforcer.
 */
public class CostAndEnforcerJob extends Job<Plan> {
    private final GroupExpression groupExpression;

    public CostAndEnforcerJob(GroupExpression groupExpression, PlannerContext context) {
        super(JobType.OPTIMIZE_CHILDREN, context);
        this.groupExpression = groupExpression;
    }

    @Override
    public void execute() {
        for (Group childGroup : groupExpression.children()) {
            if (!childGroup.isHasCost()) {
                // TODO: interim solution
                pushTask(new CostAndEnforcerJob(this.groupExpression, context));
                pushTask(new OptimizeGroupJob(childGroup, context));
                childGroup.setHasCost(true);
                return;
            }
        }
    }
}

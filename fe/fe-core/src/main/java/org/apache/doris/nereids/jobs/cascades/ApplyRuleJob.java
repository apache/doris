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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.List;

/**
 * Job to apply rule on {@link GroupExpression}.
 */
public class ApplyRuleJob extends Job<Plan> {
    private final GroupExpression groupExpression;
    private final Rule<Plan> rule;
    private final boolean exploredOnly;

    /**
     * Constructor of ApplyRuleJob.
     *
     * @param groupExpression apply rule on this {@link GroupExpression}
     * @param rule rule to be applied
     * @param context context of optimization
     */
    public ApplyRuleJob(GroupExpression groupExpression, Rule<Plan> rule, PlannerContext context) {
        super(JobType.APPLY_RULE, context);
        this.groupExpression = groupExpression;
        this.rule = rule;
        this.exploredOnly = false;
    }

    @Override
    public void execute() throws AnalysisException {
        if (groupExpression.hasApplied(rule)) {
            return;
        }

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(rule.getPattern(), groupExpression);
        for (Plan plan : groupExpressionMatching) {
            List<Plan> newPlans = rule.transform(plan, context);
            for (Plan newPlan : newPlans) {
                GroupExpression newGroupExpression = context.getOptimizerContext().getMemo()
                        .copyIn(newPlan, groupExpression.getParent(), rule.isRewrite());
                if (newPlan instanceof LogicalPlan) {
                    pushTask(new DeriveStatsJob(newGroupExpression, context));
                    if (exploredOnly) {
                        pushTask(new ExploreGroupExpressionJob(newGroupExpression, context));
                        continue;
                    }
                    pushTask(new OptimizeGroupExpressionJob(newGroupExpression, context));
                } else {
                    pushTask(new CostAndEnforcerJob(newGroupExpression, context));
                }
            }
        }
        groupExpression.setApplied(rule);
    }
}
